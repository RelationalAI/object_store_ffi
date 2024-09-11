use crate::{encryption::{CryptoMaterialProvider, CryptoScheme}, error::Error};

pub(crate) mod client;
use client::{SnowflakeClient, SnowflakeClientConfig};

pub(crate) mod kms;
use kms::{SnowflakeStageKms, SnowflakeStageKmsConfig};

use object_store::{ObjectStore, RetryConfig};
use tokio::sync::Mutex;
use std::sync::Arc;

use std::collections::HashMap;

#[derive(Debug)]
pub(crate) struct S3StageCredentialProvider {
    stage: String,
    client: Arc<SnowflakeClient>,
    cached: Mutex<Option<Arc<object_store::aws::AwsCredential>>>
}

impl S3StageCredentialProvider {
    pub(crate) fn new(stage: impl AsRef<str>, client: Arc<SnowflakeClient>) -> S3StageCredentialProvider {
        S3StageCredentialProvider { stage: stage.as_ref().to_string(), client, cached: Mutex::new(None) }
    }
}

#[async_trait::async_trait]
impl object_store::CredentialProvider for S3StageCredentialProvider {
    type Credential = object_store::aws::AwsCredential;
    async fn get_credential(&self) ->  object_store::Result<Arc<Self::Credential>> {
        let info = self.client.current_upload_info(&self.stage).await
            .map_err(|e| {
                object_store::Error::Generic {
                    store: "S3",
                    source: e.into()
                }
            })?;

        let mut locked = self.cached.lock().await;

        match locked.as_ref() {
            Some(creds) => if creds.key_id == info.stage_info.creds.aws_key_id {
                return Ok(Arc::clone(creds));
            }
            _ => {}
        }

        let creds = Arc::new(object_store::aws::AwsCredential {
            key_id: info.stage_info.creds.aws_key_id.clone(),
            secret_key: info.stage_info.creds.aws_secret_key.clone(),
            token: Some(info.stage_info.creds.aws_token.clone())
        });

        *locked = Some(Arc::clone(&creds));

        Ok(creds)
    }
}

#[derive(Clone)]
pub(crate) struct SnowflakeConfig {
    pub stage: String,
    pub client_config: SnowflakeClientConfig,
    pub kms_config: Option<SnowflakeStageKmsConfig>
}

pub(crate) fn validate_config_for_snowflake(mut map: HashMap<String, String>, retry_config: RetryConfig) -> anyhow::Result<SnowflakeConfig> {
    let mut required_or_env = |field: &str| {
        map
            .remove(field)
            .or(std::env::var(field.to_uppercase()).ok())
            .ok_or_else(|| {
                Error::required_config(field)
            })
    };

    let client_config = SnowflakeClientConfig {
        account: required_or_env("snowflake_account")?,
        database: required_or_env("snowflake_database")?,
        host: required_or_env("snowflake_host")?,
        schema: required_or_env("snowflake_schema")?,
        warehouse: map.remove("snowflake_warehouse").or(std::env::var("SNOWFLAKE_WAREHOUSE").ok()),
        retry_config
    };

    let kms_config = if let Some(scheme_str) = map.remove("snowflake_encryption_scheme") {
        Some(SnowflakeStageKmsConfig {
           crypto_scheme: match scheme_str.as_str() {
                "AES_256_GCM" => CryptoScheme::Aes256Gcm,
                "AES_128_CBC" => CryptoScheme::Aes128Cbc,
                _ => return Err(Error::invalid_config("Invalid value for snowflake_encryption_scheme").into()),
           },
           keyring_capacity: match map.remove("snowflake_keyring_capacity").map(|s| s.parse::<usize>()) {
               Some(Ok(cap)) => cap,
               Some(Err(e)) => return Err(Error::invalid_config_src("Failed to parse `snowflake_keyring_capacity`", e).into()),
               None => Default::default()
           },
           keyring_ttl: match map.remove("snowflake_keyring_ttl_secs").map(|s| s.parse::<u64>()) {
               Some(Ok(secs)) => std::time::Duration::from_secs(secs),
               Some(Err(e)) => return Err(Error::invalid_config_src("Failed to parse `snowflake_keyring_ttl_secs`", e).into()),
               None => Default::default()
           }
        })
    } else {
        None
    };

    let config = SnowflakeConfig {
        stage: map.remove("snowflake_stage")
            .ok_or_else(|| Error::required_config("snowflake_stage"))?,
        client_config,
        kms_config
    };

    for (key, _value) in map {
        return Err(Error::invalid_config(format!("Unknown config `{key}` found while validating snowflake config")).into());
    }

    Ok(config)
}

pub(crate) async fn build_store_for_snowflake_stage(config_map: HashMap<String, String>, retry_config: RetryConfig) -> anyhow::Result<(Arc<dyn ObjectStore>, Option<Arc<dyn CryptoMaterialProvider>>, String)> {
    let config = validate_config_for_snowflake(config_map, retry_config.clone())?;
    let client = SnowflakeClient::new(config.client_config);
    let info = client.current_upload_info(&config.stage).await?;

    match info.stage_info.location_type.as_ref() {
        "S3" => {
            let (bucket, stage_prefix) = info.stage_info.location.split_once('/')
                .ok_or_else(|| Error::invalid_response("Stage information from snowflake is missing the bucket name"))?;

            let provider = S3StageCredentialProvider::new(&config.stage, client.clone());
            let store = object_store::aws::AmazonS3Builder::default()
                .with_region(info.stage_info.region.clone())
                .with_bucket_name(bucket)
                .with_credentials(Arc::new(provider))
                .with_virtual_hosted_style_request(true)
                .with_unsigned_payload(true)
                .with_retry(retry_config)
                .build()?;

            if config.kms_config.is_some() && !info.stage_info.is_client_side_encrypted {
                return Err(Error::StorageNotEncrypted(config.stage.clone()).into());
            }

            let crypto_material_provider = if info.stage_info.is_client_side_encrypted {
                let kms_config = config.kms_config.unwrap_or_default();
                let stage_kms = SnowflakeStageKms::new(client, &config.stage, stage_prefix, kms_config);
                Some::<Arc<dyn CryptoMaterialProvider>>(Arc::new(stage_kms))
            } else {
                None
            };

            Ok((Arc::new(store), crypto_material_provider, stage_prefix.to_string()))
        }
        _ => {
            unimplemented!("unknown stage location type");
        }
    }
}
