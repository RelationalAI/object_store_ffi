use crate::{clients, encryption::{CryptoMaterialProvider, CryptoScheme}, error::{Error, ErrorExt, Kind as ErrorKind}, CResult, Client, ClientExtension, Context, Extension, NotifyGuard, RawConfig, RawResponse, ResponseGuard};
use crate::{RT, with_cancellation};

pub(crate) mod client;
use anyhow::Context as AnyhowContext;
use client::{NormalizedStageInfo, SnowflakeClient, SnowflakeClientConfig};

pub(crate) mod kms;
use kms::{SnowflakeStageS3Kms, SnowflakeStageAzureKms, SnowflakeStageKmsConfig};

mod resolver;

use object_store::{azure::AzureCredential, RetryConfig, ObjectStore};
use tokio::sync::Mutex;
use std::sync::Arc;

use std::collections::HashMap;
use std::ffi::{CString, c_char, c_void};
use chrono::prelude::*;

#[derive(Debug)]
pub(crate) struct SnowflakeS3Extension {
    stage: String,
    client: Arc<SnowflakeClient>
}

#[async_trait::async_trait]
impl Extension for SnowflakeS3Extension {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    async fn current_stage_info(&self) -> crate::Result<String> {
        let stage_info = &self
            .client
            .current_upload_info(&self.stage)
            .await?
            .stage_info;
        let stage_info: NormalizedStageInfo = stage_info.try_into()?;
        let string = serde_json::to_string(&stage_info)
            .context("failed to encode stage_info as json").to_err()?;
        Ok(string)
    }
}

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

        if info.stage_info.location_type != "S3" {
            return Err(object_store::Error::Generic {
                store: "S3",
                source: Error::invalid_response("Location type must be S3 for this provider").into()
            })
        }

        let new_creds = info.stage_info.creds.as_aws()
            .map_err(|e| object_store::Error::Generic {
                store: "S3",
                source: e.into()
            })?;

        let mut locked = self.cached.lock().await;

        match locked.as_ref() {
            Some(creds) => if creds.key_id == new_creds.aws_key_id {
                return Ok(Arc::clone(creds));
            }
            _ => {}
        }

        // The session token is empty when testing against minio
        let token = match new_creds.aws_token.trim() {
            "" => None,
            token => Some(token.to_string())
        };

        let creds = Arc::new(object_store::aws::AwsCredential {
            key_id: new_creds.aws_key_id.clone(),
            secret_key: new_creds.aws_secret_key.clone(),
            token
        });

        *locked = Some(Arc::clone(&creds));

        Ok(creds)
    }
}


#[derive(Debug)]
pub(crate) struct SnowflakeAzureExtension {
    stage: String,
    client: Arc<SnowflakeClient>,
}

#[async_trait::async_trait]
impl Extension for SnowflakeAzureExtension {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    async fn current_stage_info(&self) -> crate::Result<String> {
        let stage_info = &self
            .client
            .current_upload_info(&self.stage)
            .await?
            .stage_info;
        let stage_info: NormalizedStageInfo = stage_info.try_into()?;
        let string = serde_json::to_string(&stage_info)
            .context("failed to encode stage_info as json").to_err()?;
        Ok(string)
    }
}

#[derive(Debug)]
pub(crate) struct AzureStageCredentialProvider {
    stage: String,
    client: Arc<SnowflakeClient>
}

impl AzureStageCredentialProvider {
    pub(crate) fn new(stage: impl AsRef<str>, client: Arc<SnowflakeClient>) -> AzureStageCredentialProvider {
        AzureStageCredentialProvider { stage: stage.as_ref().to_string(), client }
    }
}

#[async_trait::async_trait]
impl object_store::CredentialProvider for AzureStageCredentialProvider {
    type Credential = object_store::azure::AzureCredential;
    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let mut expiration_retries = 3;
        'retry: loop {
            let info = self.client.current_upload_info(&self.stage).await
                .map_err(|e| {
                    object_store::Error::Generic {
                        store: "MicrosoftAzure",
                        source: e.into()
                    }
                })?;

            if info.stage_info.location_type != "AZURE" {
                return Err(object_store::Error::Generic {
                    store: "MicrosoftAzure",
                    source: Error::invalid_response("Location type must be AZURE for this provider").into()
                })
            }

            let new_creds = info.stage_info.creds.as_azure()
                .map_err(|e| object_store::Error::Generic {
                    store: "MicrosoftAzure",
                    source: e.into()
                })?;


            let token_bytes = new_creds.azure_sas_token.trim_start_matches('?').as_bytes();
            let new_pairs: Vec<_> = url::form_urlencoded::parse(token_bytes)
                .into_owned()
                .collect();

            if let Some((_, exp_str)) = new_pairs.iter().find(|(a, _)| a == "se") {
                if let Ok(exp_time) = exp_str.parse::<DateTime<Utc>>() {
                    if exp_time < Utc::now() + std::time::Duration::from_secs(300) {
                        let _ = self.client.refresh_upload_info(&self.stage).await
                            .map_err(|e| object_store::Error::Generic {
                                store: "MicrosoftAzure",
                                source: e.into()
                            })?;
                        if expiration_retries > 0 {
                            expiration_retries -= 1;
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                            continue 'retry;
                        } else {
                            return Err(object_store::Error::Generic {
                                store: "MicrosoftAzure",
                                source: Error::invalid_response(
                                    format!("Got expired Azure token from Snowflake after 3 retries (exp: {exp_str})")
                                ).into()
                            })
                        }
                    }
                } else {
                    return Err(object_store::Error::Generic {
                        store: "MicrosoftAzure",
                        source: Error::invalid_response(
                            "Unable to parse expiration from Azure SAS token"
                            ).into()
                    })
                }
            } else {
                return Err(object_store::Error::Generic {
                    store: "MicrosoftAzure",
                    source: Error::invalid_response(
                        "Missing expiration from Azure SAS token"
                        ).into()
                })
            }

            let creds = Arc::new(AzureCredential::SASToken(new_pairs));

            return Ok(creds)
        }
    }
}

#[repr(C)]
pub struct StageInfoResponse {
    result: CResult,
    stage_info: *mut c_char,
    error_message: *mut c_char,
    context: *const Context
}

unsafe impl Send for StageInfoResponse {}

impl RawResponse for StageInfoResponse {
    type Payload = String;
    fn result_mut(&mut self) -> &mut CResult {
        &mut self.result
    }
    fn context_mut(&mut self) -> &mut *const Context {
        &mut self.context
    }
    fn error_message_mut(&mut self) -> &mut *mut c_char {
        &mut self.error_message
    }
    fn set_payload(&mut self, payload: Option<Self::Payload>) {
        match payload {
            Some(serialized_info) => {
                let c_string = CString::new(serialized_info).expect("should not have nulls");
                self.stage_info = c_string.into_raw();
            }
            None => {
                self.stage_info = std::ptr::null_mut();
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn current_stage_info(
    config: *const RawConfig,
    response: *mut StageInfoResponse,
    handle: *const c_void
) -> CResult {
    let response = unsafe { ResponseGuard::new(response, handle) };
    let config = unsafe { & (*config) };

    match RT.get() {
        Some(runtime) => {
            runtime.spawn(async move {
                let info_op = async {
                    let client = clients()
                        .try_get_with(config.get_hash(), Client::from_raw_config(config)).await?;
                    Ok::<_, crate::Error>(client.extension.current_stage_info().await?)
                };

                with_cancellation!(info_op, response);
            });
            CResult::Ok
        }
        None => {
            response.into_error("object_store_ffi runtime not started (may be missing initialization)");
            return CResult::Error;
        }
    }
}

#[derive(Clone)]
pub(crate) struct SnowflakeConfig {
    pub stage: String,
    pub client_config: SnowflakeClientConfig,
    pub kms_config: Option<SnowflakeStageKmsConfig>
}

pub(crate) fn validate_config_for_snowflake(map: &mut HashMap<String, String>, retry_config: RetryConfig) -> crate::Result<SnowflakeConfig> {
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
        endpoint: required_or_env("snowflake_endpoint")
            .or(required_or_env("snowflake_host").map(|h| format!("https://{h}")))?,
        schema: required_or_env("snowflake_schema")?,
        warehouse: map.remove("snowflake_warehouse").or(std::env::var("SNOWFLAKE_WAREHOUSE").ok()),
        username: map.remove("snowflake_username").or(std::env::var("SNOWFLAKE_USERNAME").ok()),
        password: map.remove("snowflake_password").or(std::env::var("SNOWFLAKE_PASSWORD").ok()),
        role: map.remove("snowflake_role").or(std::env::var("SNOWFLAKE_ROLE").ok()),
        master_token_path: map.remove("snowflake_master_token_path").or(std::env::var("MASTER_TOKEN_PATH").ok()),
        stage_info_cache_ttl: map.remove("snowflake_stage_info_cache_ttl_secs")
            .map(|s| s.parse::<u64>())
            .transpose()
            .map_err(|e| Error::invalid_config_src("Failed to parse `snowflake_stage_info_cache_ttl_secs`", e))?
            .map(|n| std::time::Duration::from_secs(n)),
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
               None => 100_000
           },
           keyring_ttl: match map.remove("snowflake_keyring_ttl_secs").map(|s| s.parse::<u64>()) {
               Some(Ok(secs)) => std::time::Duration::from_secs(secs),
               Some(Err(e)) => return Err(Error::invalid_config_src("Failed to parse `snowflake_keyring_ttl_secs`", e).into()),
               None => std::time::Duration::from_secs(10 * 60)
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
        if key.starts_with("snowflake") {
            return Err(Error::invalid_config(format!("Unknown config `{key}` found while validating snowflake config")).into());
        }
    }

    Ok(config)
}

pub(crate) async fn build_store_for_snowflake_stage(
    mut config_map: HashMap<String, String>,
    retry_config: RetryConfig
) -> crate::Result<(
    Arc<dyn ObjectStore>,
    Option<Arc<dyn CryptoMaterialProvider>>,
    Option<String>,
    ClientExtension
)> {
    let config = validate_config_for_snowflake(&mut config_map, retry_config.clone())?;
    let client = SnowflakeClient::new(config.client_config);
    let info = client.current_upload_info(&config.stage).await?;

    match info.stage_info.location_type.as_ref() {
        "S3" => {
            let (bucket, stage_prefix) = info.stage_info.location.split_once('/')
                .ok_or_else(|| Error::invalid_response("Stage information from snowflake is missing the bucket name"))?;

            let provider = S3StageCredentialProvider::new(&config.stage, client.clone());
            let store = if let Some(test_endpoint) = info.stage_info.test_endpoint.as_deref() {
                config_map.insert("allow_http".into(), "true".into());
                let mut builder = object_store::aws::AmazonS3Builder::default()
                    .with_region(info.stage_info.region.clone())
                    .with_bucket_name(bucket)
                    .with_credentials(Arc::new(provider))
                    .with_virtual_hosted_style_request(false)
                    .with_unsigned_payload(true)
                    .with_retry(retry_config)
                    .with_endpoint(test_endpoint);

                    for (key, value) in config_map {
                        builder = builder.with_config(key.parse()?, value);
                    }

                    builder.build()?
            } else {
                let mut builder = object_store::aws::AmazonS3Builder::default()
                    .with_region(info.stage_info.region.clone())
                    .with_bucket_name(bucket)
                    .with_credentials(Arc::new(provider))
                    .with_virtual_hosted_style_request(true)
                    .with_unsigned_payload(true)
                    .with_retry(retry_config);

                if let Some(end_point) = info.stage_info.end_point.as_deref() {
                    builder = builder.with_endpoint(format!("https://{bucket}.{end_point}"));
                }

                for (key, value) in config_map {
                    builder = builder.with_config(key.parse()?, value);
                }

                builder.build()?
            };

            if config.kms_config.is_some() && !info.stage_info.is_client_side_encrypted {
                return Err(ErrorKind::StorageNotEncrypted(config.stage.clone()).into());
            }

            let crypto_material_provider = if info.stage_info.is_client_side_encrypted {
                let kms_config = config.kms_config.unwrap_or_default();
                let stage_kms = SnowflakeStageS3Kms::new(client.clone(), &config.stage, stage_prefix, kms_config);
                Some::<Arc<dyn CryptoMaterialProvider>>(Arc::new(stage_kms))
            } else {
                None
            };

            let extension = Arc::new(SnowflakeS3Extension {
                stage: config.stage.clone(),
                client
            });

            Ok((Arc::new(store), crypto_material_provider, Some(stage_prefix.to_string()), extension))
        }
        "AZURE" => {
            let (container, stage_prefix) = info.stage_info.location.split_once('/')
                .ok_or_else(|| Error::invalid_response("Stage information from snowflake is missing the container name"))?;
            let storage_account = info.stage_info.storage_account
                .clone()
                .ok_or_else(|| Error::invalid_response("Stage information from snowflake is missing the storage account name"))?;

            let provider = AzureStageCredentialProvider::new(&config.stage, client.clone());

            let mut builder = object_store::azure::MicrosoftAzureBuilder::default()
                .with_account(storage_account)
                .with_container_name(container)
                .with_credentials(Arc::new(provider))
                .with_retry(retry_config);

            if let Some(test_endpoint) = &info.stage_info.test_endpoint {
                builder = builder.with_endpoint(test_endpoint.to_string());
                let mut azurite_host = url::Url::parse(&test_endpoint)
                    .map_err(Error::invalid_config_err("failed to parse azurite_host"))?;
                azurite_host.set_path("");
                unsafe { std::env::set_var("AZURITE_BLOB_STORAGE_URL", azurite_host.as_str()) };
                config_map.insert("allow_invalid_certificates".into(), "true".into());
                config_map.insert("azure_storage_use_emulator".into(), "true".into());
            }

            for (key, value) in config_map {
                builder = builder.with_config(key.parse()?, value);
            }

            let store = builder.build()?;

            if config.kms_config.is_some() && !info.stage_info.is_client_side_encrypted {
                return Err(ErrorKind::StorageNotEncrypted(config.stage.clone()).into());
            }

            let crypto_material_provider = if info.stage_info.is_client_side_encrypted {
                let kms_config = config.kms_config.unwrap_or_default();
                let stage_kms = SnowflakeStageAzureKms::new(
                    client.clone(),
                    &config.stage,
                    stage_prefix,
                    kms_config,
                );
                Some::<Arc<dyn CryptoMaterialProvider>>(Arc::new(stage_kms))
            } else {
                None
            };

            let extension = Arc::new(SnowflakeAzureExtension {
                stage: config.stage.clone(),
                client
            });

            let stage_prefix = if stage_prefix.is_empty() {
                None
            } else {
                Some(stage_prefix.to_string())
            };

            Ok((Arc::new(store), crypto_material_provider, stage_prefix, extension))
        }
        _ => {
            unimplemented!("unknown stage location type: {}", info.stage_info.location_type);
        }
    }
}
