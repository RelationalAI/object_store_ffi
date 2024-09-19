use crate::{snowflake::SnowflakeClient, encryption::{CryptoMaterialProvider, CryptoScheme, ContentCryptoMaterial, Key, EncryptedKey, Iv}};

use serde::{Serialize, Deserialize};
use object_store::{Attributes, Attribute, AttributeValue};
use anyhow::anyhow;
use moka::future::Cache;
use std::sync::Arc;

type ErrorMessage = String;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MaterialDescription {
    pub smk_id: String,
    pub query_id: String,
    pub key_size: String
}

#[derive(Clone, Debug)]
pub(crate) struct SnowflakeStageKmsConfig {
    pub crypto_scheme: CryptoScheme,
    pub keyring_capacity: usize,
    pub keyring_ttl: std::time::Duration
}

impl Default for SnowflakeStageKmsConfig {
    fn default() -> Self {
        SnowflakeStageKmsConfig {
            crypto_scheme: CryptoScheme::Aes128Cbc,
            keyring_capacity: 100_000,
            keyring_ttl: std::time::Duration::from_secs(40 * 60)
        }
    }
}

#[derive(Clone)]
pub(crate) struct SnowflakeStageKms {
    client: Arc<SnowflakeClient>,
    stage: String,
    prefix: String,
    config: SnowflakeStageKmsConfig,
    keyring: Cache<String, Key>
}

impl std::fmt::Debug for SnowflakeStageKms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnowflakeStageKms")
         .field("client", &self.client)
         .field("stage", &self.stage)
         .field("config", &self.config)
         .field("keyring", &"redacted")
         .finish()
    }
}

impl SnowflakeStageKms {
    pub(crate) fn new(
        client: Arc<SnowflakeClient>,
        stage: impl Into<String>,
        prefix: impl Into<String>,
        config: SnowflakeStageKmsConfig
    ) -> SnowflakeStageKms {
        SnowflakeStageKms {
            client,
            stage: stage.into(),
            prefix: prefix.into(),
            keyring: Cache::builder()
                .max_capacity(config.keyring_capacity as u64)
                .time_to_live(config.keyring_ttl)
                .build(),
            config
        }
    }
}

#[async_trait::async_trait]
impl CryptoMaterialProvider for SnowflakeStageKms {
    async fn material_for_write(&self, _path: &str, data_len: Option<usize>) -> anyhow::Result<(ContentCryptoMaterial, Attributes)> {
        let info = self.client.current_upload_info(&self.stage).await?;

        let encryption_material = info.encryption_material.as_ref()
            .ok_or_else(|| anyhow!("stage `{}` not encrypted", self.stage))?;
        let description = MaterialDescription {
            smk_id: encryption_material.smk_id.to_string(),
            query_id: encryption_material.query_id.clone(),
            key_size: "128".to_string()
        };
        let master_key = Key::from_base64(&encryption_material.query_stage_master_key)?;

        let scheme = self.config.crypto_scheme;
        let mut material = ContentCryptoMaterial::generate(scheme);
        let encrypted_cek = material.cek.clone().encrypt_aes_128_ecb(&master_key)?;

        let mut attributes = Attributes::new();
        attributes.insert(
            Attribute::Metadata("x-amz-key".into()),
            AttributeValue::from(encrypted_cek.as_base64())
        );
        attributes.insert(
            Attribute::Metadata("x-amz-iv".into()),
            AttributeValue::from(material.iv.as_base64())
        );
        if let Some(data_len) = data_len {
            attributes.insert(
                Attribute::Metadata("x-amz-unencrypted-content-length".into()),
                AttributeValue::from(format!("{}", data_len))
            );
        }
        attributes.insert(
            Attribute::Metadata("x-amz-matdesc".into()),
            AttributeValue::from(serde_json::to_string(&description)?)
        );

        let cek_alg = match scheme {
            CryptoScheme::Aes256Gcm => {
                let cek_alg = "AES/GCM/NoPadding";
                material = material.with_aad(cek_alg);
                cek_alg
            },
            CryptoScheme::Aes128Cbc => "AES/CBC/PKCS5Padding"
        };

        attributes.insert(
            Attribute::Metadata("x-amz-cek-alg".into()),
            AttributeValue::from(cek_alg)
        );

        Ok((material, attributes))
    }

    async fn material_from_metadata(&self, path: &str, attr: &Attributes) -> anyhow::Result<ContentCryptoMaterial> {
        let path = path.strip_prefix(&self.prefix).unwrap_or(path);
        let required_attribute = |key: &'static str| {
            let v: &str = attr.get(&Attribute::Metadata(key.into()))
                .ok_or_else(|| anyhow!("missing required attribute `{}`", key))?
                .as_ref();
            Ok::<_, anyhow::Error>(v)
        };


        let material_description: MaterialDescription = serde_json::from_str(required_attribute("x-amz-matdesc")?)?;
        let master_key = self.keyring.try_get_with(material_description.query_id, async {
            let info = self.client.fetch_path_info(&self.stage, path).await
                .map_err(|e| format!("failed to fetch path info: {}", e))?;
            let position = info.src_locations.iter().position(|l| l == path)
                .ok_or_else(|| "path not found")?;
            let encryption_material = info.encryption_material.get(position)
                .cloned()
                .ok_or_else(|| "src locations and encryption material length mismatch")?
                .ok_or_else(|| "path not encrypted")?;

            let master_key = Key::from_base64(&encryption_material.query_stage_master_key)
                .map_err(|e| format!("failed to decode qsmk base64: {}", e))?;
            Ok::<_, ErrorMessage>(master_key)
        }).await.map_err(|e| anyhow!(Arc::unwrap_or_clone(e)))?;

        let cek = EncryptedKey::from_base64(required_attribute("x-amz-key")?)?;
        let cek = cek.decrypt_aes_128_ecb(&master_key)?;
        let iv = Iv::from_base64(required_attribute("x-amz-iv")?)?;
        let alg = required_attribute("x-amz-cek-alg");

        let scheme = match alg {
            Ok("AES/GCM/NoPadding") => CryptoScheme::Aes256Gcm,
            Ok("AES/CBC/PKCS5Padding") | Err(_) => CryptoScheme::Aes128Cbc,
            Ok(v) => unimplemented!("cek alg `{}` not implemented", v)
        };

        let aad = match alg {
            Ok("AES/GCM/NoPadding") => Some("AES/GCM/NoPadding".into()),
            _ => None
        };

        let content_material = ContentCryptoMaterial {
            scheme,
            cek,
            iv,
            aad
        };

        Ok(content_material)
    }
}
