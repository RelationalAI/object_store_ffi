use backoff::backoff::Backoff;
use object_store::RetryConfig;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use tokio::sync::Mutex;
use zeroize::Zeroize;
use moka::future::Cache;
use crate::error::Error;
use crate::util::{deserialize_str, deserialize_slice};
// use anyhow::anyhow;


#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum SnowflakeResponse<T> {
    Success {
        data: T,
        success: bool,
    },
    Error {
        data: SnowflakeErrorData,
        code: String,
        message: String,
        success: bool,
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SnowflakeTokenData {
    session_token: String,
    #[serde(rename = "validityInSecondsST")]
    validity_in_seconds_st: u64,
    master_token: String,
    #[serde(rename = "validityInSecondsMT")]
    validity_in_seconds_mt: u64
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SnowflakeLoginData {
    token: String,
    validity_in_seconds: u64,
    master_token: String,
    master_validity_in_seconds: u64
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SnowflakeColType {
    name: String
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SnowflakeResultChunk {
    url: String,
    row_count: usize,
    uncompressed_size: usize,
    compressed_size: usize
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SnowflakeQueryData {
    rowtype: Vec<SnowflakeColType>,
    rowset: Vec<Vec<serde_json::Value>>,
    total: usize,
    returned: usize,
    #[serde(default)]
    chunk_headers: HashMap<String, String>,
    #[serde(default)]
    chunks: Vec<SnowflakeResultChunk>
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) struct SnowflakeStageCreds {
    pub aws_key_id: String,
    pub aws_secret_key: String,
    pub aws_token: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SnowflakeStageInfo {
    pub location_type: String,
    pub location: String,
    pub path: String,
    pub region: String,
    pub storage_account: Option<String>,
    pub is_client_side_encrypted: bool,
    pub ciphers: Option<String>,
    pub creds: SnowflakeStageCreds,
    pub use_s3_regional_url: bool,
    pub end_point: Option<String>
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[derive(Clone)]
pub(crate) struct SnowflakeEncryptionMaterial {
    pub query_stage_master_key: String,
    pub query_id: String,
    pub smk_id: u64,
}

impl Drop for SnowflakeEncryptionMaterial {
    fn drop(&mut self) {
        self.query_stage_master_key.zeroize();
    }
}


#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SnowflakeUploadData {
    pub query_id: String,
    pub encryption_material: Option<SnowflakeEncryptionMaterial>,
    pub stage_info: SnowflakeStageInfo,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SnowflakeDownloadData {
    pub query_id: String,
    #[serde(rename = "src_locations")]
    pub src_locations: Vec<String>,
    pub encryption_material: Vec<Option<SnowflakeEncryptionMaterial>>,
    pub stage_info: SnowflakeStageInfo,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SnowflakeErrorData {
    query_id: String
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum SnowflakeQueryResponse {
    Success {
        data: SnowflakeQueryData,
        success: bool,
    },
    Upload {
        data: SnowflakeUploadData,
        success: bool,
    },
    Download {
        data: SnowflakeDownloadData,
        success: bool,
    },
    Error {
        data: SnowflakeErrorData,
        code: String,
        message: String,
        success: bool,
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnowflakeQueryStatus {
    id: String,
    status: String,
    state: String,
    error_code: Option<String>,
    error_message: Option<String>
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnowflakeStatusData {
    queries: Vec<SnowflakeQueryStatus>
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnowflakeStatusResponse {
    data: SnowflakeStatusData
}

#[derive(Debug, Clone)]
pub(crate) struct SnowflakeClientConfig {
    pub account: String,
    pub database: String,
    pub host: String,
    pub schema: String,
    pub warehouse: Option<String>,
    pub retry_config: RetryConfig
}

impl SnowflakeClientConfig {
    #[allow(unused)]
    pub(crate) fn from_env() -> anyhow::Result<SnowflakeClientConfig> {
        use std::env;
        Ok(SnowflakeClientConfig {
            account: env::var("SNOWFLAKE_ACCOUNT")?,
            database: env::var("SNOWFLAKE_DATABASE")?,
            host: env::var("SNOWFLAKE_HOST")?,
            schema: env::var("SNOWFLAKE_SCHEMA")?,
            warehouse: env::var("SNOWFLAKE_WAREHOUSE").ok(),
            retry_config: RetryConfig::default()
        })
    }
}

struct TokenState {
    token: String,
    expiration: Instant,
    master_token: String,
    #[allow(unused)]
    master_expiration: Instant
}

#[derive(Clone)]
pub(crate) struct SnowflakeClient {
    config: SnowflakeClientConfig,
    client: reqwest::Client,
    token: Arc<Mutex<Option<TokenState>>>,
    stage_info_cache: Cache<String, Arc<SnowflakeUploadData>>
}

impl std::fmt::Debug for SnowflakeClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnowflakeClient")
         .field("config", &self.config)
         .finish()
    }
}

impl SnowflakeClient {
    pub(crate) fn new(config: SnowflakeClientConfig) -> Arc<SnowflakeClient> {
        let client = SnowflakeClient {
            config,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(180))
                .build().unwrap(),
            token: Arc::new(Mutex::new(None)),
            stage_info_cache: Cache::builder()
                .max_capacity(10)
                // Time to live here manages the stage token lifecycle, removing it from the cache
                // prior to expiration
                .time_to_live(std::time::Duration::from_secs(40 * 60))
                .build()
        };

        let client = Arc::new(client);

        {
            let client = Arc::downgrade(&client);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(5 * 60));
                interval.tick().await;
                loop {
                    interval.tick().await;
                    if let Some(client) = client.upgrade() {
                        match client.heartbeat().await {
                            Ok(_) => {},
                            Err(e) => {
                                tracing::warn!("Heartbeat failed: {:#}", e);
                            }
                        }
                    } else {
                        // Client was dropped, stop heartbeat
                        break;
                    }
                }
            });
        }

        client
    }
    #[allow(unused)]
    pub(crate) fn from_env() -> anyhow::Result<Arc<SnowflakeClient>> {
        Ok(SnowflakeClient::new(SnowflakeClientConfig::from_env()?))
    }
    async fn heartbeat(&self) -> anyhow::Result<bool> {
        let token = {
            let locked = self.token.lock().await;
            match locked.as_ref() {
                Some(TokenState { token, .. }) => token.clone(),
                _ => {
                    return Ok(false);
                }
            }
        };

        let response = self.client.post(format!("https://{}/session/heartbeat", self.config.host))
            .header("Authorization", format!("Snowflake Token=\"{}\"", token))
            .send()
            .await?;

        response.error_for_status_ref()?;

        return Ok(true);
    }
    pub(crate) async fn token(&self) -> crate::Result<String> {
        let mut locked = self.token.lock().await;
        match locked.as_ref() {
            Some(TokenState { token, expiration, .. }) if Instant::now() + Duration::from_secs(180) < *expiration => {
                return Ok(token.clone());
            }
            _ => {}
        }

        let config = &self.config;

        if let Some(TokenState { token, master_token, .. }) = locked.as_ref() {
            // Renew
            let response = self.client.post(format!("https://{}/session/token-request", config.host))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", format!("Snowflake Token=\"{}\"", master_token))
                .query(&[
                    ("requestId", uuid::Uuid::new_v4().to_string())
                ])
                .json(&serde_json::json!({
                    "oldSessionToken": token,
                    "requestType": "RENEW"
                }))
                .send()
                .await?;

            response.error_for_status_ref()
                .inspect_err(|_| *locked = None)?;

            let token_response_bytes = response
                .bytes()
                .await
                .inspect_err(|_| *locked = None)?;

            let token_response: SnowflakeResponse<SnowflakeTokenData> = deserialize_slice(&token_response_bytes)
                .map_err(Error::deserialize_response_err("token"))
                .inspect_err(|_| *locked = None)?;

            match token_response {
                SnowflakeResponse::Success { data, .. } => {
                    *locked = Some(TokenState {
                        token: data.session_token.clone(),
                        expiration: Instant::now() + Duration::from_secs(data.validity_in_seconds_st),
                        master_token: data.master_token,
                        master_expiration: Instant::now() + Duration::from_secs(data.validity_in_seconds_mt)
                    });

                    return Ok(data.session_token);

                }
                SnowflakeResponse::Error { data, code, message, .. } => {
                    *locked = None;
                    return Err(Error::error_response(format!("Error (code: {}, query_id: {}): {}", code, data.query_id, message)).into())
                }
            }
        } else {
            // Login
            let master_token = std::fs::read_to_string(std::env::var("MASTER_TOKEN_PATH").unwrap_or("/snowflake/session/token".into()))
                .map_err(Error::invalid_config_err("Unable to access master token file"))?;

            let mut qs = vec![
                ("accountName", &config.account),
                ("databaseName", &config.database),
                ("schemaName", &config.schema),
            ];

            if let Some(warehouse) = config.warehouse.as_ref() {
                qs.push(("warehouse", warehouse));
            }

            let response = self.client.post(format!("https://{}/session/v1/login-request", config.host))
                .header("Content-Type", "application/json")
                .header("Accept", "application/snowflake")
                .query(&qs)
                .json(&serde_json::json!({
                    "data": {
                        "ACCOUNT_NAME": &config.account,
                        "TOKEN": &master_token,
                        "AUTHENTICATOR": "OAUTH"
                    }
                }))
                .send()
                .await?;

            response.error_for_status_ref()?;

            let login_response_bytes = response
                .bytes()
                .await?;

            let login_response: SnowflakeResponse<SnowflakeLoginData> = deserialize_slice(&login_response_bytes)
                .map_err(Error::deserialize_response_err("login"))?;

            match login_response {
                SnowflakeResponse::Success { data, .. } => {
                    *locked = Some(TokenState {
                        token: data.token.clone(),
                        expiration: Instant::now() + Duration::from_secs(data.validity_in_seconds),
                        master_token: data.master_token,
                        master_expiration: Instant::now() + Duration::from_secs(data.master_validity_in_seconds)
                    });

                    return Ok(data.token);

                }
                SnowflakeResponse::Error { data, code, message, .. } => {
                    return Err(Error::error_response(format!("Error (code: {}, query_id: {}): {}", code, data.query_id, message)).into())
                }
            }
        }
    }
    async fn query_impl<T: DeserializeOwned>(&self, query: impl AsRef<str>) -> crate::Result<SnowflakeResponse<T>> {
        let token = self.token().await?;
        let config = &self.config;
        let response = self.client.post(format!("https://{}/queries/v1/query-request", config.host))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("Authorization", format!("Snowflake Token=\"{}\"", token))
            .query(&[
                ("clientStartTime", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs().to_string()),
                ("requestId", uuid::Uuid::new_v4().to_string()),
                ("request_guid", uuid::Uuid::new_v4().to_string())
            ])
            .json(&serde_json::json!({
                "sqlText": query.as_ref(),
                "asyncExec": false,
                "sequenceId": 1,
                "isInternal": false
            }))
            .send()
            .await?;

        response.error_for_status_ref()?;

        // TODO: directly to Deserialize
        let response_string = response
            .text()
            .await?;

        let response = deserialize_str(&response_string)
            .map_err(Error::deserialize_response_err("query"))?;

        Ok(response)
    }

    async fn query<T: DeserializeOwned>(&self, query: impl AsRef<str>) -> crate::Result<SnowflakeResponse<T>> {
        let mut backoff = backoff::ExponentialBackoff {
            initial_interval: self.config.retry_config.backoff.init_backoff,
            max_interval: self.config.retry_config.backoff.max_backoff,
            max_elapsed_time: Some(self.config.retry_config.retry_timeout),
            ..Default::default()
        };
        let max_retries = self.config.retry_config.max_retries;
        let mut retries = 0;
        loop {
            match self.query_impl(query.as_ref()).await {
                Ok(success @ SnowflakeResponse::Success { .. }) => {
                    return Ok(success)
                }
                Ok(SnowflakeResponse::Error { data, code, message, success }) => {
                    match (backoff.next_backoff(), retries < max_retries) {
                        (Some(duration), true) => {
                            tracing::warn!(
                                "retrying snowflake query error: Error (code: {}, query_id: {}): {}",
                                code, data.query_id, message
                            );
                            retries += 1;
                            tokio::time::sleep(duration).await;
                            continue;
                        }
                        _ => {
                            return Ok(SnowflakeResponse::Error { data, code, message, success });
                        }
                    }
                }
                Err(e) => {
                    match (backoff.next_backoff(), retries < max_retries &&  e.retryable()) {
                        (Some(duration), true) => {
                            tracing::warn!("retrying snowflake query due to: {}", e);
                            retries += 1;
                            tokio::time::sleep(duration).await;
                            continue;
                        }
                        _ => {
                            return Err(e)
                        }
                    }
                }
            }
        }
    }
    pub(crate) async fn fetch_upload_info(&self, stage: impl AsRef<str>) -> crate::Result<SnowflakeUploadData> {
        let response: SnowflakeResponse<SnowflakeUploadData> = self
            .query(format!("PUT file:///tmp/whatever @{}", stage.as_ref()))
            .await?;

        match response {
            SnowflakeResponse::Success { data, .. } => {
                Ok(data)
            }
            SnowflakeResponse::Error { message, code, data, .. } => {
                if code == "333334" {
                    tracing::error!("unexpected async execution response for query {} while fetching upload info", data.query_id);
                }
                return Err(Error::error_response(format!("Error (code: {}, query_id: {}): {}", code, data.query_id, message)).into())
            }
        }
    }
    pub(crate) async fn fetch_path_info(&self, stage: impl AsRef<str>, path: impl AsRef<str>) -> crate::Result<SnowflakeDownloadData> {
        let response: SnowflakeResponse<SnowflakeDownloadData> = self
            .query(format!("GET @{}/{} file:///tmp/whatever", stage.as_ref(), path.as_ref()))
            .await?;

        match response {
            SnowflakeResponse::Success { data, .. } => {
                Ok(data)
            }
            SnowflakeResponse::Error { message, code, data, .. } => {
                if code == "333334" {
                    tracing::error!("unexpected async execution response for query {} while fetching path info", data.query_id);
                }
                return Err(Error::error_response(format!("Error (code: {}, query_id: {}): {}", code, data.query_id, message)).into())
            }
        }
    }
    #[allow(unused)]
    pub(crate) async fn get_presigned_url(&self, stage: impl AsRef<str>, path: impl AsRef<str>) -> crate::Result<String> {
        let response: SnowflakeResponse<SnowflakeQueryData> = self
            .query(format!("SELECT get_presigned_url(@{}, '{}')", stage.as_ref(), path.as_ref()))
            .await?;

        match response {
            SnowflakeResponse::Success { data, .. } => {
                let url = data.rowset
                    .get(0)
                    .and_then(|r| r.get(0))
                    .and_then(|c| c.as_str())
                    .ok_or_else(|| Error::invalid_response("Missing url from get_presigned_url response"))?;
                Ok(url.to_string())
            }
            SnowflakeResponse::Error { message, code, data, .. } => {
                if code == "333334" {
                    tracing::error!("unexpected async execution response for query {} while fetching path info", data.query_id);
                }
                return Err(Error::error_response(format!("Error (code: {}, query_id: {}): {}", code, data.query_id, message)).into())
            }
        }
    }
    pub(crate) async fn current_upload_info(&self, stage: impl AsRef<str>) -> crate::Result<Arc<SnowflakeUploadData>> {
        let stage = stage.as_ref();
        let stage_info = self.stage_info_cache.try_get_with_by_ref(stage, async {
            let info = self.fetch_upload_info(stage).await?;

            // TODO schedule task to update token before deadline
            Ok::<_, Error>(Arc::new(info))
        }).await?;
        Ok(stage_info)
    }
}



