use ::metrics::counter;
use object_store::RetryConfig;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use tokio::sync::Mutex;
use zeroize::Zeroize;
use moka::future::Cache;
use crate::{duration_on_drop, error::{Error, RetryState}, metrics};
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
pub(crate) struct SnowflakeStageAwsCreds {
    pub aws_key_id: String,
    pub aws_secret_key: String,
    pub aws_token: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) struct SnowflakeStageAzureCreds {
    pub azure_sas_token: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum SnowflakeStageCreds {
    Aws(SnowflakeStageAwsCreds),
    Azure(SnowflakeStageAzureCreds),
}

impl SnowflakeStageCreds {
    pub(crate) fn as_aws(&self) -> crate::Result<&SnowflakeStageAwsCreds> {
        match self {
            SnowflakeStageCreds::Aws(creds) => Ok(creds),
            SnowflakeStageCreds::Azure(_) => Err(Error::invalid_response("Expected AWS credentials, but got Azure ones")),
        }
    }

    pub(crate) fn as_azure(&self) -> crate::Result<&SnowflakeStageAzureCreds> {
        match self {
            SnowflakeStageCreds::Azure(creds) => Ok(creds),
            SnowflakeStageCreds::Aws(_) => Err(Error::invalid_response("Expected Azure credentials, but got AWS ones")),
        }
    }
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
    pub end_point: Option<String>,
    // This field is not part of the gateway API
    // it is only used for testing purposes
    pub test_endpoint: Option<String>
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "stage_type")]
#[non_exhaustive]
pub(crate) enum NormalizedStageInfo {
    S3 {
        bucket: String,
        prefix: String,
        region: String,
        aws_key_id: String,
        aws_secret_key: String,
        aws_token: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        end_point: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        test_endpoint: Option<String>,
    },
    BlobStorage {
        storage_account: String,
        container: String,
        prefix: String,
        azure_sas_token: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        end_point: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        test_endpoint: Option<String>
    }

}

impl TryFrom<&SnowflakeStageInfo> for NormalizedStageInfo {
    type Error = crate::error::Error;
    fn try_from(value: &SnowflakeStageInfo) -> Result<Self, Self::Error> {
        if value.location_type == "S3" {
            let (bucket, prefix) = value.location.split_once('/')
                .ok_or_else(|| Error::invalid_response("Stage information from snowflake is missing the bucket name"))?;
            let creds = value.creds.as_aws()?;
            return Ok(NormalizedStageInfo::S3 {
                bucket: bucket.to_string(),
                prefix: prefix.to_string(),
                region: value.region.clone(),
                aws_key_id: creds.aws_key_id.clone(),
                aws_secret_key: creds.aws_secret_key.clone(),
                aws_token: creds.aws_token.clone(),
                end_point: value.end_point.clone(),
                test_endpoint: value.test_endpoint.clone()
            })
        } else if value.location_type == "AZURE" {
            let (container, prefix) = value.location.split_once('/')
                .ok_or_else(|| Error::invalid_response("Stage information from snowflake is missing the container name"))?;
            let creds = value.creds.as_azure()?;
            let storage_account = value.storage_account
                .clone()
                .ok_or_else(|| Error::invalid_response("Stage information from snowflake is missing the storage account name"))?;
            return Ok(NormalizedStageInfo::BlobStorage {
                storage_account: storage_account,
                container: container.to_string(),
                prefix: prefix.to_string(),
                azure_sas_token: creds.azure_sas_token.clone(),
                end_point: value.end_point.clone(),
                test_endpoint: value.test_endpoint.clone()
            })
        } else {
            return Err(Error::not_implemented(format!("Location type {} is not implemented", value.location_type)));
        }
    }
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
    pub endpoint: String,
    pub schema: String,
    pub warehouse: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub role: Option<String>,
    pub master_token_path: Option<String>,
    pub stage_info_cache_ttl: Option<Duration>,
    pub retry_config: RetryConfig
}

impl SnowflakeClientConfig {
    #[allow(unused)]
    pub(crate) fn from_env() -> anyhow::Result<SnowflakeClientConfig> {
        use std::env;
        Ok(SnowflakeClientConfig {
            account: env::var("SNOWFLAKE_ACCOUNT")?,
            database: env::var("SNOWFLAKE_DATABASE")?,
            endpoint: env::var("SNOWFLAKE_ENDPOINT")
                .or(env::var("SNOWFLAKE_HOST").map(|h| format!("https://{h}")))?,
            schema: env::var("SNOWFLAKE_SCHEMA")?,
            warehouse: env::var("SNOWFLAKE_WAREHOUSE").ok(),
            username: env::var("SNOWFLAKE_USERNAME").ok(),
            password: env::var("SNOWFLAKE_PASSWORD").ok(),
            role: env::var("SNOWFLAKE_ROLE").ok(),
            master_token_path: env::var("MASTER_TOKEN_PATH").ok(),
            stage_info_cache_ttl: None,
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
        let cache_ttl = config.stage_info_cache_ttl.unwrap_or(Duration::from_secs(40 * 60));
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
                .time_to_live(cache_ttl)
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
    async fn heartbeat(&self) -> crate::Result<bool> {
        let token = {
            let locked = self.token.lock().await;
            match locked.as_ref() {
                Some(TokenState { token, .. }) => token.clone(),
                _ => {
                    return Ok(false);
                }
            }
        };

        let _guard = duration_on_drop!(metrics::sf_heartbeat_duration);
        let response = self.client.post(format!("{}/session/heartbeat", self.config.endpoint))
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
            let _guard = duration_on_drop!(metrics::sf_token_refresh_duration);
            // Renew
            let response = self.client.post(format!("{}/session/token-request", config.endpoint))
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
            let _guard = duration_on_drop!(metrics::sf_token_login_duration);
            let response = if let (Some(username), Some(password)) = (&config.username, &config.password) {
                // User Password Login
                let mut qs = vec![
                    ("accountName", &config.account),
                    ("databaseName", &config.database),
                    ("schemaName", &config.schema),
                ];

                if let Some(warehouse) = config.warehouse.as_ref() {
                    qs.push(("warehouse", warehouse));
                }

                if let Some(role) = config.role.as_ref() {
                    qs.push(("roleName", role));
                }

                let response = self.client.post(format!("{}/session/v1/login-request", config.endpoint))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/snowflake")
                    .query(&qs)
                    .json(&serde_json::json!({
                        "data": {
                            "PASSWORD": password,
                            "LOGIN_NAME": username,
                            "ACCOUNT_NAME": &config.account,
                            "AUTHENTICATOR": "USERNAME_PASSWORD_MFA"
                        }
                    }))
                    .send()
                    .await?;

                response
            } else {
                // Master Token Login
                let master_token = std::fs::read_to_string(config.master_token_path.as_deref().unwrap_or("/snowflake/session/token"))
                    .map_err(Error::invalid_config_err("Unable to access master token file"))?;


                let mut qs = vec![
                    ("accountName", &config.account),
                    ("databaseName", &config.database),
                    ("schemaName", &config.schema),
                ];

                if let Some(warehouse) = config.warehouse.as_ref() {
                    qs.push(("warehouse", warehouse));
                }

                let response = self.client.post(format!("{}/session/v1/login-request", config.endpoint))
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

                response
            };

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
    async fn query_impl<T: DeserializeOwned>(&self, query: impl AsRef<str>) -> crate::Result<T> {
        let token = self.token().await?;
        let _guard = duration_on_drop!(metrics::sf_query_attempt_duration);
        let config = &self.config;
        let response = self.client.post(format!("{}/queries/v1/query-request", config.endpoint))
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

        let response_string = response
            .text()
            .await?;

        let response: SnowflakeResponse<T> = deserialize_str(&response_string)
            .map_err(Error::deserialize_response_err("query"))?;

        match response {
            SnowflakeResponse::Success { data, .. } => {
                Ok(data)
            }
            SnowflakeResponse::Error { data, code, message, .. } => {
                Err(Error::error_response(format!("Error (code: {}, query_id: {}): {}", code, data.query_id, message)))
            }
        }
    }
    async fn query<T: DeserializeOwned>(&self, query: impl AsRef<str>) -> crate::Result<T> {
        let mut retry_state = RetryState::new(self.config.retry_config.clone());
        'retry: loop {
            match self.query_impl(query.as_ref()).await {
                Err(e) => {
                    match retry_state.should_retry(e) {
                        Ok((e, info, duration)) => {
                            counter!(metrics::total_sf_retries).increment(info.retries.unwrap_or(1) as u64);
                            tracing::info!("retrying snowflake query error (reason: {:?}) after {:?}: {}", info.reason, duration, e);
                            tokio::time::sleep(duration).await;
                            continue 'retry;
                        }
                        Err(e) => {
                            break Err(e);
                        }
                    }
                }
                ok => {
                    break ok;
                }
            }
        }
    }
    pub(crate) async fn fetch_upload_info(&self, stage: impl AsRef<str>) -> crate::Result<SnowflakeUploadData> {
        let _guard = duration_on_drop!(metrics::sf_fetch_upload_info_retried_duration);
        let upload_data: SnowflakeUploadData = self
            .query(format!("PUT file:///tmp/whatever @{}", stage.as_ref()))
            .await?;

        counter!(metrics::total_fetch_upload_info).increment(1);
        Ok(upload_data)
    }
    pub(crate) async fn fetch_path_info(&self, stage: impl AsRef<str>, path: impl AsRef<str>) -> crate::Result<SnowflakeDownloadData> {
        let _guard = duration_on_drop!(metrics::sf_fetch_path_info_retried_duration);
        let download_data: SnowflakeDownloadData = self
            .query(format!("GET @{}/{} file:///tmp/whatever", stage.as_ref(), path.as_ref()))
            .await?;

        counter!(metrics::total_fetch_path_info).increment(1);
        Ok(download_data)
    }
    #[allow(unused)]
    pub(crate) async fn get_presigned_url(&self, stage: impl AsRef<str>, path: impl AsRef<str>) -> crate::Result<String> {
        let _guard = duration_on_drop!(metrics::sf_get_presigned_url_retried_duration);
        let query_data: SnowflakeQueryData = self
            .query(format!("SELECT get_presigned_url(@{}, '{}')", stage.as_ref(), path.as_ref()))
            .await?;
        let url = query_data.rowset
            .get(0)
            .and_then(|r| r.get(0))
            .and_then(|c| c.as_str())
            .ok_or_else(|| Error::invalid_response("Missing url from get_presigned_url response"))?;
        Ok(url.to_string())
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use crate::{metrics::init_metrics, Client};
    use super::*;
    // use futures_util::StreamExt;
    use ::metrics::Unit;
    use metrics_util::debugging::Snapshot;
    use object_store::path::Path;

    fn format_unit(unit: Option<Unit>, v: f64) -> String {
        match unit {
            Some(Unit::Seconds) => format!("{:?}", Duration::from_secs_f64(v)),
            _ => format!("{}", v)
        }
    }
    fn print_snapshot(snapshot: Snapshot) {
        println!("=======================");
        for (key, unit, _shared, value) in snapshot.into_vec() {
            let (_kind, key) = key.into_parts();
            let f = |v| format_unit(unit, v);
            let value_str = match value {
                metrics_util::debugging::DebugValue::Histogram(mut vals) => {
                    vals.sort();
                    let p = |v: f64| {
                        if vals.len() == 0 {
                            0.0f64
                        } else {
                            *vals[((vals.len() as f64 * v).floor() as usize).min(vals.len() - 1)]
                        }
                    };
                    format!("min: {}, p50: {}, p99: {}, p999: {}, max: {}", f(p(0.0)), f(p(0.5)), f(p(0.99)), f(p(0.999)), f(p(1.0)))
                }
                metrics_util::debugging::DebugValue::Counter(v) => {
                    format!("total: {}", v)
                }
                metrics_util::debugging::DebugValue::Gauge(v) => {
                    format!("current: {}", f(*v))
                }
            };
            println!("{} {}", key.name(), value_str);
        }
        println!("=======================");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn snowflake_gateway_stress_test() -> anyhow::Result<()> {
        if std::env::var("SNOWFLAKE_HOST").is_ok() {
            let recorder = metrics_util::debugging::DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            recorder.install()?;

            init_metrics();

            let cancel_token = tokio_util::sync::CancellationToken::new();
            {
                let cancel_token = cancel_token.clone();
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(Duration::from_secs(1));
                    interval.tick().await;
                    loop {
                        tokio::select! {
                            _ = cancel_token.cancelled() => {
                                print_snapshot(snapshotter.snapshot());
                                break;
                            }
                            _ = interval.tick() => {
                                print_snapshot(snapshotter.snapshot());
                            }
                        }
                    }

                });
            }
            let _guard = cancel_token.drop_guard();

            let config_map = serde_json::from_value(serde_json::json!({
                "url": "snowflake://ENCRYPTED_STAGE_2",
                "snowflake_stage": "ENCRYPTED_STAGE_2",
                "max_retries": "3",
                // "snowflake_stage_info_cache_ttl_secs": "0",
                "snowflake_keyring_ttl_secs": "10000",
                "snowflake_encryption_scheme": "AES_128_CBC"
            }))?;

            let client = Client::from_config_map(config_map).await?;

            let n_files = 20_000;
            let concurrency = 512;

//             let t0 = std::time::Instant::now();
//             futures_util::stream::iter(0..n_files)
//                 .map(|i| {
//                     let client = client.clone();
//                     return tokio::spawn(async move {
//                         let _ = client.put(&Path::from(format!("_blobs/{:08}.bin", i)), bytes::Bytes::new()).await.unwrap();
//                     })
//                 })
//                 .buffer_unordered(64)
//                 .for_each(|_| async {}).await;
//             println!("{:?}", t0.elapsed());
//
//             let t0 = std::time::Instant::now();
//             let list = client.list(&Path::from("_blobs"), None).await.unwrap();
//             println!("{:?}", list.len());
//             println!("{:?}", t0.elapsed());

            let error_count = Arc::new(AtomicUsize::new(0));
            let (tx, rx) = flume::unbounded::<usize>();

            let mut tasks = vec![];
            for _taskn in 0..concurrency {
                let client = client.clone();
                let error_count = error_count.clone();
                let rx = rx.clone();
                tasks.push(tokio::spawn(async move {
                    loop {
                        let i = match rx.recv_async().await {
                            Ok(r) => r,
                            _ => {
                                break;
                            }
                        };

                        let mut buf = [0; 10];
                        let res = client.get(&Path::from(format!("_blobs/{:08}.bin", i)), &mut buf).await;
                        if let Err(e) = res {
                            if !format!("{e}").contains("Generic S3 error") {
                                println!("failed with: {e}");
                            }
                            error_count.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                        }
                    }
                }));
            }

            let t0 = std::time::Instant::now();
            for _round in 0..10 {
                for i in 0..n_files {
                    tx.send_async(i).await?;
                }
            }
            drop(tx);
            drop(rx);

            for task in tasks {
                task.await?;
            }
            println!("{:?}", t0.elapsed());
            println!("error_count {:?}", error_count);
        } else {
            println!("Ignoring test as it is not running on an SPCS service");
            assert!(true);
        }
        Ok(())
    }
}
