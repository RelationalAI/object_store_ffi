use futures_util::StreamExt;
use object_store::RetryConfig;
use once_cell::sync::Lazy;
use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;
use std::collections::HashMap;
use std::ffi::CString;
use std::ffi::{c_char, c_void};
use std::sync::Arc;
use anyhow::anyhow;

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use object_store::{path::Path, ObjectStore};

use moka::future::Cache;

// Our global variables needed by our library at runtime. Note that we follow Rust's
// safety rules here by making them immutable with write-exactly-once semantics using
// either Lazy or OnceCell.
static RT: OnceCell<Runtime> = OnceCell::new();
// A channel (i.e., a queue) where the GET/PUT requests from Julia are placed and where
// our dispatch task pulls requests.
static SQ: OnceCell<async_channel::Sender<Request>> = OnceCell::new();
// The ObjectStore objects contain the context for communicating with a particular
// storage bucket/account, including authentication info. This caches them so we do
// not need to pay the construction cost for each request.
static CLIENTS: Lazy<Cache<u64, Arc<dyn ObjectStore>>> = Lazy::new(|| Cache::new(10));
// Contains configuration items that affect every request globally by default,
// currently includes retry configuration.
static CONFIG: OnceCell<GlobalConfigOptions> = OnceCell::new();

fn runtime() -> &'static Runtime {
    RT.get().expect("start was not called")
}

fn global_config() -> &'static GlobalConfigOptions {
    CONFIG.get().expect("start was not called")
}

// The result type used for the API functions exposed to Julia. This is used for both
// synchronous errors, e.g. our dispatch channel is full, and for async errors such
// as HTTP connection errors as part of the async Response.
#[repr(C)]
pub enum CResult {
    Uninitialized = -1,
    Ok = 0,
    Error = 1,
    Backoff = 2,
}

// The types used for our internal dispatch mechanism, for dispatching Julia requests
// to our worker task.
enum Request {
    Get(Path, &'static mut [u8], &'static Config, &'static mut Response, Notifier),
    Put(Path, &'static [u8], &'static Config, &'static mut Response, Notifier)
}

unsafe impl Send for Request {}


// libuv is how we notify Julia tasks that their async requests are done.
// Note that this will be linked in from the Julia process, we do not try
// to link it while building this Rust lib.
extern "C" {
    fn uv_async_send(cond: *const c_void) -> i32;
}

#[derive(Debug)]
#[repr(C)]
pub struct Notifier {
    handle: *const c_void,
}

impl Notifier {
    fn notify(&self) -> i32 {
        unsafe { uv_async_send(self.handle) }
    }
}

unsafe impl Send for Notifier {}

#[repr(C)]
pub struct Config {
    config_string: *const c_char
}

unsafe impl Send for Config {}
unsafe impl Sync for Config {}

impl Config {
    fn get_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        let config_string = self.as_str();
        hasher.write(config_string.as_bytes());
        hasher.finish()
    }

    fn as_str(&self) -> &str {
        let config_string = unsafe { std::ffi::CStr::from_ptr(self.config_string) };
        config_string.to_str().expect("julia strings are valid utf8")
    }

    fn as_json(&self) -> anyhow::Result<serde_json::Value> {
        Ok(serde_json::from_str(self.as_str())?)
    }
}

pub async fn dyn_connect(config: &Config) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let value = config.as_json()
            .map_err(|e| anyhow!("failed to parse json serialized config: {}", e))?;

    let mut map: HashMap<String, String> = serde_json::from_value(value)
        .map_err(|e| anyhow!("config must be a json serialized object: {}", e))?;

    let url = map.remove("url")
        .ok_or(anyhow!("config object must have a key named 'url'"))?;

    let url = url::Url::parse(&url)
        .map_err(|e| anyhow!("failed to parse `url`: {}", e))?;

    if let Some(v) = map.remove("azurite_host") {
        let mut azurite_host = url::Url::parse(&v)
            .map_err(|e| anyhow!("failed to parse azurite_host: {}", e))?;
        azurite_host.set_path("");
        std::env::set_var("AZURITE_BLOB_STORAGE_URL", azurite_host.as_str());
        map.insert("allow_invalid_certificates".into(), "true".into());
        map.insert("azure_storage_use_emulator".into(), "true".into());
    }

    if let Some(v) = map.remove("minio_host") {
        let mut minio_host = url::Url::parse(&v)
            .map_err(|e| anyhow!("failed to parse minio_host: {}", e))?;
        minio_host.set_path("");
        // std::env::set_var("AZURITE_BLOB_STORAGE_URL", azurite_host.as_str());
        map.insert("allow_http".into(), "true".into());
        map.insert("aws_endpoint".into(), minio_host.as_str().trim_end_matches('/').to_string());
        tracing::warn!("aws endpoint: {}", map["aws_endpoint"]);
    }

    let mut retry_config = RetryConfig::default();
    if let Some(value) = map.remove("max_retries") {
        retry_config.max_retries = value.parse()
            .map_err(|e| anyhow!("failed to parse max_retries: {}", e))?;
    }
    if let Some(value) = map.remove("retry_timeout_secs") {
        retry_config.retry_timeout = std::time::Duration::from_secs(value.parse()
            .map_err(|e| anyhow!("failed to parse retry_timeout_sec: {}", e))?
        );
    }

    let client: Arc<dyn ObjectStore> = match url.scheme() {
        "s3" => {
            let mut builder = object_store::aws::AmazonS3Builder::default()
                .with_url(url)
                .with_retry(retry_config);
            for (key, value) in map {
                builder = builder.with_config(key.parse()?, value);
            }
            Arc::new(builder.build()?)
        }
        "az" | "azure" => {
            let mut builder = object_store::azure::MicrosoftAzureBuilder::default()
                .with_url(url)
                .with_retry(retry_config);
            for (key, value) in map {
                builder = builder.with_config(key.parse()?, value);
            }
            Arc::new(builder.build()?)
        }
        _ => unimplemented!("unknown url scheme")
    };
    // let (store, _) = object_store::parse_url_opts(&url, pairs)?;

    // let client: Arc<dyn ObjectStore> = Arc::new(store);

    // let ping_path: Path = "_this_file_does_not_exist".try_into().unwrap();
    // match client.get(&ping_path).await {
    //     Ok(_) | Err(object_store::Error::NotFound { .. }) => {},
    //     Err(e) => {
    //         return Err(anyhow!("failed to check store client connection: {}", e));
    //     }
    // }

    Ok(client)
}

#[derive(Default, Copy, Clone)]
#[repr(C)]
pub struct GlobalConfigOptions {
    n_threads: usize
}

// The type used to give Julia the result of an async request. It will be allocated
// by Julia as part of the request and filled in by Rust.
#[repr(C)]
pub struct Response {
    result: CResult,
    length: usize,
    error_message: *mut c_char,
}

unsafe impl Send for Response {}

impl Response {
    fn success(&mut self, length: usize) {
        self.result = CResult::Ok;
        self.length = length;
        self.error_message = std::ptr::null_mut();
    }

    fn from_error(&mut self, error: impl std::fmt::Display) {
        self.result = CResult::Error;
        self.length = 0;
        let c_string = CString::new(format!("{}", error)).expect("should not have nulls");
        self.error_message = c_string.into_raw();
    }
}

#[no_mangle]
pub extern "C" fn start(config: GlobalConfigOptions) -> CResult {
    if let Err(_) = CONFIG.set(config) {
        tracing::warn!("Tried to start() runtime multiple times!");
        return CResult::Error;
    }
    tracing_subscriber::fmt::init();

    let mut rt_builder = tokio::runtime::Builder::new_multi_thread();
    rt_builder.enable_all();

    let n_threads = global_config().n_threads;
    if n_threads > 0 {
        rt_builder.worker_threads(64.min(n_threads));
    }

    RT.set(
        rt_builder.build()
        .expect("failed to create tokio runtime")
    ).expect("runtime was set before");

    // Creates our main dispatch task that takes Julia requests from the queue and does the
    // GET or PUT. Note the 'buffer_unordered' call at the end of the map block, which lets
    // requests in the queue be processed concurrently and in any order.
    runtime().spawn(async move {
        let (tx, rx) = async_channel::bounded(16 * 1024);
        SQ.set(tx).expect("runtime already started");

        rx.map(|req| {
            async {
                match req {
                    Request::Get(p, slice, config, response, notifier) => {
                        let client = match CLIENTS.try_get_with(config.get_hash(), dyn_connect(config)).await {
                            Ok(client) => client,
                            Err(e) => {
                                response.from_error(e);
                                notifier.notify();
                                return;
                            }
                        };
                        match client.get(&p).await {
                            Ok(result) => {
                                let chunks = result.into_stream().collect::<Vec<_>>().await;
                                if let Some(Err(e)) = chunks.iter().find(|result| result.is_err()) {
                                        tracing::warn!("Error while fetching a chunk: {}", e);
                                        response.from_error(e);
                                        notifier.notify();
                                        return;
                                }
                                tokio::spawn(async move {
                                    let mut received_bytes = 0;
                                    for result in chunks {
                                        let chunk = match result {
                                            Ok(c) => c,
                                            Err(_e) => {
                                                unreachable!("checked `chunks` for errors before calling `spawn`");
                                            }
                                        };
                                        let len = chunk.len();

                                        if received_bytes + len > slice.len() {
                                            response.from_error("Supplied buffer was too small");
                                            notifier.notify();
                                            return;
                                        }

                                        slice[received_bytes..(received_bytes + len)].copy_from_slice(&chunk);
                                        received_bytes += len;
                                    }
                                    response.success(received_bytes);
                                    notifier.notify();
                                });
                            }
                            Err(e) => {
                                tracing::warn!("{}", e);
                                response.from_error(e);
                                notifier.notify();
                                return;
                            }
                        }
                    }
                    Request::Put(p, slice, config, response, notifier) => {
                        let client = match CLIENTS.try_get_with(config.get_hash(), dyn_connect(config)).await {
                            Ok(client) => client,
                            Err(e) => {
                                response.from_error(e);
                                notifier.notify();
                                return;
                            }
                        };
                        let len = slice.len();
                        match client.put(&p, slice.into()).await {
                            Ok(_) => {
                                response.success(len);
                                notifier.notify();
                                return;
                            }
                            Err(e) => {
                                tracing::warn!("{}", e);
                                response.from_error(e);
                                notifier.notify();
                                return;
                            }
                        }
                    }
                }
            }
        }).buffer_unordered(512).for_each(|_| async {}).await;
    });
    CResult::Ok
}

#[no_mangle]
pub extern "C" fn get(
    path: *const c_char,
    buffer: *mut u8,
    size: usize,
    config: *const Config,
    response: *mut Response,
    handle: *const c_void
) -> CResult {
    let response = unsafe { &mut (*response) };
    response.result = CResult::Uninitialized;
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path: Path = path.to_str().expect("invalid utf8").try_into().unwrap();
    let slice = unsafe { std::slice::from_raw_parts_mut(buffer, size) };
    let config = unsafe { & (*config) };
    let notifier = Notifier { handle };
    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::Get(path, slice, config, response, notifier)) {
                Ok(_) => CResult::Ok,
                Err(async_channel::TrySendError::Full(_)) => {
                    CResult::Backoff
                }
                Err(async_channel::TrySendError::Closed(_)) => {
                    CResult::Error
                }
            }
        }
        None => {
            return CResult::Error;
        }
    }
}

#[no_mangle]
pub extern "C" fn put(
    path: *const c_char,
    buffer: *const u8,
    size: usize,
    config: *const Config,
    response: *mut Response,
    handle: *const c_void
) -> CResult {
    let response = unsafe { &mut (*response) };
    response.result = CResult::Uninitialized;
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path: Path = path.to_str().expect("invalid utf8").try_into().unwrap();
    let slice = unsafe { std::slice::from_raw_parts(buffer, size) };
    let config = unsafe { & (*config) };
    let notifier = Notifier { handle };
    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::Put(path, slice, config, response, notifier)) {
                Ok(_) => CResult::Ok,
                Err(async_channel::TrySendError::Full(_)) => {
                    CResult::Backoff
                }
                Err(async_channel::TrySendError::Closed(_)) => {
                    CResult::Error
                }
            }
        }
        None => {
            return CResult::Error;
        }
    }
}

#[no_mangle]
pub extern "C" fn destroy_cstring(string: *mut c_char) -> CResult {
    let string = unsafe { std::ffi::CString::from_raw(string) };
    drop(string);
    CResult::Ok
}
