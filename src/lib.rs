use futures_util::StreamExt;
use object_store::RetryConfig;
use once_cell::sync::OnceCell;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;
use std::collections::HashMap;
use std::ffi::CString;
use std::ffi::{c_char, c_void};
use std::sync::Arc;
use std::time::Duration;
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
static CLIENTS: OnceCell<Cache<u64, Arc<dyn ObjectStore>>> = OnceCell::new();
// Contains configuration items that are set during initialization and do not change.
static STATIC_CONFIG: OnceCell<StaticConfig> = OnceCell::new();

fn runtime() -> &'static Runtime {
    RT.get().expect("start was not called")
}

fn clients() -> &'static Cache<u64, Arc<dyn ObjectStore>> {
    CLIENTS.get().expect("start was not called")
}

fn static_config() -> &'static StaticConfig {
    STATIC_CONFIG.get().expect("start was not called")
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
unsafe impl Sync for Notifier {}

// This is used to configure all aspects of the underlying
// object store client including credentials, request and client options.
// It has a single field `config_string` that must be a JSON serialized
// object with string keys and values.
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
        map.insert("allow_http".into(), "true".into());
        map.insert("aws_endpoint".into(), minio_host.as_str().trim_end_matches('/').to_string());
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

    Ok(client)
}

#[derive(Copy, Clone)]
#[repr(C)]
pub struct StaticConfig {
    n_threads: usize,
    cache_capacity: u64,
    cache_ttl_secs: u64,
    cache_tti_secs: u64,
    multipart_put_threshold: u64,
    multipart_get_threshold: u64,
    multipart_get_part_size: u64,
    concurrency_limit: u32,
}

impl Default for StaticConfig {
    fn default() -> Self {
        StaticConfig {
            n_threads: 0,
            cache_capacity: 20,
            cache_ttl_secs: 30 * 60,
            cache_tti_secs: 5 * 60,
            multipart_put_threshold: 8 * 1024 * 1024,
            multipart_get_threshold: 8 * 1024 * 1024,
            multipart_get_part_size: 8 * 1024 * 1024,
            concurrency_limit: 512
        }
    }
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

async fn multipart_get(slice: &'static mut [u8], path: &Path, client: &dyn ObjectStore) -> anyhow::Result<usize> {
    let part_size: usize = static_config().multipart_get_part_size as usize;
    let result = client.head(&path).await?;
    if result.size > slice.len() {
        return Err(anyhow!("Supplied buffer was too small"));
    }

    // If the object size happens to be smaller than part_size,
    // then we will end up doing a single range get of the whole
    // object.
    let mut parts = result.size / part_size;
    if result.size % part_size != 0 {
        parts += 1;
    }
    let mut part_ranges = Vec::with_capacity(parts);
    for i in 0..(parts-1) {
        part_ranges.push((i*part_size)..((i+1)*part_size));
    }
    // Last part which handles sizes not divisible by part_size
    part_ranges.push(((parts-1)*part_size)..result.size);

    let result_vec = client.get_ranges(&path, &part_ranges).await?;
    let mut accum: usize = 0;
    for i in 0..result_vec.len() {
        slice[accum..accum + result_vec[i].len()].copy_from_slice(&result_vec[i]);
        accum += result_vec[i].len();
    }

    return Ok(accum);
}

async fn multipart_put(slice: &'static [u8], path: &Path, client: &dyn ObjectStore) -> anyhow::Result<()> {
    let (multipart_id, mut writer) = client.put_multipart(&path).await?;
    match writer.write_all(slice).await {
        Ok(_) => {
            match writer.flush().await {
                Ok(_) => {
                    writer.shutdown().await?;
                    return Ok(());
                }
                Err(e) => {
                    client.abort_multipart(&path, &multipart_id).await?;
                    return Err(e.into());
                }
            }
        }
        Err(e) => {
            client.abort_multipart(&path, &multipart_id).await?;
            return Err(e.into());
        }
    };
}

#[no_mangle]
pub extern "C" fn start(
    config: StaticConfig,
    panic_cond_handle: *const c_void
) -> CResult {
    if let Err(_) = STATIC_CONFIG.set(config) {
        tracing::warn!("Tried to start() runtime multiple times!");
        return CResult::Error;
    }

    let panic_notifier = Notifier {
        handle: panic_cond_handle
    };

    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        prev(info);
        panic_notifier.notify();
    }));

    tracing_subscriber::fmt::init();

    let mut rt_builder = tokio::runtime::Builder::new_multi_thread();
    rt_builder.enable_all();

    let n_threads = static_config().n_threads;
    if n_threads > 0 {
        rt_builder.worker_threads(n_threads);
    }

    RT.set(
        rt_builder.build()
        .expect("failed to create tokio runtime")
    ).expect("runtime was set before");

    let mut cache_builder = moka::future::CacheBuilder::new(static_config().cache_capacity);
    if static_config().cache_ttl_secs != 0 {
        cache_builder = cache_builder.time_to_live(Duration::from_secs(static_config().cache_ttl_secs));
    }
    if static_config().cache_tti_secs != 0 {
        cache_builder = cache_builder.time_to_idle(Duration::from_secs(static_config().cache_tti_secs));
    }
    let cache = cache_builder.build();
        
    CLIENTS.set(cache)
        .expect("cache was set before");

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
                        let client = match clients().try_get_with(config.get_hash(), dyn_connect(config)).await {
                            Ok(client) => client,
                            Err(e) => {
                                response.from_error(e);
                                notifier.notify();
                                return;
                            }
                        };

                        // Multipart Get
                        if slice.len() > static_config().multipart_get_threshold as usize {
                            match multipart_get(slice, &p, &client).await {
                                Ok(accum) => {
                                    response.success(accum);
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

                        // Single part Get
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
                        let client = match clients().try_get_with(config.get_hash(), dyn_connect(config)).await {
                            Ok(client) => client,
                            Err(e) => {
                                response.from_error(e);
                                notifier.notify();
                                return;
                            }
                        };
                        let len = slice.len();

                        if len < static_config().multipart_put_threshold as usize {
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
                        } else {
                            match multipart_put(slice, &p, &client).await {
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
            }
        }).buffer_unordered(static_config().concurrency_limit as usize).for_each(|_| async {}).await;
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

// Only used to simulate a panic
#[no_mangle]
pub extern "C" fn _trigger_panic() -> CResult {
    runtime().spawn(async move {
        panic!("oops");
    });
    CResult::Error
}
