use futures_util::StreamExt;
use object_store::RetryConfig;
use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};
use std::collections::HashMap;
use std::ffi::{c_char, c_void, CString};
use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::anyhow;

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use object_store::{path::Path, ObjectStore};

use moka::future::Cache;

mod util;
use util::Compression;

mod error;
use error::{extract_error_info, should_retry};

mod list;
use list::{handle_list, handle_list_stream, ListResponse, ListStreamResponse};

mod crud_ops;
use crud_ops::{handle_get, handle_put, handle_delete, Response};

mod stream;
use stream::{handle_get_stream, handle_put_stream, GetStreamResponse, PutStreamResponse};

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
static CLIENTS: OnceCell<Cache<u64, (Arc<dyn ObjectStore>, ConfigMeta)>> = OnceCell::new();

// Contains configuration items that are set during initialization and do not change.
static STATIC_CONFIG: OnceCell<StaticConfig> = OnceCell::new();

fn runtime() -> &'static Runtime {
    RT.get().expect("start was not called")
}

fn clients() -> &'static Cache<u64, (Arc<dyn ObjectStore>, ConfigMeta)> {
    CLIENTS.get().expect("start was not called")
}

fn static_config() -> &'static StaticConfig {
    STATIC_CONFIG.get().expect("start was not called")
}

// The result type used for the API functions exposed to Julia. This is used for both
// synchronous errors, e.g. our dispatch channel is full, and for async errors such
// as HTTP connection errors as part of the async Response.
#[derive(Debug, PartialEq, Eq, Copy, Clone, Default)]
#[repr(C)]
pub enum CResult {
    #[default]
    Uninitialized = -1,
    Ok = 0,
    Error = 1,
    Backoff = 2,
}

#[derive(Debug, Clone, Default)]
pub struct Context {
    cancellation_token: CancellationToken
}

#[no_mangle]
pub extern "C" fn cancel_context(ctx_ptr: *const Context) -> CResult {
    let ctx = unsafe { &*ctx_ptr };
    ctx.cancellation_token.cancel();
    CResult::Ok
}

pub trait RawResponse {
    type Payload;
    fn result_mut(&mut self) -> &mut CResult;
    fn context_mut(&mut self) -> &mut *const Context;
    fn error_message_mut(&mut self) -> &mut *mut c_char;
    fn set_payload(&mut self, payload: Option<Self::Payload>);
}

struct ResponseGuard<T: RawResponse + 'static> {
    response: &'static mut T,
    context: Arc<Context>,
    handle: *const c_void
}

impl<T: RawResponse> ResponseGuard<T> {
    unsafe fn new(response_ptr: *mut T, handle: *const c_void) -> Self {
        let response = unsafe { &mut (*response_ptr) };
        *response.result_mut() = CResult::Uninitialized;
        let context = Arc::new(Context::default());
        *response.context_mut() = Arc::into_raw(context.clone());
        ResponseGuard { response, context, handle }
    }
    pub(crate) fn success(self, payload: T::Payload) {
        *self.response.result_mut() = CResult::Ok;
        self.response.set_payload(Some(payload));
        *self.response.error_message_mut() = std::ptr::null_mut();
    }
}

impl<T: RawResponse> NotifyGuard for ResponseGuard<T> {
    fn is_uninitialized(&mut self) -> bool {
        *self.response.result_mut() == CResult::Uninitialized
    }
    fn condition_handle(&self) -> *const c_void {
        self.handle
    }
    fn context(&self) -> &Context {
        &self.context
    }
    fn set_error(&mut self, error: impl std::fmt::Display) {
        *self.response.result_mut() = CResult::Error;
        self.response.set_payload(None);
        let c_string = CString::new(format!("{}", error)).expect("should not have nulls");
        *self.response.error_message_mut() = c_string.into_raw();
    }
}

impl<T: RawResponse> Drop for ResponseGuard<T> {
    fn drop(&mut self) {
        self.notify_on_drop()
    }
}

unsafe impl<T: RawResponse> Send for ResponseGuard<T> {}

// The types used for our internal dispatch mechanism, for dispatching Julia requests
// to our worker task.
enum Request {
    Get(Path, &'static mut [u8], &'static Config, ResponseGuard<Response>),
    Put(Path, &'static [u8], &'static Config, ResponseGuard<Response>),
    Delete(Path, &'static Config, ResponseGuard<Response>),
    List(Path, &'static Config, ResponseGuard<ListResponse>),
    ListStream(Path, &'static Config, ResponseGuard<ListStreamResponse>),
    GetStream(Path, usize, Compression, &'static Config, ResponseGuard<GetStreamResponse>),
    PutStream(Path, Compression, &'static Config, ResponseGuard<PutStreamResponse>)
}

unsafe impl Send for Request {}


// libuv is how we notify Julia tasks that their async requests are done.
// Note that this will be linked in from the Julia process, we do not try
// to link it while building this Rust lib.
extern "C" {
    fn uv_async_send(cond: *const c_void) -> i32;
    fn uv_is_active(cond: *const c_void) -> i32;
}

#[derive(Debug)]
#[repr(C)]
pub struct Notifier {
    handle: *const c_void,
}

impl Notifier {
    fn notify(&self) -> i32 {
        assert!(unsafe { uv_is_active(self.handle) } != 0);
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

#[derive(Debug, Clone)]
struct ConfigMeta {
    retry_config: RetryConfig
}

async fn dyn_connect(config: &Config) -> anyhow::Result<(Arc<dyn ObjectStore>, ConfigMeta)> {
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
    if let Some(value) = map.remove("initial_backoff_ms") {
        retry_config.backoff.init_backoff = std::time::Duration::from_millis(value.parse()
            .map_err(|e| anyhow!("failed to parse initial_backoff_ms: {}", e))?
        );
    }
    if let Some(value) = map.remove("max_backoff_ms") {
        retry_config.backoff.max_backoff = std::time::Duration::from_millis(value.parse()
            .map_err(|e| anyhow!("failed to parse max_backoff_ms: {}", e))?
        );
    }
    if let Some(value) = map.remove("backoff_exp_base") {
        retry_config.backoff.base = value.parse()
            .map_err(|e| anyhow!("failed to parse backoff_exp_base: {}", e))?;
    }

    let client: Arc<dyn ObjectStore> = match url.scheme() {
        "s3" => {
            let mut builder = object_store::aws::AmazonS3Builder::default()
                .with_url(url)
                .with_retry(retry_config.clone());
            for (key, value) in map {
                builder = builder.with_config(key.parse()?, value);
            }
            Arc::new(builder.build()?)
        }
        "az" | "azure" => {
            let mut builder = object_store::azure::MicrosoftAzureBuilder::default()
                .with_url(url)
                .with_retry(retry_config.clone());
            for (key, value) in map {
                builder = builder.with_config(key.parse()?, value);
            }
            Arc::new(builder.build()?)
        }
        _ => unimplemented!("unknown url scheme")
    };

    Ok((client, ConfigMeta { retry_config }))
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

macro_rules! with_retries_and_cancellation {
    ($op:expr, $response:expr, $config: expr) => {
        let start_instant = Instant::now();
        let mut retries = 0;
        'retry: loop {
            $response.ensure_active();
            tokio::select! {
                _ = $response.cancelled() => {
                    tracing::warn!("operation was cancelled");
                    $response.into_error("operation was cancelled");
                    return;
                }
                res = $op => {
                    match res {
                        Ok(v) => {
                            $response.success(v);
                            return;
                        }
                        Err(e) => {
                            if let Some(duration) = should_retry(retries, &e, start_instant.elapsed(), $config).await {
                                retries += 1;
                                tracing::info!("retrying error (reason: {:?}) after {:?}: {}", extract_error_info(&e).reason, duration, e);
                                tokio::time::sleep(duration).await;
                                continue 'retry;
                            }
                            tracing::warn!("{}", e);
                            $response.into_error(e);
                            return;
                        }
                    }
                }
            }
        }
    };
}

#[macro_export]
macro_rules! with_cancellation {
    ($op:expr, $response:expr) => {
        $response.ensure_active();
        tokio::select! {
            _ = $response.cancelled() => {
                tracing::warn!("operation was cancelled");
                $response.into_error("operation was cancelled");
                return;
            }
            res = $op => match res {
                Ok(v) => {
                    $response.success(v);
                },
                Err(e) => {
                    tracing::warn!("{}", e);
                    $response.into_error(e);
                }
            }
        }
    };
}


trait NotifyGuard {
    fn is_uninitialized(&mut self) -> bool;
    fn condition_handle(&self) -> *const c_void;
    fn context(&self) -> &Context {
        unimplemented!("missing context");
    }
    fn set_error(&mut self, error: impl std::fmt::Display);

    fn cancelled(&self) -> WaitForCancellationFuture {
        self.context().cancellation_token.cancelled()
    }
    fn into_error(mut self, error: impl std::fmt::Display) where Self: Sized {
        self.ensure_active();
        self.set_error(error);
    }
    unsafe fn notify(&self) {
        self.ensure_active();
        uv_async_send(self.condition_handle());
    }
    fn ensure_active(&self) {
        assert!(unsafe { uv_is_active(self.condition_handle()) } != 0, "handle to the condition dropped before notification");
    }
    fn notify_on_drop(&mut self) {
        self.ensure_active();
        if self.is_uninitialized() {
            self.set_error("Response was dropped before being initialized, this could be due to a Rust panic");
            unsafe { self.notify() }
        } else {
            unsafe{ self.notify() }
        }
    }
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

    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "object_store_ffi=warn,object_store=warn")
    }

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
                    Request::Get(path, slice, config, response) => {
                        with_retries_and_cancellation!(
                            handle_get(slice, &path, config),
                            response,
                            config
                        );
                    }
                    Request::Put(path, slice, config, response) => {
                        with_retries_and_cancellation!(
                            handle_put(slice, &path, config),
                            response,
                            config
                        );
                    }
                    Request::Delete(path, config, response) => {
                        with_retries_and_cancellation!(
                            handle_delete(&path, config),
                            response,
                            config
                        );
                    }
                    Request::List(prefix, config, response) => {
                        with_retries_and_cancellation!(
                            handle_list(&prefix, config),
                            response,
                            config
                        );
                    }
                    Request::ListStream(prefix, config, response) => {
                        with_retries_and_cancellation!(
                            handle_list_stream(&prefix, config),
                            response,
                            config
                        );
                    }
                    Request::GetStream(path, size_hint, compression, config, response) => {
                        with_retries_and_cancellation!(
                            handle_get_stream(&path, size_hint, compression, config),
                            response,
                            config
                        );
                    }
                    Request::PutStream(path, compression, config, response) => {
                        with_retries_and_cancellation!(
                            handle_put_stream(&path, compression, config),
                            response,
                            config
                        );
                    }
                }
            }
        }).buffer_unordered(static_config().concurrency_limit as usize).for_each(|_| async {}).await;
    });
    CResult::Ok
}

#[no_mangle]
pub extern "C" fn destroy_cstring(string: *mut c_char) -> CResult {
    let string = unsafe { std::ffi::CString::from_raw(string) };
    drop(string);
    CResult::Ok
}

#[no_mangle]
pub extern "C" fn destroy_context(ctx_ptr: *const Context) -> CResult {
    let ctx = unsafe { Arc::from_raw(ctx_ptr) };
    drop(ctx);
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
