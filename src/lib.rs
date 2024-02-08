use futures_util::{StreamExt, TryStreamExt};
use futures_util::stream::BoxStream;
use object_store::{RetryConfig, ObjectMeta};
use once_cell::sync::OnceCell;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::runtime::Runtime;
use tokio_util::io::StreamReader;
use std::collections::HashMap;
use std::ffi::CString;
use std::ffi::{c_char, c_void};
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::anyhow;

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use object_store::{path::Path, ObjectStore};

use moka::future::Cache;

mod error;

use error::{extract_error_info, should_retry, should_retry_logic, backoff_duration_for_retry};

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
#[derive(Debug, PartialEq, Eq)]
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
    Get(Path, &'static mut [u8], &'static Config, ResponseGuard),
    Put(Path, &'static [u8], &'static Config, ResponseGuard),
    Delete(Path, &'static Config, ResponseGuard),
    List(Path, &'static Config, ListResponseGuard),
    ListStream(Path, &'static Config, ListStreamResponseGuard),
    GetStream(Path, usize, &'static Config, GetStreamResponseGuard)
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

trait NotifyGuard {
    fn is_uninitialized(&self) -> bool;
    fn condition_handle(&self) -> *const c_void;
    fn set_error(&mut self, error: impl std::fmt::Display);
    fn into_error(mut self, error: impl std::fmt::Display) where Self: Sized {
        self.set_error(error);
    }
    unsafe fn notify(&self) {
        uv_async_send(self.condition_handle());
    }
    fn notify_on_drop(&mut self) {
        if self.is_uninitialized() {
            self.set_error("Response was dropped before being initialized, this could be due to a Rust panic");
            unsafe { self.notify() }
        } else {
            unsafe{ self.notify() }
        }
    }
}

// RAII Guard for a Response that ensures the awaiting Julia task will be notified
// even if this is dropped on a panic.
pub struct ResponseGuard {
    response: &'static mut Response,
    handle: *const c_void
}

impl NotifyGuard for ResponseGuard {
    fn is_uninitialized(&self) -> bool {
        self.response.result == CResult::Uninitialized
    }
    fn condition_handle(&self) -> *const c_void {
        self.handle
    }
    fn set_error(&mut self, error: impl std::fmt::Display) {
        self.response.result = CResult::Error;
        self.response.length = 0;
        let c_string = CString::new(format!("{}", error)).expect("should not have nulls");
        self.response.error_message = c_string.into_raw();
    }
}

impl ResponseGuard {
    unsafe fn new(response_ptr: *mut Response, handle: *const c_void) -> ResponseGuard {
        let response = unsafe { &mut (*response_ptr) };
        response.result = CResult::Uninitialized;

        ResponseGuard { response, handle }
    }
    fn success(self, length: usize) {
        self.response.result = CResult::Ok;
        self.response.length = length;
        self.response.error_message = std::ptr::null_mut();
    }
}

impl Drop for ResponseGuard {
    fn drop(&mut self) {
        self.notify_on_drop()
    }
}

unsafe impl Send for ResponseGuard {}

// The type used to give Julia the result of an async request. It will be allocated
// by Julia as part of the request and filled in by Rust.
#[repr(C)]
pub struct Response {
    result: CResult,
    length: usize,
    error_message: *mut c_char,
}

unsafe impl Send for Response {}

fn size_to_ranges(object_size: usize) -> Vec<Range<usize>> {
    if object_size == 0 {
        return vec![];
    }

    let part_size: usize = static_config().multipart_get_part_size as usize;

    // If the object size happens to be smaller than part_size,
    // then we will end up doing a single range get of the whole
    // object.
    let mut parts = object_size / part_size;
    if object_size % part_size != 0 {
        parts += 1;
    }
    let mut part_ranges = Vec::with_capacity(parts);
    for i in 0..(parts-1) {
        part_ranges.push((i*part_size)..((i+1)*part_size));
    }
    // Last part which handles sizes not divisible by part_size
    part_ranges.push(((parts-1)*part_size)..object_size);

    return part_ranges;
}

async fn multipart_get(slice: &mut [u8], path: &Path, client: &dyn ObjectStore) -> anyhow::Result<usize> {
    let result = client.head(&path).await?;
    if result.size > slice.len() {
        return Err(anyhow!("Supplied buffer was too small"));
    }

    let part_ranges = size_to_ranges(result.size);

    let result_vec = client.get_ranges(&path, &part_ranges).await?;
    let mut accum: usize = 0;
    for i in 0..result_vec.len() {
        slice[accum..accum + result_vec[i].len()].copy_from_slice(&result_vec[i]);
        accum += result_vec[i].len();
    }

    return Ok(accum);
}

async fn multipart_put(slice: &[u8], path: &Path, client: &dyn ObjectStore) -> anyhow::Result<()> {
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

async fn handle_get(slice: &mut [u8], path: &Path, config: &Config) -> anyhow::Result<usize> {
    let (client, _) = clients()
        .try_get_with(config.get_hash(), dyn_connect(config)).await
        .map_err(|e| anyhow!(e))?;

    // Multipart Get
    if slice.len() > static_config().multipart_get_threshold as usize {
        let accum = multipart_get(slice, path, &client).await?;
        return Ok(accum);
    }

    // Single part Get
    let body = client.get(path).await?;
    let mut chunks = body.into_stream().collect::<Vec<_>>().await;
    if let Some(pos) = chunks.iter().position(|result| result.is_err()) {
        let e = chunks.remove(pos).err().expect("already checked for error");
        tracing::warn!("Error while fetching a chunk: {}", e);
        return Err(e.into());
    }

    let mut received_bytes = 0;
    for result in chunks {
        let chunk = result.expect("checked `chunks` for errors before calling `spawn`");
        let len = chunk.len();

        if received_bytes + len > slice.len() {
            return Err(anyhow!("Supplied buffer was too small"));
        }

        slice[received_bytes..(received_bytes + len)].copy_from_slice(&chunk);
        received_bytes += len;
    }

    Ok(received_bytes)
}

async fn handle_put(slice: &'static [u8], path: &Path, config: &Config) -> anyhow::Result<usize> {
    let (client, _) = clients()
        .try_get_with(config.get_hash(), dyn_connect(config)).await
        .map_err(|e| anyhow!(e))?;

    let len = slice.len();
    if len < static_config().multipart_put_threshold as usize {
        let _ = client.put(path, slice.into()).await?;
    } else {
        let _ = multipart_put(slice, path, &client).await?;
    }

    Ok(len)
}

async fn handle_delete(path: &Path, config: &Config) -> anyhow::Result<()> {
    let (client, _) = clients()
        .try_get_with(config.get_hash(), dyn_connect(config)).await
        .map_err(|e| anyhow!(e))?;

    client.delete(path).await?;

    Ok(())
}

async fn handle_list(prefix: &Path, config: &Config) -> anyhow::Result<Vec<ObjectMeta>> {
    let (client, _) = clients()
        .try_get_with(config.get_hash(), dyn_connect(config))
        .await
        .map_err(|e| anyhow!(e))?;

    let stream = client.list(Some(&prefix));

    let entries: Vec<_> = stream.collect().await;
    let entries = entries.into_iter().collect::<Result<Vec<_>, _>>()?;
    Ok(entries)
}

async fn handle_list_stream(prefix: &Path, config: &Config) -> anyhow::Result<Box<StreamWrapper>> {
    let (client, _) = clients()
        .try_get_with(config.get_hash(), dyn_connect(config))
        .await
        .map_err(|e| anyhow!(e))?;

    let mut wrapper = Box::new(StreamWrapper {
        client,
        stream: None
    });

    let stream = wrapper.client.list(Some(&prefix)).chunks(1000).boxed();

    // Safety: This is needed because the compiler cannot infer that the stream
    // will outlive the client. We ensure this happens
    // by droping the stream before droping the Arc on destroy_list_stream
    wrapper.stream = Some(unsafe { std::mem::transmute(stream) });

    Ok(wrapper)
}

async fn handle_get_stream(path: &Path, size_hint: usize, config: &Config) -> anyhow::Result<(Box<GetStreamWrapper>, usize)> {
    let (client, config_meta) = clients()
        .try_get_with(config.get_hash(), dyn_connect(config)).await
        .map_err(|e| anyhow!(e))?;

    if size_hint > 0 && size_hint < static_config().multipart_get_threshold as usize {
        // Perform a single get without the head request
        let result = client.get(path).await?;
        let full_size = result.meta.size;
        let stream = result.into_stream().map_err(Into::into).boxed();
        let reader = StreamReader::new(stream);
        return Ok((Box::new(GetStreamWrapper { reader }), full_size));
    } else {
        // Perform head request and prefetch parts in parallel
        let meta = client.head(&path).await?;
        let part_ranges = size_to_ranges(meta.size);

        let state = (
            client,
            path.clone(),
            config_meta.clone()
        );
        let stream = futures_util::stream::iter(part_ranges)
            .scan(state, |state, range| {
                let state = state.clone();
                async move { Some((state, range)) }
            })
            .map(|((client, path, config_meta), range)| async move {
                return tokio::spawn(async move {
                    let start_instant = Instant::now();
                    let mut retries = 0;
                    'retry: loop {
                        match client.get_range(&path, range.clone()).await.map_err(Into::into) {
                            Ok(bytes) => {
                                return Ok::<_, anyhow::Error>(bytes)
                            },
                            Err(e) => {
                                if should_retry_logic(retries, &e, start_instant.elapsed(), &config_meta) {
                                    let duration = backoff_duration_for_retry(retries, &config_meta);

                                    retries += 1;
                                    tracing::info!("retrying error (reason: {:?}) after {:?}: {}", extract_error_info(&e).reason, duration, e);
                                    tokio::time::sleep(duration).await;
                                    continue 'retry;
                                }
                            }
                        }
                    }
                }).await?;
            })
            .buffered(16)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            .boxed();

        let reader = StreamReader::new(stream);
        return Ok((Box::new(GetStreamWrapper { reader }), meta.size));
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
                let start_instant = Instant::now();
                let mut retries = 0;
                match req {
                    Request::Get(path, slice, config, response) => {
                        'retry: loop {
                            match handle_get(slice, &path, config).await {
                                Ok(len) => {
                                    response.success(len);
                                    return;
                                }
                                Err(e) => {
                                    if let Some(duration) = should_retry(retries, &e, start_instant.elapsed(), config).await {
                                        retries += 1;
                                        tracing::info!("retrying error (reason: {:?}) after {:?}: {}", extract_error_info(&e).reason, duration, e);
                                        tokio::time::sleep(duration).await;
                                        continue 'retry;
                                    }
                                    tracing::warn!("{}", e);
                                    response.into_error(e);
                                    return;
                                }
                            }
                        }
                    }
                    Request::Put(path, slice, config, response) => {
                        'retry: loop {
                            match handle_put(slice, &path, config).await {
                                Ok(len) => {
                                    response.success(len);
                                    return;
                                }
                                Err(e) => {
                                    if let Some(duration) = should_retry(retries, &e, start_instant.elapsed(), config).await {
                                        retries += 1;
                                        tracing::info!("retrying error (reason: {:?}) after {:?}: {}", extract_error_info(&e).reason, duration, e);
                                        tokio::time::sleep(duration).await;
                                        continue 'retry;
                                    }
                                    tracing::warn!("{}", e);
                                    response.into_error(e);
                                    return;
                                }
                            }
                        }
                    }
                    Request::Delete(path, config, response) => {
                        'retry: loop {
                            match handle_delete(&path, config).await {
                                Ok(()) => {
                                    response.success(0);
                                    return;
                                }
                                Err(e) => {
                                    if let Some(duration) = should_retry(retries, &e, start_instant.elapsed(), config).await {
                                        retries += 1;
                                        tracing::info!("retrying error (reason: {:?}) after {:?}: {}", extract_error_info(&e).reason, duration, e);
                                        tokio::time::sleep(duration).await;
                                        continue 'retry;
                                    }
                                    tracing::warn!("{}", e);
                                    response.into_error(e);
                                    return;
                                }
                            }
                        }
                    }
                    Request::List(prefix, config, response) => {
                        'retry: loop {
                            match handle_list(&prefix, config).await {
                                Ok(entries) => {
                                    response.success(entries);
                                    return;
                                }
                                Err(e) => {
                                    if let Some(duration) = should_retry(retries, &e, start_instant.elapsed(), config).await {
                                        retries += 1;
                                        tracing::info!("retrying error (reason: {:?}) after {:?}: {}", extract_error_info(&e).reason, duration, e);
                                        tokio::time::sleep(duration).await;
                                        continue 'retry;
                                    }
                                    tracing::warn!("{}", e);
                                    response.into_error(e);
                                    return;
                                }
                            }
                        }
                    }
                    Request::ListStream(prefix, config, response) => {
                        'retry: loop {
                            match handle_list_stream(&prefix, config).await {
                                Ok(stream) => {
                                    response.success(stream);
                                    return;
                                }
                                Err(e) => {
                                    if let Some(duration) = should_retry(retries, &e, start_instant.elapsed(), config).await {
                                        retries += 1;
                                        tracing::info!("retrying error (reason: {:?}) after {:?}: {}", extract_error_info(&e).reason, duration, e);
                                        tokio::time::sleep(duration).await;
                                        continue 'retry;
                                    }
                                    tracing::warn!("{}", e);
                                    response.into_error(e);
                                    return;
                                }
                            }
                        }
                    }
                    Request::GetStream(path, size_hint, config, response) => {
                        'retry: loop {
                            match handle_get_stream(&path, size_hint, config).await {
                                Ok((stream, full_size)) => {
                                    response.success(stream, full_size);
                                    return;
                                }
                                Err(e) => {
                                    if let Some(duration) = should_retry(retries, &e, start_instant.elapsed(), config).await {
                                        retries += 1;
                                        tracing::info!("retrying error (reason: {:?}) after {:?}: {}", extract_error_info(&e).reason, duration, e);
                                        tokio::time::sleep(duration).await;
                                        continue 'retry;
                                    }
                                    tracing::warn!("{}", e);
                                    response.into_error(e);
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
    let response = unsafe { ResponseGuard::new(response, handle) };
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path: Path = path.to_str().expect("invalid utf8").try_into().unwrap();
    let slice = unsafe { std::slice::from_raw_parts_mut(buffer, size) };
    let config = unsafe { & (*config) };
    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::Get(path, slice, config, response)) {
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
    let response = unsafe { ResponseGuard::new(response, handle) };
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path: Path = path.to_str().expect("invalid utf8").try_into().unwrap();
    let slice = unsafe { std::slice::from_raw_parts(buffer, size) };
    let config = unsafe { & (*config) };
    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::Put(path, slice, config, response)) {
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
pub extern "C" fn delete(
    path: *const c_char,
    config: *const Config,
    response: *mut Response,
    handle: *const c_void
) -> CResult {
    let response = unsafe { ResponseGuard::new(response, handle) };
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path: Path = path.to_str().expect("invalid utf8").try_into().unwrap();
    let config = unsafe { & (*config) };
    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::Delete(path, config, response)) {
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

// Any non-Copy fields of ListEntry must be properly destroyed on destroy_list_entries
#[repr(C)]
pub struct ListEntry {
    location: *const c_char,
    last_modified: u64,
    size: u64,
    e_tag: *const c_char,
    version: *const c_char
}

#[repr(C)]
pub struct ListResponse {
    result: CResult,
    entries: *const ListEntry,
    entry_count: u64,
    error_message: *mut c_char
}

unsafe impl Send for ListResponse {}

// RAII Guard for a ListResponse that ensures the awaiting Julia task will be notified
// even if this is dropped on a panic.
pub struct ListResponseGuard {
    response: &'static mut ListResponse,
    handle: *const c_void
}

impl NotifyGuard for ListResponseGuard {
    fn is_uninitialized(&self) -> bool {
        self.response.result == CResult::Uninitialized
    }
    fn condition_handle(&self) -> *const c_void {
        self.handle
    }
    fn set_error(&mut self, error: impl std::fmt::Display) {
        self.response.result = CResult::Error;
        self.response.entries = std::ptr::null();
        self.response.entry_count = 0;
        let c_string = CString::new(format!("{}", error)).expect("should not have nulls");
        self.response.error_message = c_string.into_raw();
    }
}

impl ListResponseGuard {
    unsafe fn new(response_ptr: *mut ListResponse, handle: *const c_void) -> ListResponseGuard {
        let response = unsafe { &mut (*response_ptr) };
        response.result = CResult::Uninitialized;

        ListResponseGuard { response, handle }
    }
    fn success(self, entries: Vec<ObjectMeta>) {
        let entries_slice = entries.into_iter()
            .map(Into::into)
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let entry_count = entries_slice.len() as u64;
        let entries_ptr = entries_slice.as_ptr();
        std::mem::forget(entries_slice);

        self.response.result = CResult::Ok;
        self.response.entry_count = entry_count;
        self.response.entries = entries_ptr;
        self.response.error_message = std::ptr::null_mut();
    }
}

impl Drop for ListResponseGuard {
    fn drop(&mut self) {
        self.notify_on_drop()
    }
}

unsafe impl Send for ListResponseGuard {}

#[no_mangle]
pub extern "C" fn destroy_list_entries(
    entries: *mut ListEntry,
    entry_count: u64
) -> CResult {
    let boxed_slice = unsafe { Box::from_raw(std::slice::from_raw_parts_mut(entries, entry_count as usize)) };
    for entry in &*boxed_slice {
        // Safety: must properly drop all allocated fields from ListEntry here
        let _ = unsafe { CString::from_raw(entry.location.cast_mut()) };
        if !entry.e_tag.is_null() {
            let _ = unsafe { CString::from_raw(entry.e_tag.cast_mut()) };
        }
        if !entry.version.is_null() {
            let _ = unsafe { CString::from_raw(entry.version.cast_mut()) };
        }
    }
    CResult::Ok
}

impl From<object_store::ObjectMeta> for ListEntry {
    fn from(meta: object_store::ObjectMeta) -> Self {
        ListEntry {
            location: CString::new(meta.location.to_string())
                .expect("should not have nulls")
                .into_raw(),
            last_modified: meta.last_modified
                .timestamp()
                .try_into()
                .expect("is positive"),
            size: meta.size as u64,
            e_tag: match meta.e_tag {
                None => std::ptr::null(),
                Some(s) => {
                    CString::new(s)
                        .expect("should not have nulls")
                        .into_raw()
                }
            },
            version: match meta.version {
                None => std::ptr::null(),
                Some(s) => {
                    CString::new(s)
                        .expect("should not have nulls")
                        .into_raw()
                }
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn list(
    prefix: *const c_char,
    config: *const Config,
    response: *mut ListResponse,
    handle: *const c_void
) -> CResult {
    let response = unsafe { ListResponseGuard::new(response, handle) };
    let prefix = unsafe { std::ffi::CStr::from_ptr(prefix) };
    let prefix: Path = prefix.to_str().expect("invalid utf8").try_into().unwrap();
    let config = unsafe { & (*config) };
    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::List(prefix, config, response)) {
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

pub struct StreamWrapper {
    client: Arc<dyn ObjectStore>,
    stream: Option<BoxStream<'static, Vec<Result<object_store::ObjectMeta, object_store::Error>>>>
}

#[no_mangle]
pub extern "C" fn destroy_list_stream(
    stream: *mut StreamWrapper
) -> CResult {
    let mut boxed = unsafe { Box::from_raw(stream) };
    // Safety: Must drop the stream before the client here
    drop(boxed.stream.take());
    drop(boxed);
    CResult::Ok
}

#[repr(C)]
pub struct ListStreamResponse {
    result: CResult,
    stream: *mut StreamWrapper,
    error_message: *mut c_char
}

unsafe impl Send for ListStreamResponse {}

// RAII Guard for a ListResponse that ensures the awaiting Julia task will be notified
// even if this is dropped on a panic.
pub struct ListStreamResponseGuard {
    response: &'static mut ListStreamResponse,
    handle: *const c_void
}

impl NotifyGuard for ListStreamResponseGuard {
    fn is_uninitialized(&self) -> bool {
        self.response.result == CResult::Uninitialized
    }
    fn condition_handle(&self) -> *const c_void {
        self.handle
    }
    fn set_error(&mut self, error: impl std::fmt::Display) {
        self.response.result = CResult::Error;
        self.response.stream = std::ptr::null_mut();
        let c_string = CString::new(format!("{}", error)).expect("should not have nulls");
        self.response.error_message = c_string.into_raw();
    }
}

impl ListStreamResponseGuard {
    unsafe fn new(response_ptr: *mut ListStreamResponse, handle: *const c_void) -> ListStreamResponseGuard {
        let response = unsafe { &mut (*response_ptr) };
        response.result = CResult::Uninitialized;

        ListStreamResponseGuard { response, handle }
    }
    fn success(self, stream: Box<StreamWrapper>) {
        self.response.result = CResult::Ok;
        self.response.stream = Box::into_raw(stream);
        self.response.error_message = std::ptr::null_mut();
    }
}

impl Drop for ListStreamResponseGuard {
    fn drop(&mut self) {
        self.notify_on_drop()
    }
}

unsafe impl Send for ListStreamResponseGuard {}

#[no_mangle]
pub extern "C" fn list_stream(
    prefix: *const c_char,
    config: *const Config,
    response: *mut ListStreamResponse,
    handle: *const c_void
) -> CResult {
    let response = unsafe { ListStreamResponseGuard::new(response, handle) };
    let prefix = unsafe { std::ffi::CStr::from_ptr(prefix) };
    let prefix: Path = prefix.to_str().expect("invalid utf8").try_into().unwrap();
    let config = unsafe { & (*config) };
    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::ListStream(prefix, config, response)) {
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
pub extern "C" fn next_list_stream_chunk(
    stream: *mut StreamWrapper,
    response: *mut ListResponse,
    handle: *const c_void
) -> CResult {
    let response = unsafe { ListResponseGuard::new(response, handle) };
    let wrapper = match unsafe { stream.as_mut() } {
        Some(w) => w,
        None => {
            tracing::error!("null stream pointer");
            return CResult::Error;
        }
    };

    match RT.get() {
        Some(runtime) => {
            runtime.spawn(async move {
                let list_op = async {
                    let stream_ref = wrapper.stream.as_mut().unwrap();
                    let option = match stream_ref.next().await {
                        Some(vec) => {
                            vec.into_iter().collect::<Result<Vec<_>, _>>()?
                        }
                        None => {
                            vec![]
                        }
                    };
                    Ok::<_, anyhow::Error>(option)
                };

                match list_op.await {
                    Ok(entries) => {
                        response.success(entries);
                    },
                    Err(e) => {
                        tracing::warn!("{}", e);
                        response.into_error(e);
                    }
                }
            });
            CResult::Ok
        }
        None => {
            return CResult::Error;
        }
    }
}

#[repr(C)]
pub struct ReadResponse {
    result: CResult,
    length: usize,
    eof: bool,
    error_message: *mut c_char
}

unsafe impl Send for ReadResponse {}

// RAII Guard for a ListResponse that ensures the awaiting Julia task will be notified
// even if this is dropped on a panic.
pub struct ReadResponseGuard {
    response: &'static mut ReadResponse,
    handle: *const c_void
}

impl NotifyGuard for ReadResponseGuard {
    fn is_uninitialized(&self) -> bool {
        self.response.result == CResult::Uninitialized
    }
    fn condition_handle(&self) -> *const c_void {
        self.handle
    }
    fn set_error(&mut self, error: impl std::fmt::Display) {
        self.response.result = CResult::Error;
        self.response.length = 0;
        self.response.eof = false;
        let c_string = CString::new(format!("{}", error)).expect("should not have nulls");
        self.response.error_message = c_string.into_raw();
    }
}

impl ReadResponseGuard {
    unsafe fn new(response_ptr: *mut ReadResponse, handle: *const c_void) -> ReadResponseGuard {
        let response = unsafe { &mut (*response_ptr) };
        response.result = CResult::Uninitialized;

        ReadResponseGuard { response, handle }
    }
    fn success(self, length: usize, eof: bool) {
        self.response.result = CResult::Ok;
        self.response.length = length;
        self.response.eof = eof;
        self.response.error_message = std::ptr::null_mut();
    }
}

impl Drop for ReadResponseGuard {
    fn drop(&mut self) {
        self.notify_on_drop()
    }
}

unsafe impl Send for ReadResponseGuard {}

pub struct GetStreamWrapper {
    reader: StreamReader<BoxStream<'static, Result<bytes::Bytes, std::io::Error>>, bytes::Bytes>
}

#[no_mangle]
pub extern "C" fn destroy_get_stream(
    stream: *mut GetStreamWrapper
) -> CResult {
    let boxed = unsafe { Box::from_raw(stream) };
    drop(boxed);
    CResult::Ok
}

#[repr(C)]
pub struct GetStreamResponse {
    result: CResult,
    stream: *mut GetStreamWrapper,
    object_size: u64,
    error_message: *mut c_char
}

unsafe impl Send for GetStreamResponse {}

// RAII Guard for a ListResponse that ensures the awaiting Julia task will be notified
// even if this is dropped on a panic.
pub struct GetStreamResponseGuard {
    response: &'static mut GetStreamResponse,
    handle: *const c_void
}

impl NotifyGuard for GetStreamResponseGuard {
    fn is_uninitialized(&self) -> bool {
        self.response.result == CResult::Uninitialized
    }
    fn condition_handle(&self) -> *const c_void {
        self.handle
    }
    fn set_error(&mut self, error: impl std::fmt::Display) {
        self.response.result = CResult::Error;
        self.response.stream = std::ptr::null_mut();
        self.response.object_size = 0;
        let c_string = CString::new(format!("{}", error)).expect("should not have nulls");
        self.response.error_message = c_string.into_raw();
    }
}

impl GetStreamResponseGuard {
    unsafe fn new(response_ptr: *mut GetStreamResponse, handle: *const c_void) -> GetStreamResponseGuard {
        let response = unsafe { &mut (*response_ptr) };
        response.result = CResult::Uninitialized;

        GetStreamResponseGuard { response, handle }
    }
    fn success(self, stream: Box<GetStreamWrapper>, object_size: usize) {
        self.response.result = CResult::Ok;
        self.response.stream = Box::into_raw(stream);
        self.response.object_size = object_size as u64;
        self.response.error_message = std::ptr::null_mut();
    }
}

impl Drop for GetStreamResponseGuard {
    fn drop(&mut self) {
        self.notify_on_drop()
    }
}

unsafe impl Send for GetStreamResponseGuard {}

#[no_mangle]
pub extern "C" fn get_stream(
    path: *const c_char,
    size_hint: usize,
    config: *const Config,
    response: *mut GetStreamResponse,
    handle: *const c_void
) -> CResult {
    let response = unsafe { GetStreamResponseGuard::new(response, handle) };
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path: Path = path.to_str().expect("invalid utf8").try_into().unwrap();
    let config = unsafe { & (*config) };

    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::GetStream(path, size_hint, config, response)) {
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
pub extern "C" fn read_get_stream(
    stream: *mut GetStreamWrapper,
    buffer: *mut u8,
    size: usize,
    amount: usize,
    response: *mut ReadResponse,
    handle: *const c_void
) -> CResult {
    let response = unsafe { ReadResponseGuard::new(response, handle) };
    let mut slice = unsafe { std::slice::from_raw_parts_mut(buffer, size) };
    let wrapper = match unsafe { stream.as_mut() } {
        Some(w) => w,
        None => {
            tracing::error!("null stream pointer");
            return CResult::Error;
        }
    };

    match RT.get() {
        Some(runtime) => {
            runtime.spawn(async move {
                let read_op = async {
                    let amount_to_read = size.min(amount);
                    let mut bytes_read = 0;
                    while amount_to_read > bytes_read {
                        let n = wrapper.reader.read_buf(&mut slice).await?;

                        if n == 0 {
                            let eof = amount_to_read != bytes_read;
                            return Ok((bytes_read, eof))
                        } else {
                            bytes_read += n;
                        }
                    }

                    Ok::<_, anyhow::Error>((bytes_read, false))
                };

                match read_op.await {
                    Ok((bytes_read, eof)) => {
                        response.success(bytes_read, eof);
                    },
                    Err(e) => {
                        tracing::warn!("{}", e);
                        response.into_error(e);
                    }
                }
            });
            CResult::Ok
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
