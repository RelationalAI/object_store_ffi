use crate::{CResult, Config, NotifyGuard, SQ, RT, clients, dyn_connect, static_config, Request};
use crate::util::{size_to_ranges, Compression, with_decoder, with_encoder};
use crate::error::{should_retry_logic, extract_error_info, backoff_duration_for_retry};

use object_store::{path::Path, ObjectStore};

use std::time::Instant;
use bytes::Buf;
use tokio_util::io::StreamReader;
use tokio::io::{AsyncWriteExt, AsyncReadExt, AsyncRead, AsyncWrite};
use anyhow::anyhow;
use std::ffi::{c_char, c_void, CString};
use futures_util::{StreamExt, TryStreamExt};
use std::sync::Arc;

pub(crate) async fn handle_get_stream(path: &Path, size_hint: usize, compression: Compression, config: &Config) -> anyhow::Result<(Box<ReadStream>, usize)> {
    let (client, config_meta) = clients()
        .try_get_with(config.get_hash(), dyn_connect(config)).await
        .map_err(|e| anyhow!(e))?;

    if size_hint > 0 && size_hint < static_config().multipart_get_threshold as usize {
        // Perform a single get without the head request
        let result = client.get(path).await?;
        let full_size = result.meta.size;
        let stream = result.into_stream().map_err(Into::<std::io::Error>::into).boxed();
        let reader = StreamReader::new(stream);
        let decoded = with_decoder(compression, reader);
        return Ok((Box::new(ReadStream { reader: decoded }), full_size));
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
        let decoded = with_decoder(compression, reader);
        return Ok((Box::new(ReadStream { reader: decoded }), meta.size));
    }
}

pub(crate) async fn handle_put_stream(path: &Path, compression: Compression, config: &Config) -> anyhow::Result<Box<WriteStream>> {
    let (client, _) = clients()
        .try_get_with(config.get_hash(), dyn_connect(config)).await
        .map_err(|e| anyhow!(e))?;

    let (id, writer) = client.put_multipart(&path).await?;

    let encoded = with_encoder(compression, writer);
    let upload = Some(UploadState { client, id, path: path.clone() });
    return Ok(Box::new(WriteStream { upload, writer: encoded }));
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
    pub(crate) fn success(self, length: usize, eof: bool) {
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

pub struct ReadStream {
    reader: Box<dyn AsyncRead + Unpin + Send>
}

#[no_mangle]
pub extern "C" fn destroy_read_stream(
    stream: *mut ReadStream
) -> CResult {
    let boxed = unsafe { Box::from_raw(stream) };
    drop(boxed);
    CResult::Ok
}

#[repr(C)]
pub struct GetStreamResponse {
    result: CResult,
    stream: *mut ReadStream,
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
    pub(crate) fn success(self, stream: Box<ReadStream>, object_size: usize) {
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
    decompress: *const c_char,
    config: *const Config,
    response: *mut GetStreamResponse,
    handle: *const c_void
) -> CResult {
    let response = unsafe { GetStreamResponseGuard::new(response, handle) };
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path: Path = path.to_str().expect("invalid utf8").try_into().unwrap();
    let decompress = match Compression::try_from(decompress) {
        Ok(c) => c,
        Err(e) => {
            // TODO handle early errors with response.set_error
            std::mem::forget(response);
            tracing::error!("{}", e);
            return CResult::Error;
        }
    };
    let config = unsafe { & (*config) };

    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::GetStream(path, size_hint, decompress, config, response)) {
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
pub extern "C" fn read_from_stream(
    stream: *mut ReadStream,
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
            std::mem::forget(response);
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
                            return Ok((bytes_read, true))
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

#[repr(C)]
pub struct WriteResponse {
    result: CResult,
    length: usize,
    error_message: *mut c_char
}

unsafe impl Send for WriteResponse {}

// RAII Guard for a ListResponse that ensures the awaiting Julia task will be notified
// even if this is dropped on a panic.
pub struct WriteResponseGuard {
    response: &'static mut WriteResponse,
    handle: *const c_void
}

impl NotifyGuard for WriteResponseGuard {
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

impl WriteResponseGuard {
    unsafe fn new(response_ptr: *mut WriteResponse, handle: *const c_void) -> WriteResponseGuard {
        let response = unsafe { &mut (*response_ptr) };
        response.result = CResult::Uninitialized;

        WriteResponseGuard { response, handle }
    }
    pub(crate) fn success(self, length: usize) {
        self.response.result = CResult::Ok;
        self.response.length = length;
        self.response.error_message = std::ptr::null_mut();
    }
}

impl Drop for WriteResponseGuard {
    fn drop(&mut self) {
        self.notify_on_drop()
    }
}

unsafe impl Send for WriteResponseGuard {}

struct UploadState {
    client: Arc<dyn ObjectStore>,
    id: object_store::MultipartId,
    path: Path
}

impl UploadState {
    async fn cleanup(&self) {
        if let Err(e) = self.client.abort_multipart(&self.path, &self.id).await {
            tracing::error!("failed to abort multipart upload: {}", e);
        }
    }
}

pub struct WriteStream {
    upload: Option<UploadState>,
    writer: Box<dyn AsyncWrite + Unpin + Send>
}

#[no_mangle]
pub extern "C" fn destroy_write_stream(
    writer: *mut WriteStream
) -> CResult {
    let boxed = unsafe { Box::from_raw(writer) };
    drop(boxed);
    CResult::Ok
}

#[repr(C)]
pub struct PutStreamResponse {
    result: CResult,
    stream: *mut WriteStream,
    error_message: *mut c_char
}

unsafe impl Send for PutStreamResponse {}

// RAII Guard for a PutStreamResponse that ensures the awaiting Julia task will be notified
// even if this is dropped on a panic.
pub struct PutStreamResponseGuard {
    response: &'static mut PutStreamResponse,
    handle: *const c_void
}

impl NotifyGuard for PutStreamResponseGuard {
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

impl PutStreamResponseGuard {
    unsafe fn new(response_ptr: *mut PutStreamResponse, handle: *const c_void) -> PutStreamResponseGuard {
        let response = unsafe { &mut (*response_ptr) };
        response.result = CResult::Uninitialized;

        PutStreamResponseGuard { response, handle }
    }
    pub(crate) fn success(self, stream: Box<WriteStream>) {
        self.response.result = CResult::Ok;
        self.response.stream = Box::into_raw(stream);
        self.response.error_message = std::ptr::null_mut();
    }
}

impl Drop for PutStreamResponseGuard {
    fn drop(&mut self) {
        self.notify_on_drop()
    }
}

unsafe impl Send for PutStreamResponseGuard {}

#[no_mangle]
pub extern "C" fn put_stream(
    path: *const c_char,
    compress: *const c_char,
    config: *const Config,
    response: *mut PutStreamResponse,
    handle: *const c_void
) -> CResult {
    let response = unsafe { PutStreamResponseGuard::new(response, handle) };
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path: Path = path.to_str().expect("invalid utf8").try_into().unwrap();
    let compress = match Compression::try_from(compress) {
        Ok(c) => c,
        Err(e) => {
            // TODO handle early errors with response.set_error
            std::mem::forget(response);
            tracing::error!("{}", e);
            return CResult::Error;
        }
    };
    let config = unsafe { & (*config) };

    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::PutStream(path, compress, config, response)) {
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
pub extern "C" fn write_to_stream(
    stream: *mut WriteStream,
    buffer: *mut u8,
    size: usize,
    flush: bool,
    response: *mut WriteResponse,
    handle: *const c_void
) -> CResult {
    let response = unsafe { WriteResponseGuard::new(response, handle) };
    let mut slice = unsafe { std::slice::from_raw_parts(buffer, size) };
    let wrapper = match unsafe { stream.as_mut() } {
        Some(w) => w,
        None => {
            std::mem::forget(response);
            tracing::error!("null stream pointer");
            return CResult::Error;
        }
    };

    match RT.get() {
        Some(runtime) => {
            runtime.spawn(async move {
                let write_op = async {
                    let mut bytes_written = 0;
                    while slice.has_remaining() {
                        bytes_written += wrapper.writer.write_buf(&mut slice).await?;
                    }
                    if flush {
                        wrapper.writer.flush().await?;
                    }
                    Ok::<_, anyhow::Error>(bytes_written)
                };

                match write_op.await {
                    Ok(bytes_written) => {
                        response.success(bytes_written);
                    },
                    Err(e) => {
                        tracing::warn!("{}", e);
                        if let Some(upload) = wrapper.upload.as_ref() {
                            upload.cleanup().await;
                        }
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
pub extern "C" fn shutdown_write_stream(
    stream: *mut WriteStream,
    response: *mut WriteResponse,
    handle: *const c_void
) -> CResult {
    let response = unsafe { WriteResponseGuard::new(response, handle) };
    let wrapper = match unsafe { stream.as_mut() } {
        Some(w) => w,
        None => {
            std::mem::forget(response);
            tracing::error!("null stream pointer");
            return CResult::Error;
        }
    };

    match RT.get() {
        Some(runtime) => {
            runtime.spawn(async move {
                let shutdown_op = async {
                    wrapper.writer.shutdown().await?;
                    Ok::<_, anyhow::Error>(())
                };

                match shutdown_op.await {
                    Ok(_) => {
                        response.success(0);
                    },
                    Err(e) => {
                        tracing::warn!("{}", e);
                        if let Some(upload) = wrapper.upload.as_ref() {
                            upload.cleanup().await;
                        }
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
