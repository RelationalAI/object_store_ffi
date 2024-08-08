use crate::{static_config, CResult, Client, Context, NotifyGuard, RawConfig, RawResponse, Request, ResponseGuard, RT, SQ};
use crate::util::{size_to_ranges, Compression, CompressedWriter, with_decoder, cstr_to_path};
use crate::error::RetryState;
use crate::with_cancellation;

use object_store::{path::Path, ObjectStore};

use bytes::Buf;
use tokio_util::io::StreamReader;
use tokio::io::{AsyncWriteExt, AsyncReadExt, AsyncRead, AsyncBufReadExt};
use std::ffi::{c_char, c_void, c_longlong};
use futures_util::{StreamExt, TryStreamExt};

pub(crate) async fn handle_get_stream(client: Client, path: &Path, size_hint: usize, compression: Compression) -> anyhow::Result<(Box<ReadStream>, usize)> {
    if size_hint > 0 && size_hint < static_config().multipart_get_threshold as usize {
        // Perform a single get without the head request
        let result = client.store.get(path).await?;
        let full_size = result.meta.size;
        let stream = result.into_stream().map_err(Into::<std::io::Error>::into).boxed();
        let reader = StreamReader::new(stream);
        let decoded = with_decoder(compression, reader);
        return Ok((Box::new(ReadStream { reader: tokio::io::BufReader::with_capacity(64 * 1024, decoded) }), full_size));
    } else {
        // Perform head request and prefetch parts in parallel
        let meta = client.store.head(&path).await?;
        let part_ranges = size_to_ranges(meta.size);

        let state = (
            client,
            path.clone()
        );
        let stream = futures_util::stream::iter(part_ranges)
            .scan(state, |state, range| {
                let state = state.clone();
                async move { Some((state, range)) }
            })
            .map(|((client, path), range)| async move {
                return tokio::spawn(async move {
                    let mut retry_state = RetryState::new(client.config.retry_config);
                    'retry: loop {
                        match client.store.get_range(&path, range.clone()).await.map_err(Into::into) {
                            Ok(bytes) => {
                                return Ok::<_, anyhow::Error>(bytes)
                            },
                            Err(e) => {
                                match retry_state.should_retry(&e) {
                                    Ok((info, duration)) => {
                                        tracing::info!("retrying get stream error (reason: {:?}) after {:?}: {}", info.reason, duration, e);
                                        tokio::time::sleep(duration).await;
                                        continue 'retry;
                                    }
                                    Err(report) => {
                                        tracing::warn!("[get stream] {}", report);
                                        return Err(e);
                                    }
                                }
                            }
                        }
                    }
                }).await?;
            })
            .buffered(64)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            .boxed();

        let reader = StreamReader::new(stream);
        let decoded = with_decoder(compression, reader);
        return Ok((Box::new(ReadStream { reader: tokio::io::BufReader::with_capacity(64 * 1024, decoded) }), meta.size));
    }
}

pub(crate) async fn handle_put_stream(client: Client, path: &Path, compression: Compression) -> anyhow::Result<Box<WriteStream>> {
    let writer = object_store::buffered::BufWriter::with_capacity(
        client.store,
        path.clone(),
        10 * 1024 * 1024
    )
        .with_max_concurrency(64);

    let encoded = CompressedWriter::new(compression, writer);
    return Ok(Box::new(WriteStream { writer: encoded, aborted: false }));
}

#[repr(C)]
pub struct ReadResponse {
    result: CResult,
    length: usize,
    eof: bool,
    error_message: *mut c_char,
    context: *const Context,
}

unsafe impl Send for ReadResponse {}

impl RawResponse for ReadResponse {
    type Payload = (usize, bool);
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
            Some((length, eof)) => {
                self.length = length;
                self.eof = eof;
            }
            None => {
                self.length = 0;
                self.eof = false;
            }
        }
    }
}

pub struct ReadStream {
    reader: tokio::io::BufReader<Box<dyn AsyncRead + Unpin + Send>>
}

#[no_mangle]
pub extern "C" fn destroy_read_stream(
    stream: *mut ReadStream
) -> CResult {
    // Destroying complex objects is safer to do within the runtime to guard
    // against the case were the destructor needs to spawn a task
    match RT.get() {
        Some(runtime) => {
            let handle = runtime.handle();
            handle.block_on(async {
                let boxed = unsafe { Box::from_raw(stream) };
                drop(boxed);
            });
            CResult::Ok
        }
        None => {
            tracing::error!("failed to destroy read stream, runtime not started");
            CResult::Error
        }
    }
}

#[repr(C)]
pub struct GetStreamResponse {
    result: CResult,
    stream: *mut ReadStream,
    object_size: u64,
    error_message: *mut c_char,
    context: *const Context
}

unsafe impl Send for GetStreamResponse {}

impl RawResponse for GetStreamResponse {
    type Payload = (Box<ReadStream>, usize);
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
            Some((stream, object_size)) => {
                self.stream = Box::into_raw(stream);
                self.object_size = object_size as u64;
            }
            None => {
                self.stream = std::ptr::null_mut();
                self.object_size = 0;
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn get_stream(
    path: *const c_char,
    size_hint: usize,
    decompress: *const c_char,
    config: *const RawConfig,
    response: *mut GetStreamResponse,
    handle: *const c_void
) -> CResult {
    let response = unsafe { ResponseGuard::new(response, handle) };
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path = unsafe{ cstr_to_path(path) };
    let decompress = match Compression::try_from(decompress) {
        Ok(c) => c,
        Err(e) => {
            response.into_error(e);
            return CResult::Error;
        }
    };
    let config = unsafe { & (*config) };

    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::GetStream(path, size_hint, decompress, config, response)) {
                Ok(_) => CResult::Ok,
                Err(flume::TrySendError::Full(Request::GetStream(_, _, _, _, response))) => {
                    response.into_error("object_store_ffi internal channel full, backoff");
                    CResult::Backoff
                }
                Err(flume::TrySendError::Disconnected(Request::GetStream(_, _, _, _, response))) => {
                    response.into_error("object_store_ffi internal channel closed (may be missing initialization)");
                    CResult::Error
                }
                _ => unreachable!("the response type must match")
            }
        }
        None => {
            response.into_error("object_store_ffi internal channel closed (may be missing initialization)");
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
    let response = unsafe { ResponseGuard::new(response, handle) };
    let mut slice = unsafe { std::slice::from_raw_parts_mut(buffer, size) };
    let wrapper = match unsafe { stream.as_mut() } {
        Some(w) => w,
        None => {
            response.into_error("null stream pointer");
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

                with_cancellation!(read_op, response);
            });
            CResult::Ok
        }
        None => {
            response.into_error("object_store_ffi runtime not started (may be missing initialization)");
            return CResult::Error;
        }
    }
}

// Returns the amount of bytes that is currently available in the reader's buffer.
// This function is synchronous and does not block.
#[no_mangle]
pub extern "C" fn bytes_available(
    stream: *mut ReadStream
) -> c_longlong {
    let wrapper = match unsafe { stream.as_mut() } {
        Some(w) => w,
        None => {
            tracing::error!("null stream pointer");
            return -1;
        }
    };

    let available = wrapper.reader.buffer().len();
    return available as c_longlong
}

// Checks if the end of stream is reached without changing the current
// stream position. If EOS is not reached it ensures the internal buffer
// has data available for a subsequent `read_from_stream`.
#[no_mangle]
pub extern "C" fn is_end_of_stream(
    stream: *mut ReadStream,
    response: *mut ReadResponse,
    handle: *const c_void
) -> CResult {
    let response = unsafe { ResponseGuard::new(response, handle) };
    let wrapper = match unsafe { stream.as_mut() } {
        Some(w) => w,
        None => {
            response.into_error("null stream pointer");
            return CResult::Error;
        }
    };

    match RT.get() {
        Some(runtime) => {
            runtime.spawn(async move {
                let eof_op = async {
                    let (bytes_read, eof) = wrapper.reader.fill_buf().await.map(|buffer| {
                        if buffer.is_empty() {
                            (0, true)
                        } else {
                            (buffer.len(), false)
                        }
                    })?;
                    Ok::<_, anyhow::Error>((bytes_read, eof))
                };

                with_cancellation!(eof_op, response);
            });
            CResult::Ok
        }
        None => {
            response.into_error("object_store_ffi runtime not started (may be missing initialization)");
            return CResult::Error;
        }
    }
}

#[repr(C)]
pub struct WriteResponse {
    result: CResult,
    length: usize,
    error_message: *mut c_char,
    context: *const Context,
}

unsafe impl Send for WriteResponse {}

impl RawResponse for WriteResponse {
    type Payload = usize;
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
            Some(length) => {
                self.length = length;
            }
            None => {
                self.length = 0;
            }
        }
    }
}


pub struct WriteStream {
    writer: CompressedWriter<object_store::buffered::BufWriter>,
    aborted: bool
}

#[no_mangle]
pub extern "C" fn destroy_write_stream(
    writer: *mut WriteStream
) -> CResult {
    // Destroying complex objects is safer to do within the runtime to guard
    // against the case were the destructor needs to spawn a task
    match RT.get() {
        Some(runtime) => {
            let handle = runtime.handle();
            handle.block_on(async {
                let boxed = unsafe { Box::from_raw(writer) };
                drop(boxed);
            });
            CResult::Ok
        }
        None => {
            tracing::error!("failed to destroy write stream, runtime not started");
            CResult::Error
        }
    }
}

#[repr(C)]
pub struct PutStreamResponse {
    result: CResult,
    stream: *mut WriteStream,
    error_message: *mut c_char,
    context: *const Context
}

unsafe impl Send for PutStreamResponse {}

impl RawResponse for PutStreamResponse {
    type Payload = Box<WriteStream>;
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
            Some(stream) => {
                self.stream = Box::into_raw(stream);
            }
            None => {
                self.stream = std::ptr::null_mut();
            }
        }
    }
}

// Creates a `WriteStream` that splits the written data into parts and uploads them
// concurrently to form a single object.
//
// The parts have 10MB in size and up to 8 uploads are dispatched concurrently.
//
// Data can be written to the `WriteStream` using `write_to_stream`.
//
// `shutdown_write_stream` must be called on the returned `WriteStream` to finalize the
// operation once all data has been written to it.
//
// The written data can be optionally compressed by providing one of `gzip`, `deflate`, `zlib` or
// `zstd` in the `compress` argument.
#[no_mangle]
pub extern "C" fn put_stream(
    path: *const c_char,
    compress: *const c_char,
    config: *const RawConfig,
    response: *mut PutStreamResponse,
    handle: *const c_void
) -> CResult {
    let response = unsafe { ResponseGuard::new(response, handle) };
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path = unsafe{ cstr_to_path(path) };
    let compress = match Compression::try_from(compress) {
        Ok(c) => c,
        Err(e) => {
            response.into_error(e);
            return CResult::Error;
        }
    };
    let config = unsafe { & (*config) };

    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::PutStream(path, compress, config, response)) {
                Ok(_) => CResult::Ok,
                Err(flume::TrySendError::Full(Request::PutStream(_, _, _, response))) => {
                    response.into_error("object_store_ffi internal channel full, backoff");
                    CResult::Backoff
                }
                Err(flume::TrySendError::Disconnected(Request::PutStream(_, _, _, response))) => {
                    response.into_error("object_store_ffi internal channel closed (may be missing initialization)");
                    CResult::Error
                }
                _ => unreachable!("the response type must match")
            }
        }
        None => {
            response.into_error("object_store_ffi internal channel closed (may be missing initialization)");
            return CResult::Error;
        }
    }
}

// Writes bytes to the provided `WriteStream` and optionally flushes the internal buffers.
// Any data written to the stream will be buffered and split into 10 MB chunks before being sent
// concurrently (concurrency == 8) to the backend.
//
// Flushing is not required, it is only exposed to give more control over the internal buffering.
#[no_mangle]
pub extern "C" fn write_to_stream(
    stream: *mut WriteStream,
    buffer: *mut u8,
    size: usize,
    flush: bool,
    response: *mut WriteResponse,
    handle: *const c_void
) -> CResult {
    let response = unsafe { ResponseGuard::new(response, handle) };
    let mut slice = if buffer.is_null() && size == 0 {
        unsafe { std::slice::from_raw_parts(std::ptr::NonNull::dangling().as_ptr(), 0) }
    } else {
        unsafe { std::slice::from_raw_parts(buffer, size) }
    };
    let wrapper = match unsafe { stream.as_mut() } {
        Some(w) => w,
        None => {
            response.into_error("null stream pointer");
            return CResult::Error;
        }
    };

    match RT.get() {
        Some(runtime) => {
            runtime.spawn(async move {
                let abort_on_error = async {
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
                        Ok(r) => Ok(r),
                        Err(e) => {
                            if !wrapper.aborted {
                                let _ = wrapper.writer.abort().await;
                                wrapper.aborted = true;
                            }
                            Err(e)
                        }
                    }
                };

                with_cancellation!(abort_on_error, response);
            });
            CResult::Ok
        }
        None => {
            response.into_error("object_store_ffi runtime not started (may be missing initialization)");
            return CResult::Error;
        }
    }
}

// In order to be able to observe any of the data written to the `WriteStream` this function must be
// called. It ensures that all data is flushed and that the proper completion request is sent to
// the cloud backend.
//
// Failure to call this function may cause incomplete data parts to be written
// to the backend, possibly incurring storage costs.
#[no_mangle]
pub extern "C" fn shutdown_write_stream(
    stream: *mut WriteStream,
    response: *mut WriteResponse,
    handle: *const c_void
) -> CResult {
    let response = unsafe { ResponseGuard::new(response, handle) };
    let wrapper = match unsafe { stream.as_mut() } {
        Some(w) => w,
        None => {
            response.into_error("null stream pointer");
            return CResult::Error;
        }
    };

    match RT.get() {
        Some(runtime) => {
            runtime.spawn(async move {
                let shutdown_op = async {
                    wrapper.writer.shutdown().await?;
                    Ok::<_, anyhow::Error>(0)
                };

                // Manual cancellation due to cleanup
                tokio::select! {
                    _ = response.cancelled() => {
                        // TODO maybe abort on cancellation
                        tracing::warn!("operation was cancelled");
                        response.into_error("operation was cancelled");
                        return;
                    }
                    res = shutdown_op => match res {
                        Ok(v) => {
                            response.success(v);
                        },
                        Err(e) => {
                            tracing::warn!("{}", e);
                            // TODO abort upload, BufWriter currently panics if we abort here
                            wrapper.aborted = true;
                            response.into_error(e);
                        }
                    }
                }
            });
            CResult::Ok
        }
        None => {
            response.into_error("object_store_ffi runtime not started (may be missing initialization)");
            return CResult::Error;
        }
    }
}
