use crate::encryption::{CrypterReader, CrypterWriter, Mode};
use crate::{destroy_with_runtime, export_queued_op, with_retries, BoxedReader, BoxedUpload, CResult, Client, Context, NotifyGuard, RawConfig, RawResponse, Request, ResponseGuard, RT, SQ};
use crate::util::{cstr_to_path, size_to_ranges, with_decoder, BufWriter, CompressedWriter, Compression};
use crate::error::{Error, ErrorExt, Kind as ErrorKind, RetryState};
use crate::with_cancellation;

use anyhow::Context as AnyhowContext;
use object_store::{path::Path, ObjectStore};

use bytes::Buf;
use tokio_util::io::StreamReader;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use walkdir::WalkDir;
use std::ffi::{c_char, c_void, c_longlong};
use std::sync::Arc;
use futures_util::{StreamExt, TryStreamExt};
use std::path::Path as OsPath;

impl Client {
    async fn put_stream_impl(&self, path: &Path, compression: Compression) -> crate::Result<BoxedUpload> {
        let path = &self.full_path(path);
        let part_size = self.config.multipart_put_part_size;
        let concurrency = self.config.multipart_put_concurrency;
        let writer: BoxedUpload = if let Some(cryptmp) = self.crypto_material_provider.as_ref() {
            let (material, attrs) = cryptmp.material_for_write(path.as_ref(), None).await?;
            let writer = BufWriter::with_capacity(
                Arc::clone(&self.store),
                path.clone(),
                part_size
            )
                .with_attributes(attrs)
                .with_max_concurrency(concurrency);
            let encrypter_writer = CrypterWriter::new(writer, Mode::Encrypt, &material)
                .map_err(ErrorKind::ContentEncrypt)?;
            Box::new(encrypter_writer)
        } else {
            Box::new(
                BufWriter::with_capacity(
                    Arc::clone(&self.store),
                    path.clone(),
                    part_size
                )
                .with_max_concurrency(concurrency)
            )
        };

        let encoded = CompressedWriter::new(compression, writer);
        return Ok(Box::new(encoded));
    }
    pub(crate) async fn put_stream(&self, path: &Path, compression: Compression) -> crate::Result<BoxedUpload> {
        with_retries!(self, self.put_stream_impl(path, compression).await)
    }
    async fn get_stream_impl(&self, path: &Path, _size_hint: usize, compression: Compression) -> crate::Result<BoxedReader> {
        let path = &self.full_path(path);
        // Perform head request and prefetch parts in parallel
        let result = self.store.get_opts(
            &path,
            object_store::GetOptions {
                head: true,
                ..Default::default()
            }
        ).await?;
        let part_ranges = size_to_ranges(result.meta.size, self.config.multipart_get_part_size);

        let state = (
            self.clone(),
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
                                return Ok::<_, crate::error::Error>(bytes)
                            },
                            Err(e) => {
                                match retry_state.should_retry(e) {
                                    Ok((e, info, duration)) => {
                                        tracing::info!("retrying get stream error (reason: {:?}) after {:?}: {}", info.reason, duration, e);
                                        tokio::time::sleep(duration).await;
                                        continue 'retry;
                                    }
                                    Err(e) => {
                                        tracing::warn!("[get stream] {}", e);
                                        return Err(e);
                                    }
                                }
                            }
                        }
                    }
                }).await?;
            })
            .buffered(self.config.multipart_get_concurrency)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
            .boxed();

        let reader: Box<dyn AsyncBufRead + Send + Unpin> = if let Some(cryptmp) = self.crypto_material_provider.as_ref() {
            let material = cryptmp.material_from_metadata(path.as_ref(), &result.attributes).await?;
            let decrypter_reader = CrypterReader::new(StreamReader::new(stream), Mode::Decrypt, &material)
                .map_err(ErrorKind::ContentDecrypt)?;
            let buffer_reader = BufReader::with_capacity(64 * 1024, decrypter_reader);
            Box::new(buffer_reader)
        } else {
            Box::new(StreamReader::new(stream))
        };

        let decoded = with_decoder(compression, reader);
        return Ok(Box::new(decoded));
    }
    pub(crate) async fn get_stream(&self, path: &Path, size_hint: usize, compression: Compression) -> crate::Result<BoxedReader> {
        with_retries!(self, self.get_stream_impl(path, size_hint, compression).await)
    }
    pub(crate) async fn download_impl(&self, remote_path: &Path, local_path: &OsPath, compression: Compression) -> crate::Result<usize> {
        let mut writer = tokio::fs::File::create(local_path).await
            .context("failed to open file to download").to_err()?;
        let mut reader = self.get_stream_impl(remote_path, 0, compression).await?;
        let bytes = tokio::io::copy(&mut reader, &mut writer).await
            .context("failed to stream bytes from storage to file").to_err()?;
        writer.shutdown().await
            .context("failed to complete download").to_err()?;
        Ok(bytes as usize)
    }
    pub async fn download(&self, remote_path: &Path, local_path: &OsPath, compression: Compression) -> crate::Result<usize> {
        with_retries!(self, self.download_impl(remote_path, local_path, compression).await)
    }
    pub(crate) async fn download_prefix_impl(&self, remote_prefix: &Path, local_directory: &OsPath, compression: Compression) -> crate::Result<usize> {
        let values: Vec<_> = self.list_stream_impl(remote_prefix, None).await?
            .flat_map(|c| futures_util::stream::iter(c))
            .map(|result| {
                let client = self.clone();
                let local_directory = local_directory.to_path_buf();
                async move {
                    return tokio::spawn(async move {
                        let meta = result?;
                        let filename_str = meta.location.filename()
                            .ok_or_else(|| Error::invalid_response("Unable to determine the filename for remote path"))?;
                        let local_path = local_directory.join(filename_str);
                        client.download_impl(&meta.location, &local_path, compression).await?;
                        Ok::<_, crate::error::Error>(())
                    }).await?;
                }
            })
            .buffer_unordered(32)
            .try_collect().await?;
        Ok(values.len())
    }
    pub async fn download_prefix(&self, remote_prefix: &Path, local_directory: &OsPath, compression: Compression) -> crate::Result<usize> {
        with_retries!(self, self.download_prefix_impl(remote_prefix, local_directory, compression).await)
    }
    pub(crate) async fn upload_impl(&self, local_path: &OsPath, remote_path: &Path, compression: Compression) -> crate::Result<usize> {
        let mut reader = tokio::fs::File::open(local_path).await
            .context("failed to open file to upload").to_err()?;
        let mut writer = self.put_stream_impl(remote_path, compression).await?;
        let bytes = tokio::io::copy(&mut reader, &mut writer).await
            .context("failed to stream bytes from file to storage").to_err()?;
        writer.shutdown().await
            .context("failed to complete upload").to_err()?;
        Ok(bytes as usize)
    }
    pub async fn upload(&self, local_path: &OsPath, remote_path: &Path, compression: Compression) -> crate::Result<usize> {
        with_retries!(self, self.upload_impl(local_path, remote_path, compression).await)
    }
    pub(crate) async fn upload_directory_impl(&self, local_directory: &OsPath, remote_prefix: &Path, compression: Compression) -> crate::Result<usize> {
        let mut uploads = vec![];
        for entry in WalkDir::new(local_directory) {
            let entry = entry
                .context("failed to list directory")
                .to_err()?;
            let path = entry.path();
            if path.is_file() {
                let relative_path = path
                        .strip_prefix(local_directory)
                        .context("failed to get relative path for file")
                        .to_err()?;
                let remote_path = OsPath::new(remote_prefix.as_ref()).join(relative_path);
                let remote_path = Path::from(remote_path.to_str().context("invalid utf8").to_err()?);
                let local_path = path;
                uploads.push((local_path.to_path_buf(), remote_path, compression));
            }
        }

        let count = uploads.len();

        futures_util::stream::iter(uploads)
            .map(|(local_path, remote_path, compression)| {
                let client = self.clone();
                async move {
                    return tokio::spawn(async move {
                        client.upload_impl(&local_path, &remote_path, compression).await?;
                        Ok::<_, crate::error::Error>(())
                    }).await?;
                }
            })
            .buffer_unordered(32)
            .try_collect().await?;

        Ok(count)
    }
    pub async fn upload_directory(&self, local_directory: &OsPath, remote_prefix: &Path, compression: Compression) -> crate::Result<usize> {
        with_retries!(self, self.upload_directory_impl(local_directory, remote_prefix, compression).await)
    }
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
    reader: BufReader<BoxedReader>
}

impl From<BoxedReader> for Box<ReadStream> {
    fn from(value: BoxedReader) -> Self {
        return Box::new(ReadStream { reader: BufReader::with_capacity(64 * 1024, value) });
    }
}

#[no_mangle]
pub extern "C" fn destroy_read_stream(
    stream: *mut ReadStream
) -> CResult {
    destroy_with_runtime!({
        let boxed = unsafe { Box::from_raw(stream) };
        drop(boxed);
    })
}

#[repr(C)]
pub struct GetStreamResponse {
    result: CResult,
    stream: *mut ReadStream,
    // TODO get rid of this field (deprecated)
    object_size: u64,
    error_message: *mut c_char,
    context: *const Context
}

unsafe impl Send for GetStreamResponse {}

impl RawResponse for GetStreamResponse {
    type Payload = Box<ReadStream>;
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
                self.object_size = 0;
            }
            None => {
                self.stream = std::ptr::null_mut();
                self.object_size = 0;
            }
        }
    }
}

export_queued_op!(
    get_stream,
    GetStreamResponse,
    |config, response| {
        let path = unsafe { std::ffi::CStr::from_ptr(path) };
        let path = unsafe{ cstr_to_path(path) };
        let decompress = match Compression::try_from(decompress) {
            Ok(d) => d,
            Err(e) => return Err((response, e))
        };
        Ok(Request::GetStream(path, size_hint, decompress, config, response))
    },
    path: *const c_char, size_hint: usize, decompress: *const c_char
);

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
                        let len = slice.len();
                        let n = wrapper.reader.read_buf(&mut slice).await
                            .context("failed to read from stream")
                            .to_err()?;

                        if n == 0 {
                            if len == 0 {
                                // cannot determine if it is eof
                                return Ok((bytes_read, false))
                            } else {
                                return Ok((bytes_read, true))
                            }
                        } else {
                            bytes_read += n;
                        }
                    }

                    Ok::<_, Error>((bytes_read, false))
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
                    }).context("failed to fill stream buffer").to_err()?;
                    Ok::<_, Error>((bytes_read, eof))
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
    writer: BoxedUpload,
    aborted: bool
}

impl From<BoxedUpload> for Box<WriteStream> {
    fn from(value: BoxedUpload) -> Self {
        return Box::new(WriteStream { writer: value, aborted: false });
    }
}

#[no_mangle]
pub extern "C" fn destroy_write_stream(
    writer: *mut WriteStream
) -> CResult {
    destroy_with_runtime!({
        let boxed = unsafe { Box::from_raw(writer) };
        drop(boxed);
    })
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
export_queued_op!(
    put_stream,
    PutStreamResponse,
    |config, response| {
        let path = unsafe { std::ffi::CStr::from_ptr(path) };
        let path = unsafe{ cstr_to_path(path) };
        let compress = match Compression::try_from(compress) {
            Ok(c) => c,
            Err(e) => return Err((response, e))
        };
        Ok(Request::PutStream(path, compress, config, response))
    },
    path: *const c_char, compress: *const c_char
);

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
                            bytes_written += wrapper.writer.write_buf(&mut slice).await
                                .context("failed to write to stream").to_err()?;
                        }
                        if flush {
                            wrapper.writer.flush().await
                                .context("failed to flush stream buffer").to_err()?;
                        }
                        Ok::<_, Error>(bytes_written)
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
                    wrapper.writer.shutdown().await
                        .context("failed to shutdown stream")
                        .to_err()?;
                    Ok::<_, Error>(0usize)
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
