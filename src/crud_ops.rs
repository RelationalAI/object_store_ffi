use crate::{duration_on_drop, encryption::{encrypt, CrypterReader, CrypterWriter, Mode}, error::Kind as ErrorKind, export_queued_op, metrics, util::{cstr_to_path, string_to_path, BufWriter}, with_retries, BoxedReader, BoxedUpload, CResult, Client, Context, NotifyGuard, RawConfig, RawResponse, Request, ResponseGuard, SQ};

use bytes::Bytes;
use ::metrics::counter;
use object_store::{path::Path, ObjectStore};

use tokio_util::io::StreamReader;
use std::{ffi::{c_char, c_void, CString}, sync::Arc};
use futures_util::{stream, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

// The type used to give Julia the result of an async request. It will be allocated
// by Julia as part of the request and filled in by Rust.
#[repr(C)]
pub struct Response {
    result: CResult,
    length: usize,
    error_message: *mut c_char,
    context: *const Context,
}

unsafe impl Send for Response {}

impl RawResponse for Response {
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
            Some(n) => {
                self.length = n;
            }
            None => {
                self.length = 0;
            }
        }
    }
}

// ================================================================================================
// Boiler plate code for FFI structs
// Any non-copy fields of BulkFailedEntry must be properly destroyed on destroy_bulk_failed_entries
#[repr(C)]
pub struct BulkFailedEntry {
    path: *const c_char,
    error_message: *const c_char
}
unsafe impl Send for BulkFailedEntry {}

// Only stores paths of entries that the bulk operation failed on
#[repr(C)]
pub struct BulkResponse {
    result: CResult,
    failed_entries: *const BulkFailedEntry,
    failed_count: u64,
    // Generic error message for the whole operation. Not null implies failed_count = 0
    error_message: *mut c_char,
    context: *const Context
}

unsafe impl Send for BulkResponse {}

impl RawResponse for BulkResponse {
    type Payload = Vec<(Path, crate::Error)>;
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
            Some(entries) => {
                let entries = entries.into_iter().map(|(path, error)| {
                    BulkFailedEntry::new(path, error.to_string())
                }).collect::<Vec<BulkFailedEntry>>();
                let entries_slice = entries.into_boxed_slice();
                let entry_count = entries_slice.len() as u64;
                let entries_ptr = entries_slice.as_ptr();
                std::mem::forget(entries_slice);

                self.failed_count = entry_count;
                self.failed_entries = entries_ptr;
            }
            None => {
                self.failed_entries = std::ptr::null();
                self.failed_count = 0;
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn destroy_bulk_failed_entries(
    entries: *mut BulkFailedEntry,
    entry_count: u64
) -> CResult {
    let boxed_slice = unsafe { Box::from_raw(std::slice::from_raw_parts_mut(entries, entry_count as usize)) };
    for entry in &*boxed_slice {
        // Safety: must properly drop all allocated fields from BulkFailedEntry here
        let _ = unsafe { CString::from_raw(entry.path.cast_mut()) };
        let _ = unsafe { CString::from_raw(entry.error_message.cast_mut()) };
    }
    CResult::Ok
}

impl BulkFailedEntry {
    pub fn new(path: Path, error_msg:String) -> Self {
        BulkFailedEntry {
            path: CString::new(path.to_string())
                .expect("should not have nulls")
                .into_raw(),
            error_message: CString::new(error_msg.to_string())
                .expect("should not have nulls")
                .into_raw()
        }
    }
}
// ================================================================================================

async fn read_to_slice(reader: &mut BoxedReader, mut slice: &mut [u8]) -> crate::Result<usize> {
    let mut received_bytes = 0;
    loop {
        match reader.read_buf(&mut slice).await {
            Ok(0) => {
                if slice.len() == 0 {
                    // TODO is there a better way to check for this?
                    let mut scratch = [0u8; 1];
                    if let Ok(0) = reader.read_buf(&mut scratch.as_mut_slice()).await {
                        // slice was the exact size, done
                        break;
                    } else {
                        return Err(ErrorKind::BufferTooSmall.into());
                    }
                } else {
                    // done
                    break;
                }
            }
            Ok(n) => received_bytes += n,
            Err(e) => {
                let err = ErrorKind::BodyIo(e);
                tracing::warn!("Error while reading body: {}", err);
                return Err(err.into());
            }
        }
    }

    Ok(received_bytes)
}

impl Client {
    async fn get_impl(&self, path: &Path, slice: &mut [u8]) -> crate::Result<usize> {
        let guard = duration_on_drop!(metrics::get_attempt_duration);
        let path = &self.full_path(path);

        // Multipart Get
        if slice.len() > self.config.multipart_get_threshold {
            guard.discard();
            return self.multipart_get_impl(path, slice).await
        }

        // Single part Get
        let result = self.store.get(path).await?;
        let attributes = result.attributes.clone();

        let mut reader: Box<dyn AsyncRead + Send + Unpin> = Box::new(StreamReader::new(result.into_stream()));

        if let Some(cryptmp) = self.crypto_material_provider.as_ref() {
            let material = cryptmp.material_from_metadata(path.as_ref(), &attributes).await?;
            let decrypter_reader = CrypterReader::new(reader, Mode::Decrypt, &material)
                .map_err(ErrorKind::ContentDecrypt)?;
            reader = Box::new(decrypter_reader);
        }

        Ok(read_to_slice(&mut reader, slice).await?)
    }
    pub async fn get(&self, path: &Path, slice: &mut [u8]) -> crate::Result<usize> {
        counter!(metrics::total_get_ops).increment(1);
        with_retries!(self, self.get_impl(path, slice).await)
    }
    async fn put_impl(&self, path: &Path, slice: Bytes) -> crate::Result<usize> {
        let guard = duration_on_drop!(metrics::put_attempt_duration);
        let path = &self.full_path(path);
        let len = slice.len();
        if len < self.config.multipart_put_threshold {
            if let Some(cryptmp) = self.crypto_material_provider.as_ref() {
                let (material, attrs) = cryptmp.material_for_write(path.as_ref(), Some(slice.len())).await?;
                let ciphertext = encrypt(&slice, &material)
                    .map_err(ErrorKind::ContentEncrypt)?;
                let _ = self.store.put_opts(
                    path,
                    ciphertext.into(),
                    attrs.into()
                ).await?;
            } else {
                let _ = self.store.put(path, slice.into()).await?;
            }
        } else {
            guard.discard();
            return self.multipart_put_impl(path, &slice).await;
        }

        Ok(len)
    }
    pub async fn put(&self, path: &Path, slice: Bytes) -> crate::Result<usize> {
        counter!(metrics::total_put_ops).increment(1);
        with_retries!(self, self.put_impl(path, slice.clone()).await)
    }
    async fn delete_impl(&self, path: &Path) -> crate::Result<usize> {
        let _guard = duration_on_drop!(metrics::delete_attempt_duration);
        let path = &self.full_path(path);
        self.store.delete(path).await?;
        Ok(0)
    }
    pub async fn delete(&self, path: &Path) -> crate::Result<usize> {
        counter!(metrics::total_delete_ops).increment(1);
        with_retries!(self, self.delete_impl(path).await)
    }

    async fn bulk_delete_impl(&self, paths: &Vec<Path>) -> crate::Result<Vec<(Path, crate::Error)>> {
        // Add the client prefix to the provided paths if needed
        let prefixed_paths = paths.into_iter().map(|path| self.full_path(path)).collect::<Vec<Path>>();
        let stream = stream::iter(paths.iter().map(|path| Ok(path.clone()))).boxed();
        let results = self.store.delete_stream(stream)
            .collect::<Vec<_>>().await;
        // We count the number of results to raise an error if some paths were not
        // successfully processed at all.
        let num_results = results.len();
        let failures = results
            .into_iter()
            .zip(prefixed_paths.into_iter()) // Stops at the shorter iterator
            .filter_map(|(result, path)| {
                match result {
                    Ok(_) => {
                        None
                    },
                    Err(e) => match e {
                        // We treat not found as success because AWS S3 does not return an error
                        // if the object does not exist
                        object_store::Error::NotFound { .. } => {
                            None
                        },
                        _ => {
                            match self.config.prefix.as_ref() {
                                None => Some((path, e.into())),
                                Some(prefix_str) => {
                                    if let Some(stripped_str) = path.as_ref().strip_prefix(prefix_str) {
                                        let truncated = unsafe { string_to_path(stripped_str.to_string()) };
                                        Some((truncated, e.into()))
                                    } else {
                                        Some((path, e.into()))
                                    }
                                }
                            }
                        }
                    },
                }
            }).collect::<Vec<(Path, crate::Error)>>();

        // Rail guard to catch generic errors
        if num_results < paths.len() {
            if num_results == 0 {
                tracing::warn!("delete_stream returned zero results");
                Err(crate::Error::invalid_response("Some paths were not deleted"))
            } else {
                let error_string = failures[0].1.to_string();
                tracing::warn!("delete_stream returned a single generic error: {}", error_string);
                Err(crate::Error::invalid_response(error_string))
            }
        } else {
            Ok(failures)
        }
    }
    pub async fn bulk_delete(&self, paths: Vec<Path>) -> crate::Result<Vec<(Path, crate::Error)>> {
        counter!(metrics::total_bulk_delete_ops).increment(1);
        with_retries!(self, self.bulk_delete_impl(&paths).await)
    }
    async fn multipart_get_impl(&self, path: &Path, slice: &mut [u8]) -> crate::Result<usize> {
        let _guard = duration_on_drop!(metrics::multipart_get_attempt_duration);
        let result = self.store.get_opts(
            &path,
            object_store::GetOptions {
                head: true,
                ..Default::default()
            }
        ).await?;

        let part_ranges = crate::util::size_to_ranges(result.meta.size, self.config.multipart_get_part_size);
        let result_vec = self.store.get_ranges(&path, &part_ranges).await?;
        let mut reader: BoxedReader = Box::new(StreamReader::new(stream::iter(result_vec).map(|b| Ok::<_, std::io::Error>(b))));

        if let Some(cryptmp) = self.crypto_material_provider.as_ref() {
            let material = cryptmp.material_from_metadata(path.as_ref(), &result.attributes).await?;
            let decrypter_reader = CrypterReader::new(reader, Mode::Decrypt, &material)
                .map_err(ErrorKind::ContentDecrypt)?;
            reader = Box::new(decrypter_reader);
        }

        Ok(read_to_slice(&mut reader, slice).await?)
    }
    pub async fn multipart_get(&self, path: &Path, slice: &mut [u8]) -> crate::Result<usize> {
        with_retries!(self, self.multipart_get_impl(path, slice).await)
    }
    async fn multipart_put_impl(&self, path: &Path, slice: &[u8]) -> crate::Result<usize> {
        let _guard = duration_on_drop!(metrics::multipart_put_attempt_duration);
        let mut writer: BoxedUpload = if let Some(cryptmp) = self.crypto_material_provider.as_ref() {
            let (material, attrs) = cryptmp.material_for_write(path.as_ref(), Some(slice.len())).await?;
            let writer = BufWriter::with_capacity(
                Arc::clone(&self.store),
                path.clone(),
                self.config.multipart_put_part_size
            )
                .with_attributes(attrs)
                .with_max_concurrency(self.config.multipart_put_concurrency);
            let encrypter_writer = CrypterWriter::new(writer, Mode::Encrypt, &material)
                .map_err(ErrorKind::ContentEncrypt)?;
            Box::new(encrypter_writer)
        } else {
            Box::new(
                BufWriter::with_capacity(
                    Arc::clone(&self.store),
                    path.clone(),
                    self.config.multipart_put_part_size
                )
                .with_max_concurrency(self.config.multipart_put_concurrency)
            )
        };

        match writer.write_all(slice).await {
            Ok(_) => {
                match writer.flush().await {
                    Ok(_) => {
                        writer.shutdown().await
                            .map_err(ErrorKind::BodyIo)?;
                        return Ok(slice.len());
                    }
                    Err(e) => {
                        writer.abort().await?;
                        return Err(ErrorKind::BodyIo(e).into());
                    }
                }
            }
            Err(e) => {
                writer.abort().await?;
                return Err(ErrorKind::BodyIo(e).into());
            }
        };
    }
    pub async fn multipart_put(&self, path: &Path, slice: &[u8]) -> crate::Result<usize> {
        with_retries!(self, self.multipart_put_impl(path, slice).await)
    }
}

export_queued_op!(
    get,
    Response,
    |config, response| {
        let path = unsafe { std::ffi::CStr::from_ptr(path) };
        let path = unsafe{ cstr_to_path(path) };
        let slice = unsafe { std::slice::from_raw_parts_mut(buffer, size) };
        Ok(Request::Get(path, slice, config, response))
    },
    path: *const c_char, buffer: *mut u8, size: usize
);

export_queued_op!(
    put,
    Response,
    |config, response| {
        let path = unsafe { std::ffi::CStr::from_ptr(path) };
        let path = unsafe{ cstr_to_path(path) };
        let slice = unsafe { std::slice::from_raw_parts(buffer, size) };
        Ok(Request::Put(path, slice, config, response))
    },
    path: *const c_char, buffer: *const u8, size: usize
);

export_queued_op!(
    delete,
    Response,
    |config, response| {
        let path = unsafe { std::ffi::CStr::from_ptr(path) };
        let path = unsafe{ cstr_to_path(path) };
        Ok(Request::Delete(path, config, response))
    },
    path: *const c_char
);

export_queued_op!(
    bulk_delete,
    BulkResponse,
    |config, response| {
        let slice: &[*const c_char] = unsafe { std::slice::from_raw_parts(path_c_array, num_paths) };
        let paths_vec: Vec<Path> = slice.iter().map(|ptr| {
            unsafe { cstr_to_path(std::ffi::CStr::from_ptr(ptr.clone())) }
        }).collect();
        Ok(Request::BulkDelete(paths_vec, config, response))
    },
    path_c_array: *const *const c_char, num_paths: usize
);
