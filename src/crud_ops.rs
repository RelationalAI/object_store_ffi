use crate::{encryption::{encrypt, CrypterReader, CrypterWriter, Mode}, static_config, util::cstr_to_path, BoxedReader, BoxedUpload, CResult, Client, Context, NotifyGuard, RawConfig, RawResponse, Request, ResponseGuard, SQ};

use object_store::{path::Path, ObjectStore};

use anyhow::anyhow;
use tokio_util::io::StreamReader;
use std::ffi::{c_char, c_void};
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

async fn read_to_slice(reader: &mut Box<dyn AsyncRead + Send + Unpin>, mut slice: &mut [u8]) -> anyhow::Result<usize> {
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
                        return Err(anyhow!("Supplied buffer was too small"));
                    }
                } else {
                    // done
                    break;
                }
            }
            Ok(n) => received_bytes += n,
            Err(e) => {
                let err = anyhow::Error::new(e);
                tracing::warn!("Error while reading body: {:#}", err);
                return Err(err);
            }
        }
    }

    Ok(received_bytes)
}

async fn multipart_get(slice: &mut [u8], path: &Path, client: &Client) -> anyhow::Result<usize> {
    let result = client.store.get_opts(
        &path,
        object_store::GetOptions {
            head: true,
            ..Default::default()
        }
    ).await?;

    let part_ranges = crate::util::size_to_ranges(result.meta.size);
    let result_vec = client.store.get_ranges(&path, &part_ranges).await?;
    let mut reader: BoxedReader = Box::new(StreamReader::new(stream::iter(result_vec).map(|b| Ok::<_, std::io::Error>(b))));

    if let Some(cryptmp) = client.crypto_material_provider.as_ref() {
        let material = cryptmp.material_from_metadata(path.as_ref(), &result.attributes).await?;
        let decrypter_reader = CrypterReader::new(reader, Mode::Decrypt, &material)?;
        reader = Box::new(decrypter_reader);
    }

    Ok(read_to_slice(&mut reader, slice).await?)
}

async fn multipart_put(slice: &[u8], path: &Path, client: Client) -> anyhow::Result<()> {
    let mut writer: BoxedUpload = if let Some(cryptmp) = client.crypto_material_provider.as_ref() {
        let (material, attrs) = cryptmp.material_for_write(path.as_ref(), Some(slice.len())).await?;
        let writer = object_store::buffered::BufWriter::with_capacity(
            client.store,
            path.clone(),
            10 * 1024 * 1024
        )
            .with_attributes(attrs)
            .with_max_concurrency(64);
        let encrypter_writer = CrypterWriter::new(writer, Mode::Encrypt, &material)?;
        Box::new(encrypter_writer)
    } else {
        Box::new(
            object_store::buffered::BufWriter::with_capacity(
                client.store,
                path.clone(),
                10 * 1024 * 1024
            )
            .with_max_concurrency(64)
        )
    };

    match writer.write_all(slice).await {
        Ok(_) => {
            match writer.flush().await {
                Ok(_) => {
                    writer.shutdown().await?;
                    return Ok(());
                }
                Err(e) => {
                    writer.abort().await?;
                    return Err(e.into());
                }
            }
        }
        Err(e) => {
            writer.abort().await?;
            return Err(e.into());
        }
    };
}

pub(crate) async fn handle_get(client: Client, slice: &mut [u8], path: &Path) -> anyhow::Result<usize> {
    let path = &client.full_path(path);

    // Multipart Get
    if slice.len() > static_config().multipart_get_threshold as usize {
        let accum = multipart_get(slice, path, &client).await?;
        return Ok(accum);
    }

    // Single part Get
    let result = client.store.get(path).await?;
    let attributes = result.attributes.clone();

    let mut reader: Box<dyn AsyncRead + Send + Unpin> = Box::new(StreamReader::new(result.into_stream()));

    if let Some(cryptmp) = client.crypto_material_provider.as_ref() {
        let material = cryptmp.material_from_metadata(path.as_ref(), &attributes).await?;
        let decrypter_reader = CrypterReader::new(reader, Mode::Decrypt, &material)?;
        reader = Box::new(decrypter_reader);
    }

    Ok(read_to_slice(&mut reader, slice).await?)
}

pub(crate) async fn handle_put(client: Client, slice: &'static [u8], path: &Path) -> anyhow::Result<usize> {
    let path = &client.full_path(path);
    let len = slice.len();
    if len < static_config().multipart_put_threshold as usize {
        if let Some(cryptmp) = client.crypto_material_provider.as_ref() {
            let (material, attrs) = cryptmp.material_for_write(path.as_ref(), Some(slice.len())).await?;
            let ciphertext = encrypt(&slice, &material).unwrap();
            let _ = client.store.put_opts(
                path,
                ciphertext.into(),
                attrs.into()
            ).await?;
        } else {
            let _ = client.store.put(path, slice.into()).await?;
        }
    } else {
        let _ = multipart_put(slice, path, client).await?;
    }

    Ok(len)
}

pub(crate) async fn handle_delete(client: Client, path: &Path) -> anyhow::Result<usize> {
    let path = &client.full_path(path);
    client.store.delete(path).await?;

    Ok(0)
}

#[no_mangle]
pub extern "C" fn get(
    path: *const c_char,
    buffer: *mut u8,
    size: usize,
    config: *const RawConfig,
    response: *mut Response,
    handle: *const c_void
) -> CResult {
    let response = unsafe { ResponseGuard::new(response, handle) };
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path = unsafe{ cstr_to_path(path) };
    let slice = unsafe { std::slice::from_raw_parts_mut(buffer, size) };
    let config = unsafe { & (*config) };
    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::Get(path, slice, config, response)) {
                Ok(_) => CResult::Ok,
                Err(flume::TrySendError::Full(Request::Get(_, _, _, response))) => {
                    response.into_error("object_store_ffi internal channel full, backoff");
                    CResult::Backoff
                }
                Err(flume::TrySendError::Disconnected(Request::Get(_, _, _, response))) => {
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
pub extern "C" fn put(
    path: *const c_char,
    buffer: *const u8,
    size: usize,
    config: *const RawConfig,
    response: *mut Response,
    handle: *const c_void
) -> CResult {
    let response = unsafe { ResponseGuard::new(response, handle) };
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path = unsafe{ cstr_to_path(path) };
    let slice = unsafe { std::slice::from_raw_parts(buffer, size) };
    let config = unsafe { & (*config) };
    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::Put(path, slice, config, response)) {
                Ok(_) => CResult::Ok,
                Err(flume::TrySendError::Full(Request::Put(_, _, _, response))) => {
                    response.into_error("object_store_ffi internal channel full, backoff");
                    CResult::Backoff
                }
                Err(flume::TrySendError::Disconnected(Request::Put(_, _, _, response))) => {
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
pub extern "C" fn delete(
    path: *const c_char,
    config: *const RawConfig,
    response: *mut Response,
    handle: *const c_void
) -> CResult {
    let response = unsafe { ResponseGuard::new(response, handle) };
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let path = unsafe{ cstr_to_path(path) };
    let config = unsafe { & (*config) };
    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::Delete(path, config, response)) {
                Ok(_) => CResult::Ok,
                Err(flume::TrySendError::Full(Request::Delete(_, _, response))) => {
                    response.into_error("object_store_ffi internal channel full, backoff");
                    CResult::Backoff
                }
                Err(flume::TrySendError::Disconnected(Request::Delete(_, _, response))) => {
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
