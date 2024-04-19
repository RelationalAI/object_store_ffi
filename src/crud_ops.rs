use crate::{CResult, Config, NotifyGuard, SQ, clients, dyn_connect, static_config, Request, util::cstr_to_path, Context, RawResponse, ResponseGuard};

use object_store::{path::Path, ObjectStore};

use anyhow::anyhow;
use std::ffi::{c_char, c_void};
use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;

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

async fn multipart_get(slice: &mut [u8], path: &Path, client: &dyn ObjectStore) -> anyhow::Result<usize> {
    let result = client.head(&path).await?;
    if result.size > slice.len() {
        return Err(anyhow!("Supplied buffer was too small"));
    }

    let part_ranges = crate::util::size_to_ranges(result.size);

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

pub(crate) async fn handle_get(slice: &mut [u8], path: &Path, config: &Config) -> anyhow::Result<usize> {
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
    let mut batch_stream = body.into_stream().chunks(8);

    let mut received_bytes = 0;
    while let Some(batch) = batch_stream.next().await {
        for result in batch {
            let chunk = match result {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("Error while fetching a chunk: {}", e);
                    return Err(e.into());
                }
            };

            let len = chunk.len();

            if received_bytes + len > slice.len() {
                return Err(anyhow!("Supplied buffer was too small"));
            }

            slice[received_bytes..(received_bytes + len)].copy_from_slice(&chunk);
            received_bytes += len;
        }
    }

    Ok(received_bytes)
}

pub(crate) async fn handle_put(slice: &'static [u8], path: &Path, config: &Config) -> anyhow::Result<usize> {
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

pub(crate) async fn handle_delete(path: &Path, config: &Config) -> anyhow::Result<usize> {
    let (client, _) = clients()
        .try_get_with(config.get_hash(), dyn_connect(config)).await
        .map_err(|e| anyhow!(e))?;

    client.delete(path).await?;

    Ok(0)
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
    let path = unsafe{ cstr_to_path(path) };
    let slice = unsafe { std::slice::from_raw_parts_mut(buffer, size) };
    let config = unsafe { & (*config) };
    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::Get(path, slice, config, response)) {
                Ok(_) => CResult::Ok,
                Err(async_channel::TrySendError::Full(Request::Get(_, _, _, response))) => {
                    response.into_error("object_store_ffi internal channel full, backoff");
                    CResult::Backoff
                }
                Err(async_channel::TrySendError::Closed(Request::Get(_, _, _, response))) => {
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
    config: *const Config,
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
                Err(async_channel::TrySendError::Full(Request::Put(_, _, _, response))) => {
                    response.into_error("object_store_ffi internal channel full, backoff");
                    CResult::Backoff
                }
                Err(async_channel::TrySendError::Closed(Request::Put(_, _, _, response))) => {
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
    config: *const Config,
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
                Err(async_channel::TrySendError::Full(Request::Delete(_, _, response))) => {
                    response.into_error("object_store_ffi internal channel full, backoff");
                    CResult::Backoff
                }
                Err(async_channel::TrySendError::Closed(Request::Delete(_, _, response))) => {
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
