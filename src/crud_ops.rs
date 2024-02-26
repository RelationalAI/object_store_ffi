use crate::{CResult, Config, NotifyGuard, SQ, clients, dyn_connect, static_config, Request};

use object_store::{path::Path, ObjectStore};

use anyhow::anyhow;
use std::ffi::{c_char, c_void, CString};
use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;

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
    pub(crate) fn success(self, length: usize) {
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

pub(crate) async fn handle_delete(path: &Path, config: &Config) -> anyhow::Result<()> {
    let (client, _) = clients()
        .try_get_with(config.get_hash(), dyn_connect(config)).await
        .map_err(|e| anyhow!(e))?;

    client.delete(path).await?;

    Ok(())
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
    let path: Path = match Path::parse(path.to_str().expect("invalid utf8")) {
        Ok(p) => p,
        Err(e) => {
            response.into_error(e);
            return CResult::Error;
        }
    };
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
            std::mem::forget(response);
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
    let path: Path = match Path::parse(path.to_str().expect("invalid utf8")) {
        Ok(p) => p,
        Err(e) => {
            response.into_error(e);
            return CResult::Error;
        }
    };
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
            std::mem::forget(response);
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
    let path: Path = match Path::parse(path.to_str().expect("invalid utf8")) {
        Ok(p) => p,
        Err(e) => {
            response.into_error(e);
            return CResult::Error;
        }
    };
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
            std::mem::forget(response);
            return CResult::Error;
        }
    }
}
