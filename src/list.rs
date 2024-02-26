use crate::{CResult, Config, NotifyGuard, SQ, RT, clients, dyn_connect, Request};

use object_store::{path::Path, ObjectStore, ObjectMeta};

use anyhow::anyhow;
use std::ffi::{c_char, c_void, CString};
use futures_util::{StreamExt, stream::BoxStream};
use std::sync::Arc;

pub(crate) async fn handle_list(prefix: &Path, config: &Config) -> anyhow::Result<Vec<ObjectMeta>> {
    let (client, _) = clients()
        .try_get_with(config.get_hash(), dyn_connect(config))
        .await
        .map_err(|e| anyhow!(e))?;

    let stream = client.list(Some(&prefix));

    let entries: Vec<_> = stream.collect().await;
    let entries = entries.into_iter().collect::<Result<Vec<_>, _>>()?;
    Ok(entries)
}

pub(crate) async fn handle_list_stream(prefix: &Path, config: &Config) -> anyhow::Result<Box<StreamWrapper>> {
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
    pub(crate) fn success(self, entries: Vec<ObjectMeta>) {
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
    let prefix: Path = match Path::parse(prefix.to_str().expect("invalid utf8")) {
        Ok(p) => p,
        Err(e) => {
            response.into_error(e);
            return CResult::Error;
        }
    };
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
            std::mem::forget(response);
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
    pub(crate) fn success(self, stream: Box<StreamWrapper>) {
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
    let prefix: Path = match Path::parse(prefix.to_str().expect("invalid utf8")) {
        Ok(p) => p,
        Err(e) => {
            response.into_error(e);
            return CResult::Error;
        }
    };
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
            std::mem::forget(response);
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
            std::mem::forget(response);
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
            std::mem::forget(response);
            return CResult::Error;
        }
    }
}
