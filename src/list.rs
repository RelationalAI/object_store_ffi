use crate::{destroy_with_runtime, util::cstr_to_path, with_cancellation, CResult, Client, Context, NotifyGuard, RawConfig, RawResponse, Request, ResponseGuard, RT, SQ};

use object_store::{path::Path, ObjectStore, ObjectMeta};

use std::ffi::{c_char, c_void, CString};
use futures_util::{StreamExt, stream::BoxStream};
use std::sync::Arc;

pub(crate) async fn handle_list(client: Client, prefix: &Path, offset: Option<&Path>) -> anyhow::Result<Vec<ObjectMeta>> {
    let stream = if let Some(offset) = offset {
        client.store.list_with_offset(Some(&prefix), offset)
    } else {
        client.store.list(Some(&prefix))
    };

    let entries: Vec<_> = stream.collect().await;
    let entries = entries.into_iter().collect::<Result<Vec<_>, _>>()?;
    Ok(entries)
}

pub(crate) async fn handle_list_stream(client: Client, prefix: &Path, offset: Option<&Path>) -> anyhow::Result<Box<StreamWrapper>> {
    let mut wrapper = Box::new(StreamWrapper {
        client: client.store,
        stream: None
    });

    let stream = if let Some(offset) = offset {
        wrapper.client.list_with_offset(Some(&prefix), offset).chunks(1000).boxed()
    } else {
        wrapper.client.list(Some(&prefix)).chunks(1000).boxed()
    };
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
    error_message: *mut c_char,
    context: *const Context
}

unsafe impl Send for ListResponse {}

impl RawResponse for ListResponse {
    type Payload = Vec<ObjectMeta>;
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
                let entries_slice = entries.into_iter()
                    .map(Into::into)
                    .collect::<Vec<_>>()
                    .into_boxed_slice();

                let entry_count = entries_slice.len() as u64;
                let entries_ptr = entries_slice.as_ptr();
                std::mem::forget(entries_slice);

                self.entry_count = entry_count;
                self.entries = entries_ptr;
            }
            None => {
                self.entries = std::ptr::null();
                self.entry_count = 0;
            }
        }
    }
}

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
    offset: *const c_char,
    config: *const RawConfig,
    response: *mut ListResponse,
    handle: *const c_void
) -> CResult {
    let response = unsafe { ResponseGuard::new(response, handle) };
    let prefix = unsafe { std::ffi::CStr::from_ptr(prefix) };
    let prefix = unsafe{ cstr_to_path(prefix) };
    let offset = if offset.is_null() {
        None
    } else {
        Some(unsafe { cstr_to_path(std::ffi::CStr::from_ptr(offset)) })
    };
    let config = unsafe { & (*config) };
    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::List(prefix, offset, config, response)) {
                Ok(_) => CResult::Ok,
                Err(flume::TrySendError::Full(Request::List(_, _, _, response))) => {
                    response.into_error("object_store_ffi internal channel full, backoff");
                    CResult::Backoff
                }
                Err(flume::TrySendError::Disconnected(Request::List(_, _, _, response))) => {
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

pub struct StreamWrapper {
    client: Arc<dyn ObjectStore>,
    stream: Option<BoxStream<'static, Vec<Result<object_store::ObjectMeta, object_store::Error>>>>
}

#[no_mangle]
pub extern "C" fn destroy_list_stream(
    stream: *mut StreamWrapper
) -> CResult {
    destroy_with_runtime!({
        let mut boxed = unsafe { Box::from_raw(stream) };
        // Safety: Must drop the stream before the client here
        drop(boxed.stream.take());
        drop(boxed);
    })
}

#[repr(C)]
pub struct ListStreamResponse {
    result: CResult,
    stream: *mut StreamWrapper,
    error_message: *mut c_char,
    context: *const Context
}

unsafe impl Send for ListStreamResponse {}

impl RawResponse for ListStreamResponse {
    type Payload = Box<StreamWrapper>;
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

#[no_mangle]
pub extern "C" fn list_stream(
    prefix: *const c_char,
    offset: *const c_char,
    config: *const RawConfig,
    response: *mut ListStreamResponse,
    handle: *const c_void
) -> CResult {
    let response = unsafe { ResponseGuard::new(response, handle) };
    let prefix = unsafe { std::ffi::CStr::from_ptr(prefix) };
    let prefix = unsafe{ cstr_to_path(prefix) };
    let offset = if offset.is_null() {
        None
    } else {
        Some(unsafe { cstr_to_path(std::ffi::CStr::from_ptr(offset)) })
    };
    let config = unsafe { & (*config) };
    match SQ.get() {
        Some(sq) => {
            match sq.try_send(Request::ListStream(prefix, offset, config, response)) {
                Ok(_) => CResult::Ok,
                Err(flume::TrySendError::Full(Request::ListStream(_, _, _, response))) => {
                    response.into_error("object_store_ffi internal channel full, backoff");
                    CResult::Backoff
                }
                Err(flume::TrySendError::Disconnected(Request::ListStream(_, _, _, response))) => {
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
pub extern "C" fn next_list_stream_chunk(
    stream: *mut StreamWrapper,
    response: *mut ListResponse,
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

                with_cancellation!(list_op, response);
            });
            CResult::Ok
        }
        None => {
            response.into_error("object_store_ffi runtime not started (may be missing initialization)");
            return CResult::Error;
        }
    }
}
