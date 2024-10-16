use crate::{destroy_with_runtime, duration_on_drop, export_queued_op, metrics, util::{cstr_to_path, string_to_path}, with_cancellation, with_retries, CResult, Client, Context, NotifyGuard, RawConfig, RawResponse, Request, ResponseGuard, RT, SQ};

use object_store::{path::Path, ObjectStore, ObjectMeta};
use pin_project::{pin_project, pinned_drop};

use std::ffi::{c_char, c_void, CString};
use futures_util::{stream::BoxStream, Stream, StreamExt};
use std::sync::Arc;

#[pin_project(PinnedDrop)]
pub struct ListStream {
    store_ptr: Option<*const dyn ObjectStore>,
    #[pin]
    stream: BoxStream<'static, Vec<Result<object_store::ObjectMeta, object_store::Error>>>
}

impl ListStream {
    fn new(client: &Client, prefix: &Path, offset: Option<&Path>) -> ListStream {
        let base_prefix = client.config.prefix.clone();
        let store_ptr = Arc::into_raw(client.store.clone());
        // Safety: we coerce this to 'static to generate a static BoxStream from it.
        // We ensure the store will outlive the stream by manually dropping ListStream.
        let store: &'static dyn ObjectStore = unsafe { &*store_ptr };
        let stream = match (base_prefix, offset) {
            (None, None) => {
                store.list(Some(&prefix)).chunks(1000).boxed()
            }
            (None, Some(offset)) => {
                store.list_with_offset(Some(&prefix), offset).chunks(1000).boxed()
            }
            (Some(base), Some(offset)) => {
                store.list_with_offset(Some(&prefix), offset)
                    // Strip internal prefixes
                    .scan(base, |base, meta| {
                        let meta = meta.map(|mut meta| {
                            if let Some(str) = meta.location.as_ref().strip_prefix(&*base) {
                                meta.location = unsafe { string_to_path(str.to_string()) };
                                meta
                            } else {
                                meta
                            }
                        });
                        async { Some(meta) }
                    })
                    .chunks(1000)
                    .boxed()
            }
            (Some(base), None) => {
                store.list(Some(&prefix))
                    // Strip internal prefixes
                    .scan(base, |base, meta| {
                        let meta = meta.map(|mut meta| {
                            if let Some(str) = meta.location.as_ref().strip_prefix(&*base) {
                                meta.location = unsafe { string_to_path(str.to_string()) };
                                meta
                            } else {
                                meta
                            }
                        });
                        async { Some(meta) }
                    })
                    .chunks(1000)
                    .boxed()

            }
        };

        ListStream {
            store_ptr: Some(store_ptr),
            stream
        }
    }
}

#[pinned_drop]
impl PinnedDrop for ListStream {
    fn drop(mut self: std::pin::Pin<&mut Self>) {
        let ptr = self.store_ptr.take().expect("cannot drop twice");
        let arc = unsafe { Arc::from_raw(ptr) };
        let dummy_stream = Box::pin(futures_util::stream::empty::<Vec<Result<object_store::ObjectMeta, object_store::Error>>>());
        let stream = std::mem::replace(&mut self.stream, dummy_stream);
        // Safety: Must drop the stream before the arc here
        drop(stream);
        drop(arc);
    }
}

unsafe impl Send for ListStream {}

impl Stream for ListStream {
    type Item = Vec<Result<object_store::ObjectMeta, object_store::Error>>;
    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }
}

impl Client {
    async fn list_impl(&self, prefix: &Path, offset: Option<&Path>) -> crate::Result<Vec<ObjectMeta>> {
        let _guard = duration_on_drop!(metrics::list_attempt_duration);
        let stream = self.list_stream_impl(prefix, offset).await?;
        let entries: Vec<_> = stream.collect().await;
        let entries = entries.into_iter().flatten().collect::<Result<Vec<_>, _>>()?;
        Ok(entries)
    }
    pub(crate) async fn list(&self, path: &Path, offset: Option<&Path>) -> crate::Result<Vec<ObjectMeta>> {
        with_retries!(self, self.list_impl(path, offset).await)
    }
    pub(crate) async fn list_stream_impl(&self, prefix: &Path, offset: Option<&Path>) -> crate::Result<ListStream> {
        let prefix = &self.full_path(prefix);
        let offset = offset.map(|o| self.full_path(o));

        Ok(ListStream::new(&self, prefix, offset.as_ref()))
    }
    pub(crate) async fn list_stream(&self, path: &Path, offset: Option<&Path>) -> crate::Result<ListStream> {
        with_retries!(self, self.list_stream_impl(path, offset).await)
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

export_queued_op!(
    list,
    ListResponse,
    |config, response| {
        let prefix = unsafe { std::ffi::CStr::from_ptr(prefix) };
        let prefix = unsafe{ cstr_to_path(prefix) };
        let offset = if offset.is_null() {
            None
        } else {
            Some(unsafe { cstr_to_path(std::ffi::CStr::from_ptr(offset)) })
        };
        Ok(Request::List(prefix, offset, config, response))
    },
    prefix: *const c_char, offset: *const c_char
);

#[no_mangle]
pub extern "C" fn destroy_list_stream(
    stream: *mut ListStream
) -> CResult {
    destroy_with_runtime!({
        let boxed = unsafe { Box::from_raw(stream) };
        drop(boxed);
    })
}

#[repr(C)]
pub struct ListStreamResponse {
    result: CResult,
    stream: *mut ListStream,
    error_message: *mut c_char,
    context: *const Context
}

unsafe impl Send for ListStreamResponse {}

impl RawResponse for ListStreamResponse {
    type Payload = ListStream;
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
                self.stream = Box::into_raw(Box::new(stream));
            }
            None => {
                self.stream = std::ptr::null_mut();
            }
        }
    }
}

export_queued_op!(
    list_stream,
    ListStreamResponse,
    |config, response| {
        let prefix = unsafe { std::ffi::CStr::from_ptr(prefix) };
        let prefix = unsafe{ cstr_to_path(prefix) };
        let offset = if offset.is_null() {
            None
        } else {
            Some(unsafe { cstr_to_path(std::ffi::CStr::from_ptr(offset)) })
        };
        Ok(Request::ListStream(prefix, offset, config, response))
    },
    prefix: *const c_char, offset: *const c_char
);

#[no_mangle]
pub extern "C" fn next_list_stream_chunk(
    stream: *mut ListStream,
    response: *mut ListResponse,
    handle: *const c_void
) -> CResult {
    let response = unsafe { ResponseGuard::new(response, handle) };
    let stream = match unsafe { stream.as_mut() } {
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
                    let option = match stream.next().await {
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
