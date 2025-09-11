use anyhow::{anyhow, Context as AnyhowContext};
use futures_util::StreamExt;

use crate::{clients, error::ErrorExt, export_runtime_op, CResult, Client, Context, Error, RawConfig, RawResponse, Response};
use std::ffi::{c_char, c_void};

export_runtime_op!(
    test_query,
    Response,
    || {
        let query = unsafe { std::ffi::CStr::from_ptr(query) }.to_str()?.to_string();
        let slice = unsafe { std::slice::from_raw_parts_mut(buffer, size) };
        let config = unsafe { & (*config) };
        Ok((query, slice, config))
    },
    state,
    async {
        let (query, slice, config) = state;
        let client = clients().try_get_with(config.get_hash(), Client::from_raw_config(config)).await?;
        let n = client.extension.test_query(&query, slice).await?;
        Ok::<_, Error>(n)
    },
    query: *const c_char, buffer: *mut u8, size: usize, config: *const RawConfig
);

pub struct QueryStream {
    rx: flume::Receiver<crate::Result<Vec<u8>>>,
}

#[repr(C)]
pub struct QueryStreamResponse {
    result: CResult,
    stream: *mut QueryStream,
    error_message: *mut c_char,
    context: *const Context
}

unsafe impl Send for QueryStreamResponse {}

impl RawResponse for QueryStreamResponse {
    type Payload = Box<QueryStream>;
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

export_runtime_op!(
    test_query_stream,
    QueryStreamResponse,
    || {
        let query = unsafe { std::ffi::CStr::from_ptr(query) }.to_str()?.to_string();
        let config = unsafe { & (*config) };
        Ok((query, config))
    },
    state,
    async {
        let (query, config) = state;
        let client = clients().try_get_with(config.get_hash(), Client::from_raw_config(config)).await?;
        let mut stream = client.extension.test_query_stream(&query).await?;
        let (tx, rx) = flume::bounded::<crate::Result<Vec<u8>>>(64);

        // TODO: store task
        tokio::spawn(async move {
            while let Some(result) = stream.next().await {
                match tx.send_async(result).await {
                    Err(_) => break,
                    _ => {}
                }
            }
        });

        let qs = QueryStream { rx };

        Ok::<_, Error>(qs)
    },
    query: *const c_char, config: *const RawConfig
);

#[repr(C)]
pub struct NextChunkResponse {
    result: CResult,
    buffer: *mut u8,
    size: usize,
    error_message: *mut c_char,
    context: *const Context
}

unsafe impl Send for NextChunkResponse {}

impl RawResponse for NextChunkResponse {
    type Payload = Vec<u8>;
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
            Some(mut vec) => {
                self.buffer = vec.as_mut_ptr();
                self.size = vec.len();
                std::mem::forget(vec);
            }
            None => {
                self.buffer = std::ptr::null_mut();
                self.size = 0;
            }
        }
    }
}

export_runtime_op!(
    next_chunk,
    NextChunkResponse,
    || {
        if query_stream.is_null() {
            return Err(anyhow!("query stream ptr is null"));
        }
        let query_stream = unsafe { &*query_stream };
        Ok(query_stream)
    },
    qs,
    async {
        match qs.rx.recv_async().await {
            Ok(result) => {
                result
            }
            Err(_) => {
                // Disconnected, assume the stream is over (not great)
                Ok::<_, Error>(vec![])
            }
        }
    },
    query_stream: *mut QueryStream
);
