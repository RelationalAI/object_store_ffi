use crate::{encryption::{encrypt, CrypterReader, CrypterWriter, Mode}, error::RetryState, export_queued_op, static_config, util::cstr_to_path, with_retries, BoxedReader, BoxedUpload, CResult, Client, Context, NotifyGuard, RawConfig, RawResponse, Request, ResponseGuard, SQ};

use bytes::Bytes;
use object_store::{path::Path, ObjectStore};

use anyhow::anyhow;
use tokio_util::io::StreamReader;
use std::{ffi::{c_char, c_void}, sync::Arc};
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

async fn read_to_slice(reader: &mut BoxedReader, mut slice: &mut [u8]) -> anyhow::Result<usize> {
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

impl Client {
    async fn get_impl(&self, path: &Path, slice: &mut [u8]) -> anyhow::Result<usize> {
        let path = &self.full_path(path);

        // Multipart Get
        if slice.len() > static_config().multipart_get_threshold as usize {
            return self.multipart_get_impl(path, slice).await
        }

        // Single part Get
        let result = self.store.get(path).await?;
        let attributes = result.attributes.clone();

        let mut reader: Box<dyn AsyncRead + Send + Unpin> = Box::new(StreamReader::new(result.into_stream()));

        if let Some(cryptmp) = self.crypto_material_provider.as_ref() {
            let material = cryptmp.material_from_metadata(path.as_ref(), &attributes).await?;
            let decrypter_reader = CrypterReader::new(reader, Mode::Decrypt, &material)?;
            reader = Box::new(decrypter_reader);
        }

        Ok(read_to_slice(&mut reader, slice).await?)
    }
    pub async fn get(&self, path: &Path, slice: &mut [u8]) -> anyhow::Result<usize> {
        with_retries!(self, self.get_impl(path, slice).await)
    }
    async fn put_impl(&self, path: &Path, slice: Bytes) -> anyhow::Result<usize> {
        let path = &self.full_path(path);
        let len = slice.len();
        if len < static_config().multipart_put_threshold as usize {
            if let Some(cryptmp) = self.crypto_material_provider.as_ref() {
                let (material, attrs) = cryptmp.material_for_write(path.as_ref(), Some(slice.len())).await?;
                let ciphertext = if slice.is_empty() {
                    // Do not write any padding if there was no data
                    vec![]
                } else {
                    encrypt(&slice, &material)?
                };
                let _ = self.store.put_opts(
                    path,
                    ciphertext.into(),
                    attrs.into()
                ).await?;
            } else {
                let _ = self.store.put(path, slice.into()).await?;
            }
        } else {
            return self.multipart_put_impl(path, &slice).await;
        }

        Ok(len)
    }
    pub async fn put(&self, path: &Path, slice: Bytes) -> anyhow::Result<usize> {
        with_retries!(self, self.put_impl(path, slice.clone()).await)
    }
    async fn delete_impl(&self, path: &Path) -> anyhow::Result<usize> {
        let path = &self.full_path(path);
        self.store.delete(path).await?;
        Ok(0)
    }
    pub async fn delete(&self, path: &Path) -> anyhow::Result<usize> {
        with_retries!(self, self.delete_impl(path).await)
    }
    async fn multipart_get_impl(&self, path: &Path, slice: &mut [u8]) -> anyhow::Result<usize> {
        let result = self.store.get_opts(
            &path,
            object_store::GetOptions {
                head: true,
                ..Default::default()
            }
        ).await?;

        let part_ranges = crate::util::size_to_ranges(result.meta.size);
        let result_vec = self.store.get_ranges(&path, &part_ranges).await?;
        let mut reader: BoxedReader = Box::new(StreamReader::new(stream::iter(result_vec).map(|b| Ok::<_, std::io::Error>(b))));

        if let Some(cryptmp) = self.crypto_material_provider.as_ref() {
            let material = cryptmp.material_from_metadata(path.as_ref(), &result.attributes).await?;
            let decrypter_reader = CrypterReader::new(reader, Mode::Decrypt, &material)?;
            reader = Box::new(decrypter_reader);
        }

        Ok(read_to_slice(&mut reader, slice).await?)
    }
    pub async fn multipart_get(&self, path: &Path, slice: &mut [u8]) -> anyhow::Result<usize> {
        with_retries!(self, self.multipart_get_impl(path, slice).await)
    }
    async fn multipart_put_impl(&self, path: &Path, slice: &[u8]) -> anyhow::Result<usize> {
        let mut writer: BoxedUpload = if let Some(cryptmp) = self.crypto_material_provider.as_ref() {
            let (material, attrs) = cryptmp.material_for_write(path.as_ref(), Some(slice.len())).await?;
            let writer = object_store::buffered::BufWriter::with_capacity(
                Arc::clone(&self.store),
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
                    Arc::clone(&self.store),
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
                        return Ok(slice.len());
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
    pub async fn multipart_put(&self, path: &Path, slice: &[u8]) -> anyhow::Result<usize> {
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
