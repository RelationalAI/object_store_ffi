use std::sync::Arc;
use std::task::{ready, Poll};
use std::{ops::Range, str::FromStr};
use std::ffi::c_char;
use anyhow::anyhow;
use futures_util::{FutureExt, future::BoxFuture};
use object_store::path::Path;
use object_store::{Attribute, AttributeValue, Attributes, GetOptions, ObjectStore, TagSet};
use pin_project::pin_project;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite, AsyncWriteExt};
use crate::error::Error;
use crate::error::Kind as ErrorKind;
use std::error::Error as StdError;

pub(crate) fn size_to_ranges(object_size: usize, part_size: usize) -> Vec<Range<usize>> {
    if object_size == 0 {
        return vec![];
    }

    // If the object size happens to be smaller than part_size,
    // then we will end up doing a single range get of the whole
    // object.
    let mut parts = object_size / part_size;
    if object_size % part_size != 0 {
        parts += 1;
    }
    let mut part_ranges = Vec::with_capacity(parts);
    for i in 0..(parts-1) {
        part_ranges.push((i*part_size)..((i+1)*part_size));
    }
    // Last part which handles sizes not divisible by part_size
    part_ranges.push(((parts-1)*part_size)..object_size);

    return part_ranges;
}

#[derive(Debug, Copy, Clone)]
pub enum Compression {
    None,
    Gzip,
    Deflate,
    Zlib,
    Zstd
}

impl TryFrom<*const c_char> for Compression {
    type Error = anyhow::Error;
    fn try_from(value: *const c_char) -> Result<Self, Self::Error> {
        if value.is_null() {
            Ok(Compression::None)
        } else {
            let codec_str = unsafe { std::ffi::CStr::from_ptr(value) }.to_str().expect("invalid utf8");
            Compression::from_str(codec_str)
        }
    }
}

impl FromStr for Compression {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "" => Ok(Compression::None),
            "gzip" => Ok(Compression::Gzip),
            "deflate" => Ok(Compression::Deflate),
            "zlib" => Ok(Compression::Zlib),
            "zstd" => Ok(Compression::Zstd),
            c => {
                Err(anyhow!("compression codec {} not implemented", c))
            }
        }
    }
}

fn is_invalid_block_list_error(e: &std::io::Error) -> bool {
    // Descend to root error
    let mut err: &dyn StdError = e;
    let root = loop {
        if let Some(e) = err.source() {
            err = e;
        } else {
            break err;
        }
    };
    if root.to_string().contains("InvalidBlockList") {
        return true;
    }

    false
}

pub(crate) struct UploadInfo {
    store: Arc<dyn ObjectStore>,
    upload_id: String,
    path: Path
}

impl UploadInfo {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, path: Path) -> UploadInfo {
        UploadInfo {
            path,
            store,
            upload_id: format!("{:020}", rand::random::<u64>())
        }
    }
    pub(crate) fn inject(&self, attrs: &mut Attributes) {
        attrs.insert(
            Attribute::Metadata("uploadid".into()),
            AttributeValue::from(self.upload_id.clone())
        );
    }
    pub(crate) fn attributes(&self) -> Attributes {
        let mut attrs = Attributes::new();
        self.inject(&mut attrs);
        attrs
    }
    pub(crate) async fn validate_upload(&self, result: Result<(), std::io::Error>) -> std::io::Result<()> {
        if let Err(e) = result {
            // We need special handling of the InvalidBlockList error from Azure because
            // the multipart upload complete request is not idempotent and if it gets retried
            // after a success on the server it will cause this error.
            //
            // Below we verify if the successfull write was made by this operation (by comparing
            // the upload-id attribute) and return with success if so.
            if is_invalid_block_list_error(&e) {
                let head_res = self.store.get_opts(
                    &self.path,
                    GetOptions {
                        head: true,
                        ..Default::default()
                    }
                ).await;
                match head_res {
                    Ok(response) => {
                        let upload_id_matches = response
                            .attributes
                            .get(&Attribute::Metadata("uploadid".into()))
                            .filter(|v| v.as_ref() == self.upload_id)
                            .is_some();
                        if upload_id_matches {
                            // Upload id matches, consider operation as success
                            return Ok(());
                        }
                    }
                    Err(err) => {
                        // Validation head request failed, return original error
                        tracing::warn!("failed to validate upload after InvalidBlockList: {}", err);
                        return Err(e)
                    }
                }
            }

            // Return original error
            Err(e)
        } else {
            // Propagate success
            Ok(())
        }
    }
}


#[pin_project(project = StateProj)]
enum State {
    Passthrough(#[pin] object_store::buffered::BufWriter),
    Validate(BoxFuture<'static, std::io::Result<()>>),
    Moved
}

#[pin_project]
pub(crate) struct BufWriter {
    #[pin]
    state: State,
    upload_info: Option<UploadInfo>
}

impl BufWriter {
    pub(crate) fn with_capacity(store: Arc<dyn ObjectStore>, path: Path, capacity: usize) -> BufWriter {
        let upload_info = UploadInfo::new(store.clone(), path.clone());
        let inner = object_store::buffered::BufWriter::with_capacity(store, path, capacity)
            .with_attributes(upload_info.attributes());
        BufWriter { state: State::Passthrough(inner), upload_info: Some(upload_info) }
    }
    pub(crate) fn with_max_concurrency(mut self, max_concurrency: usize) -> Self {
        match self.state {
            State::Passthrough(inner) => {
                self.state = State::Passthrough(
                    inner.with_max_concurrency(max_concurrency)
                );
            }
            _ => {}
        }
        self
    }
    pub(crate) fn with_attributes(mut self, mut attributes: Attributes) -> Self {
        match self.upload_info {
            Some(ref info) => info.inject(&mut attributes),
            None => {}
        }
        match self.state {
            State::Passthrough(inner) => {
                self.state = State::Passthrough(
                    inner.with_attributes(attributes)
                );
            }
            _ => {}
        }
        self
    }
    #[allow(unused)]
    pub(crate) fn with_tags(mut self, tags: TagSet) -> Self {
        match self.state {
            State::Passthrough(inner) => {
                self.state = State::Passthrough(
                    inner.with_tags(tags)
                );
            }
            _ => {}
        }
        self
    }
}

fn other_err(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, msg)
}

impl AsyncWrite for BufWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match self.project().state.project() {
            StateProj::Passthrough(inner) => inner.poll_write(cx, buf),
            StateProj::Validate(_) => return Poll::Ready(Err(other_err("cannot write after shutdown"))),
            StateProj::Moved => unreachable!("moved")
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match self.project().state.project() {
            StateProj::Passthrough(inner) => inner.poll_flush(cx),
            StateProj::Validate(_) => return Poll::Ready(Err(other_err("cannot flush after shutdown"))),
            StateProj::Moved => unreachable!("moved")
        }
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        loop {
            assert!(!matches!(self.state, State::Moved));

            return match &mut self.state {
                State::Passthrough(_) => {
                    let State::Passthrough(mut inner) = std::mem::replace(&mut self.state, State::Moved) else { unreachable!("checked") };
                    let upload_info = self.upload_info.take().expect("only taken once");
                    self.state = State::Validate(Box::pin(async move {
                        let result = inner.shutdown().await;
                        upload_info.validate_upload(result).await?;
                        Ok(())
                    }));
                    continue;
                },
                State::Validate(fut) => ready!(fut.poll_unpin(cx)).into(),
                State::Moved => unreachable!("moved")
            }
        }
    }
}

#[async_trait::async_trait]
pub(crate) trait AsyncUpload: AsyncWrite {
    async fn abort(&mut self) -> crate::Result<()>;
}

#[async_trait::async_trait]
impl AsyncUpload for object_store::buffered::BufWriter {
    async fn abort(&mut self) -> crate::Result<()> {
        Ok(object_store::buffered::BufWriter::abort(self).await?)
    }
}

#[async_trait::async_trait]
impl AsyncUpload for BufWriter {
    async fn abort(&mut self) -> crate::Result<()> {
        match self.state {
            State::Passthrough(ref mut inner) => Ok(inner.abort().await?),
            State::Validate(_) => Err(ErrorKind::BodyIo(other_err("cannot abort after shutdown")).into()),
            State::Moved => unreachable!("moved")
        }
    }
}

#[derive(Debug)]
#[pin_project(project = EncoderProj)]
pub(crate) enum Encoder<T: AsyncWrite> {
    None(#[pin] T),
    Gzip(#[pin] async_compression::tokio::write::GzipEncoder<T>),
    Deflate(#[pin] async_compression::tokio::write::DeflateEncoder<T>),
    Zlib(#[pin] async_compression::tokio::write::ZlibEncoder<T>),
    Zstd(#[pin] async_compression::tokio::write::ZstdEncoder<T>)
}

#[pin_project]
pub(crate) struct CompressedWriter<T: AsyncWrite> {
    #[pin]
    encoder: Encoder<T>
}

impl<T: AsyncWrite> CompressedWriter<T> {
    pub fn new(compression: Compression, writer: T) -> CompressedWriter<T> {
        let encoder = match compression {
            Compression::Gzip => {
                Encoder::Gzip(async_compression::tokio::write::GzipEncoder::new(writer))
            }
            Compression::Deflate => {
                Encoder::Deflate(async_compression::tokio::write::DeflateEncoder::new(writer))
            }
            Compression::Zlib => {
                Encoder::Zlib(async_compression::tokio::write::ZlibEncoder::new(writer))
            }
            Compression::Zstd => {
                Encoder::Zstd(async_compression::tokio::write::ZstdEncoder::new(writer))
            }
            Compression::None => {
                Encoder::None(writer)
            }
        };
        CompressedWriter {
            encoder
        }
    }
}

#[async_trait::async_trait]
impl<T: AsyncUpload + Send> AsyncUpload for CompressedWriter<T> {
    async fn abort(&mut self) -> crate::Result<()> {
        let writer = match &mut self.encoder {
            Encoder::None(e) => e,
            Encoder::Gzip(e) => e.get_mut(),
            Encoder::Deflate(e) => e.get_mut(),
            Encoder::Zlib(e) => e.get_mut(),
            Encoder::Zstd(e) => e.get_mut(),
        };

        Ok(writer.abort().await?)
    }
}

#[async_trait::async_trait]
impl AsyncUpload for CompressedWriter<Box<dyn AsyncUpload + Send + Unpin>> {
    async fn abort(&mut self) -> crate::Result<()> {
        let writer = match &mut self.encoder {
            Encoder::None(e) => e,
            Encoder::Gzip(e) => e.get_mut(),
            Encoder::Deflate(e) => e.get_mut(),
            Encoder::Zlib(e) => e.get_mut(),
            Encoder::Zstd(e) => e.get_mut(),
        };

        Ok(writer.abort().await?)
    }
}

impl<T: AsyncWrite> AsyncWrite for CompressedWriter<T> {
    fn poll_write(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match self.project().encoder.project() {
            EncoderProj::None(e) => e.poll_write(cx, buf),
            EncoderProj::Gzip(e) => e.poll_write(cx, buf),
            EncoderProj::Deflate(e) => e.poll_write(cx, buf),
            EncoderProj::Zlib(e) => e.poll_write(cx, buf),
            EncoderProj::Zstd(e) => e.poll_write(cx, buf),
        }
    }
    fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>
        ) -> std::task::Poll<Result<(), std::io::Error>> {
        match self.project().encoder.project() {
            EncoderProj::None(e) => e.poll_flush(cx),
            EncoderProj::Gzip(e) => e.poll_flush(cx),
            EncoderProj::Deflate(e) => e.poll_flush(cx),
            EncoderProj::Zlib(e) => e.poll_flush(cx),
            EncoderProj::Zstd(e) => e.poll_flush(cx),
        }
    }
    fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>
        ) -> std::task::Poll<Result<(), std::io::Error>> {
        match self.project().encoder.project() {
            EncoderProj::None(e) => e.poll_shutdown(cx),
            EncoderProj::Gzip(e) => e.poll_shutdown(cx),
            EncoderProj::Deflate(e) => e.poll_shutdown(cx),
            EncoderProj::Zlib(e) => e.poll_shutdown(cx),
            EncoderProj::Zstd(e) => e.poll_shutdown(cx),
        }
    }
}

pub(crate) fn with_decoder(compression: Compression, reader: impl AsyncBufRead + Unpin + Send + 'static) -> Box<dyn AsyncRead + Unpin + Send> {
    match compression {
        Compression::Gzip => {
            return Box::new(async_compression::tokio::bufread::GzipDecoder::new(reader));
        }
        Compression::Deflate => {
            return Box::new(async_compression::tokio::bufread::DeflateDecoder::new(reader));
        }
        Compression::Zlib => {
            return Box::new(async_compression::tokio::bufread::ZlibDecoder::new(reader));
        }
        Compression::Zstd => {
            return Box::new(async_compression::tokio::bufread::ZstdDecoder::new(reader));
        }
        Compression::None => {
            return Box::new(reader)
        }
    }
}

pub(crate) fn _with_encoder(compression: Compression, writer: impl AsyncWrite + Unpin + Send + 'static) -> Box<dyn AsyncWrite + Unpin + Send> {
    match compression {
        Compression::Gzip => {
            return Box::new(async_compression::tokio::write::GzipEncoder::new(writer));
        }
        Compression::Deflate => {
            return Box::new(async_compression::tokio::write::DeflateEncoder::new(writer));
        }
        Compression::Zlib => {
            return Box::new(async_compression::tokio::write::ZlibEncoder::new(writer));
        }
        Compression::Zstd => {
            return Box::new(async_compression::tokio::write::ZstdEncoder::new(writer));
        }
        Compression::None => {
            return Box::new(writer)
        }
    }
}

// Safety: This must match the layout of object_store::path::Path
#[allow(dead_code)]
struct RawPath {
    raw: String,
}

// This is a workaround to create an object_store::path::Path from a String while skipping
// validation
pub(crate) unsafe fn cstr_to_path(cstr: &std::ffi::CStr) -> object_store::path::Path {
    let raw_path = RawPath {
        raw: cstr.to_str().expect("invalid utf8").to_string()
    };

    let path: object_store::path::Path = std::mem::transmute(raw_path);
    return path;
}

pub(crate) unsafe fn string_to_path(string: String) -> object_store::path::Path {
    let raw_path = RawPath {
        raw: string
    };

    let path: object_store::path::Path = std::mem::transmute(raw_path);
    return path;
}

pub(crate) fn deserialize_slice<'a, T>(v: &'a [u8]) -> Result<T, serde_path_to_error::Error<serde_json::Error>>
where
    T: serde::Deserialize<'a>,
{
    let de = &mut serde_json::Deserializer::from_slice(v);
    serde_path_to_error::deserialize(de)
}

pub(crate) fn deserialize_str<'a, T>(v: &'a str) -> Result<T, serde_path_to_error::Error<serde_json::Error>>
where
    T: serde::Deserialize<'a>,
{
    let de = &mut serde_json::Deserializer::from_str(v);
    serde_path_to_error::deserialize(de)
}

pub(crate) fn required_attribute<'a>(attr: &'a Attributes, key: &'static str) -> Result<&'a str, Error> {
    let v: &str = attr.get(&Attribute::Metadata(key.into()))
        .ok_or_else(|| Error::required_config(format!("missing required attribute `{}`", key)))?
        .as_ref();
    Ok::<_, Error>(v)
}
