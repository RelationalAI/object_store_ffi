use std::ops::Range;
use std::ffi::c_char;
use anyhow::anyhow;
use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncBufRead, AsyncWrite};

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
pub(crate) enum Compression {
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
            match codec_str {
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
