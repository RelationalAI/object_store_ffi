use crate::static_config;

use std::ops::Range;
use std::ffi::c_char;
use anyhow::anyhow;
use tokio::io::{AsyncRead, AsyncBufRead, AsyncWrite};

pub(crate) fn size_to_ranges(object_size: usize) -> Vec<Range<usize>> {
    if object_size == 0 {
        return vec![];
    }

    let part_size: usize = static_config().multipart_get_part_size as usize;

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

pub(crate) fn with_encoder(compression: Compression, writer: impl AsyncWrite + Unpin + Send + 'static) -> Box<dyn AsyncWrite + Unpin + Send> {
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
