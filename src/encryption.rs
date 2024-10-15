use openssl::symm::{self, encrypt_aead, decrypt_aead, Cipher, Crypter};
use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zeroize::Zeroize;
use core::{
    pin::Pin,
    task::{Context, Poll},
};
use bytes::Bytes;
use futures_util::ready;
use std::{fmt::{self, Debug}, ops::{Deref, DerefMut}};

use base64::prelude::*;
use rand::{RngCore, rngs::OsRng};
use object_store::Attributes;

use crate::util::AsyncUpload;

const AES_GCM_TAG_BYTES: usize = 16;

#[async_trait::async_trait]
pub(crate) trait CryptoMaterialProvider:
    Send
    + Sync
    + Debug
    + 'static {
    async fn material_for_write(&self, path: &str, data_len: Option<usize>) -> crate::Result<(ContentCryptoMaterial, Attributes)>;
    async fn material_from_metadata(&self, path: &str, attr: &Attributes) -> crate::Result<ContentCryptoMaterial>;
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum CryptoScheme {
    Aes256Gcm,
    Aes128Cbc
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum Mode {
    Encrypt,
    Decrypt
}

impl From<Mode> for openssl::symm::Mode {
    fn from(v: Mode) -> openssl::symm::Mode {
        match v {
            Mode::Encrypt => openssl::symm::Mode::Encrypt,
            Mode::Decrypt => openssl::symm::Mode::Decrypt
        }
    }
}

impl CryptoScheme {
    pub(crate) fn key_len(&self) -> usize {
        self.cipher().key_len()
    }
    pub(crate) fn iv_len(&self) -> usize {
        self.cipher().iv_len().expect("needs iv")
    }
    pub(crate) fn tag_len(&self) -> usize {
        match self {
            CryptoScheme::Aes256Gcm => AES_GCM_TAG_BYTES,
            CryptoScheme::Aes128Cbc => 0
        }
    }
    pub(crate) fn cipher(&self) -> Cipher {
        match self {
            CryptoScheme::Aes256Gcm => Cipher::aes_256_gcm(),
            CryptoScheme::Aes128Cbc => Cipher::aes_128_cbc(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct Iv {
    bytes: Vec<u8>
}

impl Deref for Iv {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &[u8] {
        self.bytes.as_slice()
    }
}

impl DerefMut for Iv {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.bytes.as_mut_slice()
    }
}

impl Iv {
    pub(crate) fn from_base64(iv: impl AsRef<str>) -> Result<Iv, base64::DecodeError> {
        Ok(Iv { bytes: BASE64_STANDARD.decode(iv.as_ref())? })
    }
    pub(crate) fn generate(len: usize) -> Iv {
        let mut bytes = vec![0; len];
        OsRng.fill_bytes(&mut bytes);
        Iv { bytes }
    }
    #[allow(unused)]
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }
    pub(crate) fn as_base64(&self) -> String {
        BASE64_STANDARD.encode(&self.bytes)
    }
}

#[derive(Clone)]
pub(crate) struct Key {
    bytes: Vec<u8>
}

impl Deref for Key {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &[u8] {
        self.bytes.as_slice()
    }
}

impl DerefMut for Key {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.bytes.as_mut_slice()
    }
}

impl Key {
    pub(crate) fn from_base64(key: impl AsRef<str>) -> Result<Key, base64::DecodeError> {
        Ok(Key { bytes: BASE64_STANDARD.decode(key.as_ref())? })
    }
    pub(crate) fn generate(len: usize) -> Key {
        let mut bytes = vec![0; len];
        OsRng.fill_bytes(&mut bytes);
        Key { bytes }
    }
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }
    pub(crate) fn encrypt_aes_128_ecb(self, encryption_key: &Key) -> std::io::Result<EncryptedKey> {
        let cipher = Cipher::aes_128_ecb();
        if encryption_key.len() != cipher.key_len() {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid key size"));
        }

        let encrypted_bytes = symm::encrypt(cipher, &encryption_key, None, &self)?;

        Ok(EncryptedKey { bytes: encrypted_bytes })
    }
    #[allow(unused)]
    pub(crate) fn as_base64(&self) -> String {
        BASE64_STANDARD.encode(&self.bytes)
    }
}

impl Drop for Key {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

// Always encrypted with aes_128_ecb for now
#[derive(Clone)]
pub(crate) struct EncryptedKey {
    bytes: Vec<u8>,
}

impl EncryptedKey {
    pub(crate) fn from_base64(key: impl AsRef<str>) -> Result<EncryptedKey, base64::DecodeError> {
        Ok(EncryptedKey { bytes: BASE64_STANDARD.decode(key.as_ref())? })
    }
    pub(crate) fn decrypt_aes_128_ecb(self, decryption_key: &Key) -> std::io::Result<Key> {
        let cipher = Cipher::aes_128_ecb();
        if decryption_key.len() != cipher.key_len() {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid key size"));
        }
        let bytes = symm::decrypt(cipher, &decryption_key, None, &self.bytes)?;

        Ok(Key { bytes })
    }
    pub(crate) fn as_base64(&self) -> String {
        BASE64_STANDARD.encode(&self.bytes)
    }
}

impl Drop for EncryptedKey {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

pub(crate) struct ContentCryptoMaterial {
    pub scheme: CryptoScheme,
    pub cek: Key,
    pub iv: Iv,
    pub aad: Option<Bytes>
}

impl ContentCryptoMaterial {
    pub fn generate(scheme: CryptoScheme) -> ContentCryptoMaterial {
        let cek = Key::generate(scheme.key_len());
        let iv = Iv::generate(scheme.iv_len());
        ContentCryptoMaterial { scheme, cek, iv, aad: None }
    }

    pub fn with_aad(self, aad: impl Into<Bytes>) -> ContentCryptoMaterial {
        ContentCryptoMaterial { aad: Some(aad.into()), ..self }
    }
}

pub(crate) fn encrypt(
    data: &[u8],
    material: &ContentCryptoMaterial
) -> std::io::Result<Vec<u8>> {
    match material.scheme {
        CryptoScheme::Aes128Cbc => {
            let ContentCryptoMaterial { scheme, cek, iv, .. } = material;
            Ok(symm::encrypt(scheme.cipher(), &cek, Some(&iv), data)?)

        },
        CryptoScheme::Aes256Gcm => {
            let ContentCryptoMaterial { scheme, cek, iv, aad, .. } = material;
            let aad_ref = aad.as_deref().unwrap_or_default();
            let mut tag = vec![0; scheme.tag_len()];
            let mut ciphertext = encrypt_aead(scheme.cipher(), &cek, Some(&iv), aad_ref, data, &mut tag)?;
            // Postfix tag
            ciphertext.extend_from_slice(&tag);
            Ok(ciphertext)
        }
    }
}

#[allow(unused)]
pub(crate) fn decrypt(
    ciphertext: &[u8],
    material: &ContentCryptoMaterial
) -> std::io::Result<Vec<u8>> {
    match material.scheme {
        CryptoScheme::Aes128Cbc => {
            let ContentCryptoMaterial { scheme, cek, iv, .. } = material;
            Ok(symm::decrypt(scheme.cipher(), &cek, Some(&iv), ciphertext)?)

        },
        CryptoScheme::Aes256Gcm => {
            let ContentCryptoMaterial { scheme, cek, iv, aad, .. } = material;
            let aad_ref = aad.as_deref().unwrap_or_default();
            // Postfix tag
            let tag_offset = ciphertext.len() - AES_GCM_TAG_BYTES;
            let data = decrypt_aead(scheme.cipher(), &cek, Some(&iv), aad_ref, &ciphertext[..tag_offset], &ciphertext[tag_offset..])?;
            Ok(data)
        }
    }
}

// This implementation consists of a Buffer that has three contiguous regions: the consumed
// region, the filled region and the unfilled region. The buffer starts completely unfilled and
// its state is modified by calling:
// - advance: to indicate that useful data has been writen to the buffer, increasing the filled
// region (over the unfilled one)
// - consume: to indicate that useful data has been processed out of the buffer, incresing the
// consumed region (over the filled one)
// - compact: moves any bytes of the filled region to the start of the buffer, reseting the
// consumed and unfilled regions accordingly
// - clear: resets everything to the initial state
//
//     consume ==>|   advance ==>|
// +--------------+--------------+----------+
// |   consumed   |    filled    | unfilled |
// +--------------+--------------+----------+
struct Buffer {
    buf: Vec<u8>,
    pos: usize,
    filled: usize
}

impl Buffer {
    fn with_capacity(cap: usize) -> Buffer {
        Buffer {
            buf: vec![0; cap],
            pos: 0,
            filled: 0
        }
    }

    #[inline]
    #[track_caller]
    fn consume(&mut self, n: usize) {
        assert!(self.pos + n <= self.filled);
        self.pos += n;
    }

    #[inline]
    #[track_caller]
    fn advance(&mut self, n: usize) {
        assert!(self.filled + n <= self.capacity());
        self.filled += n;
    }

    /// Returns the total capacity of the buffer.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    /// Returns a shared reference to the filled portion of the buffer.
    #[inline]
    pub fn filled(&self) -> &[u8] {
        &self.buf[self.pos..self.filled]
    }

    /// Returns a mutable reference to the filled portion of the buffer.
    #[inline]
    #[allow(unused)]
    pub fn filled_mut(&mut self) -> &mut [u8] {
        &mut self.buf[self.pos..self.filled]
    }

    #[inline]
    pub fn unfilled(&self) -> &[u8] {
        &self.buf[self.filled..]
    }

    #[inline]
    pub fn unfilled_mut(&mut self) -> &mut [u8] {
        &mut self.buf[self.filled..]
    }

    /// Returns the number of bytes at the end of the slice that have not yet been filled.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.capacity() - self.filled
    }

    /// Returns the number of bytes at the end of the slice that have not yet been filled.
    #[inline]
    #[allow(unused)]
    pub fn available(&self) -> usize {
        self.filled - self.pos
    }

    /// Clears the buffer, resetting the filled region to empty.
    #[inline]
    pub fn clear(&mut self) {
        self.pos = 0;
        self.filled = 0;
    }

    /// Compacts the filled portion to the start of the buffer.
    #[inline]
    pub fn compact(&mut self) {
        let len = self.filled - self.pos;
        self.buf.copy_within(self.pos..self.filled, 0);
        self.pos = 0;
        self.filled = len;
    }

    /// Appends data to the buffer, advancing the filled position.
    ///
    /// # Panics
    ///
    /// Panics if `self.remaining()` is less than `buf.len()`.
    #[inline]
    #[track_caller]
    pub fn put_slice(&mut self, buf: &[u8]) {
        assert!(
            self.remaining() >= buf.len(),
            "buf.len() must fit in remaining(); buf.len() = {}, remaining() = {}",
            buf.len(),
            self.remaining()
        );

        let amt = buf.len();
        // Cannot overflow, asserted above
        let end = self.filled + amt;

        // Safety: the length is asserted above
        unsafe {
            self.buf[self.filled..end]
                .as_mut_ptr()
                .cast::<u8>()
                .copy_from_nonoverlapping(buf.as_ptr(), amt);
        }

        self.filled = end;
    }
}

#[derive(Debug)]
enum State {
    Initial,
    Filling,
    Crypting,
    Flushing,
    Finalizing,
    Done,
}

// Asynchronous reader that transparently en/decrypts data on reads.
// It is capable of dealing with both AES CBC and AES GCM.
#[pin_project]
pub struct CrypterReader<R> {
    #[pin]
    reader: R,
    crypter: Crypter,
    mode: Mode,
    tag_len: usize,
    block_size: usize,
    inbuf: Buffer,
    outbuf: Buffer,
    state: State,
    last_flush: bool
}

impl<T> fmt::Debug for CrypterReader<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CrypterReader")
            .field("block_size", &self.block_size)
            .field("mode", &self.mode)
            .field("state", &self.state)
            .finish()
    }
}

impl<R: AsyncRead> CrypterReader<R> {
    pub fn new(reader: R, mode: Mode, material: &ContentCryptoMaterial) -> std::io::Result<Self> {
        CrypterReader::with_capacity(reader, mode, material, 64 * 1024)
    }
    pub fn with_capacity(reader: R, mode: Mode, material: &ContentCryptoMaterial, capacity: usize) -> std::io::Result<Self> {
        let ContentCryptoMaterial { scheme, cek, iv, aad, .. } = material;
        let cipher = scheme.cipher();
        let block_size = cipher.block_size();
        let mut crypter = Crypter::new(cipher, mode.into(), cek, Some(iv))?;
        let tag_len = scheme.tag_len();
        if let Some(aad) = aad {
            crypter.aad_update(aad)?;
        }
        Ok(Self {
            reader,
            crypter,
            mode,
            tag_len,
            block_size,
            inbuf: Buffer::with_capacity(capacity),
            outbuf: Buffer::with_capacity(block_size + block_size + tag_len),
            state: State::Initial,
            last_flush: false
        })
    }

    pub fn get_ref(&self) -> &R {
        &self.reader
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    pub fn get_pin_mut(self: Pin<&mut Self>) -> Pin<&mut R> {
        self.project().reader
    }

    pub fn into_inner(self) -> R {
        self.reader
    }
}

impl<R: AsyncRead> AsyncRead for CrypterReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(())); // Maybe should error instead
        }

        let mut this = self.project();
        let block_size = *this.block_size;

        // Size of the tag to extract from end of ciphertext
        let extract_tag_len = match this.mode {
            Mode::Encrypt => 0,
            Mode::Decrypt => *this.tag_len
        };
        // Size of the tag to append to end of ciphertext
        let append_tag_len = match this.mode {
            Mode::Encrypt => *this.tag_len,
            Mode::Decrypt => 0
        };

        loop {
            *this.state = match this.state {
                State::Initial => {
                    let mut wrapped = ReadBuf::new(this.inbuf.unfilled_mut());
                    match this.reader.as_mut().poll_read(cx, &mut wrapped) {
                        Poll::Ready(res) => res?,
                        Poll::Pending => {
                            if buf.filled().len() > 0 {
                                // There is pending data in buf return Ready instead of Pending
                                return Poll::Ready(Ok(()));
                            } else {
                                // We need to wait
                                return Poll::Pending;
                            }
                        }
                    }
                    let amt = wrapped.filled().len();
                    this.inbuf.advance(amt);
                    if amt == 0 {
                        // never crypted anything, go to State::Done without flushing
                        State::Done
                    } else {
                        // got some bytes, fill buffer
                        State::Filling
                    }
                }
                State::Filling => {
                    if this.inbuf.filled().len() <= extract_tag_len {
                        // we have up to tag_len bytes filled, bring them to the start of the
                        // buffer
                        this.inbuf.compact();
                    }
                    if this.inbuf.unfilled().len() > 0 {
                        let mut wrapped = ReadBuf::new(this.inbuf.unfilled_mut());
                        match this.reader.as_mut().poll_read(cx, &mut wrapped) {
                            Poll::Ready(res) => res?,
                            Poll::Pending => {
                                if buf.filled().len() > 0 {
                                    // There is pending data in buf return Ready instead of Pending
                                    return Poll::Ready(Ok(()));
                                } else {
                                    // We need to wait
                                    return Poll::Pending;
                                }
                            }
                        }
                        let amt = wrapped.filled().len();
                        this.inbuf.advance(amt);
                        if amt == 0 {
                            // reached reader eof
                            if this.inbuf.filled().len() <= extract_tag_len {
                                // may have enough bytes for a tag
                                State::Finalizing
                            } else {
                                // we still have some extra bytes to crypt
                                State::Crypting
                            }
                        } else {
                            State::Filling
                        }
                    } else {
                        // we are full, go crypt
                        State::Crypting
                    }
                }
                State::Crypting => {
                    if this.inbuf.filled().len() > extract_tag_len {
                        if buf.remaining() > block_size {
                            // readbuf is big enough, crypt directly into it
                            let to_crypt = (this.inbuf.filled().len() - extract_tag_len).min(buf.remaining() - block_size);
                            let amount = this.crypter.update(&this.inbuf.filled()[..to_crypt], buf.initialize_unfilled())?;
                            buf.advance(amount);
                            this.inbuf.consume(to_crypt);
                            State::Crypting
                        } else {
                            // readbuf is too small, crypt to outbuf then flush
                            let to_crypt = (this.inbuf.filled().len() - extract_tag_len).min(this.outbuf.unfilled().len() - block_size);
                            let amount = this.crypter.update(&this.inbuf.filled()[..to_crypt], this.outbuf.unfilled_mut())?;
                            this.outbuf.advance(amount);
                            this.inbuf.consume(to_crypt);
                            State::Flushing
                        }
                    } else {
                        // not enough to continue crypting, go fill
                        State::Filling
                    }
                }
                State::Flushing => {
                    if this.outbuf.filled().len() > 0 {
                        let to_copy = this.outbuf.filled().len().min(buf.remaining());
                        buf.put_slice(&this.outbuf.filled()[..to_copy]);
                        this.outbuf.consume(to_copy);
                        State::Flushing
                    } else {
                        if *this.last_flush {
                            // outbuf is empty and was last flush, done
                            this.outbuf.clear();
                            State::Done
                        } else {
                            // outbuf is empty, reset it and go crypt
                            this.outbuf.clear();
                            State::Crypting
                        }
                    }
                }
                State::Finalizing => {
                    if this.inbuf.filled().len() == extract_tag_len {
                        if extract_tag_len > 0 {
                            // we extracted some tag, set it
                            this.crypter.set_tag(this.inbuf.filled())?;
                        }

                        if buf.remaining() > block_size + append_tag_len {
                            // readbuf is big enough, finalize directly into it
                            let amt = this.crypter.finalize(buf.initialize_unfilled())?;
                            buf.advance(amt);
                            if append_tag_len > 0 {
                                // we need to append a tag
                                this.crypter.get_tag(&mut buf.initialize_unfilled()[..append_tag_len])?;
                                buf.advance(append_tag_len);
                            }
                            State::Done
                        } else {
                            // readbuf is too small, finalize to outbuf then last flush
                            let amt = this.crypter.finalize(this.outbuf.unfilled_mut())?;
                            this.outbuf.advance(amt);
                            if append_tag_len > 0 {
                                // we need to append a tag
                                this.crypter.get_tag(&mut this.outbuf.unfilled_mut()[..append_tag_len])?;
                                this.outbuf.advance(append_tag_len);
                            }
                            // Important, flagging this flush as the last one
                            *this.last_flush = true;
                            State::Flushing
                        }
                    } else {
                        // The final bytes in the buffer do not match extract_tag_len
                        debug_assert!(this.inbuf.filled().len() < extract_tag_len);
                        return Err(
                            std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "unable to read enough bytes for the required tag"
                            )
                        ).into();
                    }
                }
                State::Done => State::Done
            };

            if let State::Done = *this.state {
                return Poll::Ready(Ok(()));
            }

            if buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }
        }
    }
}

// Asynchronous writer that transparently en/decrypts data on write.
// It is capable of dealing with both AES CBC and AES GCM.
#[pin_project]
pub struct CrypterWriter<W> {
    #[pin]
    writer: W,
    crypter: Crypter,
    mode: Mode,
    extract_tag_len: usize,
    append_tag_len: usize,
    block_size: usize,
    buf: Buffer,
    tag_buf: Buffer,
    was_updated: bool,
    finalized: bool,
}

impl<W> fmt::Debug for CrypterWriter<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CrypterWriter")
            .field("block_size", &self.block_size)
            .field("mode", &self.mode)
            .field("was_updated", &self.was_updated)
            .field("finalized", &self.finalized)
            .finish()
    }
}


#[async_trait::async_trait]
impl<W: AsyncUpload + Send> AsyncUpload for CrypterWriter<W> {
    async fn abort(&mut self) -> crate::Result<()> {
        Ok(self.writer.abort().await?)
    }
}


impl<W: AsyncWrite> CrypterWriter<W> {
    pub fn new(writer: W, mode: Mode, material: &ContentCryptoMaterial) -> std::io::Result<Self> {
        Self::with_capacity(writer, mode, material, 64 * 1024)
    }

    pub fn with_capacity(writer: W, mode: Mode, material: &ContentCryptoMaterial, capacity: usize) -> std::io::Result<Self> {
        let ContentCryptoMaterial { scheme, cek, iv, aad, .. } = material;
        let cipher = scheme.cipher();
        let block_size = cipher.block_size();
        let mut crypter = Crypter::new(cipher, mode.into(), cek, Some(iv))?;
        let (extract_tag_len, append_tag_len) = match mode {
            Mode::Encrypt => (0, scheme.tag_len()),
            Mode::Decrypt => (scheme.tag_len(), 0)
        };
        if let Some(aad) = aad {
            crypter.aad_update(aad)?;
        }
        Ok(Self {
            writer,
            crypter,
            mode,
            extract_tag_len,
            append_tag_len,
            block_size,
            buf: Buffer::with_capacity(capacity),
            tag_buf: Buffer::with_capacity(extract_tag_len * 2),
            was_updated: false,
            finalized: false
        })
    }

    pub fn get_ref(&self) -> &W {
        &self.writer
    }

    pub fn get_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    pub fn get_pin_mut(self: Pin<&mut Self>) -> Pin<&mut W> {
        self.project().writer
    }

    pub fn into_inner(self) -> W {
        self.writer
    }

    fn flush_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let mut this = self.project();

        while this.buf.filled().len() > 0 {
            match ready!(this.writer.as_mut().poll_write(cx, &this.buf.filled())) {
                Ok(0) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "failed to write the buffered data",
                    )).into();
                }
                Ok(n) => this.buf.consume(n),
                Err(e) => {
                    return Err(e).into();
                }
            }
        }

        this.buf.clear();

        Poll::Ready(Ok(()))
    }

    fn finalize(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> std::io::Result<()> {
        let this = self.project();
        if *this.extract_tag_len > 0 {
            if this.tag_buf.filled().len() == *this.extract_tag_len {
                this.crypter.set_tag(this.tag_buf.filled())?;
                this.tag_buf.consume(*this.extract_tag_len);
            } else {
                // The bytes in the tag buffer do not match extract_tag_len
                debug_assert!(this.tag_buf.filled().len() < *this.extract_tag_len);
                return Err(
                    std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "not enough written bytes for the required tag"
                    )
                ).into();
            }
        }
        let amt = this.crypter.finalize(this.buf.unfilled_mut())?;
        this.buf.advance(amt);
        if *this.append_tag_len > 0 {
            this.crypter.get_tag(&mut this.buf.unfilled_mut()[..*this.append_tag_len])?;
            this.buf.advance(*this.append_tag_len);
        }
        *this.finalized = true;
        Ok(())
    }
}

impl<W: AsyncWrite> AsyncWrite for CrypterWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let block_size = self.block_size;
        let extract_tag_len = self.extract_tag_len;
        // We will only write the amount that fits our buffer.
        let upper_bound = buf.len().min(self.buf.capacity().saturating_sub(block_size));
        let required_space = upper_bound + block_size;
        // Truncate buffer to upper bound;
        let buf = &buf[..upper_bound];
        if self.buf.unfilled().len() < required_space {
            ready!(self.as_mut().flush_buf(cx))?;
        }

        let this = self.project();

        // Buffer last bytes without decrypting if we are expecting a tag
        let last_bytes_offset = buf.len().saturating_sub(extract_tag_len);
        let last = &buf[last_bytes_offset..];

        if last.len() > 0 {
            let overflow = (last.len() + this.tag_buf.filled().len()).saturating_sub(extract_tag_len);
            if overflow > 0 {
                // we have more bytes than extract_tag_len flush the excess to main buffer
                debug_assert!(overflow <= last.len());
                let amt = this.crypter.update(&this.tag_buf.filled()[..overflow], this.buf.unfilled_mut())?;
                this.buf.advance(amt);
                this.tag_buf.consume(overflow);
                // crypter was updated, needs to be finalized
                *this.was_updated = true;
            }
            this.tag_buf.compact();
            this.tag_buf.put_slice(last);
        }

        // These bytes cannot be the tag, crypt them into main buffer
        let first = &buf[..last_bytes_offset];
        if first.len() > 0 {
            let amt = this.crypter.update(first, this.buf.unfilled_mut())?;
            this.buf.advance(amt);
            // crypter was updated, needs to be finalized
            *this.was_updated = true;
        }
        Poll::Ready(Ok(first.len() + last.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        ready!(self.as_mut().flush_buf(cx))?;
        self.get_pin_mut().poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // crypter can only be finalized if it was updated at least once
        if !self.finalized && self.was_updated {
            if self.buf.unfilled().len() < self.block_size + self.append_tag_len {
                ready!(self.as_mut().flush_buf(cx))?;
            }
            self.as_mut().finalize(cx)?;
        }

        ready!(self.as_mut().flush_buf(cx))?;
        self.get_pin_mut().poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use rand::Rng;

    #[tokio::test]
    async fn crypter_reader_aes_cbc_round_trip() {
        let data: Vec<u8> = (0..100000u32).map(|n| (n % 256) as u8).collect();

        let material = ContentCryptoMaterial::generate(CryptoScheme::Aes128Cbc);

        let mut reader = CrypterReader::new(data.as_slice(), Mode::Encrypt, &material).unwrap();
        let mut ciphertext = vec![];
        reader.read_to_end(&mut ciphertext).await.unwrap();

        let mut reader = CrypterReader::new(ciphertext.as_slice(), Mode::Decrypt, &material).unwrap();
        let mut plaintext = vec![];
        reader.read_to_end(&mut plaintext).await.unwrap();
        assert_eq!(data, plaintext);
    }

    #[tokio::test]
    async fn crypter_reader_aes_gcm_round_trip() {
        let data: Vec<u8> = (0..100000u32).map(|n| (n % 256) as u8).collect();

        let material = ContentCryptoMaterial::generate(CryptoScheme::Aes256Gcm)
            .with_aad(b"test aad".as_slice());

        let mut reader = CrypterReader::new(data.as_slice(), Mode::Encrypt, &material).unwrap();
        let mut ciphertext = vec![];
        reader.read_to_end(&mut ciphertext).await.unwrap();

        let mut reader = CrypterReader::new(ciphertext.as_slice(), Mode::Decrypt, &material).unwrap();
        let mut plaintext = vec![];
        reader.read_to_end(&mut plaintext).await.unwrap();
        assert_eq!(data, plaintext);
    }

    #[tokio::test]
    async fn crypter_writer_aes_cbc_round_trip() {
        let data: Vec<u8> = (0..100000u32).map(|n| (n % 256) as u8).collect();

        let material = ContentCryptoMaterial::generate(CryptoScheme::Aes128Cbc);

        let mut ciphertext = vec![];
        let mut writer = CrypterWriter::new(&mut ciphertext, Mode::Encrypt, &material).unwrap();
        writer.write_all(&data).await.unwrap();
        writer.flush().await.unwrap();
        writer.shutdown().await.unwrap();

        let mut plaintext = vec![];
        let mut writer = CrypterWriter::new(&mut plaintext, Mode::Decrypt, &material).unwrap();
        writer.write_all(&ciphertext).await.unwrap();
        writer.flush().await.unwrap();
        writer.shutdown().await.unwrap();
        assert_eq!(data, plaintext);
    }

    #[tokio::test]
    async fn crypter_writer_aes_gcm_round_trip() {
        let data: Vec<u8> = (0..100000u32).map(|n| (n % 256) as u8).collect();

        let material = ContentCryptoMaterial::generate(CryptoScheme::Aes256Gcm)
            .with_aad(b"test aad".as_slice());

        let mut ciphertext = vec![];
        let mut writer = CrypterWriter::new(&mut ciphertext, Mode::Encrypt, &material).unwrap();
        writer.write_all(&data).await.unwrap();
        writer.flush().await.unwrap();
        writer.shutdown().await.unwrap();

        let mut plaintext = vec![];
        let mut writer = CrypterWriter::new(&mut plaintext, Mode::Decrypt, &material).unwrap();
        writer.write_all(&ciphertext).await.unwrap();
        writer.flush().await.unwrap();
        writer.shutdown().await.unwrap();
        assert_eq!(data, plaintext);
    }

    #[tokio::test]
    async fn crypter_writer_and_reader_aes_cbc_round_trip() {
        let data: Vec<u8> = (0..100000u32).map(|n| (n % 256) as u8).collect();

        let material = ContentCryptoMaterial::generate(CryptoScheme::Aes128Cbc);

        let mut ciphertext = vec![];
        let mut writer = CrypterWriter::new(&mut ciphertext, Mode::Encrypt, &material).unwrap();
        writer.write_all(&data).await.unwrap();
        writer.flush().await.unwrap();
        writer.shutdown().await.unwrap();

        let mut reader = CrypterReader::new(ciphertext.as_slice(), Mode::Decrypt, &material).unwrap();
        let mut plaintext = vec![];
        reader.read_to_end(&mut plaintext).await.unwrap();
        assert_eq!(data, plaintext);
    }

    #[tokio::test]
    async fn crypter_writer_and_reader_aes_gcm_round_trip() {
        let data: Vec<u8> = (0..100000u32).map(|n| (n % 256) as u8).collect();

        let material = ContentCryptoMaterial::generate(CryptoScheme::Aes256Gcm)
            .with_aad(b"test aad".as_slice());

        let mut ciphertext = vec![];
        let mut writer = CrypterWriter::new(&mut ciphertext, Mode::Encrypt, &material).unwrap();
        writer.write_all(&data).await.unwrap();
        writer.flush().await.unwrap();
        writer.shutdown().await.unwrap();

        let mut reader = CrypterReader::new(ciphertext.as_slice(), Mode::Decrypt, &material).unwrap();
        let mut plaintext = vec![];
        reader.read_to_end(&mut plaintext).await.unwrap();
        assert_eq!(data, plaintext);
    }

    #[tokio::test]
    async fn crypter_reader_and_writer_aes_cbc_round_trip() {
        let data: Vec<u8> = (0..100000u32).map(|n| (n % 256) as u8).collect();

        let material = ContentCryptoMaterial::generate(CryptoScheme::Aes128Cbc);

        let mut reader = CrypterReader::new(data.as_slice(), Mode::Encrypt, &material).unwrap();
        let mut ciphertext = vec![];
        reader.read_to_end(&mut ciphertext).await.unwrap();

        let mut plaintext = vec![];
        let mut writer = CrypterWriter::new(&mut plaintext, Mode::Decrypt, &material).unwrap();
        writer.write_all(&ciphertext).await.unwrap();
        writer.flush().await.unwrap();
        writer.shutdown().await.unwrap();

        assert_eq!(data, plaintext);
    }

    #[tokio::test]
    async fn crypter_reader_and_writer_aes_gcm_round_trip() {
        let data: Vec<u8> = (0..100000u32).map(|n| (n % 256) as u8).collect();

        let material = ContentCryptoMaterial::generate(CryptoScheme::Aes256Gcm)
            .with_aad(b"test aad".as_slice());

        let mut reader = CrypterReader::new(data.as_slice(), Mode::Encrypt, &material).unwrap();
        let mut ciphertext = vec![];
        reader.read_to_end(&mut ciphertext).await.unwrap();

        let mut plaintext = vec![];
        let mut writer = CrypterWriter::new(&mut plaintext, Mode::Decrypt, &material).unwrap();
        writer.write_all(&ciphertext).await.unwrap();
        writer.flush().await.unwrap();
        writer.shutdown().await.unwrap();

        assert_eq!(data, plaintext);
    }

    #[test]
    fn content_encryption_aes_cbc_round_trip() {
        let data: Vec<u8> = (0..100000u32).map(|n| (n % 256) as u8).collect();

        let material = ContentCryptoMaterial::generate(CryptoScheme::Aes128Cbc);

        let ciphertext = encrypt(&data, &material).unwrap();

        let plaintext = decrypt(&ciphertext, &material).unwrap();

        assert_eq!(data, plaintext);
    }

    #[test]
    fn content_encryption_aes_gcm_round_trip() {
        let data: Vec<u8> = (0..100000u32).map(|n| (n % 256) as u8).collect();

        let material = ContentCryptoMaterial::generate(CryptoScheme::Aes256Gcm)
            .with_aad("AES/GCM/NoPadding");

        let ciphertext = encrypt(&data, &material).unwrap();

        let plaintext = decrypt(&ciphertext, &material).unwrap();

        assert_eq!(data, plaintext);
    }

    async fn random_crypter_writer_writes(
        data: &[u8],
        mode: Mode,
        material: &ContentCryptoMaterial,
        max_write_size: usize,
        flush_prob: f64
    ) -> Vec<u8> {
        let mut result = vec![];
        let mut writer = CrypterWriter::new(&mut result, mode, &material).unwrap();
        let mut offset = 0;
        loop {
            let remaining = data.len() - offset;
            assert!(remaining >= 1);
            let upper_bound = remaining.min(max_write_size);
            let write_size = if upper_bound == 1 {
                1
            } else {
                rand::thread_rng().gen_range(1..upper_bound)
            };
            let write_end_offset = offset + write_size;
            loop {
                offset += writer.write(&data[offset..write_end_offset]).await.unwrap();
                if offset == write_end_offset { break; }
            }
            assert!(offset <= data.len());
            if offset == data.len() {
                break;
            }
            if rand::thread_rng().gen::<f64>() <= flush_prob {
                writer.flush().await.unwrap();
            }
        }
        writer.flush().await.unwrap();
        writer.shutdown().await.unwrap();
        result
    }

    async fn random_crypter_reader_reads(
        data: &[u8],
        mode: Mode,
        material: &ContentCryptoMaterial,
        max_read_size: usize
    ) -> Vec<u8> {
        let mut result = vec![0; data.len() + material.scheme.cipher().block_size() + material.scheme.tag_len()];
        let mut reader = CrypterReader::new(Cursor::new(data), mode, &material).unwrap();
        let mut offset = 0;
        loop {
            let remaining = result.len() - offset;
            if remaining == 0 {
                let mut scratch = [0u8; 1];
                if let Ok(0) = reader.read(scratch.as_mut_slice()).await {
                    break;
                } else {
                    panic!("needs to read past result len");
                }
            }
            assert!(remaining >= 1);
            let upper_bound = remaining.min(max_read_size);
            let read_size = if upper_bound == 1 {
                1
            } else {
                rand::thread_rng().gen_range(1..upper_bound)
            };
            let n = reader.read(&mut result[offset..(offset + read_size)]).await.unwrap();
            if n == 0 {
                break;
            }
            offset += n;
            assert!(offset <= result.len());
        }
        result.truncate(offset);
        result
    }

    fn compare_large_slices(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            eprintln!("expected lengths {} and {} to match", a.len(), b.len());
            return false;
        }

        for i in 0..a.len() {
            let i_a = a[i];
            let i_b = b[i];
            if i_a != i_b {
                eprintln!("expected datum {} and {} to match at index {i}", i_a, i_b);
                let preceding_range = i.saturating_sub(10)..i;
                let succeding_range = i..i.saturating_add(10).min(a.len());
                eprintln!("expected preceding: {:?} and succeding {:?}", &a[preceding_range.clone()], &b[preceding_range]);
                eprintln!("received preceding: {:?} and succeding {:?}", &a[succeding_range.clone()], &b[succeding_range]);
                return false;
            }
        }

        true
    }

    #[tokio::test]
    async fn randomized_crypter_writer_aes_cbc() {
        let size = 500 * 1024 * 1024;
        let max_write_size = 20 * 1024 * 1024;
        let material = ContentCryptoMaterial::generate(CryptoScheme::Aes128Cbc);
        let data: Vec<u8> = (0..size).map(|n| (n % 256) as u8).collect();

        for _i in 0..50 {
            let ciphertext = random_crypter_writer_writes(&data, Mode::Encrypt, &material, max_write_size, 0.1).await;

            let plaintext = decrypt(&ciphertext, &material).unwrap();
            assert!(compare_large_slices(&data, &plaintext));

            let plaintext = random_crypter_writer_writes(&ciphertext, Mode::Decrypt, &material, max_write_size, 0.1).await;
            assert!(compare_large_slices(&data, &plaintext));
        }
    }

    #[tokio::test]
    async fn randomized_crypter_reader_aes_cbc() {
        let size = 500 * 1024 * 1024;
        let max_read_size = 20 * 1024 * 1024;
        let material = ContentCryptoMaterial::generate(CryptoScheme::Aes128Cbc);
        let data: Vec<u8> = (0..size).map(|n| (n % 256) as u8).collect();

        for _i in 0..50 {
            let ciphertext = random_crypter_reader_reads(&data, Mode::Encrypt, &material, max_read_size).await;

            let plaintext = decrypt(&ciphertext, &material).unwrap();
            assert!(compare_large_slices(&data, &plaintext));

            let plaintext = random_crypter_reader_reads(&ciphertext, Mode::Decrypt, &material, max_read_size).await;
            assert!(compare_large_slices(&data, &plaintext));
        }
    }
    #[tokio::test]
    async fn randomized_crypter_writer_aes_gcm() {
        let size = 100 * 1024 * 1024;
        let max_write_size = 20 * 1024 * 1024;
        let material = ContentCryptoMaterial::generate(CryptoScheme::Aes256Gcm);
        let data: Vec<u8> = (0..size).map(|n| (n % 256) as u8).collect();

        for _i in 0..50 {
            let ciphertext = random_crypter_writer_writes(&data, Mode::Encrypt, &material, max_write_size, 0.1).await;

            let plaintext = decrypt(&ciphertext, &material).unwrap();
            assert!(compare_large_slices(&data, &plaintext));

            let plaintext = random_crypter_writer_writes(&ciphertext, Mode::Decrypt, &material, max_write_size, 0.1).await;
            assert!(compare_large_slices(&data, &plaintext));
        }
    }

    #[tokio::test]
    async fn randomized_crypter_reader_aes_gcm() {
        let size = 100 * 1024 * 1024;
        let max_read_size = 20 * 1024 * 1024;
        let material = ContentCryptoMaterial::generate(CryptoScheme::Aes256Gcm)
            .with_aad("AES/GCM/NoPadding");
        let data: Vec<u8> = (0..size).map(|n| (n % 256) as u8).collect();

        for _i in 0..50 {
            let ciphertext = random_crypter_reader_reads(&data, Mode::Encrypt, &material, max_read_size).await;

            let ciphertext2 = encrypt(&data, &material).unwrap();
            assert!(compare_large_slices(&ciphertext, &ciphertext2));

            let plaintext = decrypt(&ciphertext, &material).unwrap();
            assert!(compare_large_slices(&data, &plaintext));

            let plaintext = random_crypter_reader_reads(&ciphertext, Mode::Decrypt, &material, max_read_size).await;
            assert!(compare_large_slices(&data, &plaintext));
        }
    }
}
