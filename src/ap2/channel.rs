//! The encrypted channel a paired AirPlay 2 connection switches to.
//!
//! After the reply that finishes pairing, both ways are blocks of
//! `[length: u16 LE][ciphertext][16-byte Poly1305 tag]`, at most 1024
//! plaintext bytes each, under ChaCha20-Poly1305. The 2-byte length is the
//! AAD. The nonce is four zero bytes and a 64-bit little-endian count of
//! the blocks sent that way so far.
//!
//! The keys are HKDF-SHA512 of the pairing's shared secret: SRP's `K`
//! after transient pair-setup, X25519's after pair-verify. The sender
//! writes with `Control-Write-Encryption-Key` and reads with
//! `Control-Read-Encryption-Key`, both salted `Control-Salt`; the event
//! channel's keys are the same with `Events`.
//!
//! [`SecureRead`] decrypts under the RTSP reader
//! ([`Request::read`](crate::raop::rtsp::Request::read)), so a request is
//! read the same way encrypted or not.
//!
//! Ported from `openairplay2` 0.5's cipher.rs and crypto_stream.rs (MIT,
//! Copyright (c) 2026 Stefan Arentz).

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Nonce};
use hmac::{Hmac, Mac};
use sha2::Sha512;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf};

/// The most plaintext in one block.
const BLOCK: usize = 1024;
const TAG: usize = 16;

/// HKDF-SHA512 (RFC 5869) to one 32-byte key: one HMAC to extract, one to
/// expand, since 32 bytes is less than a hash.
pub fn hkdf_sha512(secret: &[u8], salt: &[u8], info: &[u8]) -> [u8; 32] {
    let mut extract = <Hmac<Sha512> as Mac>::new_from_slice(salt).expect("HMAC takes any key");
    extract.update(secret);
    let prk = extract.finalize().into_bytes();
    let mut expand = <Hmac<Sha512> as Mac>::new_from_slice(&prk).expect("HMAC takes any key");
    expand.update(info);
    expand.update(&[1]);
    expand.finalize().into_bytes()[..32]
        .try_into()
        .expect("32 bytes")
}

/// Which end of the connection this is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// The receiver: us.
    Accessory,
    /// The sender: a phone, or the test sender.
    Controller,
}

/// What pairing agreed on.
#[derive(Clone)]
pub struct Keys {
    secret: Vec<u8>,
}

impl std::fmt::Debug for Keys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keys").finish_non_exhaustive()
    }
}

impl Keys {
    pub fn new(secret: &[u8]) -> Self {
        Self {
            secret: secret.to_vec(),
        }
    }

    /// The RTSP connection's two halves, for `role`'s end.
    pub fn control(&self, role: Role) -> (Sealer, Opener) {
        self.pair(role, "Control")
    }

    /// The event channel's two halves, for `role`'s end, on the same
    /// convention as [`control`](Self::control): the controller writes with
    /// the `Write` key. Nothing has checked that against a real sender
    /// yet; headway:tune-assistant/language-guilt-wheat will.
    pub fn events(&self, role: Role) -> (Sealer, Opener) {
        self.pair(role, "Events")
    }

    fn pair(&self, role: Role, channel: &str) -> (Sealer, Opener) {
        let salt = format!("{channel}-Salt");
        let key = |way: &str| {
            let info = format!("{channel}-{way}-Encryption-Key");
            let key = hkdf_sha512(&self.secret, salt.as_bytes(), info.as_bytes());
            ChaCha20Poly1305::new(&key.into())
        };
        let (seal, open) = match role {
            Role::Controller => ("Write", "Read"),
            Role::Accessory => ("Read", "Write"),
        };
        (
            Sealer {
                cipher: key(seal),
                count: 0,
            },
            Opener {
                cipher: key(open),
                count: 0,
            },
        )
    }
}

fn nonce(count: u64) -> Nonce {
    let mut nonce = [0u8; 12];
    nonce[4..].copy_from_slice(&count.to_le_bytes());
    nonce.into()
}

/// Encrypts one way.
pub struct Sealer {
    cipher: ChaCha20Poly1305,
    count: u64,
}

impl Sealer {
    /// `plain` as blocks.
    pub fn seal(&mut self, plain: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(plain.len() + plain.len().div_ceil(BLOCK) * (2 + TAG));
        for block in plain.chunks(BLOCK) {
            let aad = (block.len() as u16).to_le_bytes();
            let sealed = self
                .cipher
                .encrypt(
                    &nonce(self.count),
                    Payload {
                        msg: block,
                        aad: &aad,
                    },
                )
                .expect("a block is far below ChaCha20's limit");
            self.count += 1;
            out.extend_from_slice(&aad);
            out.extend_from_slice(&sealed);
        }
        out
    }
}

/// Decrypts the other way.
pub struct Opener {
    cipher: ChaCha20Poly1305,
    count: u64,
}

impl Opener {
    /// Takes the first block off `wire` and decrypts it: `None` until a
    /// whole block is there, an error if it doesn't authenticate.
    pub fn open(&mut self, wire: &mut Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        let Some(len) = wire.get(..2) else {
            return Ok(None);
        };
        let len = usize::from(u16::from_le_bytes([len[0], len[1]]));
        if len > BLOCK {
            return Err(invalid("an encrypted block is too long"));
        }
        let end = 2 + len + TAG;
        if wire.len() < end {
            return Ok(None);
        }
        let plain = self
            .cipher
            .decrypt(
                &nonce(self.count),
                Payload {
                    msg: &wire[2..end],
                    aad: &wire[..2],
                },
            )
            .map_err(|_| invalid("an encrypted block didn't authenticate"))?;
        self.count += 1;
        wire.drain(..end);
        Ok(Some(plain))
    }
}

fn invalid(what: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what)
}

/// Reads `R` as it is, until [`encrypt`](Self::encrypt), then decrypted.
pub struct SecureRead<R> {
    inner: R,
    opener: Option<Opener>,
    /// Ciphertext short of a whole block.
    wire: Vec<u8>,
    /// Plaintext not read yet, from `at`.
    plain: Vec<u8>,
    at: usize,
}

impl<R> SecureRead<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            opener: None,
            wire: Vec::new(),
            plain: Vec::new(),
            at: 0,
        }
    }

    /// Decrypts from here on. `early` is what was read past the last
    /// plaintext message, such as what a `BufReader` above holds, which is
    /// already ciphertext.
    pub fn encrypt(&mut self, opener: Opener, early: Vec<u8>) {
        self.opener = Some(opener);
        self.wire = early;
    }

    pub fn is_encrypted(&self) -> bool {
        self.opener.is_some()
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for SecureRead<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let Some(opener) = &mut this.opener else {
            return Pin::new(&mut this.inner).poll_read(cx, buf);
        };
        loop {
            if this.at < this.plain.len() {
                let n = buf.remaining().min(this.plain.len() - this.at);
                buf.put_slice(&this.plain[this.at..this.at + n]);
                this.at += n;
                return Poll::Ready(Ok(()));
            }
            if let Some(plain) = opener.open(&mut this.wire)? {
                (this.plain, this.at) = (plain, 0);
                continue;
            }
            let mut chunk = [0u8; 4096];
            let mut read = ReadBuf::new(&mut chunk);
            ready!(Pin::new(&mut this.inner).poll_read(cx, &mut read))?;
            if read.filled().is_empty() {
                return Poll::Ready(if this.wire.is_empty() {
                    Ok(())
                } else {
                    Err(io::ErrorKind::UnexpectedEof.into())
                });
            }
            this.wire.extend_from_slice(read.filled());
        }
    }
}

/// Decrypts what `read` reads from here on, what it holds already
/// included.
pub fn encrypt_reader<R: AsyncRead + Unpin>(read: &mut BufReader<SecureRead<R>>, opener: Opener) {
    let early = read.buffer().to_vec();
    read.consume(early.len());
    read.get_mut().encrypt(opener, early);
}

/// Writes to `W` as it is, until [`encrypt`](Self::encrypt), then sealed.
pub struct SecureWrite<W> {
    inner: W,
    sealer: Option<Sealer>,
}

impl<W: AsyncWrite + Unpin> SecureWrite<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            sealer: None,
        }
    }

    pub fn encrypt(&mut self, sealer: Sealer) {
        self.sealer = Some(sealer);
    }

    /// Writes one message whole.
    pub async fn send(&mut self, message: &[u8]) -> io::Result<()> {
        match &mut self.sealer {
            Some(sealer) => self.inner.write_all(&sealer.seal(message)).await,
            None => self.inner.write_all(message).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::raop::rtsp::Request;

    fn hex(b: &[u8]) -> String {
        b.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// shairplay's vectors (made with OpenSSL), and ours for the event
    /// and pair-verify labels (made with Python's hmac): the secret is
    /// bytes 0 to 63.
    #[test]
    fn hkdf_matches_the_vectors() {
        let secret: Vec<u8> = (0..64).collect();
        let cases = [
            (
                "Control-Salt",
                "Control-Write-Encryption-Key",
                "5a6cb19bcbe7d4df2dd8279f39562f7fae2dbf73eb5a4f98849c245c82b2fe96",
            ),
            (
                "Control-Salt",
                "Control-Read-Encryption-Key",
                "c2f56f1912e2fc1f6604fd5bccab619272f687345aa4bce1c3c857421bd3b821",
            ),
            (
                "Events-Salt",
                "Events-Write-Encryption-Key",
                "4ab337f2edc98d6a5dcdebc107b1aa174f3b24908a3780294fc94dc6983ee353",
            ),
            (
                "Events-Salt",
                "Events-Read-Encryption-Key",
                "11954fc3044c580a9a64ed0b699039dfd0e7dd476eb78366ae160f2962708e18",
            ),
            (
                "Pair-Verify-Encrypt-Salt",
                "Pair-Verify-Encrypt-Info",
                "8f806335e1847b59c5004b131c4689e951ce9808993aed540856475e0dee353b",
            ),
            (
                "Pair-Setup-Encrypt-Salt",
                "Pair-Setup-Encrypt-Info",
                "b6335536162d6629e4c0bade85f1b712c85a364bab0dedb25014cfb814489273",
            ),
        ];
        for (salt, info, key) in cases {
            assert_eq!(
                hex(&hkdf_sha512(&secret, salt.as_bytes(), info.as_bytes())),
                key,
                "{info}"
            );
        }
    }

    #[test]
    fn each_end_opens_what_the_other_seals() {
        let keys = Keys::new(&[0x11; 64]);
        let (mut phone_seal, mut phone_open) = keys.control(Role::Controller);
        let (mut our_seal, mut our_open) = keys.control(Role::Accessory);
        // Three blocks, then one.
        let big: Vec<u8> = (0..2500).map(|i| i as u8).collect();
        let mut wire = phone_seal.seal(&big);
        assert_eq!(wire.len(), 2500 + 3 * (2 + TAG));
        let mut got = Vec::new();
        while let Some(plain) = our_open.open(&mut wire).unwrap() {
            got.extend(plain);
        }
        assert_eq!((got, wire.len()), (big, 0));
        let mut wire = our_seal.seal(b"RTSP/1.0 200 OK\r\n\r\n");
        assert_eq!(
            phone_open.open(&mut wire).unwrap().unwrap(),
            b"RTSP/1.0 200 OK\r\n\r\n"
        );
        // The event channel's keys are its own.
        let (mut events, _) = keys.events(Role::Controller);
        let mut wire = events.seal(b"x");
        assert!(our_open.open(&mut wire).is_err());
    }

    #[test]
    fn a_tampered_block_is_refused() {
        let keys = Keys::new(&[0x22; 64]);
        let (mut seal, _) = keys.control(Role::Controller);
        let (_, mut open) = keys.control(Role::Accessory);
        let mut wire = seal.seal(b"SETUP");
        wire[3] ^= 1;
        assert_eq!(
            open.open(&mut wire).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[tokio::test]
    async fn the_rtsp_reader_reads_through_it() {
        let keys = Keys::new(&[0x33; 64]);
        let (mut seal, _) = keys.control(Role::Controller);
        let (_, open) = keys.control(Role::Accessory);
        let mut wire = b"GET /info RTSP/1.0\r\nCSeq: 1\r\n\r\n".to_vec();
        let encrypted =
            seal.seal(b"POST /fp-setup RTSP/1.0\r\nCSeq: 2\r\nContent-Length: 3\r\n\r\nabc");
        wire.extend_from_slice(&encrypted);
        let (mut phone, ours) = tokio::io::duplex(64);
        let writer = tokio::spawn(async move {
            // A few bytes at a time, so blocks arrive split.
            for chunk in wire.chunks(7) {
                phone.write_all(chunk).await.unwrap();
            }
        });
        let mut read = BufReader::new(SecureRead::new(ours));
        let first = Request::read(&mut read).await.unwrap().unwrap();
        assert_eq!(first.uri, "/info");
        encrypt_reader(&mut read, open);
        let second = Request::read(&mut read).await.unwrap().unwrap();
        assert_eq!(
            (second.uri.as_str(), &second.body[..]),
            ("/fp-setup", &b"abc"[..])
        );
        writer.await.unwrap();
        assert_eq!(Request::read(&mut read).await.unwrap(), None);
        let mut rest = Vec::new();
        assert_eq!(read.read_to_end(&mut rest).await.unwrap(), 0);
    }
}
