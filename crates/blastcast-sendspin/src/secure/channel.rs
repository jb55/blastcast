//! The encrypted channel after the handshake: every Sendspin message is a
//! websocket binary frame holding one Noise transport ciphertext, whose
//! plaintext starts with a message type byte (`messaging.md`, Binary
//! Message ID Structure).
//!
//! Type 0 is a JSON message, which this layer turns back into the text
//! [`Frame`] the rest of the server already speaks, so the read and write
//! loops don't know whether a connection is encrypted. Type 1 is a
//! fragment of a message too long for one Noise message; any other type
//! (audio, artwork) passes through as a binary frame, type byte first.
//! Ping, pong and close stay websocket control frames, unencrypted.

use std::fmt::Display;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};

use super::noise::{CipherState, MAX_MESSAGE, NoiseError, TAG_LEN};
use crate::connection::Frame;

/// Type byte of a JSON message.
pub const JSON: u8 = 0;
/// Type byte of a fragment.
pub const FRAGMENT: u8 = 1;
/// The most plaintext, type byte included, one Noise message holds.
pub const MAX_PLAINTEXT: usize = MAX_MESSAGE - TAG_LEN;
const FIRST: u8 = 0b10;
const LAST: u8 = 0b01;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChannelError {
    #[error("{0}")]
    Noise(#[from] NoiseError),
    #[error("empty plaintext")]
    Empty,
    #[error("a JSON message is not UTF-8")]
    NotUtf8,
    #[error("a text frame after the handshake")]
    Cleartext,
    #[error("malformed fragment sequence: {0}")]
    Fragment(&'static str),
    #[error("reassembled message longer than {0} bytes")]
    TooLong(usize),
}

/// Encrypts outgoing messages, splitting any too long for one frame.
#[derive(Debug)]
pub struct Sealer {
    cipher: CipherState,
}

impl Sealer {
    pub fn new(cipher: CipherState) -> Self {
        Self { cipher }
    }

    /// The ciphertexts of one message, `plaintext` being its type byte
    /// and payload. One unless it needs fragments.
    pub fn seal(&mut self, plaintext: &[u8]) -> Result<Vec<Bytes>, NoiseError> {
        if plaintext.len() <= MAX_PLAINTEXT {
            return Ok(vec![self.cipher.encrypt(&[], plaintext)?.into()]);
        }
        let (orig_type, mut data) = (plaintext[0], &plaintext[1..]);
        let mut out = Vec::new();
        let mut first = true;
        while first || !data.is_empty() {
            let header = if first { 3 } else { 2 };
            let take = data.len().min(MAX_PLAINTEXT - header);
            let mut flags = if first { FIRST } else { 0 };
            if take == data.len() {
                flags |= LAST;
            }
            let mut frame = Vec::with_capacity(header + take);
            frame.extend_from_slice(&[FRAGMENT, flags]);
            if first {
                frame.push(orig_type);
            }
            frame.extend_from_slice(&data[..take]);
            out.push(self.cipher.encrypt(&[], &frame)?.into());
            data = &data[take..];
            first = false;
        }
        Ok(out)
    }

    /// Seals a [`Frame`]: text as a type 0 message, binary as it is.
    /// Control frames pass through.
    pub fn seal_frame(&mut self, frame: Frame) -> Result<Vec<Frame>, NoiseError> {
        let sealed = match frame {
            Frame::Text(text) => {
                let mut plain = Vec::with_capacity(1 + text.len());
                plain.push(JSON);
                plain.extend_from_slice(text.as_bytes());
                self.seal(&plain)?
            }
            Frame::Binary(data) if !data.is_empty() => self.seal(&data)?,
            Frame::Binary(_) => return Ok(Vec::new()),
            other => return Ok(vec![other]),
        };
        Ok(sealed.into_iter().map(Frame::Binary).collect())
    }
}

/// Decrypts incoming frames and puts fragments back together.
#[derive(Debug)]
pub struct Opener {
    cipher: CipherState,
    /// The type and data so far of a fragmented message in flight.
    partial: Option<(u8, Vec<u8>)>,
    max_len: usize,
}

impl Opener {
    /// `max_len` bounds a reassembled message.
    pub fn new(cipher: CipherState, max_len: usize) -> Self {
        Self {
            cipher,
            partial: None,
            max_len,
        }
    }

    /// One ciphertext in; a whole message (type byte first) out, or
    /// `None` while a fragmented one is still arriving.
    pub fn open(&mut self, ciphertext: &[u8]) -> Result<Option<Vec<u8>>, ChannelError> {
        let plain = self.cipher.decrypt(&[], ciphertext)?;
        let &kind = plain.first().ok_or(ChannelError::Empty)?;
        if kind != FRAGMENT {
            if self.partial.is_some() {
                return Err(ChannelError::Fragment(
                    "a whole message inside a fragmented one",
                ));
            }
            return Ok(Some(plain));
        }
        let &flags = plain.get(1).ok_or(ChannelError::Fragment("no flags"))?;
        if flags & !(FIRST | LAST) != 0 {
            return Err(ChannelError::Fragment("reserved flag bits set"));
        }
        let data = if flags & FIRST != 0 {
            if self.partial.is_some() {
                return Err(ChannelError::Fragment(
                    "a first fragment inside another message",
                ));
            }
            let &orig = plain
                .get(2)
                .ok_or(ChannelError::Fragment("no original type"))?;
            if orig == FRAGMENT {
                return Err(ChannelError::Fragment("original type 1"));
            }
            self.partial = Some((orig, Vec::new()));
            &plain[3..]
        } else {
            &plain[2..]
        };
        let (orig, buf) = self.partial.as_mut().ok_or(ChannelError::Fragment(
            "a fragment with no message in flight",
        ))?;
        if buf.len() + data.len() + 1 > self.max_len {
            return Err(ChannelError::TooLong(self.max_len));
        }
        buf.extend_from_slice(data);
        if flags & LAST == 0 {
            return Ok(None);
        }
        let mut whole = Vec::with_capacity(buf.len() + 1);
        whole.push(*orig);
        whole.append(buf);
        self.partial = None;
        Ok(Some(whole))
    }

    /// Opens a [`Frame`]: a JSON message comes out as text, anything else
    /// as binary with its type byte. Control frames pass through; a text
    /// frame is an error, since after the handshake everything is
    /// encrypted.
    pub fn open_frame(&mut self, frame: Frame) -> Result<Option<Frame>, ChannelError> {
        match frame {
            Frame::Binary(data) => self.open(&data)?.map(decoded).transpose(),
            Frame::Text(_) => Err(ChannelError::Cleartext),
            other => Ok(Some(other)),
        }
    }
}

fn decoded(plain: Vec<u8>) -> Result<Frame, ChannelError> {
    if plain[0] == JSON {
        let text = String::from_utf8(plain[1..].to_vec()).map_err(|_| ChannelError::NotUtf8)?;
        Ok(Frame::Text(text))
    } else {
        Ok(Frame::Binary(plain.into()))
    }
}

/// A websocket stream with its frames decrypted. Any decryption or
/// framing error is the stream's last item: the connection is over.
pub struct Decrypting<St> {
    inner: St,
    opener: Opener,
    failed: bool,
}

impl<St> Decrypting<St> {
    pub fn new(inner: St, opener: Opener) -> Self {
        Self {
            inner,
            opener,
            failed: false,
        }
    }
}

impl<St, E> Stream for Decrypting<St>
where
    St: Stream<Item = Result<Frame, E>> + Unpin,
    E: Display,
{
    type Item = Result<Frame, String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.failed {
                return Poll::Ready(None);
            }
            let frame = match ready!(self.inner.poll_next_unpin(cx)) {
                None => return Poll::Ready(None),
                Some(Err(e)) => return Poll::Ready(Some(Err(e.to_string()))),
                Some(Ok(frame)) => frame,
            };
            match self.opener.open_frame(frame) {
                Ok(Some(frame)) => return Poll::Ready(Some(Ok(frame))),
                Ok(None) => continue,
                Err(e) => {
                    self.failed = true;
                    return Poll::Ready(Some(Err(format!("encrypted channel: {e}"))));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secure::noise::{Handshake, Suite, Transport};
    use x25519_dalek::{PublicKey, StaticSecret};

    fn pair() -> (Transport, Transport) {
        let (s, c) = (StaticSecret::from([1; 32]), StaticSecret::from([2; 32]));
        let (s_pub, c_pub) = (
            PublicKey::from(&s).to_bytes(),
            PublicKey::from(&c).to_bytes(),
        );
        let suite = Suite::ChaChaPoly;
        let mut server =
            Handshake::initiator(suite, b"", s, c_pub, StaticSecret::from([3; 32]), [5; 32]);
        let mut client = Handshake::responder(suite, b"", c, s_pub, StaticSecret::from([4; 32]));
        client.read(&server.write(b"{}").unwrap()).unwrap();
        client.set_psk([5; 32]);
        server.read(&client.write(b"{}").unwrap()).unwrap();
        (server.finish().unwrap(), client.finish().unwrap())
    }

    #[test]
    fn json_and_binary_round_trip() {
        let (s, c) = pair();
        let mut sealer = Sealer::new(s.send);
        let mut opener = Opener::new(c.recv, 1 << 20);
        for frame in [
            Frame::Text(r#"{"type":"server/hello"}"#.into()),
            Frame::Binary(Bytes::from_static(&[4, 1, 2, 3])),
        ] {
            let sealed = sealer.seal_frame(frame.clone()).unwrap();
            assert_eq!(sealed.len(), 1);
            let Frame::Binary(ct) = &sealed[0] else {
                panic!()
            };
            assert_eq!(ct.len(), 16 + frame_len(&frame));
            assert_eq!(opener.open_frame(sealed[0].clone()).unwrap(), Some(frame));
        }
        assert_eq!(sealer.seal_frame(Frame::Close).unwrap(), [Frame::Close]);
        assert_eq!(
            opener.open_frame(Frame::Text("x".into())),
            Err(ChannelError::Cleartext)
        );
    }

    fn frame_len(f: &Frame) -> usize {
        match f {
            Frame::Text(t) => 1 + t.len(),
            Frame::Binary(b) => b.len(),
            _ => 0,
        }
    }

    #[test]
    fn long_messages_are_fragmented_and_reassembled() {
        let (s, c) = pair();
        let mut sealer = Sealer::new(s.send);
        let mut opener = Opener::new(c.recv, 1 << 20);
        let mut big = vec![8u8];
        big.extend((0..150_000).map(|i| i as u8));
        let frames = sealer.seal(&big).unwrap();
        assert_eq!(frames.len(), 3);
        assert!(frames.iter().all(|f| f.len() <= MAX_MESSAGE));
        assert_eq!(opener.open(&frames[0]).unwrap(), None);
        assert_eq!(opener.open(&frames[1]).unwrap(), None);
        assert_eq!(opener.open(&frames[2]).unwrap(), Some(big));

        // Exactly one frame's worth isn't fragmented.
        let fits = vec![0u8; MAX_PLAINTEXT];
        assert_eq!(sealer.seal(&fits).unwrap().len(), 1);
        let one_more = vec![0u8; MAX_PLAINTEXT + 1];
        let frames = sealer.seal(&one_more).unwrap();
        assert_eq!(frames.len(), 2);
    }

    #[test]
    fn malformed_fragment_sequences_are_errors() {
        let (s, c) = pair();
        let mut send = s.send;
        let mut opener = Opener::new(c.recv, 1 << 20);
        let mut put = |plain: &[u8]| send.encrypt(&[], plain).unwrap();
        // A continuation with nothing in flight.
        assert!(matches!(
            opener.open(&put(&[1, 0, 9])),
            Err(ChannelError::Fragment(_))
        ));

        let (s, c) = pair();
        let mut send = s.send;
        let mut opener = Opener::new(c.recv, 1 << 20);
        let mut put = |plain: &[u8]| send.encrypt(&[], plain).unwrap();
        assert_eq!(opener.open(&put(&[1, FIRST, 0, b'{'])).unwrap(), None);
        // A whole message while one is in flight.
        assert!(matches!(
            opener.open(&put(&[0, b'x'])),
            Err(ChannelError::Fragment(_))
        ));

        let (s, c) = pair();
        let mut send = s.send;
        let mut opener = Opener::new(c.recv, 1 << 20);
        assert!(matches!(
            opener.open(&send.encrypt(&[], &[1, FIRST | 4, 0]).unwrap()),
            Err(ChannelError::Fragment(_))
        ));
        assert!(matches!(
            opener.open(&send.encrypt(&[], &[1, FIRST, 1]).unwrap()),
            Err(ChannelError::Fragment(_))
        ));
    }

    #[test]
    fn reassembly_is_bounded() {
        let (s, c) = pair();
        let mut sealer = Sealer::new(s.send);
        let mut opener = Opener::new(c.recv, 100_000);
        let big = vec![0u8; 150_000];
        let frames = sealer.seal(&big).unwrap();
        assert_eq!(opener.open(&frames[0]).unwrap(), None);
        assert_eq!(opener.open(&frames[1]), Err(ChannelError::TooLong(100_000)));
    }
}
