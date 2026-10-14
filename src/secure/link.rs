//! A connection during its setup, on either side: cleartext JSON until
//! the handshake, encrypted JSON after it, one message at a time. Once set
//! up it splits into the stream and sink the read and write loops use.

use std::collections::VecDeque;
use std::fmt::Display;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::time::timeout;

use super::channel::{Decrypting, Opener, Sealer};
use super::noise::Transport;
use crate::connection::Frame;

/// The longest message accepted, reassembled, during and after setup.
pub(crate) const MAX_LEN: usize = 1 << 20;

/// Why a link stopped. Each closes the connection without a word, as the
/// spec asks of every handshake and pairing failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LinkError {
    #[error("nothing within {0:?}")]
    Timeout(Duration),
    #[error("closed")]
    Closed,
    #[error("read failed: {0}")]
    Read(String),
    #[error("write failed: {0}")]
    Write(String),
    #[error("{0}")]
    Protocol(String),
}

impl LinkError {
    pub fn protocol(what: impl Display) -> Self {
        LinkError::Protocol(what.to_string())
    }
}

/// One message: its type, its payload, and the exact text it came as.
#[derive(Debug, Clone)]
pub struct Message {
    pub kind: String,
    pub payload: Value,
    pub raw: String,
}

impl Message {
    pub fn parse(raw: String) -> Result<Self, LinkError> {
        #[derive(serde::Deserialize)]
        struct Envelope {
            #[serde(rename = "type")]
            kind: String,
            #[serde(default)]
            payload: Value,
        }
        let env: Envelope = serde_json::from_str(&raw)
            .map_err(|e| LinkError::protocol(format!("not a message: {e}")))?;
        Ok(Self {
            kind: env.kind,
            payload: env.payload,
            raw,
        })
    }

    /// The payload as `T`; a missing payload reads as `{}`.
    pub fn payload<T: DeserializeOwned>(&self) -> Result<T, LinkError> {
        let payload = match &self.payload {
            Value::Null => Value::Object(Default::default()),
            v => v.clone(),
        };
        serde_json::from_value(payload)
            .map_err(|e| LinkError::protocol(format!("bad {} payload: {e}", self.kind)))
    }
}

/// `{"type": kind, "payload": payload}`.
pub fn envelope(kind: &str, payload: impl Serialize) -> String {
    serde_json::json!({ "type": kind, "payload": payload }).to_string()
}

pub struct Link<St, Si> {
    stream: St,
    sink: Si,
    /// Set once the handshake is done.
    channel: Option<(Sealer, Opener)>,
    handshake_hash: [u8; 32],
}

impl<St, Si, E> Link<St, Si>
where
    St: Stream<Item = Result<Frame, E>> + Unpin,
    E: Display,
    Si: Sink<Frame> + Unpin,
    Si::Error: Display,
{
    pub fn new(stream: St, sink: Si) -> Self {
        Self {
            stream,
            sink,
            channel: None,
            handshake_hash: [0; 32],
        }
    }

    /// Switches to (new) transport keys.
    pub fn secure(&mut self, transport: Transport) {
        self.handshake_hash = transport.handshake_hash;
        self.channel = Some((
            Sealer::new(transport.send),
            Opener::new(transport.recv, MAX_LEN),
        ));
    }

    pub fn is_secure(&self) -> bool {
        self.channel.is_some()
    }

    /// The last handshake's `h`.
    pub fn handshake_hash(&self) -> [u8; 32] {
        self.handshake_hash
    }

    /// Sends a message: as a text frame before the handshake, encrypted
    /// after it.
    pub async fn send(&mut self, kind: &str, payload: impl Serialize) -> Result<String, LinkError> {
        let text = envelope(kind, payload);
        self.send_raw(text.clone()).await?;
        Ok(text)
    }

    pub async fn send_raw(&mut self, text: String) -> Result<(), LinkError> {
        let frames = match &mut self.channel {
            Some((sealer, _)) => sealer
                .seal_frame(Frame::Text(text))
                .map_err(LinkError::protocol)?,
            None => vec![Frame::Text(text)],
        };
        for frame in frames {
            self.sink
                .send(frame)
                .await
                .map_err(|e| LinkError::Write(e.to_string()))?;
        }
        Ok(())
    }

    pub async fn send_frame(&mut self, frame: Frame) -> Result<(), LinkError> {
        self.sink
            .send(frame)
            .await
            .map_err(|e| LinkError::Write(e.to_string()))
    }

    /// The next message, within `wait`. Pings and pongs are skipped.
    pub async fn recv(&mut self, wait: Duration) -> Result<Message, LinkError> {
        match timeout(wait, self.recv_any()).await {
            Ok(r) => r,
            Err(_) => Err(LinkError::Timeout(wait)),
        }
    }

    /// The next message, however long it takes. Cancel-safe while no
    /// fragmented message is half read.
    pub async fn recv_any(&mut self) -> Result<Message, LinkError> {
        loop {
            match self.recv_frame().await? {
                Frame::Text(text) => return Message::parse(text),
                Frame::Binary(data) => {
                    tracing::debug!(
                        kind = data.first(),
                        "ignoring a binary message during setup"
                    );
                }
                Frame::Ping(_) | Frame::Pong(_) | Frame::Close => {}
            }
        }
    }

    /// The next text or binary frame, decrypted once the link is secure:
    /// a JSON message as text, anything else as binary, type byte first.
    pub async fn recv_frame(&mut self) -> Result<Frame, LinkError> {
        loop {
            let frame = match self.stream.next().await {
                None | Some(Ok(Frame::Close)) => return Err(LinkError::Closed),
                Some(Err(e)) => return Err(LinkError::Read(e.to_string())),
                Some(Ok(frame)) => frame,
            };
            let frame = match (&mut self.channel, frame) {
                (_, Frame::Ping(_) | Frame::Pong(_)) => continue,
                (_, Frame::Close) => return Err(LinkError::Closed),
                (None, Frame::Text(text)) => Frame::Text(text),
                (None, Frame::Binary(_)) => {
                    return Err(LinkError::protocol("a binary frame before the handshake"));
                }
                (Some((_, opener)), frame) => {
                    match opener.open_frame(frame).map_err(LinkError::protocol)? {
                        Some(frame) => frame,
                        None => continue,
                    }
                }
            };
            return Ok(frame);
        }
    }

    /// Closes the socket without a message.
    pub async fn close(mut self) {
        let _ = timeout(Duration::from_secs(2), async {
            let _ = self.sink.send(Frame::Close).await;
            let _ = self.sink.close().await;
        })
        .await;
    }

    /// The encrypted stream and sink, for the read and write loops.
    pub fn split(self) -> (Decrypting<St>, Encrypting<Si>) {
        let (sealer, opener) = self.channel.expect("split after the handshake");
        (
            Decrypting::new(self.stream, opener),
            Encrypting::new(self.sink, sealer),
        )
    }
}

/// A websocket sink that encrypts each [`Frame`] written to it. A frame
/// that can't be sealed (the nonce ran out) becomes a close.
pub struct Encrypting<Si> {
    inner: Si,
    sealer: Sealer,
    pending: VecDeque<Frame>,
}

impl<Si> Encrypting<Si> {
    pub fn new(inner: Si, sealer: Sealer) -> Self {
        Self {
            inner,
            sealer,
            pending: VecDeque::new(),
        }
    }
}

impl<Si: Sink<Frame> + Unpin> Encrypting<Si> {
    fn drain(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Si::Error>> {
        while let Some(frame) = self.pending.pop_front() {
            if self.inner.poll_ready_unpin(cx)?.is_pending() {
                self.pending.push_front(frame);
                return Poll::Pending;
            }
            self.inner.start_send_unpin(frame)?;
        }
        Poll::Ready(Ok(()))
    }
}

impl<Si: Sink<Frame> + Unpin> Sink<Frame> for Encrypting<Si> {
    type Error = Si::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.drain(cx))?;
        self.inner.poll_ready_unpin(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, frame: Frame) -> Result<(), Self::Error> {
        let frames = self.sealer.seal_frame(frame).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "can't encrypt a frame; closing");
            vec![Frame::Close]
        });
        self.pending.extend(frames);
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.drain(cx))?;
        self.inner.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.drain(cx))?;
        self.inner.poll_close_unpin(cx)
    }
}
