//! One client connection after its hello: what the client said it is, the
//! queues that feed its socket, and the writer task that drains them.
//!
//! Control messages and audio have separate queues, and the writer always
//! takes a waiting control message first, so a `server/time` reply never
//! sits behind a second of audio. Messages are serialized in the writer, at
//! the moment they are written, not when they are queued.

use std::fmt::Display;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use futures_util::{Sink, SinkExt};
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, MissedTickBehavior, timeout};

use crate::protocol::{
    ClientHello, ConnectionReason, GoodbyeReason, HeaderLayout, Role, ServerMessage,
};

/// Control messages that may wait for the writer. A client that lets this
/// many pile up isn't reading, and is disconnected.
const CONTROL_QUEUE: usize = 256;
/// Binary frames that may wait for the writer: about 5 s of 20 ms chunks.
const AUDIO_QUEUE: usize = 256;

/// A websocket frame, whatever websocket library carries it: the listener
/// adapts axum's messages to these, and a dialer adapts its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Text(String),
    Binary(Bytes),
    Ping(Bytes),
    Pong(Bytes),
    Close,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SendError {
    #[error("the connection is closed")]
    Closed,
    #[error("the client hasn't sent its first client/state yet")]
    NotReady,
    #[error("the send queue is full")]
    Full,
}

/// A connected client. The server holds one per `client_id`; protocol code
/// (time sync, streaming, groups) sends to it through this.
#[derive(Debug)]
pub struct Connection {
    hello: ClientHello,
    active_roles: Vec<String>,
    reason: ConnectionReason,
    layout: Mutex<HeaderLayout>,
    control: mpsc::Sender<ServerMessage>,
    audio: mpsc::Sender<Bytes>,
    /// Set by the first `client/state`. Audio waits for it.
    ready: AtomicBool,
    goodbye: Mutex<Option<GoodbyeReason>>,
    closed: watch::Sender<bool>,
}

/// The receiving ends of a connection's queues, for its writer.
pub(crate) struct Queues {
    control: mpsc::Receiver<ServerMessage>,
    audio: mpsc::Receiver<Bytes>,
}

impl Connection {
    pub(crate) fn new(
        hello: ClientHello,
        active_roles: Vec<String>,
        reason: ConnectionReason,
    ) -> (Self, Queues) {
        let (control, control_rx) = mpsc::channel(CONTROL_QUEUE);
        let (audio, audio_rx) = mpsc::channel(AUDIO_QUEUE);
        let conn = Self {
            hello,
            active_roles,
            reason,
            // Every plaintext v1 client reads the 9-byte header; the Noise
            // handshake will switch this.
            layout: Mutex::new(HeaderLayout::Legacy),
            control,
            audio,
            ready: AtomicBool::new(false),
            goodbye: Mutex::new(None),
            closed: watch::channel(false).0,
        };
        let queues = Queues {
            control: control_rx,
            audio: audio_rx,
        };
        (conn, queues)
    }

    pub fn client_id(&self) -> &str {
        &self.hello.client_id
    }

    pub fn name(&self) -> &str {
        &self.hello.name
    }

    pub fn hello(&self) -> &ClientHello {
        &self.hello
    }

    /// The roles the server took, as the client spelled them.
    pub fn active_roles(&self) -> &[String] {
        &self.active_roles
    }

    /// Whether a role of `family` (`player`, `metadata`) is active.
    pub fn has_role(&self, family: &str) -> bool {
        self.active_roles
            .iter()
            .any(|r| Role::parse(r).family == family)
    }

    pub fn connection_reason(&self) -> ConnectionReason {
        self.reason
    }

    /// The audio chunk header this client reads.
    pub fn header_layout(&self) -> HeaderLayout {
        *self.layout.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn set_header_layout(&self, layout: HeaderLayout) {
        *self.layout.lock().unwrap_or_else(PoisonError::into_inner) = layout;
    }

    /// Whether the client has sent a `client/state`, so audio may flow.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Marks the client ready; true the first time.
    pub(crate) fn mark_ready(&self) -> bool {
        !self.ready.swap(true, Ordering::AcqRel)
    }

    /// The reason the client gave in `client/goodbye`, once it has.
    pub fn goodbye(&self) -> Option<GoodbyeReason> {
        *self.goodbye.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn set_goodbye(&self, reason: GoodbyeReason) {
        *self.goodbye.lock().unwrap_or_else(PoisonError::into_inner) = Some(reason);
    }

    /// Queues a control message. A client too far behind to take it is
    /// disconnected.
    pub fn send(&self, msg: ServerMessage) -> Result<(), SendError> {
        match self.control.try_send(msg) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(SendError::Closed),
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(
                    client_id = self.client_id(),
                    "control queue full, disconnecting"
                );
                self.close();
                Err(SendError::Full)
            }
        }
    }

    /// Queues a binary frame (an encoded audio chunk or artwork). Refused
    /// until the client's first `client/state`, and when the queue is full,
    /// which is the streamer's to handle.
    pub fn send_audio(&self, frame: Bytes) -> Result<(), SendError> {
        if self.is_closed() {
            return Err(SendError::Closed);
        }
        if !self.is_ready() {
            return Err(SendError::NotReady);
        }
        self.audio.try_send(frame).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => SendError::Full,
            mpsc::error::TrySendError::Closed(_) => SendError::Closed,
        })
    }

    /// Ends the connection: the writer sends a close frame and the read
    /// loop stops.
    pub fn close(&self) {
        self.closed.send_replace(true);
    }

    pub fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }

    pub(crate) fn closed(&self) -> watch::Receiver<bool> {
        self.closed.subscribe()
    }
}

/// Drains a connection's queues into its socket, control first, and pings
/// every `ping_interval`. Ends when the connection is closed or a write
/// fails or takes longer than `write_timeout`, closing the connection.
pub(crate) async fn write_loop<Si>(
    conn: &Connection,
    mut sink: Si,
    mut queues: Queues,
    ping_interval: Duration,
    write_timeout: Duration,
) where
    Si: Sink<Frame> + Unpin,
    Si::Error: Display,
{
    let mut closed = conn.closed();
    let mut ping = tokio::time::interval_at(Instant::now() + ping_interval, ping_interval);
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let frame = tokio::select! {
            biased;
            _ = closed.wait_for(|c| *c) => break,
            Some(msg) = queues.control.recv() => Frame::Text(msg.to_json()),
            Some(chunk) = queues.audio.recv() => Frame::Binary(chunk),
            _ = ping.tick() => Frame::Ping(Bytes::new()),
        };
        match timeout(write_timeout, sink.send(frame)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::debug!(client_id = conn.client_id(), error = %e, "write failed");
                conn.close();
                return;
            }
            Err(_) => {
                tracing::warn!(
                    client_id = conn.client_id(),
                    ?write_timeout,
                    "write timed out, disconnecting"
                );
                conn.close();
                return;
            }
        }
    }
    let _ = timeout(write_timeout, async {
        let _ = sink.send(Frame::Close).await;
        let _ = sink.close().await;
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ServerHello, StreamClear};
    use futures_util::StreamExt;

    fn conn(roles: &[&str]) -> (Connection, Queues) {
        let hello = ClientHello {
            client_id: "c1".into(),
            name: "C1".into(),
            ..Default::default()
        };
        let roles = roles.iter().map(|r| r.to_string()).collect();
        Connection::new(hello, roles, ConnectionReason::Playback)
    }

    #[test]
    fn audio_waits_for_the_first_client_state() {
        let (c, _q) = conn(&["player@v1"]);
        assert_eq!(
            c.send_audio(Bytes::from_static(b"x")),
            Err(SendError::NotReady)
        );
        assert!(c.mark_ready());
        assert!(!c.mark_ready());
        assert_eq!(c.send_audio(Bytes::from_static(b"x")), Ok(()));
        c.close();
        assert_eq!(
            c.send_audio(Bytes::from_static(b"x")),
            Err(SendError::Closed)
        );
    }

    #[test]
    fn has_role_matches_by_family() {
        let (c, _q) = conn(&["player@v1", "metadata"]);
        assert!(c.has_role("player"));
        assert!(c.has_role("metadata"));
        assert!(!c.has_role("artwork"));
        assert_eq!(c.header_layout(), HeaderLayout::Legacy);
    }

    #[test]
    fn a_full_control_queue_closes_the_connection() {
        let (c, _q) = conn(&[]);
        let msg = ServerMessage::StreamClear(StreamClear::default());
        for _ in 0..CONTROL_QUEUE {
            c.send(msg.clone()).unwrap();
        }
        assert_eq!(c.send(msg), Err(SendError::Full));
        assert!(c.is_closed());
    }

    /// With the socket backed up, a control message queued behind audio is
    /// written before the rest of that audio.
    #[tokio::test]
    async fn control_overtakes_queued_audio() {
        let (c, q) = conn(&["player@v1"]);
        c.mark_ready();
        for i in 0..5u8 {
            c.send_audio(Bytes::from(vec![i])).unwrap();
        }
        // Capacity 0 plus one sender: the writer can put one frame in
        // flight, then blocks until the test reads.
        let (sink, mut socket) = futures_channel::mpsc::channel::<Frame>(0);
        let c = std::sync::Arc::new(c);
        let writer = {
            let c = c.clone();
            tokio::spawn(async move {
                write_loop(&c, sink, q, Duration::from_secs(60), Duration::from_secs(5)).await
            })
        };
        tokio::task::yield_now().await;
        c.send(ServerMessage::Hello(ServerHello {
            server_id: "s".into(),
            name: "S".into(),
            version: 1,
            active_roles: vec![],
            connection_reason: ConnectionReason::Playback,
        }))
        .unwrap();

        let mut frames = Vec::new();
        for _ in 0..6 {
            frames.push(socket.next().await.unwrap());
        }
        let text_at = frames
            .iter()
            .position(|f| matches!(f, Frame::Text(_)))
            .unwrap();
        assert!(text_at <= 2, "control written at {text_at}: {frames:?}");
        c.close();
        assert_eq!(socket.next().await, Some(Frame::Close));
        writer.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn pings_on_the_interval() {
        let (c, q) = conn(&[]);
        let (sink, mut socket) = futures_channel::mpsc::unbounded::<Frame>();
        let c = std::sync::Arc::new(c);
        let writer = {
            let c = c.clone();
            tokio::spawn(async move {
                write_loop(&c, sink, q, Duration::from_secs(30), Duration::from_secs(5)).await
            })
        };
        let start = Instant::now();
        assert_eq!(socket.next().await, Some(Frame::Ping(Bytes::new())));
        assert_eq!(start.elapsed(), Duration::from_secs(30));
        assert_eq!(socket.next().await, Some(Frame::Ping(Bytes::new())));
        assert_eq!(start.elapsed(), Duration::from_secs(60));
        c.close();
        writer.await.unwrap();
    }
}
