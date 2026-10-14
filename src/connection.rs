//! One client connection after its hello: what the client said it is, the
//! queues that feed its socket, and the writer task that drains them.
//!
//! Control messages and audio have separate queues, and the writer always
//! takes a waiting control message first, so a `server/time` reply never
//! sits behind a second of audio. Messages are serialized in the writer, at
//! the moment they are written, not when they are queued: the writer waits
//! until the socket can take a frame, then stamps `server_transmitted` from
//! the server clock and writes it, as the spec's transmit-timestamp rule
//! asks.
//!
//! Because control overtakes audio, a `stream/start`, `stream/clear` or
//! `stream/end` for the player would otherwise be followed by chunks queued
//! before it, from the stream it replaces. Each of those messages starts a
//! new audio epoch when it is queued, and the writer drops chunks queued in
//! an older one. Artwork goes on the audio queue too, but untagged: no
//! audio stream message makes an image stale.

use std::fmt::Display;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use futures_util::{Sink, SinkExt, future};
use tas_audio::ServerClock;
use tas_telemetry::Telemetry;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, MissedTickBehavior, timeout};

use crate::protocol::{
    ArtworkChannelSupport, ClientHello, ClientState, ConnectionReason, GoodbyeReason, HeaderLayout,
    RequestFormatArtwork, RequestFormatPlayer, Role, ServerMessage, binary,
};
use crate::secure::session::Security;
use crate::stats::LinkStats;
use crate::status::{PlayerStatus, StatusChange};

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
    #[error("the client is unavailable: its output is in use by something else")]
    Unavailable,
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
    clock: ServerClock,
    layout: Mutex<HeaderLayout>,
    /// How an encrypted client got in; `None` for a legacy one.
    security: Mutex<Option<Security>>,
    control: mpsc::Sender<ServerMessage>,
    /// Tagged with the audio epoch; artwork is untagged.
    audio: mpsc::Sender<(Option<u64>, Bytes)>,
    /// Bumped by each player `stream/start`, `stream/clear` and
    /// `stream/end`; audio is queued tagged with it.
    audio_epoch: AtomicU64,
    /// Set by the first `client/state`. Audio waits for it.
    ready: AtomicBool,
    status: watch::Sender<PlayerStatus>,
    /// Every `stream/request-format` the client sent, merged: a later
    /// request's fields override an earlier one's.
    format_request: watch::Sender<Option<RequestFormatPlayer>>,
    /// The artwork channels from the hello, as `stream/request-format`
    /// has changed them since.
    artwork: Mutex<Vec<ArtworkChannelSupport>>,
    goodbye: Mutex<Option<GoodbyeReason>>,
    closed: watch::Sender<bool>,
    stats: LinkStats,
}

/// The receiving ends of a connection's queues, for its writer.
pub(crate) struct Queues {
    pub(crate) control: mpsc::Receiver<ServerMessage>,
    pub(crate) audio: mpsc::Receiver<(Option<u64>, Bytes)>,
}

impl Connection {
    pub(crate) fn new(
        hello: ClientHello,
        active_roles: Vec<String>,
        reason: ConnectionReason,
        clock: ServerClock,
    ) -> (Self, Queues) {
        let (control, control_rx) = mpsc::channel(CONTROL_QUEUE);
        let (audio, audio_rx) = mpsc::channel(AUDIO_QUEUE);
        let status = watch::channel(PlayerStatus::from_hello(&hello)).0;
        let artwork = hello.artwork().map_or_else(Vec::new, |a| {
            let mut channels = a.channels.clone();
            channels.truncate(binary::ARTWORK_CHANNELS.into());
            channels
        });
        let conn = Self {
            hello,
            active_roles,
            reason,
            clock,
            // Every plaintext v1 client reads the 9-byte header; an
            // encrypted one is switched to the 13-byte one.
            layout: Mutex::new(HeaderLayout::Legacy),
            security: Mutex::new(None),
            control,
            audio,
            audio_epoch: AtomicU64::new(0),
            ready: AtomicBool::new(false),
            status,
            format_request: watch::channel(None).0,
            artwork: Mutex::new(artwork),
            goodbye: Mutex::new(None),
            closed: watch::channel(false).0,
            stats: LinkStats::new(clock.now_us()),
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

    /// The server clock, which every time sent to this client is read from.
    pub fn clock(&self) -> ServerClock {
        self.clock
    }

    /// The audio chunk header this client reads.
    pub fn header_layout(&self) -> HeaderLayout {
        *self.layout.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn set_header_layout(&self, layout: HeaderLayout) {
        *self.layout.lock().unwrap_or_else(PoisonError::into_inner) = layout;
    }

    /// How an encrypted client got in; `None` for a legacy plaintext one.
    pub fn security(&self) -> Option<Security> {
        self.security
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn set_security(&self, security: Security) {
        *self.security.lock().unwrap_or_else(PoisonError::into_inner) = Some(security);
    }

    /// Whether the client has sent a `client/state`, so audio may flow.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Marks the client ready; true the first time.
    fn mark_ready(&self) -> bool {
        !self.ready.swap(true, Ordering::AcqRel)
    }

    /// Sends this connection's telemetry to `telemetry`, as the client's
    /// id. Before anything is measured: what came before is not kept.
    pub(crate) fn with_telemetry(mut self, telemetry: &Telemetry) -> Self {
        let probe = telemetry.probe(self.client_id());
        self.stats = LinkStats::with_probe(self.clock.now_us(), probe);
        self
    }

    /// What the server has measured about this connection, for `/debug`
    /// and telemetry.
    pub fn stats(&self) -> &LinkStats {
        &self.stats
    }

    /// Records what went wrong, for `/debug`, at the current time.
    pub fn record_error(&self, message: impl ToString) {
        self.stats.error(message, self.clock.now_us());
    }

    /// Frames waiting in the audio queue for the socket.
    pub fn audio_queued(&self) -> usize {
        AUDIO_QUEUE - self.audio.capacity()
    }

    /// What the client last reported about itself.
    pub fn player_status(&self) -> PlayerStatus {
        self.status.borrow().clone()
    }

    /// Sees every change to [`Connection::player_status`]: availability,
    /// volume, and the timing a streamer schedules by.
    pub fn watch_player_status(&self) -> watch::Receiver<PlayerStatus> {
        self.status.subscribe()
    }

    /// Whether the client has said it can take part in playback.
    pub fn is_available(&self) -> bool {
        self.status.borrow().available
    }

    /// Merges a `client/state` into the status, marking the client ready.
    /// Watchers are woken only when something changed.
    pub(crate) fn apply_state(&self, state: &ClientState) -> StatusChange {
        let first = self.mark_ready();
        let mut change = StatusChange::default();
        self.status.send_if_modified(|status| {
            change = status.apply(state, first);
            change.changed
        });
        change
    }

    /// The player format the client asked for with
    /// `stream/request-format`, if it has. Fields it never named are
    /// `None`, meaning whatever it is being sent now.
    pub fn format_request(&self) -> Option<RequestFormatPlayer> {
        *self.format_request.borrow()
    }

    /// Sees every `stream/request-format`, for the stream to answer.
    pub fn watch_format_request(&self) -> watch::Receiver<Option<RequestFormatPlayer>> {
        self.format_request.subscribe()
    }

    /// Records a `stream/request-format`. Watchers are woken even when it
    /// asks for nothing new, since the spec answers every request with a
    /// `stream/start`.
    pub(crate) fn request_format(&self, request: RequestFormatPlayer) {
        self.format_request.send_modify(|current| {
            let old = current.unwrap_or_default();
            *current = Some(RequestFormatPlayer {
                codec: request.codec.or(old.codec),
                channels: request.channels.or(old.channels),
                sample_rate: request.sample_rate.or(old.sample_rate),
                bit_depth: request.bit_depth.or(old.bit_depth),
            });
        });
    }

    /// The artwork channels the client takes now, channel 0 first. Empty
    /// for a client without the artwork role's support object.
    pub fn artwork_channels(&self) -> Vec<ArtworkChannelSupport> {
        self.artwork
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Applies an artwork `stream/request-format` to its channel. False
    /// for a channel the hello didn't declare.
    pub(crate) fn request_artwork(&self, request: &RequestFormatArtwork) -> bool {
        let mut channels = self.artwork.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(channel) = channels.get_mut(usize::from(request.channel)) else {
            return false;
        };
        if let Some(source) = &request.source {
            channel.source.clone_from(source);
        }
        if let Some(format) = &request.format {
            channel.format.clone_from(format);
        }
        channel.media_width = request.media_width.unwrap_or(channel.media_width);
        channel.media_height = request.media_height.unwrap_or(channel.media_height);
        true
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
        if msg.restarts_player_stream() {
            self.audio_epoch.fetch_add(1, Ordering::AcqRel);
        }
        match self.control.try_send(msg) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(SendError::Closed),
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(
                    client_id = self.client_id(),
                    "control queue full, disconnecting"
                );
                self.record_error("control queue full, disconnected");
                self.close();
                Err(SendError::Full)
            }
        }
    }

    /// Queues an encoded audio chunk. Refused until the client's first
    /// `client/state`, while it is unavailable, and when the queue is full,
    /// which is the streamer's to handle.
    pub fn send_audio(&self, frame: Bytes) -> Result<(), SendError> {
        self.send_binary(Some(self.audio_epoch()), frame)
    }

    /// Queues an artwork frame, refused as [`Connection::send_audio`] is.
    /// It is never dropped as stale audio is.
    pub fn send_artwork(&self, frame: Bytes) -> Result<(), SendError> {
        self.send_binary(None, frame)
    }

    fn send_binary(&self, epoch: Option<u64>, frame: Bytes) -> Result<(), SendError> {
        if self.is_closed() {
            return Err(SendError::Closed);
        }
        if !self.is_ready() {
            return Err(SendError::NotReady);
        }
        if !self.is_available() {
            return Err(SendError::Unavailable);
        }
        self.audio.try_send((epoch, frame)).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => SendError::Full,
            mpsc::error::TrySendError::Closed(_) => SendError::Closed,
        })
    }

    /// The epoch audio queued now is tagged with; the writer drops frames
    /// of an older one.
    pub(crate) fn audio_epoch(&self) -> u64 {
        self.audio_epoch.load(Ordering::Acquire)
    }

    /// How many more binary frames the audio queue takes before
    /// [`Connection::send_audio`] answers `Full`.
    pub fn audio_queue_room(&self) -> usize {
        self.audio.capacity()
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
///
/// A control message is stamped and serialized only once the socket is
/// ready for it, so its `server_transmitted` leaves out the time it spent
/// queued and the time the socket was backed up.
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
        let next = tokio::select! {
            biased;
            _ = closed.wait_for(|c| *c) => break,
            Some(msg) = queues.control.recv() => Next::Control(msg),
            Some((epoch, chunk)) = queues.audio.recv() => {
                if epoch.is_some_and(|e| e != conn.audio_epoch()) {
                    continue;
                }
                Next::Frame(Frame::Binary(chunk))
            }
            _ = ping.tick() => {
                let stamp = LinkStats::ping_payload(conn.clock.now_us());
                Next::Frame(Frame::Ping(Bytes::copy_from_slice(&stamp)))
            }
        };
        let write = async {
            future::poll_fn(|cx| sink.poll_ready_unpin(cx)).await?;
            let frame = match next {
                Next::Control(mut msg) => {
                    msg.stamp_transmitted(conn.clock.now_us());
                    Frame::Text(msg.to_json())
                }
                Next::Frame(Frame::Binary(chunk))
                    if conn.header_layout() == HeaderLayout::SendAhead =>
                {
                    Frame::Binary(binary::restamp_send_ahead(chunk, conn.clock.now_us()))
                }
                Next::Frame(frame) => frame,
            };
            sink.start_send_unpin(frame)?;
            sink.flush().await
        };
        match timeout(write_timeout, write).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::debug!(client_id = conn.client_id(), error = %e, "write failed");
                conn.record_error(format!("write failed: {e}"));
                conn.close();
                return;
            }
            Err(_) => {
                tracing::warn!(
                    client_id = conn.client_id(),
                    ?write_timeout,
                    "write timed out, disconnecting"
                );
                conn.record_error(format!("write timed out after {write_timeout:?}"));
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

/// What the writer took off its queues, before it is stamped.
enum Next {
    Control(ServerMessage),
    Frame(Frame),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ServerHello, ServerTime, StreamClear, StreamEnd};
    use futures_util::StreamExt;

    fn conn(roles: &[&str]) -> (Connection, Queues) {
        let hello = ClientHello {
            client_id: "c1".into(),
            name: "C1".into(),
            ..Default::default()
        };
        let roles = roles.iter().map(|r| r.to_string()).collect();
        Connection::new(hello, roles, ConnectionReason::Playback, ServerClock::new())
    }

    fn state(available: bool) -> ClientState {
        ClientState {
            available: Some(available),
            ..Default::default()
        }
    }

    #[test]
    fn audio_waits_for_the_first_client_state() {
        let (c, _q) = conn(&["player@v1"]);
        assert_eq!(
            c.send_audio(Bytes::from_static(b"x")),
            Err(SendError::NotReady)
        );
        assert!(c.apply_state(&state(true)).first);
        assert!(!c.apply_state(&state(true)).first);
        assert_eq!(c.send_audio(Bytes::from_static(b"x")), Ok(()));
        c.close();
        assert_eq!(
            c.send_audio(Bytes::from_static(b"x")),
            Err(SendError::Closed)
        );
    }

    #[test]
    fn audio_stops_while_the_client_is_unavailable() {
        let (c, _q) = conn(&["player@v1"]);
        let mut watch = c.watch_player_status();
        c.apply_state(&state(true));
        assert!(watch.has_changed().unwrap());
        assert!(watch.borrow_and_update().available);

        assert!(c.apply_state(&state(false)).became_unavailable);
        assert_eq!(
            c.send_audio(Bytes::from_static(b"x")),
            Err(SendError::Unavailable)
        );
        assert!(!watch.borrow_and_update().available);

        c.apply_state(&state(false));
        assert!(!watch.has_changed().unwrap(), "no change, no wake");
        c.apply_state(&state(true));
        assert_eq!(c.send_audio(Bytes::from_static(b"x")), Ok(()));
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
        c.apply_state(&state(true));
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

    /// Audio queued before a `stream/end` belongs to the stream it ends,
    /// so it is dropped rather than written after it. Audio queued after
    /// it goes out.
    #[tokio::test]
    async fn a_stream_end_drops_the_audio_queued_before_it() {
        let (c, q) = conn(&["player@v1"]);
        c.apply_state(&state(true));
        c.send_audio(Bytes::from_static(b"old")).unwrap();
        c.send(ServerMessage::StreamEnd(StreamEnd::default()))
            .unwrap();
        c.send_audio(Bytes::from_static(b"new")).unwrap();
        // Not the player's stream: the epoch stays.
        c.send(ServerMessage::StreamClear(StreamClear {
            server_transmitted: None,
            roles: Some(vec!["visualizer".into()]),
        }))
        .unwrap();
        let (sink, mut socket) = futures_channel::mpsc::unbounded::<Frame>();
        let c = std::sync::Arc::new(c);
        let writer = {
            let c = c.clone();
            tokio::spawn(async move {
                write_loop(&c, sink, q, Duration::from_secs(60), Duration::from_secs(5)).await
            })
        };
        let mut frames = Vec::new();
        for _ in 0..3 {
            frames.push(socket.next().await.unwrap());
        }
        assert!(matches!(&frames[0], Frame::Text(t) if t.contains("stream/end")));
        assert!(matches!(&frames[1], Frame::Text(t) if t.contains("stream/clear")));
        assert_eq!(frames[2], Frame::Binary(Bytes::from_static(b"new")));
        c.close();
        writer.await.unwrap();
    }

    /// `server_transmitted` is the time the socket took the message, not
    /// the time it was queued: here the socket is backed up for 250 ms.
    #[tokio::test(start_paused = true)]
    async fn stamps_transmit_time_when_written_not_when_queued() {
        let (c, q) = conn(&["player@v1"]);
        c.apply_state(&state(true));
        let (sink, mut socket) = futures_channel::mpsc::channel::<Frame>(0);
        let c = std::sync::Arc::new(c);
        let writer = {
            let c = c.clone();
            tokio::spawn(async move {
                write_loop(&c, sink, q, Duration::from_secs(60), Duration::from_secs(5)).await
            })
        };
        // The audio frame takes the socket's one slot, so the reply waits.
        c.send_audio(Bytes::from_static(b"a")).unwrap();
        tokio::task::yield_now().await;
        let queued_at = c.clock().now_us();
        c.send(ServerMessage::Time(ServerTime {
            client_transmitted: 42,
            server_received: queued_at,
            server_transmitted: 0,
        }))
        .unwrap();
        tokio::time::advance(Duration::from_millis(250)).await;

        assert_eq!(
            socket.next().await,
            Some(Frame::Binary(Bytes::from_static(b"a")))
        );
        let Some(Frame::Text(text)) = socket.next().await else {
            panic!("expected server/time");
        };
        let ServerMessage::Time(t) = ServerMessage::decode(&text).unwrap() else {
            panic!("expected server/time, got {text}");
        };
        assert_eq!(t.client_transmitted, 42);
        assert_eq!(t.server_received, queued_at);
        assert_eq!(t.server_transmitted, queued_at + 250_000);
        c.close();
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
        // Each carries the server clock as it went out, for the RTT.
        let stamped = |c: &Connection| {
            Some(Frame::Ping(Bytes::copy_from_slice(
                &LinkStats::ping_payload(c.clock().now_us()),
            )))
        };
        assert_eq!(socket.next().await, stamped(&c));
        assert_eq!(start.elapsed(), Duration::from_secs(30));
        assert_eq!(socket.next().await, stamped(&c));
        assert_eq!(start.elapsed(), Duration::from_secs(60));
        c.close();
        writer.await.unwrap();
    }
}
