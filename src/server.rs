//! The Sendspin websocket server: the listener on `/sendspin`, the hello
//! handshake, role activation and each connection's lifecycle, ported from
//! sendspin-go's `server.go`.
//!
//! A connection's first frame must be `client/hello`. The server answers
//! `server/hello` with the roles it took, then reads until the client says
//! `client/goodbye`, closes, or goes quiet for two ping intervals. A client
//! with the player role is a player in the [`PlayerRegistry`] for as long as
//! it is connected; a second connection with the same `client_id` replaces
//! the first.
//!
//! Clock sync is the server's half of NTP: each `client/time` is answered
//! with `server/time`, `server_received` read as the frame arrives and
//! `server_transmitted` by the writer as it writes. The client runs the
//! time filter; the server only answers, and answers fast.

use std::collections::HashMap;
use std::fmt::{self, Display};
use std::io;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, State};
use axum::response::Response;
use axum::routing::get;
use futures_util::{Sink, SinkExt, Stream, StreamExt, future};
use tas_audio::ServerClock;
use tas_core::{PlayerFeature, PlayerRegistry, PlayerState};
use tokio::net::TcpListener;
use tokio::time::timeout;

use crate::connection::{Connection, Frame, write_loop};
use crate::player::SendspinPlayer;
use crate::protocol::{
    ClientHello, ClientMessage, ClientState, ClientTime, ConnectionReason, DecodeError,
    GoodbyeReason, PROTOCOL_VERSION, PlayerCommandKind, Role, ServerHello, ServerMessage,
    ServerTime, roles,
};
use crate::status::PlayerStatus;

/// Where clients connect, as advertised in the mDNS TXT `path`.
pub const PATH: &str = "/sendspin";
/// The `provider` of every player this server registers.
pub const PROVIDER: &str = "sendspin";
/// The largest frame a client may send; hellos and states are small.
pub(crate) const MAX_MESSAGE: usize = 1 << 20;
/// sendspin-go's limits on a hello.
const MAX_ID_LEN: usize = 256;
const MAX_ROLES: usize = 20;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub server_id: String,
    /// Shown by clients; also the mDNS TXT `name`.
    pub name: String,
    /// The versioned roles this server can take, e.g. `player@v1`.
    pub roles: Vec<String>,
    /// A ping goes out this often; a client silent for two of these is
    /// disconnected.
    pub ping_interval: Duration,
    /// How long a new connection has to send `client/hello`.
    pub hello_timeout: Duration,
    /// How long one frame may take to write before the client is dropped.
    pub write_timeout: Duration,
    /// The clock every time on the wire is read from: `server/time`,
    /// `server_transmitted` and audio chunk timestamps. Give the server the
    /// one the audio pipeline paces with.
    pub clock: ServerClock,
}

impl ServerConfig {
    pub fn new(server_id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            server_id: server_id.into(),
            name: name.into(),
            roles: vec![roles::PLAYER_V1.to_owned()],
            ping_interval: Duration::from_secs(30),
            hello_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            clock: ServerClock::new(),
        }
    }
}

/// Why a connection was dropped before its hello was accepted.
#[derive(Debug, thiserror::Error)]
pub enum HandshakeError {
    #[error("no client/hello within {0:?}")]
    Timeout(Duration),
    #[error("closed before client/hello")]
    Closed,
    #[error("read failed before client/hello: {0}")]
    Read(String),
    #[error("first frame was not text")]
    NotText,
    #[error("bad client/hello: {0}")]
    Decode(#[from] DecodeError),
    #[error("first message was not client/hello")]
    NotHello,
    #[error("client/hello {0}")]
    Invalid(&'static str),
}

/// How a connection ended.
#[derive(Debug)]
pub enum Disconnect {
    /// The connection never got past `client/hello`.
    Refused(HandshakeError),
    /// A connection that completed its hello is over.
    Ended {
        client_id: String,
        /// What the client said in `client/goodbye`, if it said goodbye.
        goodbye: Option<GoodbyeReason>,
        /// A newer connection from the same client took over.
        replaced: bool,
    },
}

impl Disconnect {
    /// Whether a server that dialed this client should dial it again. Only
    /// after a `restart` goodbye, or a drop with no goodbye, which the spec
    /// says to read as a restart. A refused hello waits for the client to
    /// announce itself again.
    pub fn should_redial(&self) -> bool {
        matches!(
            self,
            Disconnect::Ended {
                goodbye: None | Some(GoodbyeReason::Restart),
                replaced: false,
                ..
            }
        )
    }
}

/// Share it as `Arc<SendspinServer>`.
pub struct SendspinServer {
    config: ServerConfig,
    registry: Arc<PlayerRegistry>,
    connections: Mutex<HashMap<String, Arc<Connection>>>,
}

impl fmt::Debug for SendspinServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendspinServer")
            .field("config", &self.config)
            .field("connections", &self.lock().keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl SendspinServer {
    pub fn new(config: ServerConfig, registry: Arc<PlayerRegistry>) -> Arc<Self> {
        Arc::new(Self {
            config,
            registry,
            connections: Mutex::new(HashMap::new()),
        })
    }

    pub fn config(&self) -> &ServerConfig {
        &self.config
    }

    /// Accepts websocket connections on [`PATH`] until the listener fails.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> io::Result<()> {
        let app = Router::new()
            .route(PATH, get(upgrade))
            .with_state(self)
            .into_make_service_with_connect_info::<SocketAddr>();
        axum::serve(listener, app).await
    }

    /// The live connection of `client_id`.
    pub fn connection(&self, client_id: &str) -> Option<Arc<Connection>> {
        self.lock().get(client_id).cloned()
    }

    pub fn connections(&self) -> Vec<Arc<Connection>> {
        self.lock().values().cloned().collect()
    }

    /// Serves one accepted axum websocket.
    pub async fn accept(self: &Arc<Self>, socket: WebSocket) -> Disconnect {
        let (sink, stream) = socket.split();
        let stream = stream.map(|m| m.map(frame_from_axum));
        let sink = sink.with(|f: Frame| future::ready(Ok::<_, axum::Error>(frame_to_axum(f))));
        self.run_connection(stream, sink, ConnectionReason::Playback)
            .await
    }

    /// Serves one connection over any websocket adapted to [`Frame`]s, from
    /// the hello to the close. A dialer runs the same thing on the sockets
    /// it opens, with its own `reason`.
    pub async fn run_connection<St, Si, E>(
        self: &Arc<Self>,
        mut stream: St,
        mut sink: Si,
        reason: ConnectionReason,
    ) -> Disconnect
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin + Send + 'static,
        Si::Error: Display,
    {
        let hello = match self.read_hello(&mut stream).await {
            Ok(hello) => hello,
            Err(e) => {
                tracing::warn!(error = %e, "refusing sendspin connection");
                let _ = timeout(self.config.write_timeout, sink.send(Frame::Close)).await;
                return Disconnect::Refused(e);
            }
        };
        let active_roles = self.activate_roles(&hello.supported_roles);
        tracing::info!(
            client_id = hello.client_id,
            name = hello.name,
            ?active_roles,
            "sendspin client connected"
        );
        let (conn, queues) = Connection::new(hello, active_roles, reason, self.config.clock);
        let conn = Arc::new(conn);
        // Queued before anything else can be, so it is the first message.
        let _ = conn.send(ServerMessage::Hello(ServerHello {
            server_id: self.config.server_id.clone(),
            name: self.config.name.clone(),
            version: PROTOCOL_VERSION,
            active_roles: conn.active_roles().to_vec(),
            connection_reason: reason,
        }));
        // Registered before the writer starts, so a client that has its
        // server/hello is already in the registry.
        self.register(&conn);
        let writer = {
            let conn = conn.clone();
            let (ping, write) = (self.config.ping_interval, self.config.write_timeout);
            tokio::spawn(async move { write_loop(&conn, sink, queues, ping, write).await })
        };

        self.read_loop(&conn, &mut stream).await;
        conn.close();
        let _ = writer.await;
        let replaced = !self.unregister(&conn);
        tracing::info!(
            client_id = conn.client_id(),
            goodbye = ?conn.goodbye(),
            replaced,
            "sendspin client disconnected"
        );
        Disconnect::Ended {
            client_id: conn.client_id().to_owned(),
            goodbye: conn.goodbye(),
            replaced,
        }
    }

    /// The client's roles this server takes: for each family, the first
    /// one the client lists that the server supports, as the client wrote
    /// it. A bare legacy role (`player`) matches any version of its family.
    pub fn activate_roles(&self, client_roles: &[String]) -> Vec<String> {
        let mut families = Vec::new();
        let mut active = Vec::new();
        for role in client_roles {
            let theirs = Role::parse(role);
            if families.contains(&theirs.family) {
                continue;
            }
            let supported = self.config.roles.iter().any(|ours| {
                let ours = Role::parse(ours);
                ours.family == theirs.family
                    && (theirs.version.is_none() || theirs.version == ours.version)
            });
            if supported {
                families.push(theirs.family);
                active.push(role.clone());
            }
        }
        active
    }

    async fn read_hello<St, E>(&self, stream: &mut St) -> Result<ClientHello, HandshakeError>
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
    {
        let deadline = self.config.hello_timeout;
        let text = loop {
            match timeout(deadline, stream.next()).await {
                Err(_) => return Err(HandshakeError::Timeout(deadline)),
                Ok(None | Some(Ok(Frame::Close))) => return Err(HandshakeError::Closed),
                Ok(Some(Err(e))) => return Err(HandshakeError::Read(e.to_string())),
                Ok(Some(Ok(Frame::Ping(_) | Frame::Pong(_)))) => continue,
                Ok(Some(Ok(Frame::Binary(_)))) => return Err(HandshakeError::NotText),
                Ok(Some(Ok(Frame::Text(text)))) => break text,
            }
        };
        let ClientMessage::Hello(hello) = ClientMessage::decode(&text)? else {
            return Err(HandshakeError::NotHello);
        };
        let mut hello = *hello;
        if hello.client_id.is_empty() {
            return Err(HandshakeError::Invalid("has no client_id"));
        }
        if hello.client_id.len() > MAX_ID_LEN || hello.name.len() > MAX_ID_LEN {
            return Err(HandshakeError::Invalid("client_id or name is too long"));
        }
        if hello.supported_roles.len() > MAX_ROLES {
            return Err(HandshakeError::Invalid("lists too many roles"));
        }
        if hello.name.is_empty() {
            hello.name = hello.client_id.clone();
        }
        Ok(hello)
    }

    async fn read_loop<St, E>(&self, conn: &Arc<Connection>, stream: &mut St)
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
    {
        let mut closed = conn.closed();
        // Any frame counts as life; the pings make an idle client answer.
        let idle = self.config.ping_interval * 2;
        loop {
            let next = tokio::select! {
                _ = closed.wait_for(|c| *c) => return,
                next = timeout(idle, stream.next()) => next,
            };
            let frame = match next {
                Err(_) => {
                    tracing::warn!(client_id = conn.client_id(), ?idle, "client went quiet");
                    return;
                }
                Ok(None | Some(Ok(Frame::Close))) => return,
                Ok(Some(Err(e))) => {
                    tracing::debug!(client_id = conn.client_id(), error = %e, "read failed");
                    return;
                }
                Ok(Some(Ok(frame))) => frame,
            };
            // Before anything else, for a client/time's server_received.
            let received_us = conn.clock().now_us();
            match frame {
                Frame::Text(text) => {
                    if self.dispatch(conn, &text, received_us).is_break() {
                        return;
                    }
                }
                Frame::Binary(_) => {
                    tracing::debug!(client_id = conn.client_id(), "ignoring a binary frame");
                }
                Frame::Ping(_) | Frame::Pong(_) | Frame::Close => {}
            }
        }
    }

    /// Handles one text message, which arrived at `received_us` on the
    /// server clock. Breaks when the connection should end.
    fn dispatch(&self, conn: &Arc<Connection>, text: &str, received_us: i64) -> ControlFlow<()> {
        let client_id = conn.client_id();
        let msg = match ClientMessage::decode(text) {
            Ok(msg) => msg,
            Err(DecodeError::UnknownType(kind)) => {
                tracing::debug!(client_id, kind, "ignoring unknown message type");
                return ControlFlow::Continue(());
            }
            Err(e) => {
                tracing::warn!(client_id, error = %e, "ignoring a bad message");
                return ControlFlow::Continue(());
            }
        };
        match msg {
            ClientMessage::Hello(_) => {
                tracing::warn!(client_id, "ignoring a second client/hello");
            }
            ClientMessage::State(state) => self.on_client_state(conn, &state),
            ClientMessage::Goodbye(goodbye) => {
                let reason = goodbye.reason.unwrap_or(GoodbyeReason::Unknown);
                tracing::info!(client_id, ?reason, "client said goodbye");
                conn.set_goodbye(reason);
                // The spec has the server initiate the disconnect.
                return ControlFlow::Break(());
            }
            ClientMessage::Time(time) => on_client_time(conn, time, received_us),
            ClientMessage::RequestFormat(request) => match request.player {
                Some(player) if conn.has_role("player") => {
                    tracing::info!(client_id, ?player, "player asked for another format");
                    conn.request_format(player);
                }
                _ => tracing::debug!(client_id, text, "format request not handled"),
            },
            ClientMessage::Command(_) => {
                tracing::debug!(client_id, text, "not handled yet");
            }
        }
        ControlFlow::Continue(())
    }

    /// Merges a `client/state` into the connection's status and mirrors
    /// it into the registry. The player's report is the truth: volume and
    /// mute only change here, never when a command is sent.
    fn on_client_state(&self, conn: &Arc<Connection>, state: &ClientState) {
        let client_id = conn.client_id();
        let change = conn.apply_state(state);
        if change.first {
            tracing::debug!(client_id, "first client/state, ready");
        }
        if change.became_unavailable {
            // Its output is taken. The audio gate already holds its stream
            // back; leaving its group to a stopped solo group is the group
            // layer's, which watches the status for this edge. It is never
            // rejoined automatically when it comes back.
            tracing::info!(client_id, "client became unavailable");
        }
        if change.changed && conn.has_role("player") && self.is_current(conn) {
            let status = conn.player_status();
            let _ = self
                .registry
                .update(client_id, |p| mirror_status(p, &status));
        }
    }

    /// Makes `conn` the connection of its client, closing and unregistering
    /// any older one, and registers its player.
    fn register(&self, conn: &Arc<Connection>) {
        let client_id = conn.client_id();
        let mut connections = self.lock();
        if let Some(old) = connections.insert(client_id.to_owned(), conn.clone()) {
            tracing::info!(client_id, "new connection replaces the old one");
            old.close();
            self.remove_player(client_id);
        }
        if conn.has_role("player") {
            // Unavailable until its first client/state says otherwise.
            let mut state = PlayerState::new(client_id, PROVIDER, conn.name());
            mirror_status(&mut state, &conn.player_status());
            let control = Arc::new(SendspinPlayer::new(conn.clone()));
            if let Err(e) = self.registry.add(state, control) {
                tracing::warn!(client_id, error = %e, "can't register the player");
            }
        }
    }

    /// Forgets `conn` and its player, unless a newer connection has taken
    /// its place. Returns whether it was still the current one.
    fn unregister(&self, conn: &Arc<Connection>) -> bool {
        let client_id = conn.client_id();
        let mut connections = self.lock();
        if !connections
            .get(client_id)
            .is_some_and(|c| Arc::ptr_eq(c, conn))
        {
            return false;
        }
        connections.remove(client_id);
        self.remove_player(client_id);
        true
    }

    /// Unregisters a player of ours. Another provider's player with the
    /// same id is left alone.
    fn remove_player(&self, player_id: &str) {
        if self
            .registry
            .get(player_id)
            .is_some_and(|p| p.provider == PROVIDER)
        {
            self.registry.remove(player_id);
        }
    }

    fn is_current(&self, conn: &Arc<Connection>) -> bool {
        self.lock()
            .get(conn.client_id())
            .is_some_and(|c| Arc::ptr_eq(c, conn))
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Arc<Connection>>> {
        self.connections
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// Copies what a player reported into its registry state: availability,
/// volume and mute, and the volume features its commands allow.
fn mirror_status(p: &mut PlayerState, status: &PlayerStatus) {
    p.available = status.available;
    p.volume_level = status.volume;
    p.volume_muted = status.muted;
    for (command, feature) in [
        (PlayerCommandKind::Volume, PlayerFeature::VolumeSet),
        (PlayerCommandKind::Mute, PlayerFeature::VolumeMute),
    ] {
        if status.supports(command) {
            p.supported_features.insert(feature);
        } else {
            p.supported_features.remove(&feature);
        }
    }
}

/// Answers a `client/time`. The writer stamps `server_transmitted`.
fn on_client_time(conn: &Connection, time: ClientTime, received_us: i64) {
    let _ = conn.send(ServerMessage::Time(ServerTime {
        client_transmitted: time.client_transmitted,
        server_received: received_us,
        server_transmitted: 0,
    }));
}

async fn upgrade(
    State(server): State<Arc<SendspinServer>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.max_message_size(MAX_MESSAGE)
        .on_upgrade(move |socket| async move {
            tracing::debug!(%addr, "sendspin websocket opened");
            server.accept(socket).await;
        })
}

fn frame_from_axum(msg: Message) -> Frame {
    match msg {
        Message::Text(text) => Frame::Text(text.as_str().to_owned()),
        Message::Binary(data) => Frame::Binary(data),
        Message::Ping(data) => Frame::Ping(data),
        Message::Pong(data) => Frame::Pong(data),
        Message::Close(_) => Frame::Close,
    }
}

fn frame_to_axum(frame: Frame) -> Message {
    match frame {
        Frame::Text(text) => Message::Text(text.into()),
        Frame::Binary(data) => Message::Binary(data),
        Frame::Ping(data) => Message::Ping(data),
        Frame::Pong(data) => Message::Pong(data),
        Frame::Close => Message::Close(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server(roles: &[&str]) -> Arc<SendspinServer> {
        let mut config = ServerConfig::new("srv", "Test");
        config.roles = roles.iter().map(|r| r.to_string()).collect();
        SendspinServer::new(config, Arc::new(PlayerRegistry::default()))
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn activates_the_first_supported_version_of_each_family() {
        let s = server(&["player@v1", "metadata@v1"]);
        let got = s.activate_roles(&strings(&[
            "player@v2",
            "player@v1",
            "controller@v1",
            "metadata@v1",
            "player",
            "metadata@v1",
        ]));
        assert_eq!(got, ["player@v1", "metadata@v1"]);
    }

    #[test]
    fn a_bare_legacy_role_is_echoed_as_written() {
        let s = server(&["player@v1"]);
        assert_eq!(s.activate_roles(&strings(&["player"])), ["player"]);
        assert!(s.activate_roles(&strings(&["artwork@v1"])).is_empty());
    }

    #[test]
    fn redials_only_after_a_restart_or_a_silent_drop() {
        let ended = |goodbye, replaced| Disconnect::Ended {
            client_id: "c".into(),
            goodbye,
            replaced,
        };
        assert!(ended(None, false).should_redial());
        assert!(ended(Some(GoodbyeReason::Restart), false).should_redial());
        assert!(!ended(Some(GoodbyeReason::Restart), true).should_redial());
        assert!(!ended(Some(GoodbyeReason::Shutdown), false).should_redial());
        assert!(!ended(Some(GoodbyeReason::AnotherServer), false).should_redial());
        assert!(!ended(Some(GoodbyeReason::UserRequest), false).should_redial());
        assert!(!Disconnect::Refused(HandshakeError::NotHello).should_redial());
    }
}
