//! The Sendspin websocket server: the listener on `/sendspin`, the hello
//! handshake, role activation and each connection's lifecycle, ported from
//! sendspin-go's `server.go`.
//!
//! A connection's first frame must be `client/hello`. The server answers
//! `server/hello` with the roles it took, then reads until the client says
//! `client/goodbye`, closes, or goes quiet for two ping intervals. A client
//! with the player role is a player of the server's [`Host`] for as long as
//! it is connected; a second connection with the same `client_id` replaces
//! the first. Every client is in one [`Groups`] group from its hello on.
//! A controller's `client/command`, every status change and an artwork
//! `stream/request-format` go to the groups, which run the controller,
//! metadata and artwork roles.
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
use blastcast::ServerClock;
use blastcast_telemetry::Telemetry;
use futures_util::{Sink, SinkExt, Stream, StreamExt, future};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tracing::Instrument;

use crate::connection::{Connection, Frame, write_loop};
use crate::group::Groups;
use crate::host::Host;
use crate::player::SendspinPlayer;
use crate::protocol::{
    ClientHello, ClientMessage, ClientState, ClientTime, ConnectionReason, DecodeError,
    GoodbyeReason, HeaderLayout, PROTOCOL_VERSION, Role, ServerHello, ServerMessage, ServerTime,
    roles,
};
use crate::secure::link::Link;
use crate::secure::session::{Outcome, Security};
use crate::secure::trust::{MemoryTrust, Trust};
use crate::stats::LastError;

/// Where clients connect, as advertised in the mDNS TXT `path`.
pub const PATH: &str = "/sendspin";
/// The largest frame a client may send; hellos and states are small.
pub(crate) const MAX_MESSAGE: usize = 1 << 20;
/// The departed clients whose last error `/debug` keeps.
const MAX_DEPARTED: usize = 64;
/// sendspin-go's limits on a hello.
const MAX_ID_LEN: usize = 256;
const MAX_ROLES: usize = 20;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub server_id: String,
    /// Shown by clients; also the mDNS TXT `name`.
    pub name: String,
    /// The versioned roles this server can take, e.g. `player@v1`. All of
    /// player, controller, metadata and artwork by default.
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
    /// The identity, pairings and approvals of encrypted clients. In
    /// memory unless the app keeps them with a [`Trust`] of its own.
    pub trust: Arc<dyn Trust>,
    /// Whether to take the legacy plaintext v1 handshake as well as the
    /// encrypted one, as Music Assistant's `allow_legacy_clients` does.
    /// On by default, since that is what current devices speak.
    pub allow_legacy_clients: bool,
    /// Whether an encrypted client that offers unpaired access is
    /// approved, and plays, as soon as it connects. Music Assistant
    /// approves guest access this way. Off, a client waits at empty
    /// activities until it is approved or paired.
    pub auto_approve_unpaired: bool,
    /// Where each connection's and each stream's timings go. Off by
    /// default.
    pub telemetry: Telemetry,
}

impl ServerConfig {
    pub fn new(server_id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            server_id: server_id.into(),
            name: name.into(),
            roles: [
                roles::PLAYER_V1,
                roles::CONTROLLER_V1,
                roles::METADATA_V1,
                roles::ARTWORK_V1,
            ]
            .map(str::to_owned)
            .to_vec(),
            ping_interval: Duration::from_secs(30),
            hello_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            clock: ServerClock::new(),
            trust: Arc::new(MemoryTrust::new()),
            allow_legacy_clients: true,
            auto_approve_unpaired: true,
            telemetry: Telemetry::default(),
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
    #[error("a legacy plaintext client, and allow_legacy_clients is off")]
    Legacy,
    #[error("bad client/init: {0}")]
    Init(String),
    #[error("noise handshake failed: {0}")]
    Noise(String),
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
    host: Arc<dyn Host>,
    connections: Mutex<HashMap<String, Arc<Connection>>>,
    groups: Groups,
    /// Clients that disconnected, by `client_id`, for `/debug`.
    departed: Mutex<HashMap<String, Departed>>,
    /// Encrypted clients held at setup, by `client_id`.
    held: Mutex<HashMap<String, Held>>,
    /// The last pairing attempt with each client: when, and how it went.
    pairings: Mutex<HashMap<String, (i64, String)>>,
}

/// An encrypted client connected but not playing: unapproved, unpaired,
/// or pairing.
#[derive(Debug, Clone)]
pub(crate) struct Held {
    pub(crate) name: String,
    pub(crate) why: String,
    pub(crate) since_us: i64,
}

/// How a client's last connection ended.
#[derive(Debug, Clone)]
pub(crate) struct Departed {
    pub(crate) name: String,
    /// On the server clock.
    pub(crate) at_us: i64,
    pub(crate) goodbye: Option<GoodbyeReason>,
    pub(crate) last_error: Option<LastError>,
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
    /// Call it inside a Tokio runtime: the groups run on a task of their
    /// own. `host` hears about the players; [`NoHost`] hears nothing.
    ///
    /// [`NoHost`]: crate::NoHost
    pub fn new(config: ServerConfig, host: Arc<dyn Host>) -> Arc<Self> {
        let groups = Groups::with_telemetry(config.clock, config.telemetry.clone());
        Self::with_groups(config, groups, host)
    }

    /// With groups made beforehand, for a host that follows them from the
    /// start (a [`Groups::watch`] it takes before any client connects).
    /// Make them with the config's clock and telemetry.
    pub fn with_groups(config: ServerConfig, groups: Groups, host: Arc<dyn Host>) -> Arc<Self> {
        Arc::new(Self {
            groups,
            config,
            host,
            connections: Mutex::new(HashMap::new()),
            departed: Mutex::new(HashMap::new()),
            held: Mutex::new(HashMap::new()),
            pairings: Mutex::new(HashMap::new()),
        })
    }

    pub fn config(&self) -> &ServerConfig {
        &self.config
    }

    /// The groups every connected client is in.
    pub fn groups(&self) -> &Groups {
        &self.groups
    }

    /// Accepts websocket connections on [`PATH`] until the listener fails.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> io::Result<()> {
        let app = Router::new()
            .route(PATH, get(upgrade))
            .with_state(self)
            .into_make_service_with_connect_info::<SocketAddr>();
        axum::serve(listener, app).await
    }

    /// `client_id`'s player, if it is connected with the player role.
    pub fn player(&self, client_id: &str) -> Option<SendspinPlayer> {
        let conn = self.connection(client_id)?;
        conn.has_role("player").then(|| self.control(conn))
    }

    fn control(&self, conn: Arc<Connection>) -> SendspinPlayer {
        SendspinPlayer::new(conn, self.groups.clone())
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
    /// its first message to the close. A dialer runs the same thing on the
    /// sockets it opens, with its own `reason`.
    ///
    /// The first message says which protocol the client speaks:
    /// `client/init` starts the encrypted handshake, `client/hello` is a
    /// legacy plaintext client, taken only while `allow_legacy_clients`.
    pub async fn run_connection<St, Si, E>(
        self: &Arc<Self>,
        stream: St,
        sink: Si,
        reason: ConnectionReason,
    ) -> Disconnect
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin + Send + 'static,
        Si::Error: Display,
    {
        let allow_legacy = self.config.allow_legacy_clients;
        self.run(stream, sink, reason, allow_legacy).await
    }

    /// [`SendspinServer::run_connection`] for a client in this process,
    /// such as an AirPlay bridge: it speaks the plaintext protocol whatever
    /// `allow_legacy_clients` says, since nothing crosses a network.
    pub async fn run_local_connection<St, Si, E>(
        self: &Arc<Self>,
        stream: St,
        sink: Si,
        reason: ConnectionReason,
    ) -> Disconnect
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin + Send + 'static,
        Si::Error: Display,
    {
        self.run(stream, sink, reason, true).await
    }

    async fn run<St, Si, E>(
        self: &Arc<Self>,
        mut stream: St,
        mut sink: Si,
        reason: ConnectionReason,
        allow_legacy: bool,
    ) -> Disconnect
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin + Send + 'static,
        Si::Error: Display,
    {
        let first = match self.read_first(&mut stream).await {
            Ok(text) if is_init(&text) => {
                let link = Link::new(stream, sink);
                return self.serve_secure(link, text, reason).await;
            }
            Ok(_) if !allow_legacy => Err(HandshakeError::Legacy),
            Ok(text) => decode_hello(&text),
            Err(e) => Err(e),
        };
        let hello = match first {
            Ok(hello) => hello,
            Err(e) => {
                tracing::warn!(error = %e, "refusing sendspin connection");
                let _ = timeout(self.config.write_timeout, sink.send(Frame::Close)).await;
                return Disconnect::Refused(e);
            }
        };
        // Every log line of the connection, and of its writer, says whose.
        let span = tracing::info_span!(
            "sendspin",
            client_id = %hello.client_id,
            name = %hello.name,
        );
        self.serve_client(hello, stream, sink, reason, None)
            .instrument(span)
            .await
    }

    /// An encrypted connection: setup, then the same serving as a legacy
    /// one over the decrypted stream and encrypting sink.
    async fn serve_secure<St, Si, E>(
        self: &Arc<Self>,
        link: Link<St, Si>,
        init: String,
        reason: ConnectionReason,
    ) -> Disconnect
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin + Send + 'static,
        Si::Error: Display,
    {
        match self.secure_setup(link, init, reason).await {
            Outcome::Ready {
                link,
                hello,
                active_roles,
                security,
            } => {
                let span = tracing::info_span!(
                    "sendspin",
                    client_id = %hello.client_id,
                    name = %hello.name,
                );
                let (stream, sink) = link.split();
                let secure = Some((active_roles, security));
                self.serve_client(hello, stream, sink, reason, secure)
                    .instrument(span)
                    .await
            }
            Outcome::Ended {
                client_id,
                goodbye,
                error,
            } => {
                tracing::info!(
                    client_id,
                    ?goodbye,
                    ?error,
                    "encrypted client left during setup"
                );
                Disconnect::Ended {
                    client_id,
                    goodbye,
                    replaced: false,
                }
            }
            Outcome::Refused(e) => {
                tracing::warn!(error = %e, "refusing encrypted sendspin connection");
                Disconnect::Refused(e)
            }
        }
    }

    /// Serves a client past its hello. `secure` is the roles an encrypted
    /// client's `server/activate` gave it, and how it got in; a legacy
    /// client is sent `server/hello` here instead.
    async fn serve_client<St, Si, E>(
        self: &Arc<Self>,
        hello: ClientHello,
        mut stream: St,
        sink: Si,
        reason: ConnectionReason,
        secure: Option<(Vec<String>, Security)>,
    ) -> Disconnect
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin + Send + 'static,
        Si::Error: Display,
    {
        let (active_roles, security) = match secure {
            Some((roles, security)) => (roles, Some(security)),
            None => (self.activate_roles(&hello.supported_roles), None),
        };
        tracing::info!(
            ?active_roles,
            ?reason,
            ?security,
            "sendspin client connected"
        );
        let (conn, queues) = Connection::new(hello, active_roles, reason, self.config.clock);
        let conn = Arc::new(conn.with_telemetry(&self.config.telemetry));
        if let Some(security) = security {
            // The current spec's audio chunk carries send_ahead.
            conn.set_header_layout(HeaderLayout::SendAhead);
            conn.set_security(security);
        } else {
            // Queued before anything else can be, so it is the first
            // message.
            let _ = conn.send(ServerMessage::Hello(ServerHello {
                server_id: self.config.server_id.clone(),
                name: self.config.name.clone(),
                version: PROTOCOL_VERSION,
                active_roles: conn.active_roles().to_vec(),
                connection_reason: reason,
            }));
        }
        // Registered before the writer starts, so a client that has its
        // server/hello is already the host's player.
        self.register(&conn);
        let writer = {
            let conn = conn.clone();
            let (ping, write) = (self.config.ping_interval, self.config.write_timeout);
            tokio::spawn(
                async move { write_loop(&conn, sink, queues, ping, write).await }.in_current_span(),
            )
        };

        self.read_loop(&conn, &mut stream).await;
        conn.close();
        self.groups.disconnect(conn.clone());
        let _ = writer.await;
        let replaced = !self.unregister(&conn);
        tracing::info!(goodbye = ?conn.goodbye(), replaced, "sendspin client disconnected");
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

    /// The first text frame: `client/init` or a legacy `client/hello`.
    async fn read_first<St, E>(&self, stream: &mut St) -> Result<String, HandshakeError>
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
    {
        let deadline = self.config.hello_timeout;
        loop {
            match timeout(deadline, stream.next()).await {
                Err(_) => return Err(HandshakeError::Timeout(deadline)),
                Ok(None | Some(Ok(Frame::Close))) => return Err(HandshakeError::Closed),
                Ok(Some(Err(e))) => return Err(HandshakeError::Read(e.to_string())),
                Ok(Some(Ok(Frame::Ping(_) | Frame::Pong(_)))) => continue,
                Ok(Some(Ok(Frame::Binary(_)))) => return Err(HandshakeError::NotText),
                Ok(Some(Ok(Frame::Text(text)))) => return Ok(text),
            }
        }
    }

    /// Clients that are connected but can't play yet, for `/debug`.
    pub(crate) fn held(&self) -> MutexGuard<'_, HashMap<String, Held>> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Notes that `client_id` is held at setup, and why.
    pub(crate) fn hold(&self, client_id: &str, name: &str, why: &str) {
        self.held().insert(
            client_id.to_owned(),
            Held {
                name: name.to_owned(),
                why: why.to_owned(),
                since_us: self.config.clock.now_us(),
            },
        );
    }

    pub(crate) fn unhold(&self, client_id: &str) {
        self.held().remove(client_id);
    }

    /// Ends the operator's pairing request for `client_id`, noting how.
    pub(crate) fn end_request(&self, client_id: &str, outcome: &str) {
        if let Err(e) = self.config.trust.end_pairing_request(client_id) {
            tracing::warn!(client_id, error = %e, "can't clear the pairing request");
        }
        self.record_pairing(client_id, outcome);
    }

    /// How the last pairing attempt with `client_id` went, for `/debug`.
    pub(crate) fn record_pairing(&self, client_id: &str, outcome: &str) {
        self.pairings
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                client_id.to_owned(),
                (self.config.clock.now_us(), outcome.to_owned()),
            );
    }

    pub(crate) fn pairing_outcomes(&self) -> HashMap<String, (i64, String)> {
        self.pairings
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
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
                    conn.record_error(format!("went quiet for {idle:?}"));
                    return;
                }
                Ok(None | Some(Ok(Frame::Close))) => return,
                Ok(Some(Err(e))) => {
                    tracing::debug!(client_id = conn.client_id(), error = %e, "read failed");
                    conn.record_error(format!("read failed: {e}"));
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
                Frame::Pong(payload) => conn.stats().pong(&payload, received_us),
                Frame::Ping(_) | Frame::Close => {}
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
                conn.record_error(format!("bad message: {e}"));
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
            ClientMessage::RequestFormat(request) => {
                match request.player {
                    Some(player) if conn.has_role("player") => {
                        tracing::info!(client_id, ?player, "player asked for another format");
                        conn.request_format(player);
                    }
                    Some(_) => {
                        tracing::debug!(client_id, "player format request from a non-player")
                    }
                    None => {}
                }
                match request.artwork {
                    Some(artwork) if conn.has_role("artwork") => {
                        tracing::info!(client_id, ?artwork, "artwork channel changed");
                        if conn.request_artwork(&artwork) {
                            self.groups.refresh();
                        }
                    }
                    Some(_) => {
                        tracing::debug!(client_id, "artwork request from a non-artwork client")
                    }
                    None => {}
                }
            }
            ClientMessage::Command(command) => match command.controller {
                Some(controller) if conn.has_role("controller") => {
                    self.groups.command(conn.clone(), controller);
                }
                _ => tracing::debug!(client_id, text, "command not handled"),
            },
        }
        ControlFlow::Continue(())
    }

    /// Merges a `client/state` into the connection's status and tells the
    /// host. The player's report is the truth: volume and mute only change
    /// here, never when a command is sent.
    fn on_client_state(&self, conn: &Arc<Connection>, state: &ClientState) {
        let client_id = conn.client_id();
        let change = conn.apply_state(state);
        if change.first {
            tracing::debug!(client_id, "first client/state, ready");
            self.apply_saved_delay(conn);
        }
        if change.became_unavailable {
            // Its output is taken. The audio gate already holds its stream
            // back; the groups move it to a stopped group of its own, and
            // never back when it returns.
            tracing::info!(client_id, "client became unavailable");
            self.groups.unavailable(conn.clone());
        }
        if change.changed || change.first {
            // Group volume, and whether artwork may be sent, follow it.
            self.groups.refresh();
        }
        if change.changed && conn.has_role("player") && self.is_current(conn) {
            self.host.player_status(client_id, &conn.player_status());
        }
    }

    /// Sends the player the static delay the host has for it, once its
    /// first `client/state` has said which commands it takes. A delay of 0
    /// leaves the player's own.
    fn apply_saved_delay(&self, conn: &Arc<Connection>) {
        let client_id = conn.client_id();
        let Some(ms) = self.host.output_delay_ms(client_id) else {
            return;
        };
        if ms == 0 || ms == conn.player_status().timing.output_delay_ms {
            return;
        }
        if !conn.has_role("player") || !self.is_current(conn) {
            return;
        }
        match self.control(conn.clone()).set_output_delay(ms) {
            Ok(()) => tracing::info!(client_id, ms, "sent the saved output delay"),
            Err(e) => {
                tracing::warn!(client_id, ms, error = %e, "can't send the saved output delay")
            }
        }
    }

    /// Makes `conn` the connection of its client, closing and unregistering
    /// any older one, and hands its player to the host.
    fn register(&self, conn: &Arc<Connection>) {
        let client_id = conn.client_id();
        let mut connections = self.lock();
        let old = connections.insert(client_id.to_owned(), conn.clone());
        // After server/hello, which is already queued. Before the old
        // connection closes: its read loop then queues the disconnect,
        // which must reach the groups after this to be ignored, or the
        // reconnect loses its group.
        self.groups.connect(conn.clone());
        if let Some(old) = old {
            tracing::info!(client_id, "new connection replaces the old one");
            old.close();
            self.remove_player(&old);
        }
        if conn.has_role("player") {
            self.host.player_connected(self.control(conn.clone()));
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
        drop(connections);
        self.remove_player(conn);
        self.depart(conn);
        true
    }

    /// Remembers how `conn`, the client's current connection, ended.
    fn depart(&self, conn: &Connection) {
        let mut departed = self.departed();
        if departed.len() >= MAX_DEPARTED
            && !departed.contains_key(conn.client_id())
            && let Some(oldest) = departed
                .iter()
                .min_by_key(|(_, d)| d.at_us)
                .map(|(id, _)| id.clone())
        {
            departed.remove(&oldest);
        }
        departed.insert(
            conn.client_id().to_owned(),
            Departed {
                name: conn.name().to_owned(),
                at_us: self.config.clock.now_us(),
                goodbye: conn.goodbye(),
                last_error: conn.stats().last_error(),
            },
        );
    }

    pub(crate) fn departed(&self) -> MutexGuard<'_, HashMap<String, Departed>> {
        self.departed.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Tells the host `conn`'s player is gone, if it had one.
    fn remove_player(&self, conn: &Connection) {
        if conn.has_role("player") {
            self.host.player_gone(conn.client_id());
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

/// Whether a first message is `client/init`, the encrypted handshake.
fn is_init(text: &str) -> bool {
    #[derive(serde::Deserialize)]
    struct Kind<'a> {
        #[serde(rename = "type", borrow)]
        kind: std::borrow::Cow<'a, str>,
    }
    serde_json::from_str::<Kind<'_>>(text).is_ok_and(|k| k.kind == "client/init")
}

/// A legacy first message, which must be `client/hello`.
fn decode_hello(text: &str) -> Result<ClientHello, HandshakeError> {
    let ClientMessage::Hello(hello) = ClientMessage::decode(text)? else {
        return Err(HandshakeError::NotHello);
    };
    check_hello(*hello)
}

/// sendspin-go's limits on a hello, and its name defaulted to its id.
pub(crate) fn check_hello(mut hello: ClientHello) -> Result<ClientHello, HandshakeError> {
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

/// Answers a `client/time`. The writer stamps `server_transmitted`.
fn on_client_time(conn: &Connection, time: ClientTime, received_us: i64) {
    conn.stats()
        .time_request(time.client_transmitted, received_us);
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
        SendspinServer::new(config, Arc::new(crate::NoHost))
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[tokio::test]
    async fn activates_the_first_supported_version_of_each_family() {
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

    #[tokio::test]
    async fn a_bare_legacy_role_is_echoed_as_written() {
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
