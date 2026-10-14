//! The dialer against fake players on real sockets: each fake listens like
//! a speaker advertising `_sendspin._tcp`, and the dialer connects to it.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tas_core::PlayerRegistry;
use tas_mdns::SendspinClient;
use tas_sendspin::protocol::{
    ClientGoodbye, ClientHello, ClientMessage, ConnectionReason, GoodbyeReason, ServerMessage,
};
use tas_sendspin::{Dialer, DialerConfig, SendspinServer, ServerConfig};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const WAIT: Duration = Duration::from_secs(5);
/// Long enough that a redial that shouldn't happen would have.
const QUIET: Duration = Duration::from_millis(400);

/// A fake player: its own websocket listener.
struct Speaker {
    listener: TcpListener,
    addr: SocketAddr,
}

impl Speaker {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        Self { listener, addr }
    }

    fn advert(&self, fullname: &str) -> SendspinClient {
        SendspinClient {
            fullname: fullname.into(),
            name: "Kitchen".into(),
            addr: self.addr,
            path: "/sendspin".into(),
        }
    }

    /// The next connection the dialer opens, or None within `wait`.
    async fn accept_within(&self, wait: Duration) -> Option<WebSocketStream<TcpStream>> {
        let (tcp, _) = timeout(wait, self.listener.accept()).await.ok()?.unwrap();
        Some(tokio_tungstenite::accept_async(tcp).await.unwrap())
    }

    async fn accept(&self) -> WebSocketStream<TcpStream> {
        self.accept_within(WAIT).await.expect("never dialed")
    }

    /// Accepts a dial and completes the hello as `client_id`.
    async fn handshake(&self, client_id: &str) -> WebSocketStream<TcpStream> {
        let mut ws = self.accept().await;
        send(&mut ws, &hello(client_id)).await;
        match next_server_message(&mut ws).await {
            ServerMessage::Hello(h) => {
                assert_eq!(h.connection_reason, ConnectionReason::Discovery);
                assert_eq!(h.active_roles, ["player@v1"]);
            }
            other => panic!("expected server/hello, got {other:?}"),
        }
        ws
    }
}

fn hello(client_id: &str) -> ClientMessage {
    ClientMessage::Hello(Box::new(ClientHello {
        client_id: client_id.into(),
        name: format!("{client_id} name"),
        version: 1,
        supported_roles: vec!["player@v1".into()],
        ..Default::default()
    }))
}

fn goodbye(reason: GoodbyeReason) -> ClientMessage {
    ClientMessage::Goodbye(ClientGoodbye {
        reason: Some(reason),
    })
}

async fn send<S>(ws: &mut WebSocketStream<S>, msg: &ClientMessage)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    ws.send(Message::text(msg.to_json())).await.unwrap();
}

async fn next_server_message<S>(ws: &mut WebSocketStream<S>) -> ServerMessage
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        match timeout(WAIT, ws.next()).await.expect("timed out") {
            Some(Ok(Message::Text(text))) => return ServerMessage::decode(&text).unwrap(),
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            other => panic!("expected a text message, got {other:?}"),
        }
    }
}

/// Says goodbye and waits for the server to close, as the spec has it.
async fn say_goodbye(mut ws: WebSocketStream<TcpStream>, reason: GoodbyeReason) {
    send(&mut ws, &goodbye(reason)).await;
    while let Ok(Some(Ok(msg))) = timeout(WAIT, ws.next()).await {
        if msg.is_close() {
            break;
        }
    }
}

fn fast() -> DialerConfig {
    DialerConfig {
        base_backoff: Duration::from_millis(20),
        max_backoff: Duration::from_millis(100),
        stable_after: Duration::ZERO,
        rejected_retry: Duration::from_millis(800),
        connect_timeout: Duration::from_secs(2),
    }
}

struct Harness {
    server: Arc<SendspinServer>,
    registry: Arc<PlayerRegistry>,
    dialer: Arc<Dialer>,
}

fn start(config: DialerConfig) -> Harness {
    let registry = Arc::new(PlayerRegistry::default());
    let server = SendspinServer::new(
        ServerConfig::new("tas-server-id", "Tune Assistant"),
        registry.clone(),
    );
    let dialer = Dialer::new(server.clone(), config);
    Harness {
        server,
        registry,
        dialer,
    }
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "never: {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn dials_a_discovered_player_and_registers_it() {
    let h = start(fast());
    let speaker = Speaker::new().await;
    h.dialer
        .resolved(&speaker.advert("kitchen._sendspin._tcp.local."));
    let _ws = speaker.handshake("kitchen").await;
    eventually("player registered", || h.registry.get("kitchen").is_some()).await;
    assert!(h.server.connection("kitchen").is_some());
    assert!(h.dialer.is_active("kitchen._sendspin._tcp.local."));
}

#[tokio::test]
async fn an_advert_that_resolves_again_is_dialed_once() {
    let h = start(fast());
    let speaker = Speaker::new().await;
    let advert = speaker.advert("kitchen._sendspin._tcp.local.");
    h.dialer.resolved(&advert);
    h.dialer.resolved(&advert);
    let _ws = speaker.handshake("kitchen").await;
    h.dialer.resolved(&advert);
    assert!(speaker.accept_within(QUIET).await.is_none());
}

#[tokio::test]
async fn redials_after_a_drop_without_goodbye() {
    let h = start(fast());
    let speaker = Speaker::new().await;
    h.dialer
        .resolved(&speaker.advert("kitchen._sendspin._tcp.local."));
    let ws = speaker.handshake("kitchen").await;
    drop(ws);
    let _ws = speaker.handshake("kitchen").await;
}

#[tokio::test]
async fn redials_after_a_restart_goodbye() {
    let h = start(fast());
    let speaker = Speaker::new().await;
    h.dialer
        .add_manual(&format!("ws://{}/sendspin", speaker.addr));
    let ws = speaker.handshake("kitchen").await;
    say_goodbye(ws, GoodbyeReason::Restart).await;
    let _ws = speaker.handshake("kitchen").await;
}

#[tokio::test]
async fn another_server_waits_the_rejected_retry() {
    let h = start(fast());
    let speaker = Speaker::new().await;
    h.dialer
        .resolved(&speaker.advert("kitchen._sendspin._tcp.local."));
    let ws = speaker.handshake("kitchen").await;
    say_goodbye(ws, GoodbyeReason::AnotherServer).await;
    assert!(speaker.accept_within(QUIET).await.is_none());
    let _ws = speaker.handshake("kitchen").await;
}

#[tokio::test]
async fn user_request_parks_until_the_advert_returns() {
    let h = start(fast());
    let speaker = Speaker::new().await;
    let advert = speaker.advert("kitchen._sendspin._tcp.local.");
    h.dialer.resolved(&advert);
    let ws = speaker.handshake("kitchen").await;
    say_goodbye(ws, GoodbyeReason::UserRequest).await;
    eventually("parked", || !h.dialer.is_active(&advert.fullname)).await;
    // A re-resolve of the same advert isn't a new announcement.
    h.dialer.resolved(&advert);
    assert!(speaker.accept_within(QUIET).await.is_none());
    h.dialer.removed(&advert.fullname);
    h.dialer.resolved(&advert);
    let _ws = speaker.handshake("kitchen").await;
}

#[tokio::test]
async fn an_unreachable_player_is_retried_with_backoff() {
    let h = start(fast());
    // Take a free port, then free it, so the first dials are refused.
    let addr = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    h.dialer.add_manual(&format!("ws://{addr}/sendspin"));
    tokio::time::sleep(Duration::from_millis(150)).await;
    let speaker = Speaker {
        listener: TcpListener::bind(addr).await.unwrap(),
        addr,
    };
    let _ws = speaker.handshake("kitchen").await;
}

#[tokio::test]
async fn a_removed_advert_stops_retrying() {
    let h = start(fast());
    let addr = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let advert = SendspinClient {
        fullname: "gone._sendspin._tcp.local.".into(),
        name: "Gone".into(),
        addr,
        path: "/sendspin".into(),
    };
    h.dialer.resolved(&advert);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(h.dialer.is_active(&advert.fullname));
    h.dialer.removed(&advert.fullname);
    eventually("stopped", || !h.dialer.is_active(&advert.fullname)).await;
}

#[tokio::test]
async fn a_client_connected_another_way_is_not_dialed_over() {
    let h = start(DialerConfig {
        rejected_retry: Duration::from_millis(50),
        ..fast()
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_url = format!("ws://{}/sendspin", listener.local_addr().unwrap());
    tokio::spawn(h.server.clone().serve(listener));

    let speaker = Speaker::new().await;
    h.dialer
        .resolved(&speaker.advert("kitchen._sendspin._tcp.local."));
    let mut dialed = speaker.handshake("kitchen").await;

    // The same client connects to the listener too: that connection takes
    // over, and the dialer leaves it alone while it lasts.
    let (mut inbound, _): (WebSocketStream<MaybeTlsStream<TcpStream>>, _) =
        tokio_tungstenite::connect_async(&server_url).await.unwrap();
    send(&mut inbound, &hello("kitchen")).await;
    assert!(matches!(
        next_server_message(&mut inbound).await,
        ServerMessage::Hello(_)
    ));
    while let Ok(Some(Ok(msg))) = timeout(WAIT, dialed.next()).await {
        if msg.is_close() {
            break;
        }
    }
    assert!(speaker.accept_within(QUIET).await.is_none());

    // Once it ends, the dialer takes the client back.
    send(&mut inbound, &goodbye(GoodbyeReason::Shutdown)).await;
    let _ws = speaker.handshake("kitchen").await;
    eventually("dialed connection is current", || {
        h.server
            .connection("kitchen")
            .is_some_and(|c| c.connection_reason() == ConnectionReason::Discovery)
    })
    .await;
}
