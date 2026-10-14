//! The Sendspin server against a websocket client on a real socket: the
//! handshake, registration, the audio gate, goodbye and replacement.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tas_core::PlayerRegistry;
use tas_sendspin::protocol::{
    ClientGoodbye, ClientHello, ClientMessage, ClientState, ConnectionReason, GoodbyeReason,
    PlayerStateReport, PlayerSyncState, ServerMessage,
};
use tas_sendspin::{Disconnect, Frame, SendError, SendspinServer, ServerConfig};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

const WAIT: Duration = Duration::from_secs(5);

struct Harness {
    server: Arc<SendspinServer>,
    registry: Arc<PlayerRegistry>,
    url: String,
}

async fn start(config: ServerConfig) -> Harness {
    let registry = Arc::new(PlayerRegistry::default());
    let server = SendspinServer::new(config, registry.clone());
    // Music Assistant may hold 8927 on the host running the tests.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/sendspin", listener.local_addr().unwrap());
    tokio::spawn(server.clone().serve(listener));
    Harness {
        server,
        registry,
        url,
    }
}

fn config() -> ServerConfig {
    ServerConfig::new("tas-server-id", "Tune Assistant")
}

fn hello(client_id: &str, roles: &[&str]) -> ClientMessage {
    ClientMessage::Hello(Box::new(ClientHello {
        client_id: client_id.into(),
        name: format!("{client_id} name"),
        version: 1,
        supported_roles: roles.iter().map(|r| r.to_string()).collect(),
        ..Default::default()
    }))
}

fn synchronized() -> ClientMessage {
    ClientMessage::State(ClientState {
        player: Some(PlayerStateReport {
            state: Some(PlayerSyncState::Synchronized),
            volume: Some(80),
            muted: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    })
}

async fn connect(url: &str) -> Client {
    tokio_tungstenite::connect_async(url).await.unwrap().0
}

async fn send(ws: &mut Client, msg: &ClientMessage) {
    ws.send(Message::text(msg.to_json())).await.unwrap();
}

/// The next frame that isn't a ping or pong.
async fn next(ws: &mut Client) -> Option<Message> {
    loop {
        match tokio::time::timeout(WAIT, ws.next())
            .await
            .expect("timed out")
        {
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            Some(Ok(msg)) => return Some(msg),
            Some(Err(_)) | None => return None,
        }
    }
}

async fn next_server_message(ws: &mut Client) -> ServerMessage {
    match next(ws).await {
        Some(Message::Text(text)) => ServerMessage::decode(&text).unwrap(),
        other => panic!("expected a text message, got {other:?}"),
    }
}

/// Connects and completes the hello, returning the client and its
/// `server/hello`.
async fn handshake(url: &str, client_id: &str, roles: &[&str]) -> (Client, ServerMessage) {
    let mut ws = connect(url).await;
    send(&mut ws, &hello(client_id, roles)).await;
    let reply = next_server_message(&mut ws).await;
    (ws, reply)
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "never: {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Whether the server has closed the connection.
async fn closed(ws: &mut Client) -> bool {
    matches!(next(ws).await, None | Some(Message::Close(_)))
}

#[tokio::test]
async fn hello_gets_server_hello_with_the_activated_roles() {
    let h = start(config()).await;
    let (_ws, reply) = handshake(&h.url, "kitchen", &["player@v1", "metadata@v1"]).await;
    let ServerMessage::Hello(hello) = reply else {
        panic!("expected server/hello, got {reply:?}");
    };
    assert_eq!(hello.server_id, "tas-server-id");
    assert_eq!(hello.name, "Tune Assistant");
    assert_eq!(hello.version, 1);
    assert_eq!(hello.active_roles, ["player@v1"]);
    assert_eq!(hello.connection_reason, ConnectionReason::Playback);
}

#[tokio::test]
async fn a_player_is_registered_while_connected() {
    let h = start(config()).await;
    let (mut ws, _) = handshake(&h.url, "kitchen", &["player@v1"]).await;

    let player = h.registry.get("kitchen").expect("registered after hello");
    assert_eq!(player.provider, "sendspin");
    assert_eq!(player.name, "kitchen name");
    assert!(!player.available, "unavailable until client/state");

    send(&mut ws, &synchronized()).await;
    eventually("available after client/state", || {
        h.registry.get("kitchen").is_some_and(|p| p.available)
    })
    .await;

    ws.close(None).await.unwrap();
    eventually("unregistered after close", || {
        !h.registry.contains("kitchen")
    })
    .await;
    assert!(h.server.connection("kitchen").is_none());
}

#[tokio::test]
async fn a_client_without_the_player_role_is_not_a_player() {
    let h = start(config()).await;
    let (_ws, reply) = handshake(&h.url, "remote", &["controller@v1"]).await;
    let ServerMessage::Hello(hello) = reply else {
        panic!("expected server/hello");
    };
    assert!(hello.active_roles.is_empty());
    assert!(h.server.connection("remote").is_some());
    assert!(!h.registry.contains("remote"));
}

#[tokio::test]
async fn audio_is_held_back_until_the_first_client_state() {
    let h = start(config()).await;
    let (mut ws, _) = handshake(&h.url, "kitchen", &["player@v1"]).await;
    let conn = h.server.connection("kitchen").unwrap();
    assert_eq!(
        conn.send_audio(Bytes::from_static(b"\x04early")),
        Err(SendError::NotReady)
    );

    send(&mut ws, &synchronized()).await;
    eventually("ready after client/state", || conn.is_ready()).await;
    conn.send_audio(Bytes::from_static(b"\x04chunk")).unwrap();
    assert_eq!(
        next(&mut ws).await,
        Some(Message::Binary(Bytes::from_static(b"\x04chunk")))
    );
}

#[tokio::test]
async fn the_first_message_must_be_a_hello() {
    let h = start(config()).await;
    let mut ws = connect(&h.url).await;
    send(&mut ws, &synchronized()).await;
    assert!(closed(&mut ws).await);
    assert!(h.server.connections().is_empty());

    let mut ws = connect(&h.url).await;
    ws.send(Message::text(
        r#"{"type":"client/hello","payload":{"client_id":"","name":"x"}}"#,
    ))
    .await
    .unwrap();
    assert!(closed(&mut ws).await);
}

#[tokio::test]
async fn unknown_and_malformed_messages_keep_the_connection() {
    let h = start(config()).await;
    let (mut ws, _) = handshake(&h.url, "kitchen", &["player@v1"]).await;
    ws.send(Message::text(r#"{"type":"client/teleport","payload":{}}"#))
        .await
        .unwrap();
    ws.send(Message::text("not json")).await.unwrap();
    send(&mut ws, &synchronized()).await;
    eventually("still served", || {
        h.registry.get("kitchen").is_some_and(|p| p.available)
    })
    .await;
}

#[tokio::test]
async fn goodbye_ends_the_connection() {
    let h = start(config()).await;
    let (mut ws, _) = handshake(&h.url, "kitchen", &["player@v1"]).await;
    send(
        &mut ws,
        &ClientMessage::Goodbye(ClientGoodbye {
            reason: Some(GoodbyeReason::Restart),
        }),
    )
    .await;
    assert!(closed(&mut ws).await, "the server closes after a goodbye");
    eventually("unregistered", || !h.registry.contains("kitchen")).await;
}

#[tokio::test]
async fn a_reconnect_replaces_the_old_connection() {
    let h = start(config()).await;
    let (mut old, _) = handshake(&h.url, "kitchen", &["player@v1"]).await;
    let old_conn = h.server.connection("kitchen").unwrap();
    let (mut new, _) = handshake(&h.url, "kitchen", &["player@v1"]).await;

    assert!(closed(&mut old).await, "the old connection is closed");
    assert!(old_conn.is_closed());
    // The old connection's cleanup must not take the new one's player.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(h.registry.contains("kitchen"));
    let conn = h.server.connection("kitchen").unwrap();
    assert!(!Arc::ptr_eq(&conn, &old_conn));

    send(&mut new, &synchronized()).await;
    eventually("the new connection drives the player", || {
        h.registry.get("kitchen").is_some_and(|p| p.available)
    })
    .await;
}

#[tokio::test]
async fn pings_and_drops_a_silent_client() {
    let mut cfg = config();
    cfg.ping_interval = Duration::from_millis(100);
    let h = start(cfg).await;
    let (mut ws, _) = handshake(&h.url, "kitchen", &["player@v1"]).await;

    // tungstenite answers pings only while it is being read, so read raw.
    let ping = tokio::time::timeout(WAIT, ws.next()).await.unwrap();
    assert!(matches!(ping, Some(Ok(Message::Ping(_)))), "{ping:?}");

    // A client that stops reading stops answering; it is dropped.
    eventually("dropped", || !h.registry.contains("kitchen")).await;
}

/// `run_connection` over in-memory channels, as a dialer would call it,
/// reports how the connection ended.
#[tokio::test]
async fn run_connection_reports_the_goodbye() {
    let registry = Arc::new(PlayerRegistry::default());
    let server = SendspinServer::new(config(), registry.clone());
    let (to_server, from_client) = futures_channel::mpsc::unbounded::<Result<Frame, String>>();
    let (to_client, mut from_server) = futures_channel::mpsc::unbounded::<Frame>();

    let text = |m: ClientMessage| Ok(Frame::Text(m.to_json()));
    to_server
        .unbounded_send(text(hello("dialed", &["player@v1"])))
        .unwrap();
    let run = tokio::spawn({
        let server = server.clone();
        async move {
            server
                .run_connection(from_client, to_client, ConnectionReason::Discovery)
                .await
        }
    });

    let Some(Frame::Text(reply)) = from_server.next().await else {
        panic!("expected server/hello");
    };
    let ServerMessage::Hello(reply) = ServerMessage::decode(&reply).unwrap() else {
        panic!("expected server/hello");
    };
    assert_eq!(reply.connection_reason, ConnectionReason::Discovery);

    to_server
        .unbounded_send(text(ClientMessage::Goodbye(ClientGoodbye {
            reason: Some(GoodbyeReason::Shutdown),
        })))
        .unwrap();
    let end = tokio::time::timeout(WAIT, run).await.unwrap().unwrap();
    let Disconnect::Ended {
        ref client_id,
        goodbye,
        replaced,
    } = end
    else {
        panic!("expected Ended, got {end:?}");
    };
    assert_eq!(client_id, "dialed");
    assert_eq!(goodbye, Some(GoodbyeReason::Shutdown));
    assert!(!replaced);
    assert!(!end.should_redial());
    assert!(!registry.contains("dialed"));
    assert_eq!(from_server.next().await, Some(Frame::Close));
}

#[tokio::test]
async fn a_silent_connection_is_refused() {
    let mut cfg = config();
    cfg.hello_timeout = Duration::from_millis(50);
    let server = SendspinServer::new(cfg, Arc::new(PlayerRegistry::default()));
    let (_to_server, from_client) = futures_channel::mpsc::unbounded::<Result<Frame, String>>();
    let (to_client, _from_server) = futures_channel::mpsc::unbounded::<Frame>();
    let end = server
        .run_connection(from_client, to_client, ConnectionReason::Playback)
        .await;
    assert!(
        matches!(
            end,
            Disconnect::Refused(tas_sendspin::HandshakeError::Timeout(_))
        ),
        "{end:?}"
    );
}
