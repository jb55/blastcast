//! Encrypted Sendspin clients against the server on a real socket: the
//! Noise handshake in both suites, unpaired access and approval, the three
//! pairing methods with the re-handshake after each, a client that lost
//! its pairing, `send_ahead` on audio, and `allow_legacy_clients`.
//!
//! The client is `blastcast_sendspin::secure::client`, the protocol's other
//! half; its crypto is checked against aiosendspin's libraries in the
//! unit tests, so this checks the two halves' sequencing.

use std::sync::Arc;
use std::time::Duration;

use blastcast_sendspin::protocol::binary::{self, AUDIO_CHUNK};
use blastcast_sendspin::protocol::{
    BinaryFrame, ClientHello, ClientMessage, Codec, ConnectionReason, HeaderLayout, PlayerSupport,
    StreamFormat,
};
use blastcast_sendspin::secure::client::{Activated, Client, ClientSpec, Record};
use blastcast_sendspin::secure::keys::{Identity, PairingToken};
use blastcast_sendspin::secure::noise::Suite;
use blastcast_sendspin::secure::session::PskKind;
use blastcast_sendspin::secure::trust::{MemoryTrust, PairingRequest, Trust, enter_code};
use blastcast_sendspin::{Disconnect, Frame, HandshakeError, NoHost, SendspinServer, ServerConfig};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

const WAIT: Duration = Duration::from_secs(10);

struct Harness {
    server: Arc<SendspinServer>,
    trust: Arc<MemoryTrust>,
    url: String,
}

async fn start(tweak: impl FnOnce(&mut ServerConfig)) -> Harness {
    let trust = Arc::new(MemoryTrust::new());
    let mut config = ServerConfig::new("legacy-id", "Tune Assistant");
    config.trust = trust.clone();
    config.hello_timeout = Duration::from_secs(5);
    tweak(&mut config);
    let server = SendspinServer::new(config, Arc::new(NoHost));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/sendspin", listener.local_addr().unwrap());
    tokio::spawn(server.clone().serve(listener));
    Harness { server, trust, url }
}

/// A `client/hello` payload: a PCM player, with the given pairing
/// methods and unpaired access.
fn hello(name: &str, methods: Value, unpaired: bool) -> Value {
    let hello = ClientHello {
        name: name.into(),
        supported_roles: vec!["player@v1".into()],
        player_v1_support: Some(PlayerSupport {
            supported_formats: vec![StreamFormat {
                codec: Codec::Pcm,
                channels: 2,
                sample_rate: 48_000,
                bit_depth: 16,
            }],
            buffer_capacity: 1 << 20,
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut v = serde_json::to_value(hello).unwrap();
    let obj = v.as_object_mut().unwrap();
    obj.remove("client_id");
    obj.remove("version");
    obj.insert("supported_pair_methods".into(), methods);
    obj.insert("unpaired_access".into(), json!({ "enabled": unpaired }));
    v
}

fn spec(hello: Value) -> ClientSpec {
    ClientSpec {
        identity: Identity::generate(),
        suite: Suite::ChaChaPoly,
        hello,
        records: Vec::new(),
        pairing_psk: None,
        static_code: None,
        gesture: None,
    }
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type Stream = futures_util::stream::Map<
    futures_util::stream::SplitStream<Ws>,
    fn(
        Result<Message, tokio_tungstenite::tungstenite::Error>,
    ) -> Result<Frame, tokio_tungstenite::tungstenite::Error>,
>;
type Sink = std::pin::Pin<
    Box<dyn futures_util::Sink<Frame, Error = tokio_tungstenite::tungstenite::Error> + Send>,
>;
type TestClient = Client<Stream, Sink>;

fn from_ws(
    m: Result<Message, tokio_tungstenite::tungstenite::Error>,
) -> Result<Frame, tokio_tungstenite::tungstenite::Error> {
    m.map(|m| match m {
        Message::Text(t) => Frame::Text(t.as_str().to_owned()),
        Message::Binary(b) => Frame::Binary(b),
        Message::Ping(b) => Frame::Ping(b),
        Message::Pong(b) => Frame::Pong(b),
        Message::Close(_) | Message::Frame(_) => Frame::Close,
    })
}

fn to_ws(f: Frame) -> Message {
    match f {
        Frame::Text(t) => Message::text(t),
        Frame::Binary(b) => Message::Binary(b),
        Frame::Ping(b) => Message::Ping(b),
        Frame::Pong(b) => Message::Pong(b),
        Frame::Close => Message::Close(None),
    }
}

async fn socket(url: &str) -> (Stream, Sink) {
    let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let (sink, stream) = ws.split();
    let stream = stream.map(from_ws as fn(_) -> _);
    let sink: Sink = Box::pin(sink.with(|f| futures_util::future::ok(to_ws(f))));
    (stream, sink)
}

async fn connect(url: &str, spec: ClientSpec) -> (TestClient, mpsc::UnboundedReceiver<String>) {
    let (stream, sink) = socket(url).await;
    let (codes, shown) = mpsc::unbounded_channel();
    let client = tokio::time::timeout(WAIT, Client::connect(stream, sink, spec, codes))
        .await
        .expect("handshake timed out")
        .expect("handshake failed");
    (client, shown)
}

async fn activated(client: &mut TestClient) -> Activated {
    tokio::time::timeout(WAIT, client.run_until_activated())
        .await
        .expect("timed out waiting for server/activate")
        .expect("connection failed")
}

/// Runs the client in the background until it is activated.
fn spawn_until_activated(
    mut client: TestClient,
) -> tokio::task::JoinHandle<(TestClient, Activated)> {
    tokio::spawn(async move {
        let a = activated(&mut client).await;
        (client, a)
    })
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn playing(category: &str) -> Activated {
    Activated {
        activities: vec!["playback".into()],
        active_roles: vec!["player@v1".into()],
        psk_category: category.into(),
    }
}

fn idle(category: &str) -> Activated {
    Activated {
        activities: vec![],
        active_roles: vec![],
        psk_category: category.into(),
    }
}

#[tokio::test]
async fn guest_access_is_approved_and_audio_carries_send_ahead() {
    for suite in [Suite::ChaChaPoly, Suite::AesGcm] {
        let h = start(|_| {}).await;
        let mut spec = spec(hello("Kitchen", json!({}), true));
        spec.suite = suite;
        let client_id = spec.identity.id();
        let (mut client, _) = connect(&h.url, spec).await;
        assert_eq!(client.server_id(), h.trust.identity().id());
        assert_eq!(activated(&mut client).await, playing("sn"), "{suite:?}");
        assert!(h.trust.is_approved(&client_id), "approval is kept");

        eventually("registered", || h.server.connection(&client_id).is_some()).await;
        let conn = h.server.connection(&client_id).unwrap();
        assert_eq!(conn.name(), "Kitchen");
        assert_eq!(conn.header_layout(), HeaderLayout::SendAhead);
        assert_eq!(conn.security().unwrap().psk, PskKind::Sentinel);

        // Audio waits for client/state, then goes out encrypted with a
        // send_ahead stamped as it is written.
        client
            .link
            .send(
                "client/state",
                json!({ "available": true, "player": {"volume": 50} }),
            )
            .await
            .unwrap();
        eventually("ready", || conn.is_ready()).await;
        let play_at = conn.clock().now_us() + 2_000_000;
        let chunk = binary::audio_chunk(HeaderLayout::SendAhead, play_at, 0, &[1, 2, 3, 4]);
        conn.send_audio(Bytes::from(chunk)).unwrap();
        let frame = loop {
            match tokio::time::timeout(WAIT, client.link.recv_frame())
                .await
                .unwrap()
                .unwrap()
            {
                Frame::Binary(b) if b.first() == Some(&AUDIO_CHUNK) => break b,
                _ => continue,
            }
        };
        let f = BinaryFrame::decode(&frame, HeaderLayout::SendAhead).unwrap();
        assert_eq!(f.timestamp_us, play_at);
        assert_eq!(f.payload, [1, 2, 3, 4]);
        let lead = f.send_ahead_us.unwrap();
        assert!((1_000_000..=2_000_000).contains(&lead), "send_ahead {lead}");
    }
}

#[tokio::test]
async fn without_auto_approval_an_unpaired_client_waits_until_approved() {
    let h = start(|c| c.auto_approve_unpaired = false).await;
    let spec = spec(hello("Den", json!({}), true));
    let client_id = spec.identity.id();
    let (mut client, _) = connect(&h.url, spec.clone()).await;
    assert_eq!(activated(&mut client).await, idle("sn"));
    eventually("held", || {
        h.server
            .debug_report()
            .held
            .iter()
            .any(|c| c.client_id == client_id)
    })
    .await;
    assert!(h.server.connection(&client_id).is_none());

    // Approved while held, it plays at once.
    h.trust.approve(&client_id).unwrap();
    assert_eq!(activated(&mut client).await, playing("sn"));
    eventually("playing", || h.server.connection(&client_id).is_some()).await;
    assert!(h.server.debug_report().held.is_empty());

    // Its next connection plays from the start.
    drop(client);
    let (mut client, _) = connect(&h.url, spec).await;
    assert_eq!(activated(&mut client).await, playing("sn"));
}

#[tokio::test]
async fn a_static_code_pairs_a_held_client_and_it_reconnects_paired() {
    let h = start(|_| {}).await;
    let mut spec = spec(hello(
        "Office",
        json!({ "static_pairing_code": { "locations": ["device"] } }),
        false,
    ));
    spec.static_code = Some("12345678".into());
    spec.gesture = Some(Duration::from_millis(200));
    let client_id = spec.identity.id();
    let (mut client, _) = connect(&h.url, spec).await;
    // No unpaired access: held at empty activities.
    assert_eq!(activated(&mut client).await, idle("sn"));

    h.trust
        .request_pairing(
            &client_id,
            PairingRequest::static_code("1234-5678").unwrap(),
        )
        .unwrap();
    assert_eq!(activated(&mut client).await, playing("lt"));
    assert_eq!(
        client.log,
        ["pairing static_pairing_code", "paired", "rehandshake lt"]
    );
    let record = client.spec.records[0].clone();
    assert_eq!(record.server_id, h.trust.identity().id());
    assert_eq!(h.trust.long_term_psk(&client_id), Some(record.psk));
    assert_eq!(h.trust.pairing_request(&client_id), None);
    eventually("playing", || h.server.connection(&client_id).is_some()).await;
    assert_eq!(
        h.server
            .connection(&client_id)
            .unwrap()
            .security()
            .unwrap()
            .psk,
        PskKind::LongTerm
    );

    // The next connection matches the long-term PSK from the start.
    let spec = client.spec.clone();
    drop(client);
    let (mut client, _) = connect(&h.url, spec).await;
    assert_eq!(activated(&mut client).await, playing("lt"));
    assert!(client.log.is_empty());
}

#[tokio::test]
async fn a_wrong_static_code_aborts_and_the_client_stays_held() {
    let h = start(|_| {}).await;
    let mut spec = spec(hello("Office", json!({ "static_pairing_code": {} }), false));
    spec.static_code = Some("12345678".into());
    let client_id = spec.identity.id();
    h.trust
        .request_pairing(&client_id, PairingRequest::static_code("87654321").unwrap())
        .unwrap();
    let (mut client, _) = connect(&h.url, spec).await;
    assert_eq!(activated(&mut client).await, idle("sn"));
    assert_eq!(
        client.log,
        ["pairing static_pairing_code", "server_kc mismatch"]
    );
    assert_eq!(h.trust.long_term_psk(&client_id), None);
    assert_eq!(
        h.trust.pairing_request(&client_id),
        None,
        "one try per request"
    );
    let report = h.server.debug_report();
    let outcome = report
        .pairings
        .iter()
        .find(|p| p.client_id == client_id)
        .unwrap();
    assert!(outcome.outcome.contains("aborted"), "{outcome:?}");
}

#[tokio::test]
async fn a_dynamic_code_pairs_once_the_operator_types_it() {
    let h = start(|_| {}).await;
    let spec = spec(hello(
        "Bedroom",
        json!({ "dynamic_pairing_code": { "out_channels": ["display"], "formats": ["digits", "qr_code"] } }),
        true,
    ));
    let client_id = spec.identity.id();
    h.trust
        .request_pairing(&client_id, PairingRequest::DynamicCode { code: None })
        .unwrap();
    let (client, mut shown) = connect(&h.url, spec).await;
    let running = spawn_until_activated(client);
    let code = tokio::time::timeout(WAIT, shown.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(code.len(), 6);
    assert!(enter_code(h.trust.as_ref(), &client_id, &code).unwrap());
    let (client, activated) = running.await.unwrap();
    assert_eq!(activated, playing("lt"));
    assert_eq!(
        client.log,
        ["pairing dynamic_pairing_code", "paired", "rehandshake lt"]
    );
    assert!(
        !h.trust.is_approved(&client_id),
        "pairing replaces approval"
    );
}

#[tokio::test]
async fn a_dynamic_code_for_a_speaker_without_a_display_is_declined() {
    let h = start(|_| {}).await;
    let spec = spec(hello(
        "Speaker",
        json!({ "dynamic_pairing_code": { "out_channels": ["speaker"], "formats": ["digits"],
            "digit_audio": { "codec": "pcm", "sample_rate": 16000, "bit_depth": 16, "max_bytes": 100000 } } }),
        true,
    ));
    let client_id = spec.identity.id();
    h.trust
        .request_pairing(&client_id, PairingRequest::DynamicCode { code: None })
        .unwrap();
    let (mut client, _) = connect(&h.url, spec).await;
    // Declined before any activation; the guest plays.
    assert_eq!(activated(&mut client).await, playing("sn"));
    assert!(client.log.is_empty());
    let report = h.server.debug_report();
    assert!(report.pairings[0].outcome.contains("display"), "{report:?}");
}

#[tokio::test]
async fn a_pairing_token_pairs_with_the_pairing_psk() {
    let h = start(|_| {}).await;
    let mut spec = spec(hello("Garage", json!({ "pairing_psk": {} }), false));
    spec.pairing_psk = Some([0x42; 32]);
    let client_id = spec.identity.id();
    let token = PairingToken::PairingPsk {
        client_key: spec.identity.public(),
        pairing_psk: [0x42; 32],
    }
    .encode();
    let (named, request) = PairingRequest::from_token(&token).unwrap();
    assert_eq!(named, client_id);
    h.trust.request_pairing(&client_id, request).unwrap();

    let (mut client, _) = connect(&h.url, spec).await;
    assert_eq!(activated(&mut client).await, playing("lt"));
    assert_eq!(
        client.log,
        ["pairing pairing_psk", "paired", "rehandshake lt"]
    );
    assert!(h.trust.long_term_psk(&client_id).is_some());
}

#[tokio::test]
async fn a_token_requested_while_held_re_handshakes_to_the_pairing_psk() {
    let h = start(|_| {}).await;
    let mut spec = spec(hello("Garage", json!({ "pairing_psk": {} }), false));
    spec.pairing_psk = Some([0x43; 32]);
    let client_id = spec.identity.id();
    let (mut client, _) = connect(&h.url, spec).await;
    assert_eq!(activated(&mut client).await, idle("sn"));
    h.trust
        .request_pairing(
            &client_id,
            PairingRequest::PairingPsk {
                pairing_psk: [0x43; 32],
            },
        )
        .unwrap();
    assert_eq!(activated(&mut client).await, playing("lt"));
    assert_eq!(
        client.log,
        [
            "rehandshake pr",
            "pairing pairing_psk",
            "paired",
            "rehandshake lt"
        ]
    );
}

#[tokio::test]
async fn a_client_that_lost_its_pairing_is_held_not_played() {
    let h = start(|_| {}).await;
    let spec = spec(hello("Porch", json!({ "static_pairing_code": {} }), true));
    let client_id = spec.identity.id();
    // The server remembers a pairing the client has forgotten.
    h.trust.pair(&client_id, [7; 32]).unwrap();
    let (mut client, _) = connect(&h.url, spec).await;
    // Message 2 verified only under the Sentinel: no playback, even with
    // guest access and auto-approval.
    assert_eq!(activated(&mut client).await, idle("sn"));
    let report = h.server.debug_report();
    let held = report
        .held
        .iter()
        .find(|c| c.client_id == client_id)
        .unwrap();
    assert!(held.why.contains("lost its pairing"), "{held:?}");
    assert_eq!(
        h.trust.long_term_psk(&client_id),
        Some([7; 32]),
        "the record stays"
    );
}

#[tokio::test]
async fn a_record_for_another_server_is_not_used() {
    let h = start(|_| {}).await;
    let mut spec = spec(hello("Attic", json!({}), true));
    let client_id = spec.identity.id();
    h.trust.pair(&client_id, [8; 32]).unwrap();
    spec.records.push(Record {
        server_id: Identity::generate().id(),
        psk: [8; 32],
    });
    let (mut client, _) = connect(&h.url, spec).await;
    assert_eq!(activated(&mut client).await, idle("sn"));
}

#[tokio::test]
async fn legacy_clients_are_refused_when_not_allowed() {
    let h = start(|c| c.allow_legacy_clients = false).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(&h.url).await.unwrap();
    let hello = ClientMessage::Hello(Box::new(ClientHello {
        client_id: "legacy".into(),
        name: "Legacy".into(),
        version: 1,
        supported_roles: vec!["player@v1".into()],
        ..Default::default()
    }));
    ws.send(Message::text(hello.to_json())).await.unwrap();
    let closed = loop {
        match tokio::time::timeout(WAIT, ws.next()).await.unwrap() {
            Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break true,
            Some(Ok(Message::Text(t))) => panic!("legacy client was answered: {t}"),
            Some(Ok(_)) => continue,
        }
    };
    assert!(closed);
    assert!(h.server.connection("legacy").is_none());

    // An in-process client still may.
    let (to_server, from_client) = futures_channel::mpsc::unbounded::<Frame>();
    let (to_client, mut from_server) = futures_channel::mpsc::unbounded::<Frame>();
    to_server
        .unbounded_send(Frame::Text(hello.to_json()))
        .unwrap();
    let server = h.server.clone();
    tokio::spawn(async move {
        server
            .run_local_connection(
                from_client.map(Ok::<_, std::convert::Infallible>),
                to_client,
                ConnectionReason::Playback,
            )
            .await
    });
    let first = tokio::time::timeout(WAIT, from_server.next())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(first, Frame::Text(t) if t.contains("server/hello")));
    drop(to_server);
}

#[tokio::test]
async fn a_bad_client_init_is_refused() {
    let h = start(|_| {}).await;
    for init in [
        json!({ "client_id": Identity::generate().id(), "version": 2, "suite": "25519_ChaChaPoly_SHA256" }),
        json!({ "client_id": Identity::generate().id(), "version": 1, "suite": "25519_Nope_SHA256" }),
        json!({ "client_id": "not-a-key", "version": 1, "suite": "25519_ChaChaPoly_SHA256" }),
    ] {
        let (to_server, from_client) = futures_channel::mpsc::unbounded::<Frame>();
        let (to_client, _from_server) = futures_channel::mpsc::unbounded::<Frame>();
        let text = json!({ "type": "client/init", "payload": init }).to_string();
        to_server.unbounded_send(Frame::Text(text)).unwrap();
        let outcome = h
            .server
            .run_connection(
                from_client.map(Ok::<_, std::convert::Infallible>),
                to_client,
                ConnectionReason::Playback,
            )
            .await;
        assert!(
            matches!(outcome, Disconnect::Refused(HandshakeError::Init(_))),
            "{init}: {outcome:?}"
        );
    }
}

#[tokio::test]
async fn a_dialed_client_is_activated_without_playback() {
    let h = start(|_| {}).await;
    let spec = spec(hello("Dialed", json!({}), true));
    // The client end of an in-memory socket, the server dialing it.
    let (to_server, from_client) = futures_channel::mpsc::unbounded::<Frame>();
    let (to_client, from_server) = futures_channel::mpsc::unbounded::<Frame>();
    let server = h.server.clone();
    tokio::spawn(async move {
        server
            .run_connection(
                from_client.map(Ok::<_, std::convert::Infallible>),
                to_client,
                ConnectionReason::Discovery,
            )
            .await
    });
    let (codes, _shown) = mpsc::unbounded_channel();
    let mut client = Client::connect(
        from_server.map(Ok::<_, std::convert::Infallible>),
        to_server,
        spec,
        codes,
    )
    .await
    .unwrap();
    let a = tokio::time::timeout(WAIT, client.run_until_activated())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.activities, Vec::<String>::new());
    assert_eq!(a.active_roles, ["player@v1"]);
}
