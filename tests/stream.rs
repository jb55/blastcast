//! A test tone streamed to a websocket player on a real socket, through the
//! server's own connection, writer and wire encoding: `stream/start`, then
//! 20 ms PCM chunks on one timeline a send-ahead early, then `stream/end`;
//! and a `stream/request-format` for FLAC answered on the same socket.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tas_audio::{SESSION_FORMAT, ServerClock, SineSource};
use tas_core::PlayerRegistry;
use tas_sendspin::protocol::{
    BinaryFrame, ClientHello, ClientMessage, ClientState, Codec, HeaderLayout, PlayerSupport,
    RequestFormatPlayer, ServerMessage, StreamFormat, StreamRequestFormat,
};
use tas_sendspin::stream::DEFAULT_SEND_AHEAD_US;
use tas_sendspin::{AudioStream, SendspinServer, ServerConfig};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

const WAIT: Duration = Duration::from_secs(5);
/// How late a timer may wake on a loaded test machine.
const JITTER_US: i64 = 20_000;
const PCM_48K_16: StreamFormat = StreamFormat {
    codec: Codec::Pcm,
    channels: 2,
    sample_rate: 48_000,
    bit_depth: 16,
};

async fn start(clock: ServerClock) -> (Arc<SendspinServer>, String) {
    let mut config = ServerConfig::new("tas-server-id", "Tune Assistant");
    config.clock = clock;
    let server = SendspinServer::new(config, Arc::new(PlayerRegistry::default()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/sendspin", listener.local_addr().unwrap());
    tokio::spawn(server.clone().serve(listener));
    (server, url)
}

async fn send(ws: &mut Client, msg: &ClientMessage) {
    ws.send(Message::text(msg.to_json())).await.unwrap();
}

async fn next(ws: &mut Client) -> Message {
    loop {
        let msg = tokio::time::timeout(WAIT, ws.next())
            .await
            .expect("timed out")
            .expect("closed")
            .unwrap();
        if !matches!(msg, Message::Ping(_) | Message::Pong(_)) {
            return msg;
        }
    }
}

async fn next_text(ws: &mut Client) -> ServerMessage {
    loop {
        if let Message::Text(text) = next(ws).await {
            return ServerMessage::decode(&text).unwrap();
        }
    }
}

const FLAC_48K_16: StreamFormat = StreamFormat {
    codec: Codec::Flac,
    ..PCM_48K_16
};

/// Says hello as a player listing sendspin-go's PCM formats, and reports
/// itself available.
async fn player(url: &str) -> Client {
    player_with(
        url,
        vec![
            StreamFormat {
                sample_rate: 192_000,
                bit_depth: 24,
                ..PCM_48K_16
            },
            PCM_48K_16,
        ],
    )
    .await
}

async fn player_with(url: &str, supported_formats: Vec<StreamFormat>) -> Client {
    let mut ws = tokio_tungstenite::connect_async(url).await.unwrap().0;
    let hello = ClientMessage::Hello(Box::new(ClientHello {
        client_id: "ear".into(),
        name: "Ear".into(),
        version: 1,
        supported_roles: vec!["player@v1".into()],
        player_v1_support: Some(PlayerSupport {
            supported_formats,
            buffer_capacity: 1 << 20,
            ..Default::default()
        }),
        ..Default::default()
    }));
    send(&mut ws, &hello).await;
    assert!(matches!(next_text(&mut ws).await, ServerMessage::Hello(_)));
    assert!(matches!(
        next_text(&mut ws).await,
        ServerMessage::GroupUpdate(_)
    ));
    let state = ClientState {
        available: Some(true),
        ..Default::default()
    };
    send(&mut ws, &ClientMessage::State(state)).await;
    ws
}

async fn available(server: &SendspinServer) -> Arc<tas_sendspin::Connection> {
    loop {
        match server.connection("ear") {
            Some(conn) if conn.is_available() => break conn,
            _ => tokio::time::sleep(Duration::from_millis(5)).await,
        }
    }
}

#[tokio::test]
async fn a_player_gets_a_timed_test_tone() {
    let clock = ServerClock::new();
    let (server, url) = start(clock).await;
    let mut ws = player(&url).await;
    let conn = available(&server).await;

    let sine = SineSource::new(SESSION_FORMAT, 440.0).paced(clock);
    let stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    stream.add_player(conn);

    let ServerMessage::StreamStart(start) = next_text(&mut ws).await else {
        panic!("expected stream/start first");
    };
    assert_eq!(start.player.unwrap().format(), PCM_48K_16);

    let mut timestamps = Vec::new();
    while timestamps.len() < 15 {
        let Message::Binary(frame) = next(&mut ws).await else {
            panic!("expected audio");
        };
        let arrived = clock.now_us();
        let f = BinaryFrame::decode(&frame, HeaderLayout::Legacy).unwrap();
        assert_eq!(f.payload.len(), 960 * 4, "20 ms of s16le stereo");
        // A send-ahead early. A source that woke late once shifts the
        // timeline by its lateness, so the lead can exceed it by that much.
        let lead = f.timestamp_us - arrived;
        assert!(
            lead > DEFAULT_SEND_AHEAD_US / 2 && lead <= DEFAULT_SEND_AHEAD_US + JITTER_US,
            "chunk for {} arrived at {arrived}: {lead} µs ahead",
            f.timestamp_us
        );
        if timestamps.is_empty() {
            // A -6 dBFS sine from phase zero, in both channels.
            let s: Vec<i16> = f
                .payload
                .chunks(2)
                .map(|b| i16::from_le_bytes([b[0], b[1]]))
                .collect();
            assert_eq!((s[0], s[1]), (0, 0));
            assert_eq!(s[2], s[3]);
            assert!(s.iter().all(|v| v.unsigned_abs() <= 16_384));
            assert!(s.iter().any(|v| v.unsigned_abs() > 16_000));
        }
        timestamps.push(f.timestamp_us);
    }
    for pair in timestamps.windows(2) {
        assert_eq!(pair[1] - pair[0], 20_000, "{timestamps:?}");
    }

    stream.stop().await;
    loop {
        match next(&mut ws).await {
            Message::Binary(_) => {}
            Message::Text(text) => {
                assert!(matches!(
                    ServerMessage::decode(&text).unwrap(),
                    ServerMessage::StreamEnd(_)
                ));
                break;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}

#[tokio::test]
async fn a_player_switches_to_flac_on_request() {
    let clock = ServerClock::new();
    let (server, url) = start(clock).await;
    let mut ws = player_with(&url, vec![PCM_48K_16, FLAC_48K_16]).await;
    let conn = available(&server).await;
    let sine = SineSource::new(SESSION_FORMAT, 440.0).paced(clock);
    let stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    stream.add_player(conn);

    let ServerMessage::StreamStart(start) = next_text(&mut ws).await else {
        panic!("expected stream/start first");
    };
    assert_eq!(start.player.unwrap().format(), PCM_48K_16);
    let request = StreamRequestFormat {
        player: Some(RequestFormatPlayer {
            codec: Some(Codec::Flac),
            ..Default::default()
        }),
        artwork: None,
    };
    send(&mut ws, &ClientMessage::RequestFormat(request)).await;

    // PCM until the answer, then FLAC frames behind its header.
    let start = loop {
        match next(&mut ws).await {
            Message::Binary(_) => {}
            Message::Text(text) => match ServerMessage::decode(&text).unwrap() {
                ServerMessage::StreamStart(start) => break start.player.unwrap(),
                other => panic!("unexpected {other:?}"),
            },
            other => panic!("unexpected {other:?}"),
        }
    };
    assert_eq!(start.format(), FLAC_48K_16);
    let header = start.codec_header.expect("FLAC has a codec header");
    use base64::Engine;
    let mut flac = base64::engine::general_purpose::STANDARD
        .decode(header)
        .unwrap();
    for _ in 0..5 {
        let Message::Binary(frame) = next(&mut ws).await else {
            panic!("expected audio");
        };
        let f = BinaryFrame::decode(&frame, HeaderLayout::Legacy).unwrap();
        flac.extend_from_slice(f.payload);
    }
    let mut reader = claxon::FlacReader::new(&flac[..]).unwrap();
    assert_eq!(reader.samples().count(), 5 * 960 * 2);
    stream.stop().await;
}
