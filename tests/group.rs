//! Two websocket players in one group on a real socket and the real clock:
//! the den joins the kitchen's playing group late, then the kitchen
//! leaves. Every chunk both get is the same audio at the same timestamp,
//! and the den's stream runs on without a gap.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tas_audio::{SESSION_FORMAT, ServerClock, SineSource};
use tas_core::PlayerRegistry;
use tas_sendspin::protocol::{
    BinaryFrame, ClientHello, ClientMessage, ClientState, Codec, HeaderLayout, PlaybackState,
    PlayerSupport, ServerMessage, StreamFormat,
};
use tas_sendspin::{SendspinServer, ServerConfig};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

const PCM_48K_16: StreamFormat = StreamFormat {
    codec: Codec::Pcm,
    channels: 2,
    sample_rate: 48_000,
    bit_depth: 16,
};

#[derive(Debug)]
enum Got {
    Text(ServerMessage),
    Audio(i64, Bytes),
}

/// Connects a player and forwards everything it receives.
async fn player(url: &str, client_id: &str) -> mpsc::UnboundedReceiver<Got> {
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let hello = ClientMessage::Hello(Box::new(ClientHello {
        client_id: client_id.into(),
        name: client_id.into(),
        version: 1,
        supported_roles: vec!["player@v1".into()],
        player_v1_support: Some(PlayerSupport {
            supported_formats: vec![PCM_48K_16],
            buffer_capacity: 1 << 20,
            ..Default::default()
        }),
        ..Default::default()
    }));
    ws.send(Message::text(hello.to_json())).await.unwrap();
    let state = ClientMessage::State(ClientState {
        available: Some(true),
        ..Default::default()
    });
    ws.send(Message::text(state.to_json())).await.unwrap();
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(Ok(msg)) = ws.next().await {
            let got = match msg {
                Message::Text(text) => Got::Text(ServerMessage::decode(&text).unwrap()),
                Message::Binary(frame) => {
                    let f = BinaryFrame::decode(&frame, HeaderLayout::Legacy).unwrap();
                    Got::Audio(f.timestamp_us, Bytes::copy_from_slice(f.payload))
                }
                _ => continue,
            };
            if tx.send(got).is_err() {
                break;
            }
        }
    });
    rx
}

/// What arrived so far: control messages by type, and audio chunks.
fn drain(rx: &mut mpsc::UnboundedReceiver<Got>) -> (Vec<String>, Vec<(i64, Bytes)>) {
    let (mut texts, mut audio) = (Vec::new(), Vec::new());
    while let Ok(got) = rx.try_recv() {
        match got {
            Got::Text(ServerMessage::GroupUpdate(u)) => {
                let state = match u.playback_state.unwrap() {
                    PlaybackState::Playing => "playing",
                    _ => "stopped",
                };
                texts.push(format!("group {state} {}", u.group_name.unwrap()));
            }
            Got::Text(ServerMessage::Hello(_)) => texts.push("hello".into()),
            Got::Text(ServerMessage::StreamStart(_)) => texts.push("start".into()),
            Got::Text(ServerMessage::StreamEnd(_)) => texts.push("end".into()),
            Got::Text(other) => panic!("unexpected {other:?}"),
            Got::Audio(ts, payload) => audio.push((ts, payload)),
        }
    }
    (texts, audio)
}

async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

#[tokio::test]
async fn players_stay_in_sync_as_one_joins_and_the_other_leaves() {
    let clock = ServerClock::new();
    let mut config = ServerConfig::new("tas-server-id", "Tune Assistant");
    config.clock = clock;
    let server = SendspinServer::new(config, Arc::new(PlayerRegistry::default()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/sendspin", listener.local_addr().unwrap());
    tokio::spawn(server.clone().serve(listener));

    let mut kitchen = player(&url, "kitchen").await;
    let mut den = player(&url, "den").await;
    let groups = server.groups();
    let ready = |id: &str| server.connection(id).is_some_and(|c| c.is_available());
    while !(ready("kitchen") && ready("den")) {
        sleep_ms(5).await;
    }
    let group = groups.group_of("kitchen").unwrap().id;

    let sine = SineSource::new(SESSION_FORMAT, 440.0).paced(clock);
    groups.play(&group, Box::new(sine)).await.unwrap();
    sleep_ms(300).await;
    groups.join("den", &group).await.unwrap();
    sleep_ms(400).await;
    let solo = groups.leave("kitchen").await.unwrap();
    sleep_ms(400).await;
    groups.stop(&group).await.unwrap();
    sleep_ms(100).await;

    let (k_texts, k) = drain(&mut kitchen);
    let (d_texts, d) = drain(&mut den);
    assert_eq!(
        k_texts,
        [
            "hello",
            "group stopped kitchen",
            "group playing kitchen",
            "start",
            "end",
            "group stopped kitchen"
        ]
    );
    assert_eq!(
        d_texts,
        [
            "hello",
            "group stopped den",
            "group playing kitchen",
            "start",
            "end",
            "group stopped kitchen"
        ]
    );
    assert_eq!(groups.group_of("kitchen").unwrap().id, solo);

    // While both were in the group, every chunk the den got the kitchen
    // got too: the same bytes for the same instant, 0 µs apart.
    let both: Vec<_> = d.iter().filter(|c| k.contains(c)).collect();
    assert!(both.len() >= 15, "{} chunks in common", both.len());
    let overlap = d.iter().take_while(|c| k.contains(c)).count();
    assert_eq!(
        overlap,
        both.len(),
        "the den's first chunks are the kitchen's"
    );
    for (ts, _) in &d[..overlap] {
        assert_eq!(k.iter().filter(|c| c.0 == *ts).count(), 1);
    }
    // And the den's stream ran on, without a gap, after the kitchen left.
    assert!(
        d.len() >= overlap + 15,
        "{} after the kitchen left",
        d.len() - overlap
    );
    for pair in d.windows(2) {
        assert_eq!(pair[1].0 - pair[0].0, 20_000);
    }
}
