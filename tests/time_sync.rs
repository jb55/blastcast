//! Clock sync against a websocket client on a real socket: the client runs
//! the Sendspin time filter on the server's `server/time` replies, as a
//! speaker does, and must end up knowing the server clock to ±0.5 ms.

mod harness;

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tas_audio::ServerClock;
use tas_core::PlayerRegistry;
use tas_sendspin::protocol::{ClientHello, ClientMessage, ClientTime, ServerMessage, ServerTime};
use tas_sendspin::{SendspinServer, ServerConfig};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use harness::time_filter::{ClientClock, TimeFilter};

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

const WAIT: Duration = Duration::from_secs(5);

async fn start(clock: ServerClock) -> String {
    let mut config = ServerConfig::new("tas-server-id", "Tune Assistant");
    config.clock = clock;
    let server = SendspinServer::new(config, Arc::new(PlayerRegistry::default()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/sendspin", listener.local_addr().unwrap());
    tokio::spawn(server.serve(listener));
    url
}

async fn send(ws: &mut Client, msg: &ClientMessage) {
    ws.send(Message::text(msg.to_json())).await.unwrap();
}

/// The next `server/time`, with the client's clock when it arrived (T4).
async fn next_time(ws: &mut Client, clock: &ClientClock) -> (ServerTime, i64) {
    loop {
        let msg = tokio::time::timeout(WAIT, ws.next())
            .await
            .expect("timed out")
            .expect("closed")
            .unwrap();
        let arrived = clock.now_us();
        if let Message::Text(text) = msg {
            match ServerMessage::decode(&text).unwrap() {
                ServerMessage::Time(t) => return (t, arrived),
                other => panic!("expected server/time, got {other:?}"),
            }
        }
    }
}

async fn handshake(url: &str) -> Client {
    let mut ws = tokio_tungstenite::connect_async(url).await.unwrap().0;
    let hello = ClientMessage::Hello(Box::new(ClientHello {
        client_id: "timer".into(),
        name: "Timer".into(),
        version: 1,
        supported_roles: vec!["player@v1".into()],
        ..Default::default()
    }));
    send(&mut ws, &hello).await;
    let reply = ws.next().await.unwrap().unwrap();
    let Message::Text(text) = reply else {
        panic!("expected server/hello, got {reply:?}");
    };
    assert!(matches!(
        ServerMessage::decode(&text).unwrap(),
        ServerMessage::Hello(_)
    ));
    let Some(Ok(Message::Text(text))) = ws.next().await else {
        panic!("expected group/update");
    };
    assert!(matches!(
        ServerMessage::decode(&text).unwrap(),
        ServerMessage::GroupUpdate(_)
    ));
    ws
}

#[tokio::test]
async fn a_client_time_filter_converges_to_half_a_millisecond() {
    let server_clock = ServerClock::new();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let client = ClientClock {
        epoch: Instant::now(),
        base_us: 5_000_000_000,
    };
    // Server minus client, exactly: both count from their epochs.
    let true_offset = server_clock.us_at(client.epoch) - client.base_us;

    let url = start(server_clock).await;
    let mut ws = handshake(&url).await;
    let mut filter = TimeFilter::new();
    // Before a client/state, as a player does: it can't report available
    // until its filter has converged.
    for _ in 0..60 {
        let t1 = client.now_us();
        send(
            &mut ws,
            &ClientMessage::Time(ClientTime {
                client_transmitted: t1,
            }),
        )
        .await;
        let (reply, t4) = next_time(&mut ws, &client).await;
        assert_eq!(reply.client_transmitted, t1);
        let (t2, t3) = (reply.server_received, reply.server_transmitted);
        assert!(t2 <= t3, "received {t2} after transmitted {t3}");
        // The server's stamps sit between the client's send and receive,
        // give or take the microsecond each clock truncates.
        assert!(t1 + true_offset <= t2 + 1 && t3 <= t4 + true_offset + 1);
        filter.update(((t2 - t1) + (t3 - t4)) / 2, ((t4 - t1) - (t3 - t2)) / 2, t4);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let error = (filter.offset - true_offset as f64).abs();
    assert!(
        error <= 500.0,
        "offset {} vs true {true_offset}: off by {error} µs (filter error {} µs)",
        filter.offset,
        filter.error_us()
    );
    // A chunk stamped for 100 ms from now on the server clock maps to 100 ms
    // from now on the client's.
    let play_at = server_clock.now_us() + 100_000;
    let local = filter.compute_client_time(play_at);
    let expected = play_at - true_offset;
    assert!(
        (local - expected).abs() <= 500,
        "chunk at {play_at} mapped to {local}, expected {expected}"
    );
}
