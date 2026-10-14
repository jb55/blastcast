//! Clock sync against a websocket client on a real socket: the client runs
//! the Sendspin time filter on the server's `server/time` replies, as a
//! speaker does, and must end up knowing the server clock to ±0.5 ms.

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

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

const WAIT: Duration = Duration::from_secs(5);

/// sendspin-go's `pkg/sync/timefilter.go`: a two-dimensional Kalman filter
/// over clock offset and drift, with the upstream time-filter defaults.
/// Times are in microseconds; the offset is server minus client.
struct TimeFilter {
    last_update: i64,
    offset: f64,
    drift: f64,
    offset_cov: f64,
    offset_drift_cov: f64,
    drift_cov: f64,
    use_drift: bool,
    count: u32,
}

impl TimeFilter {
    const PROCESS_VARIANCE: f64 = 0.0;
    const DRIFT_PROCESS_VARIANCE: f64 = 1e-11 * 1e-11;
    const FORGET_VARIANCE_FACTOR: f64 = 2.0 * 2.0;
    const ADAPTIVE_CUTOFF: f64 = 3.0;
    const MIN_SAMPLES: u32 = 100;
    const DRIFT_SIGNIFICANCE_SQ: f64 = 2.0 * 2.0;
    const MAX_ERROR_SCALE: f64 = 0.5;

    fn new() -> Self {
        Self {
            last_update: 0,
            offset: 0.0,
            drift: 0.0,
            offset_cov: f64::INFINITY,
            offset_drift_cov: 0.0,
            drift_cov: 0.0,
            use_drift: false,
            count: 0,
        }
    }

    /// `measurement` is ((T2-T1)+(T3-T4))/2, `max_error` ((T4-T1)-(T3-T2))/2,
    /// `time_added` the client's T4.
    fn update(&mut self, measurement: i64, max_error: i64, time_added: i64) {
        if time_added <= self.last_update {
            return;
        }
        let dt = (time_added - self.last_update) as f64;
        let dt_sq = dt * dt;
        self.last_update = time_added;
        let meas_var = (max_error as f64 * Self::MAX_ERROR_SCALE).powi(2);
        let measurement = measurement as f64;

        if self.count == 0 {
            self.count += 1;
            self.offset = measurement;
            self.offset_cov = meas_var;
            self.drift = 0.0;
            return;
        }
        if self.count == 1 {
            self.count += 1;
            self.drift = (measurement - self.offset) / dt;
            self.offset = measurement;
            self.drift_cov = (self.offset_cov + meas_var) / dt_sq;
            self.offset_cov = meas_var;
            return;
        }

        let pred_offset = self.offset + self.drift * dt;
        let mut new_drift_cov = self.drift_cov + dt * Self::DRIFT_PROCESS_VARIANCE;
        let mut new_offset_drift_cov = self.offset_drift_cov + self.drift_cov * dt;
        let mut new_offset_cov = self.offset_cov
            + 2.0 * self.offset_drift_cov * dt
            + self.drift_cov * dt_sq
            + dt * Self::PROCESS_VARIANCE;

        let residual = measurement - pred_offset;
        let cutoff = max_error as f64 * Self::ADAPTIVE_CUTOFF;
        if self.count < Self::MIN_SAMPLES {
            self.count += 1;
        } else if residual.abs() > cutoff {
            new_drift_cov *= Self::FORGET_VARIANCE_FACTOR;
            new_offset_drift_cov *= Self::FORGET_VARIANCE_FACTOR;
            new_offset_cov *= Self::FORGET_VARIANCE_FACTOR;
        }

        let inv_s = 1.0 / (new_offset_cov + meas_var);
        let offset_gain = new_offset_cov * inv_s;
        let drift_gain = new_offset_drift_cov * inv_s;
        self.offset = pred_offset + offset_gain * residual;
        self.drift += drift_gain * residual;
        self.drift_cov = new_drift_cov - drift_gain * new_offset_drift_cov;
        self.offset_drift_cov = new_offset_drift_cov - drift_gain * new_offset_cov;
        self.offset_cov = new_offset_cov - offset_gain * new_offset_cov;
        self.use_drift = self.drift * self.drift > Self::DRIFT_SIGNIFICANCE_SQ * self.drift_cov;
    }

    /// A server timestamp on the client's clock, as a player schedules a
    /// chunk.
    fn compute_client_time(&self, server_time: i64) -> i64 {
        let drift = if self.use_drift { self.drift } else { 0.0 };
        ((server_time as f64 - self.offset + drift * self.last_update as f64) / (1.0 + drift))
            .round() as i64
    }

    /// The estimated standard deviation of the offset.
    fn error_us(&self) -> f64 {
        self.offset_cov.sqrt()
    }
}

/// The client's own monotonic clock, deliberately far from the server's.
struct ClientClock {
    epoch: Instant,
    base_us: i64,
}

impl ClientClock {
    fn now_us(&self) -> i64 {
        self.base_us + i64::try_from(self.epoch.elapsed().as_micros()).unwrap()
    }
}

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
