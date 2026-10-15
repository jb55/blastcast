//! The relay end to end, on localhost: a phone (our RAOP sender, which
//! anchors its audio as a phone does) into the AirPlay receiver, and
//! Sendspin speakers out, over in-memory frames to the real server. Every
//! speaker plays the same frame at the same instant, at the phone's time.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use blastcast::ServerClock;
use blastcast_airplay::{NativeConfig, Receiver, ReceiverConfig};
use blastcast_airplay_out::{NtpClock, RaopSession};
use blastcast_relay::{Relay, receiver_info};
use blastcast_sendspin::protocol::{
    BinaryFrame, ClientHello, ClientMessage, ClientState, Codec, ConnectionReason, HeaderLayout,
    PlaybackState, PlayerStateReport, PlayerSupport, PlayerSyncState, StreamFormat, binary, roles,
};
use blastcast_sendspin::{Frame, SendspinServer, ServerConfig};
use futures_util::StreamExt;
use tokio::time::Instant;

const WAIT: Duration = Duration::from_secs(10);

/// Two seconds and a half ahead, as a phone stamps its audio: by when the
/// receiver's opening burst of timing requests is done.
const LEAD_US: i64 = 2_500_000;

struct Rig {
    clock: ServerClock,
    relay: Arc<Relay>,
    airplay: SocketAddr,
    receiver: Receiver,
}

impl Rig {
    async fn start() -> Self {
        let clock = ServerClock::new();
        let mut sendspin = ServerConfig::new("relay-test", "Relay");
        sendspin.clock = clock;
        let relay = Relay::new(sendspin);
        tokio::spawn(relay.clone().run());

        let mut native = NativeConfig::new("relay-test", clock, None);
        native.latency = Duration::from_millis(100);
        native.end_after = Duration::from_millis(300);
        // Tests run side by side; any port will do.
        native.udp_ports = None;
        native.tcp_ports = None;
        let port = free_port().await;
        let receiver = Receiver::spawn(
            ReceiverConfig::new(native),
            receiver_info("Relay", port),
            relay.handler(),
            Some(50),
        );
        let airplay = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
        eventually("the receiver listens", || {
            std::net::TcpStream::connect(airplay).is_ok()
        })
        .await;
        Self {
            clock,
            relay,
            airplay,
            receiver,
        }
    }

    fn server(&self) -> &Arc<SendspinServer> {
        self.relay.server()
    }

    /// The group `client_id` is in, and who is in it.
    fn group_of(&self, client_id: &str) -> Option<(String, Vec<String>)> {
        let g = self.server().groups().group_of(client_id)?;
        Some((g.id, g.members))
    }

    /// What the stream trimmed from the phone's lead for `client_id`, in µs.
    fn trimmed_us(&self, client_id: &str) -> i64 {
        let conn = self.server().connection(client_id).expect("connected");
        let stats = conn.stats().snapshot(self.clock.now_us(), 0);
        let stream = stats.stream.expect("streamed to");
        (stream.trimmed_lead_ms * 1000.0).round() as i64
    }
}

/// A free port, by binding and letting go of one.
async fn free_port() -> u16 {
    let l = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    l.local_addr().unwrap().port()
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A Sendspin speaker over in-memory frames, recording each audio chunk's
/// timestamp and frame count.
struct Speaker {
    chunks: Arc<Mutex<Vec<(i64, usize)>>>,
}

impl Speaker {
    async fn connect(server: &Arc<SendspinServer>, client_id: &str) -> Self {
        let (to_server, from_client) = futures_channel::mpsc::unbounded::<Result<Frame, String>>();
        let (to_client, mut from_server) = futures_channel::mpsc::unbounded::<Frame>();
        let send = |m: ClientMessage| {
            to_server
                .unbounded_send(Ok(Frame::Text(m.to_json())))
                .unwrap()
        };
        send(ClientMessage::Hello(Box::new(ClientHello {
            client_id: client_id.into(),
            name: client_id.into(),
            version: 1,
            supported_roles: vec![roles::PLAYER_V1.into()],
            player_v1_support: Some(PlayerSupport {
                supported_formats: vec![StreamFormat {
                    codec: Codec::Pcm,
                    channels: 2,
                    sample_rate: 48_000,
                    bit_depth: 16,
                }],
                buffer_capacity: 10_000_000,
                ..Default::default()
            }),
            ..Default::default()
        })));
        send(ClientMessage::State(ClientState {
            player: Some(PlayerStateReport {
                state: Some(PlayerSyncState::Synchronized),
                ..Default::default()
            }),
            ..Default::default()
        }));
        tokio::spawn({
            let server = server.clone();
            async move {
                server
                    .run_connection(from_client, to_client, ConnectionReason::Playback)
                    .await
            }
        });
        let chunks = Arc::new(Mutex::new(Vec::new()));
        tokio::spawn({
            let chunks = chunks.clone();
            async move {
                // Held so the server's read of us stays open.
                let _to_server = to_server;
                while let Some(frame) = from_server.next().await {
                    if let Frame::Binary(data) = frame
                        && let Ok(f) = BinaryFrame::decode(&data, HeaderLayout::Legacy)
                        && f.kind == binary::AUDIO_CHUNK
                    {
                        chunks
                            .lock()
                            .unwrap()
                            .push((f.timestamp_us, f.payload.len() / 4));
                    }
                }
            }
        });
        Self { chunks }
    }

    fn chunks(&self) -> Vec<(i64, usize)> {
        self.chunks.lock().unwrap().clone()
    }
}

fn tone(frames: usize) -> Vec<i16> {
    (0..frames * 2)
        .map(|i| (((i / 2) as f32 * 0.03).sin() * 9000.0) as i16)
        .collect()
}

#[tokio::test]
async fn a_phone_plays_on_every_speaker_in_sync_at_its_own_time() {
    let rig = Rig::start().await;
    let den = Speaker::connect(rig.server(), "den").await;
    let kitchen = Speaker::connect(rig.server(), "kitchen").await;
    eventually("both speakers in one group", || {
        rig.group_of("den")
            .is_some_and(|(_, members)| members.len() == 2)
    })
    .await;
    let (group, _) = rig.group_of("den").unwrap();
    assert_eq!(rig.group_of("kitchen").unwrap().0, group);

    // The phone's clock is the server clock, so `t` is when it means its
    // first frame to play, on ours.
    let ntp = NtpClock::new(rig.clock);
    let mut phone = RaopSession::connect(rig.airplay, ntp, -15.0).await.unwrap();
    let t = rig.clock.now_us() + LEAD_US;
    phone.anchor(t).await.unwrap();
    phone.push(&tone(44_100)).await.unwrap();
    phone.finish().await.unwrap();

    eventually("audio on both speakers", || {
        den.chunks().len() >= 10 && kitchen.chunks().len() >= 10
    })
    .await;
    let (den_first, _) = den.chunks()[0];
    let (kitchen_first, _) = kitchen.chunks()[0];
    // The bridge's bound (blastcast-airplay-out's tests/bridge.rs): the
    // first frame plays at the same instant on both.
    assert!(
        (den_first - kitchen_first).abs() <= 50,
        "den plays at {den_first}, kitchen at {kitchen_first}"
    );
    assert_eq!(
        den.chunks()[..10],
        kitchen.chunks()[..10],
        "one stream, one timeline"
    );
    // The phone's time, less the lead an AirPlay 1 stream trims (most of
    // its two seconds, which nothing plays to), to within the clock map's
    // error on localhost.
    let trimmed = rig.trimmed_us("den");
    assert!(trimmed > 1_500_000, "trimmed {trimmed} µs");
    assert!(
        (den_first - (t - trimmed)).abs() < 500,
        "first chunk at {den_first}, the phone's time less the trim is {}",
        t - trimmed
    );

    // A speaker that connects mid-session joins the group and plays on its
    // timeline: what it is sent, the others were.
    let porch = Speaker::connect(rig.server(), "porch").await;
    eventually("the late speaker joins", || {
        rig.group_of("porch").is_some_and(|(id, _)| id == group)
    })
    .await;
    phone.push(&tone(44_100)).await.unwrap();
    phone.finish().await.unwrap();
    eventually("audio on the late speaker", || !porch.chunks().is_empty()).await;
    let (porch_first, _) = porch.chunks()[0];
    eventually("den is sent the late speaker's first chunk", || {
        den.chunks().iter().any(|&(ts, _)| ts == porch_first)
    })
    .await;

    // The phone leaves: once the session ends, the group stops.
    phone.teardown().await;
    eventually("the group stops", || {
        rig.server()
            .groups()
            .get(&group)
            .is_some_and(|g| g.stream.is_none() && g.playback_state != PlaybackState::Playing)
    })
    .await;
    rig.receiver.stop().await;
}

/// A session that starts before any speaker connects plays on the first
/// one that does.
#[tokio::test]
async fn a_speaker_that_connects_mid_session_plays_it() {
    let rig = Rig::start().await;
    let ntp = NtpClock::new(rig.clock);
    let mut phone = RaopSession::connect(rig.airplay, ntp, -15.0).await.unwrap();
    phone.anchor(rig.clock.now_us() + LEAD_US).await.unwrap();
    phone.push(&tone(44_100 / 4)).await.unwrap();

    let den = Speaker::connect(rig.server(), "den").await;
    eventually("den is a player", || rig.group_of("den").is_some()).await;
    for _ in 0..8 {
        phone.push(&tone(44_100 / 4)).await.unwrap();
    }
    phone.finish().await.unwrap();
    eventually("audio on den", || !den.chunks().is_empty()).await;

    phone.teardown().await;
    rig.receiver.stop().await;
}
