//! AirPlay speakers as Sendspin players: the bridge against the real
//! Sendspin server and the fake speaker, alone and grouped with a
//! Sendspin speaker on one timeline.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;
use tas_airplay_out::testing::FakeSpeaker;
use tas_airplay_out::{AirplayOutputs, RaopDevice};
use tas_audio::{SESSION_FORMAT, SineSource};
use tas_core::{PlayerCommand, PlayerRegistry};
use tas_sendspin::protocol::{
    BinaryFrame, ClientHello, ClientMessage, ClientState, Codec, ConnectionReason, HeaderLayout,
    PlayerStateReport, PlayerSupport, PlayerSyncState, StreamFormat, binary, roles,
};
use tas_sendspin::{Frame, SendspinServer, ServerConfig};

const WAIT: Duration = Duration::from_secs(10);
const AP: &str = "apa1b2c3d4e5f6";

struct Setup {
    registry: Arc<PlayerRegistry>,
    server: Arc<SendspinServer>,
    outputs: Arc<AirplayOutputs>,
    speaker: FakeSpeaker,
}

async fn setup() -> Setup {
    let registry = Arc::new(PlayerRegistry::default());
    let server = SendspinServer::new(ServerConfig::new("tas", "Tune Assistant"), registry.clone());
    let outputs = AirplayOutputs::with_local_addrs(server.clone(), vec![]);
    let speaker = FakeSpeaker::start().await;
    outputs.add(device(speaker.addr()));
    let s = Setup {
        registry,
        server,
        outputs,
        speaker,
    };
    s.eventually("the speaker is an available player", |s| {
        s.registry.get(AP).is_some_and(|p| p.available)
    })
    .await;
    s
}

impl Setup {
    async fn eventually(&self, what: &str, mut f: impl FnMut(&Self) -> bool) {
        let deadline = tokio::time::Instant::now() + WAIT;
        while !f(self) {
            assert!(tokio::time::Instant::now() < deadline, "never: {what}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

fn device(addr: SocketAddr) -> RaopDevice {
    RaopDevice {
        player_id: AP.into(),
        name: "Kitchen AirPlay".into(),
        addr,
        model: Some("ShairportSync".into()),
        manufacturer: None,
        mac: Some("A1B2C3D4E5F6".into()),
        fullname: "A1B2C3D4E5F6@Kitchen AirPlay._raop._tcp.local.".into(),
    }
}

/// One second of a 48 kHz sine, read as fast as the stream likes.
fn second_of_sine() -> Box<SineSource> {
    Box::new(SineSource::new(SESSION_FORMAT, 440.0).limit_frames(48_000))
}

/// A Sendspin speaker over in-memory frames, recording each audio chunk's
/// timestamp and frame count.
struct SendspinSpeaker {
    chunks: Arc<Mutex<Vec<(i64, usize)>>>,
}

async fn sendspin_speaker(server: &Arc<SendspinServer>, client_id: &str) -> SendspinSpeaker {
    let (to_server, from_client) = futures_channel::mpsc::unbounded::<Result<Frame, String>>();
    let (to_client, mut from_server) = futures_channel::mpsc::unbounded::<Frame>();
    let format = StreamFormat {
        codec: Codec::Pcm,
        channels: 2,
        sample_rate: 48_000,
        bit_depth: 16,
    };
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
            supported_formats: vec![format],
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
    SendspinSpeaker { chunks }
}

#[tokio::test]
async fn an_airplay_speaker_is_a_sendspin_player() {
    let s = setup().await;
    let p = s.registry.get(AP).unwrap();
    assert_eq!(p.name, "Kitchen AirPlay");
    assert_eq!(p.provider, "sendspin");
    assert_eq!(p.volume_level, Some(50));
    assert_eq!(p.device_info.model, "ShairportSync");
    assert_eq!(s.outputs.speakers().len(), 1);

    // Volume is the bridge's own until a stream opens a session.
    s.registry
        .execute(AP, PlayerCommand::VolumeSet(30))
        .await
        .unwrap();
    s.eventually("the volume is reported back", |s| {
        s.registry.get(AP).unwrap().volume_level == Some(30)
    })
    .await;
    assert!(s.speaker.log().requests.is_empty(), "no session yet");

    s.outputs
        .removed("A1B2C3D4E5F6@Kitchen AirPlay._raop._tcp.local.");
    s.eventually("the player is gone", |s| !s.registry.contains(AP))
        .await;
}

#[tokio::test]
async fn plays_a_stream_and_lingers_after_it() {
    let s = setup().await;
    s.registry
        .execute(AP, PlayerCommand::VolumeSet(80))
        .await
        .unwrap();
    s.registry.play_source(AP, second_of_sine()).await.unwrap();
    s.speaker
        .wait_for("a second of audio", WAIT, |l| {
            l.samples().len() / 2 >= 43_000
        })
        .await;
    {
        let log = s.speaker.log();
        assert_eq!(
            log.methods()[..5],
            ["OPTIONS", "ANNOUNCE", "SETUP", "RECORD", "SET_PARAMETER"]
        );
        assert_eq!(log.requests[4].body, "volume: -6.000000\r\n");
        let frames = log.samples().len() / 2;
        // 48 000 frames at 48 kHz, resampled: 44 100, less the last
        // chunk the stream's resampler holds back.
        assert!((43_000..=44_100).contains(&frames), "{frames} frames");
        let loud = log.samples().iter().filter(|s| s.abs() > 8_000).count();
        assert!(loud > 20_000, "it is the sine, not silence");
        assert!(!log.syncs.is_empty());
    }
    // A volume change while the session is open goes to the speaker.
    s.registry
        .execute(AP, PlayerCommand::VolumeSet(20))
        .await
        .unwrap();
    s.speaker
        .wait_for("the new volume", WAIT, |l| {
            l.requests
                .iter()
                .any(|r| r.body == "volume: -24.000000\r\n")
        })
        .await;
    // Some seconds after the stream ends, the session closes.
    s.speaker
        .wait_for("a teardown", WAIT, |l| l.methods().contains(&"TEARDOWN"))
        .await;
}

#[tokio::test]
async fn plays_in_sync_with_a_sendspin_speaker() {
    let s = setup().await;
    let sendspin = sendspin_speaker(&s.server, "den").await;
    s.eventually("the sendspin speaker is available", |s| {
        s.registry.get("den").is_some_and(|p| p.available)
    })
    .await;
    s.registry
        .set_members("den", &[AP.to_owned()], &[])
        .await
        .unwrap();
    s.eventually("the airplay speaker follows den", |s| {
        s.registry.get(AP).unwrap().synced_to.as_deref() == Some("den")
    })
    .await;

    s.registry
        .play_source("den", second_of_sine())
        .await
        .unwrap();
    s.speaker
        .wait_for("a second of audio", WAIT, |l| {
            l.samples().len() / 2 >= 43_000 && !l.syncs.is_empty()
        })
        .await;
    let (first_us, _) = sendspin.chunks.lock().unwrap()[0];
    let rtp0 = s.speaker.log().packets[0].header.timestamp;
    let airplay_us = s.speaker.play_time_us(&s.outputs.ntp(), rtp0).unwrap();
    // The first frame plays at the same instant on both, to within a
    // frame of rounding at each rate.
    assert!(
        (airplay_us - first_us).abs() <= 50,
        "airplay plays at {airplay_us}, sendspin at {first_us}"
    );
    // And so does the last.
    let (last_us, frames) = *sendspin.chunks.lock().unwrap().last().unwrap();
    let end_sendspin = last_us + frames as i64 * 1_000_000 / 48_000;
    let log = s.speaker.log();
    let last = log.packets.last().unwrap();
    let end_rtp = last
        .header
        .timestamp
        .wrapping_add((last.samples.len() / 2) as u32);
    drop(log);
    let end_airplay = s.speaker.play_time_us(&s.outputs.ntp(), end_rtp).unwrap();
    // The 44.1 kHz stream ends one chunk early: the Sendspin stream's
    // resampler keeps its last block when the source ends.
    let apart = end_airplay - end_sendspin;
    assert!(
        (-21_000..=50).contains(&apart),
        "the streams end {apart}us apart"
    );
}

#[tokio::test]
async fn leaves_its_group_when_another_sender_takes_the_speaker() {
    let s = setup().await;
    let _sendspin = sendspin_speaker(&s.server, "den").await;
    s.eventually("den is available", |s| {
        s.registry.get("den").is_some_and(|p| p.available)
    })
    .await;
    s.registry
        .set_members("den", &[AP.to_owned()], &[])
        .await
        .unwrap();
    let live = SineSource::new(SESSION_FORMAT, 440.0).paced(s.server.config().clock);
    s.registry.play_source("den", Box::new(live)).await.unwrap();
    s.speaker
        .wait_for("audio", WAIT, |l| !l.packets.is_empty())
        .await;
    s.speaker.hang_up();
    s.eventually("the airplay speaker left den", |s| {
        s.registry.get(AP).unwrap().synced_to.is_none()
    })
    .await;
    let p = s.registry.get(AP).unwrap();
    assert!(p.available, "and is free to be picked again");
}
