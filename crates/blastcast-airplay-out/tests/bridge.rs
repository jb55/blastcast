//! AirPlay speakers as Sendspin players: the bridge against the real
//! Sendspin server and the fake speaker, alone and grouped with a
//! Sendspin speaker on one timeline.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use blastcast::{IntoTimed, SESSION_FORMAT};
use blastcast_airplay_out::testing::FakeSpeaker;
use blastcast_airplay_out::{AirplayOutputs, RaopDevice};
use blastcast_audio::SineSource;
use blastcast_sendspin::protocol::PlaybackState;
use blastcast_sendspin::protocol::{
    BinaryFrame, ClientHello, ClientMessage, ClientState, Codec, ConnectionReason, HeaderLayout,
    PlayerStateReport, PlayerSupport, PlayerSyncState, StreamFormat, binary, roles,
};
use blastcast_sendspin::testing::RecordingHost;
use blastcast_sendspin::{Frame, SendspinPlayer, SendspinServer, ServerConfig};
use blastcast_telemetry::{Point, Series, SpeakerState, Telemetry};
use futures_util::StreamExt;

const WAIT: Duration = Duration::from_secs(10);
const AP: &str = "apa1b2c3d4e5f6";

struct Setup {
    host: Arc<RecordingHost>,
    server: Arc<SendspinServer>,
    outputs: Arc<AirplayOutputs>,
    speaker: FakeSpeaker,
    hub: Telemetry,
}

async fn setup() -> Setup {
    let host = RecordingHost::new();
    let mut config = ServerConfig::new("tas", "Tune Assistant");
    config.telemetry = Telemetry::new();
    let hub = config.telemetry.clone();
    let server = SendspinServer::new(config, host.clone());
    let outputs = AirplayOutputs::with_local_addrs(server.clone(), vec![]);
    let speaker = FakeSpeaker::start().await;
    outputs.add(device(speaker.addr()));
    let s = Setup {
        host,
        server,
        outputs,
        speaker,
        hub,
    };
    s.eventually("the speaker is an available player", |s| {
        s.host.is_available(AP)
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

    /// `client_id`'s player, to command.
    fn player(&self, client_id: &str) -> SendspinPlayer {
        self.server.player(client_id).expect("connected")
    }

    /// The player `client_id` follows: the first of its group, when that
    /// is someone else.
    fn synced_to(&self, client_id: &str) -> Option<String> {
        let group = self.server.groups().group_of(client_id)?;
        let leader = group.members.first()?;
        (leader != client_id).then(|| leader.clone())
    }

    /// Every point of `series` on the airplay speaker.
    fn points(&self, series: Series) -> Vec<Point> {
        let now = self.server.config().clock.now_us();
        self.hub
            .subscribe(now)
            .history
            .into_iter()
            .filter(|t| t.series == series && &*t.entity == AP)
            .flat_map(|t| t.points)
            .collect()
    }

    /// The airplay speaker's RAOP session state, as `/v1/ws` lists it.
    fn session_state(&self) -> Option<SpeakerState> {
        let all = self.outputs.roster().all();
        let speaker = all.iter().find(|s| s.id == AP).expect("listed");
        speaker.session.map(|s| s.state)
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
    let p = s.host.get(AP).unwrap();
    assert_eq!(p.name, "Kitchen AirPlay");
    assert_eq!(p.status.volume, Some(50));
    let hello = s.server.connection(AP).unwrap().hello().clone();
    let model = hello.device_info.and_then(|d| d.product_name);
    assert_eq!(model.as_deref(), Some("ShairportSync"));
    assert_eq!(s.outputs.speakers().len(), 1);

    // Volume is the bridge's own until a stream opens a session.
    s.player(AP).set_volume(30).unwrap();
    s.eventually("the volume is reported back", |s| {
        s.host.get(AP).unwrap().status.volume == Some(30)
    })
    .await;
    assert!(s.speaker.log().requests.is_empty(), "no session yet");

    s.outputs
        .removed("A1B2C3D4E5F6@Kitchen AirPlay._raop._tcp.local.");
    s.eventually("the player is gone", |s| !s.host.contains(AP))
        .await;
}

#[tokio::test]
async fn plays_a_stream_and_lingers_after_it() {
    let s = setup().await;
    s.player(AP).set_volume(80).unwrap();
    s.player(AP)
        .play(second_of_sine().into_timed())
        .await
        .unwrap();
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
    // What the session reports: the speaker's latency, its anchor, and
    // the packets it sends again when asked.
    let latency = s.points(Series::AudioLatency);
    assert_eq!(latency.len(), 1);
    assert_eq!(latency[0].value, 250_000.0, "11 025 frames");
    let anchors = s.points(Series::Anchor);
    assert_eq!(anchors.len(), 1, "{anchors:?}");
    let first = s.speaker.log().packets[0].header.seq;
    s.speaker.ask_resend(first, 2).await;
    s.eventually("the resend counted", |s| {
        s.points(Series::Resent)
            .last()
            .is_some_and(|p| p.value == 2.0)
    })
    .await;
    let roster = s.outputs.roster().all();
    let session = roster[0].session.expect("a session");
    assert_eq!(session.audio_latency_us, Some(250_000));
    assert!(
        matches!(
            session.state,
            SpeakerState::Playing | SpeakerState::Lingering
        ),
        "{session:?}"
    );
    // A volume change while the session is open goes to the speaker.
    s.player(AP).set_volume(20).unwrap();
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
    // It ran out rather than stopped, so there was nothing to flush.
    assert!(!s.speaker.log().methods().contains(&"FLUSH"));
    assert!(s.points(Series::Flush).is_empty());
    s.eventually("the session unlisted", |s| s.session_state().is_none())
        .await;
}

#[tokio::test]
async fn plays_in_sync_with_a_sendspin_speaker() {
    let s = setup().await;
    let sendspin = sendspin_speaker(&s.server, "den").await;
    s.eventually("the sendspin speaker is available", |s| {
        s.host.is_available("den")
    })
    .await;
    s.player("den")
        .set_members(&[AP.to_owned()], &[])
        .await
        .unwrap();
    s.eventually("the airplay speaker follows den", |s| {
        s.synced_to(AP).as_deref() == Some("den")
    })
    .await;

    s.player("den")
        .play(second_of_sine().into_timed())
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
    s.eventually("den is available", |s| s.host.is_available("den"))
        .await;
    s.player("den")
        .set_members(&[AP.to_owned()], &[])
        .await
        .unwrap();
    let live = SineSource::new(SESSION_FORMAT, 440.0).paced(s.server.config().clock);
    s.player("den")
        .play(Box::new(live).into_timed())
        .await
        .unwrap();
    s.speaker
        .wait_for("audio", WAIT, |l| !l.packets.is_empty())
        .await;
    s.speaker.hang_up();
    s.eventually("the airplay speaker left den", |s| {
        s.synced_to(AP).is_none()
    })
    .await;
    assert!(s.host.is_available(AP), "and is free to be picked again");
}

/// Thirty seconds of a 48 kHz sine, read as far ahead as the speaker's
/// buffer takes, so the pacer has seconds queued when the stream stops.
fn half_a_minute_of_sine() -> Box<SineSource> {
    Box::new(SineSource::new(SESSION_FORMAT, 440.0).limit_frames(30 * 48_000))
}

impl Setup {
    /// Asserts that the stream just stopped on the airplay speaker goes
    /// quiet at once: a FLUSH, no audio after it, and a teardown once the
    /// session has lingered.
    async fn goes_quiet(&self, stopped: tokio::time::Instant) {
        self.speaker
            .wait_for("a flush", WAIT, |l| l.methods().contains(&"FLUSH"))
            .await;
        let flushed = stopped.elapsed();
        assert!(
            flushed < Duration::from_millis(500),
            "flushed {flushed:?} after the stop"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        let after_flush = self.speaker.log().packets.len();
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        let later = self.speaker.log().packets.len();
        assert_eq!(later, after_flush, "audio kept coming after the stop");
        self.speaker
            .wait_for("a teardown", WAIT, |l| l.methods().contains(&"TEARDOWN"))
            .await;
        let log = self.speaker.log();
        assert_eq!(
            log.methods().iter().filter(|m| **m == "FLUSH").count(),
            1,
            "{:?}",
            log.methods()
        );
        assert_eq!(log.packets.len(), after_flush, "nothing while it lingered");
    }
}

#[tokio::test]
async fn a_stop_mid_stream_goes_quiet_at_once() {
    let s = setup().await;
    s.player(AP)
        .play(half_a_minute_of_sine().into_timed())
        .await
        .unwrap();
    s.speaker
        .wait_for("half a second of audio", WAIT, |l| {
            l.samples().len() / 2 >= 22_050
        })
        .await;
    let stopped = tokio::time::Instant::now();
    s.player(AP).stop().await.unwrap();
    s.goes_quiet(stopped).await;
    // The flush is a mark, as the stream ends.
    assert_eq!(s.points(Series::Flush).len(), 1);
    let ended = s.points(Series::StreamEnd);
    let flushed = s.points(Series::Flush)[0].t_us;
    assert!(
        ended.iter().any(|p| (flushed - p.t_us).abs() < 500_000),
        "{ended:?} {flushed}"
    );
}

#[tokio::test]
async fn ungrouping_mid_stream_goes_quiet_at_once() {
    let s = setup().await;
    let sendspin = sendspin_speaker(&s.server, "den").await;
    s.eventually("den is available", |s| s.host.is_available("den"))
        .await;
    s.player("den")
        .set_members(&[AP.to_owned()], &[])
        .await
        .unwrap();
    s.player("den")
        .play(half_a_minute_of_sine().into_timed())
        .await
        .unwrap();
    s.speaker
        .wait_for("half a second of audio", WAIT, |l| {
            l.samples().len() / 2 >= 22_050
        })
        .await;
    let stopped = tokio::time::Instant::now();
    s.player("den")
        .set_members(&[], &[AP.to_owned()])
        .await
        .unwrap();
    s.goes_quiet(stopped).await;
    // Den plays on without it.
    assert!(!sendspin.chunks.lock().unwrap().is_empty());
    assert_eq!(
        s.server.groups().group_of("den").unwrap().playback_state,
        PlaybackState::Playing
    );
}
