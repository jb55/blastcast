//! Interop sync: four fake players in one group on a real socket and the
//! real clock, each on its own clock, its own time filter and its own
//! codec, play the same noise. Through a late join, a `stream/clear`, two
//! codec switches and a leave, every pair of players plays the same audio
//! within ±1 ms of each other, and nobody's audio has a hole it shouldn't.
//!
//! The skew is measured from what the players *play*, not from what they
//! are sent: each decodes its chunks and schedules them through its
//! filter, and a cross-correlation of two players' output finds how far
//! apart the same audio came out (see `harness::playout`). That takes in
//! the filter's error, the codecs' delays (Opus's lookahead above all) and
//! the timestamps a late joiner or a format switch is given.

mod harness;

use std::sync::Arc;
use std::time::Duration;

use blastcast::ServerClock;
use harness::noise::NoiseSource;
use harness::playout::{self, Playout};
use harness::{FakePlayer, Spec, format};
use tas_core::PlayerRegistry;
use tas_sendspin::protocol::{Codec, RequestFormatPlayer};
use tas_sendspin::{SendspinServer, ServerConfig};
use tokio::net::TcpListener;

/// How long each step of the scenario plays before the next.
const PHASE_US: i64 = 700_000;
/// Where in a phase the skew is measured: past the 250 ms send-ahead of
/// any change, and before the next one.
const WINDOW_FROM_US: i64 = 400_000;
const WINDOW_FRAMES: usize = 4_800;
/// The spec's sync floor.
const MAX_SKEW_US: f64 = 1_000.0;
/// The same audio: lossless pairs land at 1.0, Opus a little under.
const MIN_CORRELATION: f64 = 0.9;
/// The most a player may play past `stream/end`: its arrival.
const END_SLACK_US: f64 = 50_000.0;

async fn sleep_us(us: i64) {
    tokio::time::sleep(Duration::from_micros(us as u64)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn players_stay_within_a_millisecond_through_join_clear_switch_and_leave() {
    let clock = ServerClock::new();
    let mut config = ServerConfig::new("tas-server-id", "Tune Assistant");
    config.clock = clock;
    let server = SendspinServer::new(config, Arc::new(PlayerRegistry::default()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/sendspin", listener.local_addr().unwrap());
    tokio::spawn(server.clone().serve(listener));

    let pcm16 = format(Codec::Pcm, 16);
    let specs = [
        ("pcm", vec![pcm16], 5_000_000_000),
        ("flac", vec![format(Codec::Flac, 16), pcm16], 1_000),
        ("opus", vec![format(Codec::Opus, 16), pcm16], 3_000_000),
        (
            "pcm24",
            vec![format(Codec::Pcm, 24), format(Codec::Opus, 16)],
            77_777_777,
        ),
    ];
    let mut players = Vec::new();
    for (client_id, formats, clock_base_us) in specs {
        let spec = Spec {
            client_id,
            formats,
            clock_base_us,
        };
        players.push(FakePlayer::connect(&url, clock, spec).await);
    }
    let [pcm, flac, opus, pcm24] = &players[..] else {
        unreachable!()
    };
    let ready = |id: &str| server.connection(id).is_some_and(|c| c.is_available());
    while !players.iter().all(|p| ready(p.client_id)) {
        sleep_us(5_000).await;
    }

    // Play to three, then the scenario, one phase each.
    let groups = server.groups();
    let group = groups.group_of("pcm").unwrap().id;
    groups.join("flac", &group).await.unwrap();
    groups.join("pcm24", &group).await.unwrap();
    let t_play = clock.now_us();
    groups
        .play(&group, Box::new(NoiseSource::new(1, clock)))
        .await
        .unwrap();
    sleep_us(PHASE_US).await;

    let t_join = clock.now_us();
    groups.join("opus", &group).await.unwrap();
    sleep_us(PHASE_US).await;

    let t_clear = clock.now_us();
    groups
        .play(&group, Box::new(NoiseSource::new(2, clock)))
        .await
        .unwrap();
    sleep_us(PHASE_US).await;

    let t_switch = clock.now_us();
    flac.request_format(RequestFormatPlayer {
        codec: Some(Codec::Pcm),
        bit_depth: Some(16),
        ..Default::default()
    });
    pcm24.request_format(RequestFormatPlayer {
        codec: Some(Codec::Opus),
        ..Default::default()
    });
    sleep_us(PHASE_US).await;

    let t_leave = clock.now_us();
    groups.leave("pcm").await.unwrap();
    sleep_us(PHASE_US).await;

    let t_stop = clock.now_us();
    groups.stop(&group).await.unwrap();
    sleep_us(100_000).await;

    // What each was told.
    for p in &players {
        let log = p.log();
        let errors: Vec<_> = log.iter().filter(|l| l.starts_with("error")).collect();
        assert!(errors.is_empty(), "{}: {errors:?}", p.client_id);
        let filter = p.offset_error_us();
        assert!(
            filter <= 500.0,
            "{}: filter off by {filter} µs",
            p.client_id
        );
    }
    let starts = |p: &FakePlayer| -> Vec<String> {
        p.log()
            .into_iter()
            .filter(|l| l.starts_with("start"))
            .collect()
    };
    assert_eq!(starts(pcm), ["start Pcm/16"]);
    assert_eq!(starts(flac), ["start Flac/16", "start Pcm/16"]);
    assert_eq!(starts(opus).len(), 1, "{:?}", opus.log());
    assert!(starts(opus)[0].starts_with("start Opus"));
    assert_eq!(starts(pcm24).len(), 2, "{:?}", pcm24.log());
    assert_eq!(starts(pcm24)[0], "start Pcm/24");
    assert!(starts(pcm24)[1].starts_with("start Opus"));
    for p in [pcm, flac, pcm24, opus] {
        assert_eq!(
            p.log().iter().filter(|l| *l == "clear").count(),
            1,
            "{}: {:?}",
            p.client_id,
            p.log()
        );
    }
    assert_eq!(pcm.log().last().unwrap(), "group stopped");

    // When each played what.
    let out: Vec<Playout> = players.iter().map(FakePlayer::playout).collect();
    let [o_pcm, o_flac, o_opus, o_pcm24] = &out[..] else {
        unreachable!()
    };

    // No holes but the ones a clear makes: from the first chunk of each
    // stream until the next clear or end.
    let us = |t: i64| t as f64;
    let first = 300_000;
    for (name, o, from, to) in [
        ("pcm", o_pcm, t_play + first, t_clear),
        ("flac", o_flac, t_play + first, t_clear),
        ("pcm24", o_pcm24, t_play + first, t_clear),
        ("opus", o_opus, t_join + first, t_clear),
        ("pcm", o_pcm, t_clear + first, t_leave),
        ("flac", o_flac, t_clear + first, t_stop),
        ("opus", o_opus, t_clear + first, t_stop),
        ("pcm24", o_pcm24, t_clear + first, t_stop),
    ] {
        let holes = o.holes(us(from), us(to));
        assert!(
            holes.is_empty(),
            "{name}: holes {:?} in {from}..{to}",
            holes
                .iter()
                .map(|(a, b)| (a - us(from), b - a))
                .collect::<Vec<_>>()
        );
    }
    // The one who left stopped when told to.
    let pcm_end = o_pcm.end_us().unwrap();
    assert!(
        pcm_end <= us(t_leave) + END_SLACK_US,
        "pcm played {} µs past leaving",
        pcm_end - us(t_leave)
    );

    // Every pair in the group, once each phase has settled.
    let names = ["pcm", "flac", "opus", "pcm24"];
    let phases: [(&str, i64, &[usize]); 5] = [
        ("steady", t_play, &[0, 1, 3]),
        ("late join", t_join, &[0, 1, 2, 3]),
        ("clear", t_clear, &[0, 1, 2, 3]),
        ("codec switch", t_switch, &[0, 1, 2, 3]),
        ("leave", t_leave, &[1, 2, 3]),
    ];
    let mut failures = Vec::new();
    for (phase, at, members) in phases {
        let from = us(at + WINDOW_FROM_US);
        for (n, &i) in members.iter().enumerate() {
            for &j in &members[n + 1..] {
                let pair = format!("{phase:>12}: {:>5} → {:<5}", names[i], names[j]);
                match playout::skew(&out[i], &out[j], from, WINDOW_FRAMES) {
                    Ok(s) => {
                        eprintln!(
                            "{pair} skew {:>+7.1} µs, correlation {:.3}",
                            s.lag_us, s.correlation
                        );
                        if s.lag_us.abs() > MAX_SKEW_US || s.correlation < MIN_CORRELATION {
                            failures.push(format!("{pair} {s:?}"));
                        }
                    }
                    Err(e) => failures.push(format!("{pair} {e}")),
                }
            }
        }
    }
    assert!(failures.is_empty(), "out of sync:\n{}", failures.join("\n"));
}
