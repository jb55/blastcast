//! `/debug`'s Sendspin report against fake players on a real socket: while
//! a group plays, each player shows its clock-sync samples, a ping round
//! trip, its codec, send-ahead and buffered audio; once one says goodbye it
//! moves to `departed`.

mod harness;

use std::sync::Arc;
use std::time::Duration;

use harness::noise::NoiseSource;
use harness::{FakePlayer, Spec, format};
use tas_audio::ServerClock;
use tas_core::PlayerRegistry;
use tas_sendspin::protocol::{ClientGoodbye, ClientMessage, Codec, GoodbyeReason};
use tas_sendspin::{DebugReport, SendspinServer, ServerConfig};
use tokio::net::TcpListener;

const WAIT: Duration = Duration::from_secs(5);

async fn until(server: &SendspinServer, ok: impl Fn(&DebugReport) -> bool) -> DebugReport {
    tokio::time::timeout(WAIT, async {
        loop {
            let report = server.debug_report();
            if ok(&report) {
                return report;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out: {:#?}", server.debug_report()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_report_shows_sync_stream_and_departures() {
    let clock = ServerClock::new();
    let mut config = ServerConfig::new("tas-server-id", "Tune Assistant");
    config.clock = clock;
    // Often enough for a round trip to come back within the test.
    config.ping_interval = Duration::from_millis(50);
    let server = SendspinServer::new(config, Arc::new(PlayerRegistry::default()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/sendspin", listener.local_addr().unwrap());
    tokio::spawn(server.clone().serve(listener));

    let flac = FakePlayer::connect(
        &url,
        clock,
        Spec {
            client_id: "flac",
            formats: vec![format(Codec::Flac, 16)],
            clock_base_us: 1_000,
        },
    )
    .await;
    let pcm = FakePlayer::connect(
        &url,
        clock,
        Spec {
            client_id: "pcm",
            formats: vec![format(Codec::Pcm, 16)],
            clock_base_us: 9_000_000,
        },
    )
    .await;
    let groups = server.groups();
    until(&server, |r| {
        r.clients.len() == 2 && r.clients.iter().all(|c| c.available)
    })
    .await;
    let group = groups.group_of("flac").unwrap().id;
    groups.join("pcm", &group).await.unwrap();
    groups
        .play(&group, Box::new(NoiseSource::new(1, clock)))
        .await
        .unwrap();

    let report = until(&server, |r| {
        r.clients.iter().all(|c| {
            c.stats.clock_sync.rtt_ms.is_some()
                && c.stats.stream.as_ref().is_some_and(|s| s.chunks_sent > 10)
        })
    })
    .await;
    for c in &report.clients {
        assert_eq!(c.group_id.as_deref(), Some(group.as_str()));
        let sync = &c.stats.clock_sync;
        assert!(sync.samples >= 8, "a burst on connecting: {sync:?}");
        assert!(sync.offset_us.is_some() && sync.jitter_us.is_some());
        let stream = c.stats.stream.as_ref().unwrap();
        assert!(stream.playing);
        // No buffer figures reported: the default 250 ms.
        assert_eq!(stream.send_ahead_ms, 250.0);
        assert!(stream.buffered_ms > 0.0, "{stream:?}");
        assert_eq!(stream.chunks_dropped, 0);
        assert_eq!(c.stats.last_error, None);
    }
    let codecs: Vec<_> = report
        .clients
        .iter()
        .map(|c| (c.client_id.as_str(), c.stats.stream.as_ref().unwrap().codec))
        .collect();
    assert_eq!(codecs, [("flac", Codec::Flac), ("pcm", Codec::Pcm)]);
    assert_eq!(report.groups.len(), 1);
    assert_eq!(report.groups[0].members.len(), 2);

    pcm.send(ClientMessage::Goodbye(ClientGoodbye {
        reason: Some(GoodbyeReason::Shutdown),
    }));
    let report = until(&server, |r| r.departed.len() == 1).await;
    assert_eq!(report.clients.len(), 1);
    assert_eq!(report.departed[0].client_id, "pcm");
    assert_eq!(report.departed[0].goodbye, Some(GoodbyeReason::Shutdown));
    let json = serde_json::to_value(&report).unwrap();
    assert!(
        json["clients"][0]["clock_sync"]["rtt_ms"].is_number(),
        "{json}"
    );
    drop(flac);
}
