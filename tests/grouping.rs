//! Ad-hoc grouping through the registry, MA's way, on real Sendspin
//! players: `set_members` builds a group, the leader leaves it while it
//! plays and the rest play on without a gap, a member going unavailable
//! drops out of its leader's `group_members`, and an idle leader leaving
//! dissolves its group.

mod harness;

use std::sync::Arc;
use std::time::Duration;

use blastcast::ServerClock;
use harness::noise::NoiseSource;
use harness::{FakePlayer, Spec, format};
use tas_core::player::PlaybackState;
use tas_core::{PlayerFeature, PlayerRegistry, PlayerState};
use tas_sendspin::protocol::{ClientMessage, ClientState, Codec};
use tas_sendspin::{SendspinServer, ServerConfig};
use tokio::net::TcpListener;

const PHASE: Duration = Duration::from_millis(700);
/// Past the send-ahead of the first chunks.
const FIRST_US: f64 = 300_000.0;
/// The most a player may play past `stream/end`: its arrival.
const END_SLACK_US: f64 = 50_000.0;
const WAIT: Duration = Duration::from_secs(5);

fn ids(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "never: {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grouping_follows_music_assistant_with_a_seamless_leader_handoff() {
    let clock = ServerClock::new();
    let mut config = ServerConfig::new("tas-server-id", "Tune Assistant");
    config.clock = clock;
    let registry = Arc::new(PlayerRegistry::default());
    let server = SendspinServer::new(config, registry.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/sendspin", listener.local_addr().unwrap());
    tokio::spawn(server.clone().serve(listener));

    let mut players = Vec::new();
    for (i, client_id) in ["kitchen", "den", "porch"].into_iter().enumerate() {
        let spec = Spec {
            client_id,
            formats: vec![format(Codec::Pcm, 16)],
            clock_base_us: 1_000 + i as i64 * 7_000_000,
        };
        players.push(FakePlayer::connect(&url, clock, spec).await);
    }
    let [kitchen, den, porch] = &players[..] else {
        unreachable!()
    };
    let get = |id: &str| -> PlayerState { registry.get(id).unwrap() };
    eventually("every player registered and available", || {
        ["kitchen", "den", "porch"]
            .iter()
            .all(|id| registry.get(id).is_some_and(|p| p.available))
    })
    .await;
    assert!(get("den").supports(PlayerFeature::SetMembers));
    assert_eq!(
        registry.can_group_with("kitchen"),
        ["den", "porch"].map(String::from).into()
    );

    // Grouped: the leader lists itself first, the followers name it, and
    // all three are in one Sendspin group. The registry has it as soon as
    // the command returns.
    registry
        .set_members("kitchen", &ids(&["den", "porch"]), &[])
        .await
        .unwrap();
    assert_eq!(
        get("kitchen").group_members,
        ids(&["kitchen", "den", "porch"])
    );
    assert_eq!(get("kitchen").synced_to, None);
    for id in ["den", "porch"] {
        assert_eq!(get(id).synced_to.as_deref(), Some("kitchen"));
        assert!(get(id).group_members.is_empty());
    }
    let groups = server.groups();
    let group = groups.group_of("kitchen").unwrap();
    assert_eq!(group.members, ids(&["kitchen", "den", "porch"]));
    assert!(registry.can_group_with("den").is_empty(), "a follower");

    let t_play = clock.now_us() as f64;
    groups
        .play(&group.id, Box::new(NoiseSource::new(1, clock)))
        .await
        .unwrap();
    eventually("the mirror shows them playing", || {
        ["kitchen", "den", "porch"]
            .iter()
            .all(|id| get(id).playback_state == PlaybackState::Playing)
    })
    .await;
    tokio::time::sleep(PHASE).await;

    // The leader leaves while playing: the den leads the rest, on the same
    // stream, and the kitchen is alone and stopped.
    let t_handoff = clock.now_us() as f64;
    registry.ungroup("kitchen").await.unwrap();
    assert_eq!(get("den").group_members, ids(&["den", "porch"]));
    assert_eq!(get("porch").synced_to.as_deref(), Some("den"));
    assert_eq!(get("den").playback_state, PlaybackState::Playing);
    assert!(get("kitchen").group_members.is_empty());
    assert_eq!(get("kitchen").synced_to, None);
    assert_eq!(get("kitchen").playback_state, PlaybackState::Idle);
    assert_eq!(groups.group_of("den").unwrap().id, group.id);
    tokio::time::sleep(PHASE).await;

    // The porch's output is taken: the server moves it out by itself, and
    // its leader's members follow.
    porch.send(ClientMessage::State(ClientState {
        available: Some(false),
        ..Default::default()
    }));
    eventually("the porch drops out of the den's members", || {
        get("den").group_members.is_empty() && get("porch").synced_to.is_none()
    })
    .await;
    assert_eq!(get("den").playback_state, PlaybackState::Playing);
    tokio::time::sleep(PHASE).await;

    let t_stop = clock.now_us() as f64;
    groups.stop(&group.id).await.unwrap();
    eventually("the den is idle", || {
        get("den").playback_state == PlaybackState::Idle
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // One stream for the den the whole way, and no hole in it.
    let starts = |p: &FakePlayer| p.log().iter().filter(|l| l.starts_with("start")).count();
    assert_eq!(starts(den), 1, "{:?}", den.log());
    assert_eq!(starts(kitchen), 1, "{:?}", kitchen.log());
    let holes = den.playout().holes(t_play + FIRST_US, t_stop);
    assert!(holes.is_empty(), "den: holes {holes:?}");
    // The kitchen stopped when it left.
    let kitchen_end = kitchen.playout().end_us().unwrap();
    assert!(
        kitchen_end <= t_handoff + END_SLACK_US,
        "the kitchen played {} µs past leaving",
        kitchen_end - t_handoff
    );
    for p in &players {
        let errors: Vec<_> = p
            .log()
            .into_iter()
            .filter(|l| l.starts_with("error"))
            .collect();
        assert!(errors.is_empty(), "{}: {errors:?}", p.client_id);
    }

    // An idle leader leaving dissolves its group.
    registry.group("kitchen", "den").await.unwrap();
    assert_eq!(get("den").group_members, ids(&["den", "kitchen"]));
    registry.ungroup("den").await.unwrap();
    for id in ["den", "kitchen"] {
        assert!(get(id).group_members.is_empty(), "{id}");
        assert_eq!(get(id).synced_to, None, "{id}");
    }
    assert_ne!(
        groups.group_of("den").unwrap().id,
        groups.group_of("kitchen").unwrap().id
    );
}
