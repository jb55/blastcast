//! A persistent group player on real Sendspin players: powering it on
//! syncs its members into one Sendspin group, it reports its leader's
//! playback, a member that drops out and comes back is synced in again
//! while the group plays, and powering it off stops and dissolves it.

mod harness;

use std::sync::Arc;
use std::time::Duration;

use harness::noise::NoiseSource;
use harness::{FakePlayer, Spec, format};
use tas_audio::ServerClock;
use tas_core::player::{PlaybackState, PlayerType};
use tas_core::state::StateStore;
use tas_core::syncgroup::SyncGroups;
use tas_core::{PlayerCommand, PlayerRegistry, PlayerState};
use tas_sendspin::protocol::{ClientMessage, ClientState, Codec};
use tas_sendspin::{SendspinServer, ServerConfig};
use tokio::net::TcpListener;

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
async fn a_group_player_forms_follows_its_members_and_dissolves() {
    let clock = ServerClock::new();
    let mut config = ServerConfig::new("tas-server-id", "Tune Assistant");
    config.clock = clock;
    let registry = Arc::new(PlayerRegistry::default());
    let server = SendspinServer::new(config, registry.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/sendspin", listener.local_addr().unwrap());
    tokio::spawn(server.clone().serve(listener));

    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(StateStore::open(dir.path()).unwrap());
    let sync_groups = SyncGroups::new(&registry, state.clone());
    tokio::spawn(sync_groups.clone().run());
    sync_groups.load().unwrap();

    let mut players = Vec::new();
    for (i, client_id) in ["kitchen", "den", "porch"].into_iter().enumerate() {
        let spec = Spec {
            client_id,
            formats: vec![format(Codec::Pcm, 16)],
            clock_base_us: 1_000 + i as i64 * 7_000_000,
        };
        players.push(FakePlayer::connect(&url, clock, spec).await);
    }
    let [_kitchen, den, _porch] = &players[..] else {
        unreachable!()
    };
    let get = |id: &str| -> PlayerState { registry.get(id).unwrap() };
    eventually("every player registered and available", || {
        ["kitchen", "den", "porch"]
            .iter()
            .all(|id| registry.get(id).is_some_and(|p| p.available))
    })
    .await;

    let group = sync_groups
        .create("Downstairs", &ids(&["kitchen", "den"]), false)
        .unwrap();
    let gid = group.player_id.clone();
    assert_eq!(group.player_type, PlayerType::Group);
    assert_eq!(group.group_members, ids(&["kitchen", "den"]));
    assert!(state.read(|s| s.groups.contains_key(&gid)), "saved");

    // Powered on: one Sendspin group, the kitchen leading it.
    registry
        .execute(&gid, PlayerCommand::Power(true))
        .await
        .unwrap();
    let groups = server.groups();
    let sendspin_group = groups.group_of("kitchen").unwrap();
    assert_eq!(sendspin_group.members, ids(&["kitchen", "den"]));
    assert_eq!(get("den").synced_to.as_deref(), Some("kitchen"));
    assert_eq!(get(&gid).group_members, ids(&["kitchen", "den"]));
    for id in ["kitchen", "den"] {
        assert_eq!(get(id).active_group.as_deref(), Some(gid.as_str()), "{id}");
    }
    assert_eq!(get("porch").active_group, None);

    // A source playing on the leader shows on the group player.
    groups
        .play(&sendspin_group.id, Box::new(NoiseSource::new(1, clock)))
        .await
        .unwrap();
    eventually("the group player is playing", || {
        get(&gid).playback_state == PlaybackState::Playing
    })
    .await;

    // The den's output is taken: it drops out. When it is back, the group
    // takes it in again, into the stream that is still playing.
    den.send(ClientMessage::State(ClientState {
        available: Some(false),
        ..Default::default()
    }));
    eventually("the den drops out", || {
        get(&gid).group_members == ids(&["kitchen"])
    })
    .await;
    den.send(ClientMessage::State(ClientState {
        available: Some(true),
        ..Default::default()
    }));
    eventually("the den is back in", || {
        get(&gid).group_members == ids(&["kitchen", "den"])
    })
    .await;
    assert_eq!(groups.group_of("den").unwrap().id, sendspin_group.id);
    eventually("the den plays again", || {
        get("den").playback_state == PlaybackState::Playing
    })
    .await;
    eventually("the den gets the stream again", || {
        den.log().iter().filter(|l| l.starts_with("start")).count() == 2
    })
    .await;

    // Stop keeps it formed; power off dissolves it.
    registry.execute(&gid, PlayerCommand::Stop).await.unwrap();
    assert_eq!(get("den").synced_to.as_deref(), Some("kitchen"));
    registry
        .execute(&gid, PlayerCommand::Power(false))
        .await
        .unwrap();
    assert_ne!(
        groups.group_of("kitchen").unwrap().id,
        groups.group_of("den").unwrap().id
    );
    for id in ["kitchen", "den"] {
        assert_eq!(get(id).active_group, None, "{id}");
        assert_eq!(get(id).synced_to, None, "{id}");
        assert_eq!(get(id).playback_state, PlaybackState::Idle, "{id}");
    }
    let g = get(&gid);
    assert_eq!(g.powered, Some(false));
    assert_eq!(g.playback_state, PlaybackState::Idle);

    // Removed: gone from the registry and from the saved state.
    sync_groups.remove(&gid).await.unwrap();
    assert!(registry.get(&gid).is_none());
    assert!(state.read(|s| s.groups.is_empty()));
}
