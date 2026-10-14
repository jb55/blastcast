//! Groups against fake connections on tokio's paused clock.

use std::time::Duration;

use tas_audio::{SESSION_FORMAT, SineSource};

use super::*;
use crate::connection::Queues;
use crate::protocol::{
    BinaryFrame, ClientHello, ClientState, Codec, ConnectionReason, HeaderLayout,
    PlayerCommandKind, PlayerSupport, StreamFormat, binary,
};

const PCM: StreamFormat = StreamFormat {
    codec: Codec::Pcm,
    channels: 2,
    sample_rate: 48_000,
    bit_depth: 16,
};

fn client(clock: ServerClock, id: &str) -> (Arc<Connection>, Queues) {
    let hello = ClientHello {
        client_id: id.into(),
        name: format!("{id} speaker"),
        player_v1_support: Some(PlayerSupport {
            supported_formats: vec![PCM],
            supported_commands: vec![PlayerCommandKind::Volume, PlayerCommandKind::Mute],
            ..Default::default()
        }),
        ..Default::default()
    };
    let (conn, queues) = Connection::new(
        hello,
        vec!["player@v1".into()],
        ConnectionReason::Playback,
        clock,
    );
    let conn = Arc::new(conn);
    available(&conn, true);
    (conn, queues)
}

fn available(conn: &Connection, available: bool) {
    conn.apply_state(&ClientState {
        available: Some(available),
        ..Default::default()
    });
}

async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

/// Control messages queued so far: `start`, `end`, `clear`,
/// `group <state> <name>` and `command <kind>`.
fn control(q: &mut Queues) -> Vec<String> {
    let mut out = Vec::new();
    while let Ok(msg) = q.control.try_recv() {
        out.push(match msg {
            ServerMessage::StreamStart(_) => "start".into(),
            ServerMessage::StreamClear(_) => "clear".into(),
            ServerMessage::StreamEnd(_) => "end".into(),
            ServerMessage::GroupUpdate(u) => {
                let state = match u.playback_state.expect("full state") {
                    PlaybackState::Playing => "playing",
                    PlaybackState::Stopped => "stopped",
                    other => panic!("{other:?}"),
                };
                assert!(u.group_id.is_some(), "full state");
                format!("group {state} {}", u.group_name.expect("full state"))
            }
            ServerMessage::Command(c) => format!("command {:?}", c.player.unwrap().command),
            other => panic!("unexpected {other:?}"),
        });
    }
    out
}

/// Audio chunks queued in the current epoch: what the writer would send.
fn audio(conn: &Connection, q: &mut Queues) -> Vec<(i64, Vec<u8>)> {
    let epoch = conn.audio_epoch();
    let mut out = Vec::new();
    while let Ok((e, frame)) = q.audio.try_recv() {
        if e != epoch {
            continue;
        }
        let f = BinaryFrame::decode(&frame, HeaderLayout::Legacy).unwrap();
        assert_eq!(f.kind, binary::AUDIO_CHUNK);
        out.push((f.timestamp_us, f.payload.to_vec()));
    }
    out
}

fn tone(clock: ServerClock) -> Box<dyn LiveSource> {
    Box::new(SineSource::new(SESSION_FORMAT, 440.0).paced(clock))
}

/// Each chunk starts where the one before it ends.
fn assert_contiguous(chunks: &[(i64, Vec<u8>)]) {
    for pair in chunks.windows(2) {
        let frames = (pair[0].1.len() / 4) as i64;
        assert_eq!(pair[1].0, pair[0].0 + frames * 1_000_000 / 48_000);
    }
}

#[tokio::test(start_paused = true)]
async fn a_client_gets_a_stopped_group_of_its_own() {
    let clock = ServerClock::new();
    let groups = Groups::new(clock);
    let (kitchen, mut kq) = client(clock, "kitchen");
    groups.connect(kitchen.clone());
    sleep_ms(1).await;

    assert_eq!(control(&mut kq), ["group stopped kitchen speaker"]);
    let g = groups.group_of("kitchen").unwrap();
    assert_eq!(g.members, ["kitchen"]);
    assert_eq!(g.playback_state, PlaybackState::Stopped);
    assert_eq!(groups.list(), std::slice::from_ref(&g));

    groups.rename(&g.id, "Kitchen").await.unwrap();
    assert_eq!(control(&mut kq), ["group stopped Kitchen"]);

    groups.disconnect(kitchen);
    sleep_ms(1).await;
    assert!(groups.list().is_empty(), "an empty group is dropped");
}

#[tokio::test(start_paused = true)]
async fn a_group_plays_one_timeline_to_all_its_players() {
    let clock = ServerClock::new();
    let groups = Groups::new(clock);
    let (kitchen, mut kq) = client(clock, "kitchen");
    let (den, mut dq) = client(clock, "den");
    groups.connect(kitchen.clone());
    groups.connect(den.clone());
    sleep_ms(1).await;
    let group = groups.group_of("kitchen").unwrap().id;
    groups.join("den", &group).await.unwrap();
    assert_eq!(groups.get(&group).unwrap().members, ["kitchen", "den"]);
    assert_eq!(groups.list().len(), 1, "the den's own group is gone");

    groups.play(&group, tone(clock)).await.unwrap();
    sleep_ms(100).await;
    assert_eq!(
        control(&mut kq),
        [
            "group stopped kitchen speaker",
            "group playing kitchen speaker",
            "start"
        ]
    );
    assert_eq!(
        control(&mut dq),
        [
            "group stopped den speaker",
            "group stopped kitchen speaker",
            "group playing kitchen speaker",
            "start"
        ]
    );
    let k = audio(&kitchen, &mut kq);
    assert_eq!(k.len(), 4, "a chunk each 20 ms from 21 ms");
    assert_eq!(audio(&den, &mut dq), k);

    groups.stop(&group).await.unwrap();
    assert_eq!(control(&mut kq), ["end", "group stopped kitchen speaker"]);
    assert_eq!(control(&mut dq), ["end", "group stopped kitchen speaker"]);
}

/// The card's test: two players in one group stay in sync while one joins
/// and the other leaves. Here in sync means the same bytes at the same
/// timestamp, so 0 µs apart.
#[tokio::test(start_paused = true)]
async fn players_stay_in_sync_as_one_joins_and_the_other_leaves() {
    let clock = ServerClock::new();
    let groups = Groups::new(clock);
    let (kitchen, mut kq) = client(clock, "kitchen");
    let (den, mut dq) = client(clock, "den");
    groups.connect(kitchen.clone());
    groups.connect(den.clone());
    sleep_ms(1).await;
    let group = groups.group_of("kitchen").unwrap().id;
    groups.play(&group, tone(clock)).await.unwrap();
    sleep_ms(200).await;
    control(&mut dq);

    groups.join("den", &group).await.unwrap();
    sleep_ms(200).await;
    assert_eq!(control(&mut dq), ["group playing kitchen speaker", "start"]);
    let k = audio(&kitchen, &mut kq);
    let d = audio(&den, &mut dq);
    assert!(d.len() >= 10, "{}", d.len());
    // The den plays the kitchen's latest chunks: the same audio, at the
    // same time.
    assert_eq!(d[..], k[k.len() - d.len()..]);

    let den_solo = groups.leave("kitchen").await.unwrap();
    assert_ne!(den_solo, group);
    sleep_ms(200).await;
    assert_eq!(
        control(&mut kq),
        [
            "group stopped kitchen speaker",
            "group playing kitchen speaker",
            "start",
            "end",
            "group stopped kitchen speaker"
        ]
    );
    assert!(audio(&kitchen, &mut kq).is_empty());
    // The den carries on in the group without a gap.
    let after = audio(&den, &mut dq);
    assert_eq!(after.len(), 10);
    assert_contiguous(&[d, after].concat());
    let g = groups.get(&group).unwrap();
    assert_eq!(
        (g.members, g.playback_state),
        (vec!["den".into()], PlaybackState::Playing)
    );
}

#[tokio::test(start_paused = true)]
async fn moving_between_playing_groups_ends_the_old_stream_first() {
    let clock = ServerClock::new();
    let groups = Groups::new(clock);
    let (kitchen, mut kq) = client(clock, "kitchen");
    let (den, mut dq) = client(clock, "den");
    groups.connect(kitchen.clone());
    groups.connect(den.clone());
    sleep_ms(1).await;
    let kitchen_group = groups.group_of("kitchen").unwrap().id;
    let den_group = groups.group_of("den").unwrap().id;
    groups.play(&kitchen_group, tone(clock)).await.unwrap();
    groups.play(&den_group, tone(clock)).await.unwrap();
    sleep_ms(100).await;
    control(&mut dq);
    audio(&den, &mut dq);

    groups.join("den", &kitchen_group).await.unwrap();
    sleep_ms(100).await;
    assert_eq!(
        control(&mut dq),
        ["end", "group playing kitchen speaker", "start"]
    );
    assert!(groups.get(&den_group).is_none());
    let k = audio(&kitchen, &mut kq);
    let d = audio(&den, &mut dq);
    assert_eq!(d[..], k[k.len() - d.len()..], "only the new group's audio");
}

#[tokio::test(start_paused = true)]
async fn an_unavailable_member_moves_to_a_group_of_its_own() {
    let clock = ServerClock::new();
    let groups = Groups::new(clock);
    let (kitchen, mut kq) = client(clock, "kitchen");
    let (den, mut dq) = client(clock, "den");
    groups.connect(kitchen.clone());
    groups.connect(den.clone());
    sleep_ms(1).await;
    let group = groups.group_of("kitchen").unwrap().id;
    groups.join("den", &group).await.unwrap();
    groups.play(&group, tone(clock)).await.unwrap();
    sleep_ms(100).await;
    control(&mut dq);

    available(&den, false);
    groups.unavailable(den.clone());
    sleep_ms(1).await;
    assert_eq!(control(&mut dq), ["end", "group stopped den speaker"]);
    assert_eq!(groups.previous_group("den"), Some(group.clone()));
    assert_eq!(groups.get(&group).unwrap().members, ["kitchen"]);

    // Back, it stays where it is.
    available(&den, true);
    sleep_ms(100).await;
    assert!(control(&mut dq).is_empty());
    assert!(audio(&den, &mut dq).is_empty());
    assert_ne!(groups.group_of("den").unwrap().id, group);

    // Alone and playing, its group stops instead.
    control(&mut kq);
    available(&kitchen, false);
    groups.unavailable(kitchen.clone());
    sleep_ms(1).await;
    assert_eq!(control(&mut kq), ["end", "group stopped kitchen speaker"]);
    assert_eq!(groups.previous_group("kitchen"), None);
    assert_eq!(
        groups.get(&group).unwrap().playback_state,
        PlaybackState::Stopped
    );
}

#[tokio::test(start_paused = true)]
async fn volume_and_mute_go_to_every_member_that_takes_them() {
    let clock = ServerClock::new();
    let groups = Groups::new(clock);
    let (kitchen, mut kq) = client(clock, "kitchen");
    let (den, mut dq) = client(clock, "den");
    let hello = ClientHello {
        client_id: "attic".into(),
        name: "attic".into(),
        player_v1_support: Some(PlayerSupport {
            supported_formats: vec![PCM],
            ..Default::default()
        }),
        ..Default::default()
    };
    let (attic, mut aq) = Connection::new(
        hello,
        vec!["player@v1".into()],
        ConnectionReason::Playback,
        clock,
    );
    let attic = Arc::new(attic);
    for conn in [&kitchen, &den, &attic] {
        groups.connect(conn.clone());
    }
    sleep_ms(1).await;
    let group = groups.group_of("kitchen").unwrap().id;
    groups.join("den", &group).await.unwrap();
    groups.join("attic", &group).await.unwrap();
    for q in [&mut kq, &mut dq, &mut aq] {
        control(q);
    }

    groups.set_volume(&group, 40).await.unwrap();
    groups.set_mute(&group, true).await.unwrap();
    assert_eq!(control(&mut kq), ["command Volume", "command Mute"]);
    assert_eq!(control(&mut dq), ["command Volume", "command Mute"]);
    assert!(control(&mut aq).is_empty(), "it listed neither");
    assert!(matches!(
        groups.set_volume("nope", 1).await,
        Err(GroupError::NoGroup(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn a_group_whose_stream_plays_out_is_stopped() {
    let clock = ServerClock::new();
    let groups = Groups::new(clock);
    let (kitchen, mut kq) = client(clock, "kitchen");
    groups.connect(kitchen.clone());
    sleep_ms(1).await;
    let group = groups.group_of("kitchen").unwrap().id;
    let short = SineSource::new(SESSION_FORMAT, 440.0)
        .limit_frames(4_800)
        .paced(clock);
    groups.play(&group, Box::new(short)).await.unwrap();
    assert!(matches!(
        groups.queue_next("nope", tone(clock)).await,
        Err(GroupError::NoGroup(_))
    ));
    sleep_ms(1000).await;
    assert_eq!(
        control(&mut kq),
        [
            "group stopped kitchen speaker",
            "group playing kitchen speaker",
            "start",
            "end",
            "group stopped kitchen speaker"
        ]
    );
    assert!(matches!(
        groups.queue_next(&group, tone(clock)).await,
        Err(GroupError::NotPlaying(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn a_reconnect_keeps_the_clients_group() {
    let clock = ServerClock::new();
    let groups = Groups::new(clock);
    let (kitchen, _kq) = client(clock, "kitchen");
    let (den, _dq) = client(clock, "den");
    groups.connect(kitchen.clone());
    groups.connect(den.clone());
    sleep_ms(1).await;
    let group = groups.group_of("kitchen").unwrap().id;
    groups.join("den", &group).await.unwrap();
    groups.play(&group, tone(clock)).await.unwrap();

    let (den2, mut dq2) = client(clock, "den");
    groups.connect(den2.clone());
    // The old connection's end comes after the new one took its place.
    groups.disconnect(den.clone());
    sleep_ms(100).await;
    assert_eq!(groups.get(&group).unwrap().members, ["kitchen", "den"]);
    assert_eq!(
        control(&mut dq2),
        ["group playing kitchen speaker", "start"]
    );
    assert!(!audio(&den2, &mut dq2).is_empty());
}
