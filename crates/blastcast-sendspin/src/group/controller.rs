//! The controller role's arithmetic, kept apart from the actor so it can be
//! tested on plain values: group volume and mute as the spec reads and
//! writes them, which commands a group offers, and the `switch` cycle.

use crate::protocol::{MediaCommand, PlaybackState};

use super::GroupInfo;

/// Every command a controller can send, in the order `supported_commands`
/// lists them.
const ALL_COMMANDS: [MediaCommand; 15] = [
    MediaCommand::Play,
    MediaCommand::Pause,
    MediaCommand::Stop,
    MediaCommand::Next,
    MediaCommand::Previous,
    MediaCommand::Volume,
    MediaCommand::Mute,
    MediaCommand::RepeatOff,
    MediaCommand::RepeatOne,
    MediaCommand::RepeatAll,
    MediaCommand::Shuffle,
    MediaCommand::Unshuffle,
    MediaCommand::Switch,
    MediaCommand::Seek,
    MediaCommand::SeekRelative,
];

/// The commands the groups carry out themselves; a transport's list never
/// adds or removes them.
const GROUP_COMMANDS: [MediaCommand; 3] = [
    MediaCommand::Volume,
    MediaCommand::Mute,
    MediaCommand::Switch,
];

/// What decides a group's `supported_commands`.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Offer<'a> {
    /// The transport's own commands, if the group has one.
    pub transport: &'a [MediaCommand],
    /// Whether the transport knows how far a seek may go.
    pub seekable: bool,
    /// Whether the group's stream is running, so the groups can stop it.
    pub playing: bool,
    /// Whether some member takes a volume command, and a mute command.
    pub volume: bool,
    pub mute: bool,
}

/// The group's `supported_commands`. `stop` is offered while it plays even
/// with no transport, since stopping the stream is the groups' own; `seek`
/// only with a known `seek_max_ms`, as the spec requires.
pub(crate) fn supported_commands(offer: Offer<'_>) -> Vec<MediaCommand> {
    ALL_COMMANDS
        .into_iter()
        .filter(|&c| match c {
            MediaCommand::Volume => offer.volume,
            MediaCommand::Mute => offer.mute,
            MediaCommand::Switch => true,
            MediaCommand::Stop => offer.playing || offer.transport.contains(&c),
            MediaCommand::Seek => offer.seekable && offer.transport.contains(&c),
            c => !GROUP_COMMANDS.contains(&c) && offer.transport.contains(&c),
        })
        .collect()
}

/// Group volume: the average of the volumes of the players that take a
/// volume command, rounded. `None` when none do (the spec then reports 100
/// and drops `volume`).
pub(crate) fn group_volume(volumes: &[u8]) -> Option<u8> {
    if volumes.is_empty() {
        return None;
    }
    let sum: u32 = volumes.iter().map(|&v| u32::from(v)).sum();
    let n = volumes.len() as u32;
    Some(((sum + n / 2) / n) as u8)
}

/// Group mute: true only when every player that takes a mute command is
/// muted. `None` when none do.
pub(crate) fn group_muted(muted: &[bool]) -> Option<bool> {
    (!muted.is_empty()).then(|| muted.iter().all(|&m| m))
}

/// Each player's new volume for a group volume of `requested`, by the
/// spec's algorithm: move every player by the same delta from the average,
/// and hand what a clamped player couldn't take to the others, until the
/// delta is spent or every player is at a bound. Relative levels are kept
/// as far as the bounds allow.
pub(crate) fn redistribute_volume(current: &[u8], requested: u8) -> Vec<u8> {
    if current.is_empty() {
        return Vec::new();
    }
    let n = current.len() as f64;
    let average = current.iter().map(|&v| f64::from(v)).sum::<f64>() / n;
    let mut volumes: Vec<f64> = current.iter().map(|&v| f64::from(v)).collect();
    let mut free = vec![true; current.len()];
    // The delta still to apply, summed over the players.
    let mut pending = (f64::from(requested.min(100)) - average) * n;
    while pending.abs() > 1e-9 {
        let open = free.iter().filter(|&&f| f).count();
        if open == 0 {
            break;
        }
        let share = pending / open as f64;
        pending = 0.0;
        for (v, free) in volumes.iter_mut().zip(&mut free) {
            if !*free {
                continue;
            }
            let proposed = *v + share;
            *v = proposed.clamp(0.0, 100.0);
            if *v != proposed {
                pending += proposed - *v;
                *free = false;
            }
        }
    }
    volumes.into_iter().map(|v| v.round() as u8).collect()
}

/// Where `switch` moves a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SwitchTo {
    /// Join this group.
    Group(String),
    /// Leave for a group of its own.
    Solo,
}

/// The next stop in `client_id`'s switch cycle, or `None` to stay.
///
/// A client still in the solo group going unavailable put it in goes back to
/// its `previous` group first, if that still exists. Otherwise the cycle is
/// the playing groups with several members, then the playing groups of one
/// player, then (for a player) a group of its own; the client moves to the
/// entry after the one it is in, or the first if it is in none.
///
/// Groups are sorted by their members other than the client. Counting the
/// client too, joining a lone player would make its group one "of several"
/// and move it to the front of the cycle, and `switch` would bounce between
/// two groups without ever reaching the end.
pub(crate) fn switch_target(
    client_id: &str,
    is_player: bool,
    groups: &[GroupInfo],
    previous: Option<&str>,
) -> Option<SwitchTo> {
    if let Some(previous) = previous
        && groups.iter().any(|g| g.id == previous)
    {
        return Some(SwitchTo::Group(previous.to_owned()));
    }
    let current = groups
        .iter()
        .find(|g| g.members.iter().any(|m| m == client_id))?;
    let others = |g: &GroupInfo| g.members.iter().filter(|m| *m != client_id).count();
    let playing = |g: &&GroupInfo| g.playback_state == PlaybackState::Playing;
    let mut cycle: Vec<SwitchTo> = groups
        .iter()
        .filter(playing)
        .filter(|g| others(g) > 1)
        .chain(groups.iter().filter(playing).filter(|g| others(g) == 1))
        .map(|g| SwitchTo::Group(g.id.clone()))
        .collect();
    if is_player {
        cycle.push(SwitchTo::Solo);
    }
    let alone = current.members.len() == 1;
    let here = if alone {
        SwitchTo::Solo
    } else {
        SwitchTo::Group(current.id.clone())
    };
    let next = match cycle.iter().position(|e| *e == here) {
        Some(i) => cycle.get((i + 1) % cycle.len()),
        None => cycle.first(),
    }?;
    (*next != here).then(|| next.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redistributes_by_the_spec() {
        assert_eq!(redistribute_volume(&[20, 40, 60], 50), [30, 50, 70]);
        assert_eq!(
            redistribute_volume(&[20, 40, 60], 100),
            [100, 100, 100],
            "100% reaches every player"
        );
        assert_eq!(
            redistribute_volume(&[0, 50], 15),
            [0, 30],
            "the quiet player's share goes to the other"
        );
        assert_eq!(redistribute_volume(&[90, 50], 100), [100, 100]);
        assert_eq!(redistribute_volume(&[10, 90], 0), [0, 0]);
        assert_eq!(redistribute_volume(&[30], 77), [77]);
        assert_eq!(redistribute_volume(&[], 50), [] as [u8; 0]);
        let out = redistribute_volume(&[5, 10, 95], 60);
        assert_eq!(group_volume(&out), Some(60), "{out:?}");
    }

    #[test]
    fn reads_group_volume_and_mute() {
        assert_eq!(group_volume(&[20, 41]), Some(31));
        assert_eq!(group_volume(&[]), None);
        assert_eq!(group_muted(&[true, true]), Some(true));
        assert_eq!(group_muted(&[true, false]), Some(false));
        assert_eq!(group_muted(&[]), None);
    }

    #[test]
    fn offers_the_transport_and_the_groups_own_commands() {
        use MediaCommand::*;
        let none = supported_commands(Offer::default());
        assert_eq!(none, [Switch]);

        let airplay = supported_commands(Offer {
            playing: true,
            volume: true,
            mute: true,
            ..Default::default()
        });
        assert_eq!(airplay, [Stop, Volume, Mute, Switch]);

        let full = supported_commands(Offer {
            transport: &[Seek, Play, Pause, Next, Volume, Shuffle, SeekRelative],
            seekable: false,
            ..Default::default()
        });
        assert_eq!(
            full,
            [Play, Pause, Next, Shuffle, Switch, SeekRelative],
            "volume is the members', seek needs seek_max_ms"
        );
        let seekable = supported_commands(Offer {
            transport: &[Seek],
            seekable: true,
            ..Default::default()
        });
        assert_eq!(seekable, [Switch, Seek]);
    }

    fn group(id: &str, members: &[&str], playing: bool) -> GroupInfo {
        GroupInfo {
            id: id.into(),
            name: id.into(),
            members: members.iter().map(|m| m.to_string()).collect(),
            playback_state: if playing {
                PlaybackState::Playing
            } else {
                PlaybackState::Stopped
            },
            stream: None,
        }
    }

    fn to(id: &str) -> Option<SwitchTo> {
        Some(SwitchTo::Group(id.into()))
    }

    #[test]
    fn switch_cycles_through_playing_groups_then_solo() {
        let groups = [
            group("a", &["k", "d"], true),
            group("b", &["x", "y"], true),
            group("c", &["z"], true),
            group("q", &["me"], false),
            group("s", &["w", "v"], false),
        ];
        assert_eq!(switch_target("me", true, &groups, None), to("a"));
        let mut moved = groups.to_vec();
        moved[0].members.push("me".into());
        moved.remove(3);
        assert_eq!(switch_target("me", true, &moved, None), to("b"));
        moved[0].members.pop();
        moved[1].members.push("me".into());
        assert_eq!(switch_target("me", true, &moved, None), to("c"));
        moved[1].members.pop();
        moved[2].members.push("me".into());
        assert_eq!(
            switch_target("me", true, &moved, None),
            Some(SwitchTo::Solo),
            "then a group of its own"
        );
        assert_eq!(
            switch_target("w", true, &groups, None),
            to("a"),
            "a stopped group isn't in the cycle"
        );
    }

    #[test]
    fn joining_a_lone_player_doesnt_restart_the_cycle() {
        let groups = [group("a", &["k", "me"], true), group("b", &["z"], true)];
        assert_eq!(switch_target("me", true, &groups, None), to("b"));
        let groups = [group("a", &["k"], true), group("b", &["z", "me"], true)];
        assert_eq!(
            switch_target("me", true, &groups, None),
            Some(SwitchTo::Solo),
            "b is one other player, so after a, not before it"
        );
    }

    #[test]
    fn a_controller_without_a_player_never_goes_solo() {
        let groups = [group("a", &["k", "me"], true), group("q", &["z"], false)];
        assert_eq!(switch_target("me", false, &groups, None), None, "no other");
        let groups = [group("a", &["k"], true), group("q", &["me"], false)];
        assert_eq!(switch_target("me", false, &groups, None), to("a"));
    }

    #[test]
    fn switch_goes_back_to_the_previous_group_first() {
        let groups = [
            group("a", &["k", "d"], true),
            group("p", &["x", "y"], false),
            group("q", &["me"], false),
        ];
        assert_eq!(switch_target("me", true, &groups, Some("p")), to("p"));
        assert_eq!(
            switch_target("me", true, &groups, Some("gone")),
            to("a"),
            "a dropped previous group is skipped"
        );
        assert_eq!(
            switch_target("me", true, &[group("q", &["me"], false)], None),
            None
        );
    }
}
