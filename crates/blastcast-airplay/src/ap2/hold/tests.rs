use super::*;
use crate::ap2::testing::ios_u64;

const PACKET: i64 = 352;
/// A phone 7 s ahead of us.
const OFFSET: i64 = 7_000_000;
const TIMELINE: u64 = 0x66aa_bbff_fecc_ddee;

fn map() -> ClockMap {
    ClockMap {
        offset_us: OFFSET,
        ..ClockMap::identity()
    }
}

/// Frame `f` of the sender's audio: its number on the left, negated on
/// the right, so where a frame went can be told.
fn frame(f: i64) -> [i16; 2] {
    let v = (f.rem_euclid(30_000)) as i16;
    [v, -v]
}

/// Packet `i` from RTP time `first`.
fn packet(first: u32, i: i64) -> (u32, Vec<i16>) {
    let rtp = first.wrapping_add((i * PACKET) as u32);
    let base = i * PACKET;
    (rtp, (base..base + PACKET).flat_map(frame).collect())
}

/// Packets `0..n` from `first`, held.
fn held(first: u32, n: i64) -> Hold {
    let mut hold = Hold::default();
    for i in 0..n {
        let (rtp, frames) = packet(first, i);
        assert_eq!(hold.insert(rtp, frames), Inserted::Held);
    }
    hold
}

/// Frame `rtp` plays at our `at_us`.
fn anchor(rtp: u32, at_us: i64) -> Option<RateAnchor> {
    Some(RateAnchor {
        timeline: TIMELINE,
        rtp,
        remote_us: at_us + OFFSET,
    })
}

/// When frame `f` (counted from the anchor's) plays, from `at_us`.
fn at(at_us: i64, f: i64) -> i64 {
    at_us + f * 1_000_000 / 44_100
}

/// The sender's frame numbers in `ready`, in order.
fn numbers(ready: &[Ready]) -> Vec<i16> {
    ready
        .iter()
        .flat_map(|r| r.frames.iter().step_by(2).copied())
        .collect()
}

fn frames(ready: &[Ready]) -> i64 {
    ready.iter().map(|r| r.frames.len() as i64 / 2).sum()
}

/// Packets across RTP's wrap, anchored: each goes `HAND_OVER_LEAD_US`
/// before it plays, stamped where the anchor puts its first frame, and
/// what is due at once goes as one chunk.
#[test]
fn audio_goes_a_lead_ahead_stamped_by_the_anchor() {
    let first = u32::MAX - 10 * PACKET as u32;
    let mut hold = held(first, 100);
    let map = map();
    let t = 5_000_000;
    // Held, not anchored: nothing goes.
    assert_eq!(hold.ready(0, &map, HAND_OVER_LEAD_US), []);
    assert_eq!(hold.next_due(&map, HAND_OVER_LEAD_US), None);

    // Anchored at the fifth packet, so the four before it have played.
    let rtp = first.wrapping_add(4 * PACKET as u32);
    assert!(!hold.set_rate(Some(true), anchor(rtp, t)));
    assert_eq!(hold.held_frames(), 96 * PACKET as u64);
    assert_eq!(
        hold.next_due(&map, HAND_OVER_LEAD_US),
        Some(t - HAND_OVER_LEAD_US)
    );
    assert_eq!(
        hold.ready(t - HAND_OVER_LEAD_US - 1, &map, HAND_OVER_LEAD_US),
        []
    );

    // A few packets' worth due at once: one chunk.
    let now = t - HAND_OVER_LEAD_US + 30_000;
    let ready = hold.ready(now, &map, HAND_OVER_LEAD_US);
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].play_at_us, t);
    let n = frames(&ready);
    assert_eq!(
        n,
        4 * PACKET,
        "every packet starting by {}",
        now + HAND_OVER_LEAD_US
    );
    let want: Vec<i16> = (4 * PACKET..8 * PACKET).map(|f| frame(f)[0]).collect();
    assert_eq!(numbers(&ready), want);

    // Then one packet at a time, each where the frames before it end.
    let mut sent = n;
    while sent < 96 * PACKET {
        let due = hold.next_due(&map, HAND_OVER_LEAD_US).unwrap();
        let ready = hold.ready(due, &map, HAND_OVER_LEAD_US);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].play_at_us, at(t, sent), "after {sent} frames");
        assert_eq!(ready[0].frames[..2], frame(4 * PACKET + sent));
        sent += frames(&ready);
    }
    assert_eq!(hold.next_due(&map, HAND_OVER_LEAD_US), None);
    // What has played is let go.
    hold.ready(at(t, sent), &map, HAND_OVER_LEAD_US);
    assert_eq!(hold.held_frames(), 0);
}

/// FLUSHBUFFERED from and until: exactly that range goes, packets in it
/// still to come are dropped as they come, and a gap that short after
/// audio handed over goes as silence, so the stamps still hold.
#[test]
fn a_ranged_flush_drops_exactly_its_range() {
    let mut hold = held(0, 20);
    let (from, until) = (5 * PACKET + 100, 8 * PACKET + 50);
    let (dropped, flush) = hold.flush(Some(from as u32), until as u32);
    assert_eq!(dropped, (until - from) as u64);
    assert!(!flush, "nothing was handed over");
    assert_eq!(hold.held_frames(), 20 * PACKET as u64 - dropped);
    // The packet in the range sent again, and one past it.
    let (rtp, frames6) = packet(0, 6);
    assert_eq!(hold.insert(rtp, frames6), Inserted::Flushed);
    let (rtp, frames20) = packet(0, 20);
    assert_eq!(hold.insert(rtp, frames20), Inserted::Held);

    let t = 1_000_000;
    hold.set_rate(Some(true), anchor(0, t));
    let ready = hold.ready(0, &map(), HAND_OVER_LEAD_US);
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].play_at_us, t);
    let got = numbers(&ready);
    assert_eq!(got.len() as i64, 21 * PACKET);
    for (f, &v) in got.iter().enumerate() {
        let f = f as i64;
        let want = if (from..until).contains(&f) {
            0
        } else {
            frame(f)[0]
        };
        assert_eq!(v, want, "frame {f}");
    }
}

/// A ranged flush reaching into what was handed over flushes the sink,
/// and what is still to play goes again, less the range.
#[test]
fn a_ranged_flush_into_what_went_flushes_the_sink() {
    let mut hold = held(0, 300);
    let t = 4_000_000;
    hold.set_rate(Some(true), anchor(0, t));
    let map = map();
    // 3 s of lead: the first 1.5 s of audio has gone.
    let now = t - 1_500_000;
    let went = frames(&hold.ready(now, &map, HAND_OVER_LEAD_US));
    assert!(went > 44_100, "{went}");
    let (from, until) = (100 * PACKET, 110 * PACKET);
    let (_, flush) = hold.flush(Some(from as u32), until as u32);
    assert!(flush);
    let ready = hold.ready(now, &map, HAND_OVER_LEAD_US);
    // From the start again, since none of it has played.
    assert_eq!(ready[0].play_at_us, t);
    let got = numbers(&ready);
    assert_eq!(got[..from as usize], numbers_of(0..from));
    assert!(got[from as usize..until as usize].iter().all(|&v| v == 0));
}

fn numbers_of(frames: std::ops::Range<i64>) -> Vec<i16> {
    frames.map(|f| frame(f)[0]).collect()
}

/// FLUSHBUFFERED with only until: everything before it goes, and the
/// sink is flushed if any of it was handed over.
#[test]
fn a_flush_until_drops_everything_before() {
    let mut hold = held(1000, 50);
    let t = 2_000_000;
    hold.set_rate(Some(true), anchor(1000, t));
    let map = map();
    assert!(!hold.ready(0, &map, HAND_OVER_LEAD_US).is_empty());
    let until = 1000 + 30 * PACKET as u32;
    let (dropped, flush) = hold.flush(None, until);
    assert_eq!(dropped, 30 * PACKET as u64);
    assert!(flush);
    let (rtp, frames) = packet(1000, 3);
    assert_eq!(hold.insert(rtp, frames), Inserted::Flushed);
    let ready = hold.ready(0, &map, HAND_OVER_LEAD_US);
    assert_eq!(ready[0].play_at_us, at(t, 30 * PACKET));
    assert_eq!(numbers(&ready), numbers_of(30 * PACKET..50 * PACKET));
    // Nothing went since, so a second flush owes the sink nothing.
    assert_eq!(hold.flush(None, until), (0, true));
    assert_eq!(hold.flush(None, until), (0, false));
}

/// Rate 0 flushes what went and sends nothing more; rate 1 at the frame
/// it paused on plays from that frame at the new time, with nothing lost
/// and nothing played twice, though most of it had gone before the pause.
#[test]
fn a_pause_and_resume_leave_no_gap() {
    let mut hold = held(0, 600);
    let t = 2_000_000;
    let map = map();
    hold.set_rate(Some(true), anchor(0, t));
    let mut got = Vec::new();
    let mut now = 0;
    while now <= t + 500_000 {
        got.extend(hold.ready(now, &map, HAND_OVER_LEAD_US));
        now += 10_000;
    }
    // Paused half a second in, mid-packet.
    let paused = 22_050;
    let gone = frames(&got);
    assert!(gone > paused + 44_100, "{gone}");
    assert!(hold.set_rate(Some(false), anchor(paused as u32, t + 500_000)));
    assert_eq!(hold.ready(now, &map, HAND_OVER_LEAD_US), []);
    assert_eq!(hold.next_due(&map, HAND_OVER_LEAD_US), None);
    // Nothing went while paused, so a second pause owes nothing.
    assert!(!hold.set_rate(Some(false), None));

    let t2 = 60_000_000;
    assert!(!hold.set_rate(Some(true), anchor(paused as u32, t2)));
    let ready = hold.ready(t2 - HAND_OVER_LEAD_US + 100_000, &map, HAND_OVER_LEAD_US);
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].play_at_us, t2);
    let resumed = numbers(&ready);
    assert_eq!(resumed[..1000], numbers_of(paused..paused + 1000));
    // Contiguous on to the end: a frame each, at the new time.
    let mut f = paused + frames(&ready);
    while let Some(due) = hold.next_due(&map, HAND_OVER_LEAD_US) {
        for r in hold.ready(due, &map, HAND_OVER_LEAD_US) {
            assert_eq!(r.play_at_us, at(t2, f - paused));
            assert_eq!(r.frames[..2], frame(f));
            f += r.frames.len() as i64 / 2;
        }
    }
    assert_eq!(f, 600 * PACKET);
}

/// An anchor that follows from the last changes nothing; one that moves
/// the timeline flushes the sink, and what hasn't played goes again at
/// its new times.
#[test]
fn only_an_anchor_that_moves_the_timeline_flushes() {
    let mut hold = held(0, 300);
    let t = 2_000_000;
    let map = map();
    hold.set_rate(Some(true), anchor(0, t));
    let went = frames(&hold.ready(0, &map, HAND_OVER_LEAD_US));
    // The same timeline, said again at a later frame, to within a µs.
    let f = 44_100;
    assert!(!hold.set_rate(Some(true), anchor(f as u32, at(t, f))));
    assert_eq!(hold.ready(0, &map, HAND_OVER_LEAD_US), []);
    // Moved 50 ms later.
    assert!(hold.set_rate(Some(true), anchor(f as u32, at(t, f) + 50_000)));
    let ready = hold.ready(100_000, &map, HAND_OVER_LEAD_US);
    assert_eq!(ready[0].play_at_us, at(t, f) + 50_000);
    assert_eq!(ready[0].frames[..2], frame(f));
    assert!(frames(&ready) < went);
    // A timeline that isn't the last one's moves it too.
    let mut other = anchor(f as u32, at(t, f) + 50_000).unwrap();
    other.timeline += 1;
    assert!(hold.set_rate(Some(true), Some(other)));
}

/// A packet whose time has passed is dropped, never handed over late.
#[test]
fn late_audio_is_dropped() {
    let mut hold = held(0, 100);
    let t = 1_000_000;
    hold.set_rate(Some(true), anchor(0, t));
    // 10 packets late, and the eleventh partly.
    let now = at(t, 10 * PACKET + 100);
    let ready = hold.ready(now, &map(), HAND_OVER_LEAD_US);
    assert_eq!(hold.take_late(), 11 * PACKET as u64);
    assert_eq!(ready[0].play_at_us, at(t, 11 * PACKET));
    assert_eq!(ready[0].frames[..2], frame(11 * PACKET));
    let (rtp, frames) = packet(0, 3);
    assert_eq!(hold.insert(rtp, frames), Inserted::Late);
}

/// The hold fills to `AUDIO_BUFFER_SIZE` of decoded audio, and is read
/// again once some has played.
#[test]
fn the_hold_is_bounded() {
    let mut hold = Hold::default();
    let mut i = 0;
    while !hold.is_full() {
        let (rtp, frames) = packet(0, i);
        hold.insert(rtp, frames);
        i += 1;
    }
    assert_eq!(i, MAX_HELD_FRAMES.div_ceil(PACKET as u64) as i64);
    const { assert!(MAX_HELD_FRAMES * 4 == AUDIO_BUFFER_SIZE) };
    let (rtp, frames) = packet(0, 0);
    assert_eq!(hold.insert(rtp, frames), Inserted::Duplicate);
    let t = 1_000_000;
    hold.set_rate(Some(true), anchor(0, t));
    let map = map();
    hold.ready(t, &map, HAND_OVER_LEAD_US);
    hold.ready(t + 1_000_000, &map, HAND_OVER_LEAD_US);
    assert!(!hold.is_full());
}

#[test]
fn network_time_round_trips_to_the_microsecond() {
    for ns in [0, 999, 1_000, 123_456_789_012, 1_700_000_000_999_999_999] {
        let (secs, frac) = network_time(ns);
        assert_eq!(network_time_us(secs, frac), (ns / 1000) as i64, "{ns}");
    }
    // Half a second.
    assert_eq!(network_time_us(3, 1 << 63), 3_500_000);
}

#[test]
fn requests_read_as_commands() {
    let write = |d: plist::Dictionary| {
        let mut out = Vec::new();
        Value::Dictionary(d).to_writer_binary(&mut out).unwrap();
        out
    };
    let mut d = plist::Dictionary::new();
    d.insert("rate".into(), Value::from(1u64));
    d.insert("rtpTime".into(), Value::from(4_000_000_000u64));
    d.insert("networkTimeSecs".into(), Value::from(12u64));
    d.insert("networkTimeFrac".into(), Value::from(1u64 << 62));
    d.insert("networkTimeTimelineID".into(), Value::from(TIMELINE));
    assert_eq!(
        Command::rate(&write(d)),
        Some(Command::Rate {
            playing: Some(true),
            anchor: Some(RateAnchor {
                timeline: TIMELINE,
                rtp: 4_000_000_000,
                remote_us: 12_250_000,
            }),
        })
    );
    // As iOS writes them: 8-byte signed integers, so a fraction past half
    // a second and a clock id with its top bit set read as negative.
    let soundbar = 0x984e_8aff_fe92_2389;
    let mut d = plist::Dictionary::new();
    d.insert("rate".into(), Value::from(1i64));
    d.insert("rtpTime".into(), Value::from(1_067_985_882i64));
    d.insert("networkTimeSecs".into(), Value::from(85_141i64));
    d.insert("networkTimeFrac".into(), ios_u64(3 << 62));
    d.insert("networkTimeTimelineID".into(), ios_u64(soundbar));
    assert_eq!(
        Command::rate(&write(d)),
        Some(Command::Rate {
            playing: Some(true),
            anchor: Some(RateAnchor {
                timeline: soundbar,
                rtp: 1_067_985_882,
                remote_us: 85_141_750_000,
            }),
        })
    );

    let mut d = plist::Dictionary::new();
    d.insert("rate".into(), Value::Real(0.0));
    assert_eq!(
        Command::rate(&write(d)),
        Some(Command::Rate {
            playing: Some(false),
            anchor: None
        })
    );

    let mut d = plist::Dictionary::new();
    d.insert("flushUntilSeq".into(), Value::from(9u64));
    d.insert("flushUntilTS".into(), Value::from(3168u64));
    assert_eq!(
        Command::flush(&write(d.clone())),
        Some(Command::Flush {
            from: None,
            until: 3168
        })
    );
    d.insert("flushFromSeq".into(), Value::from(5u64));
    d.insert("flushFromTS".into(), Value::from(1760u64));
    assert_eq!(
        Command::flush(&write(d)),
        Some(Command::Flush {
            from: Some(1760),
            until: 3168
        })
    );
    assert_eq!(Command::flush(&write(plist::Dictionary::new())), None);
    assert_eq!(Command::rate(b"not a plist"), None);
}

/// Paused is said once it pauses after playing, and Resumed once it plays
/// again; not before it first plays, nor for a rate that changes nothing.
#[test]
fn paused_and_resumed_follow_the_rate_after_it_first_plays() {
    let mut hold = held(1000, 10);
    hold.set_rate(Some(false), anchor(1000, 0));
    assert!(!hold.is_paused(), "not playing yet isn't paused");
    hold.set_rate(Some(true), anchor(1000, 0));
    assert!(!hold.is_paused());
    hold.set_rate(Some(true), None);
    assert!(!hold.is_paused());
    hold.set_rate(Some(false), anchor(1352, 10_000));
    assert!(hold.is_paused());
    hold.set_rate(None, None);
    assert!(hold.is_paused(), "a rate that changes nothing");
    hold.set_rate(Some(true), anchor(1352, 50_000));
    assert!(!hold.is_paused());
}

/// The grandmaster changes and no new anchor comes: the anchor's timeline
/// loses its map. The hold plays on the last map, then moves the anchor
/// onto the new grandmaster's timeline once that maps, at the same server
/// times.
#[test]
fn an_anchor_follows_the_grandmaster_onto_its_timeline() {
    const SOUNDBAR: u64 = 0x984e_8aff_fe92_2389;
    let mut hold = held(1000, 10);
    hold.set_rate(Some(true), anchor(1000, 50_000));
    let mut timeline = Timeline::default();
    let tv = map();
    // The soundbar's clock runs 3 s and 20 ppm away from the TV's.
    let soundbar = ClockMap {
        offset_us: OFFSET + 3_000_000,
        drift_ppm: 20.0,
        at_us: 40_000,
        ..ClockMap::identity()
    };
    let before = hold.play_at_us(1000 + 5 * PACKET as u32, &tv).unwrap();

    // Never mapped: nothing.
    let mut fresh = held(1000, 1);
    fresh.set_rate(Some(true), anchor(1000, 50_000));
    assert_eq!(Timeline::default().map(&mut fresh, |_| None, None), None);

    let maps = |t: u64| (t == TIMELINE).then_some(tv);
    assert_eq!(timeline.map(&mut hold, maps, Some(TIMELINE)), Some(tv));
    // The TV's map is gone and the soundbar's hasn't come: the last map.
    assert_eq!(timeline.map(&mut hold, |_| None, Some(SOUNDBAR)), Some(tv));
    assert_eq!(hold.timeline(), Some(TIMELINE));
    // The soundbar's maps: the anchor moves onto it.
    let maps = |t: u64| (t == SOUNDBAR).then_some(soundbar);
    assert_eq!(
        timeline.map(&mut hold, maps, Some(SOUNDBAR)),
        Some(soundbar)
    );
    assert_eq!(hold.timeline(), Some(SOUNDBAR));
    let after = hold
        .play_at_us(1000 + 5 * PACKET as u32, &soundbar)
        .unwrap();
    assert!((after - before).abs() <= 1, "{before} → {after}");
    // And stays there.
    assert_eq!(
        timeline.map(&mut hold, maps, Some(SOUNDBAR)),
        Some(soundbar)
    );

    // A phone anchor on the soundbar's timeline where the move put it
    // changes nothing; nothing is flushed.
    let remote_us = soundbar.to_remote_us(50_000);
    let again = RateAnchor {
        timeline: SOUNDBAR,
        rtp: 1000,
        remote_us,
    };
    assert!(!hold.set_rate(Some(true), Some(again)));
}
