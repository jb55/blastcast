//! The jitter buffer: packets in, PCM out in real time on our clock.
//!
//! **When audio plays.** The first packet after a start, flush or underrun
//! anchors the stream: it plays [`Playout::new`]'s latency after it
//! arrived, and every later frame follows it on our clock. The latency is
//! the window a lost packet has to be sent again, and what reordering and
//! Wi-Fi jitter may cost. It is not the sender's two seconds: a sender
//! sends each packet as it plays on its own clock, so the audio comes out
//! the latency after it was sent, not two seconds after. The sender's own
//! timeline, which video lip sync needs, is kept beside this: each
//! [`Played`] carries its RTP time for [`crate::raop::clock`] to place.
//!
//! **What is played.** Packets are placed by RTP time, unwrapped across
//! 2^32, so order and duplicates don't matter. When a frame's time comes:
//!
//! - its packet is there: it plays;
//! - a later packet is there but not this one: the gap plays as silence,
//!   up to a packet's worth at a time, since its resend didn't come in
//!   time. A gap over [`MAX_GAP_FRAMES`] is the sender jumping its
//!   timestamps, and is skipped over, not played;
//! - nothing is there: we wait, and after a latency more of nothing the
//!   stream is **idle** (the sender paused or went away). The next packet
//!   anchors it afresh.
//!
//! A packet that arrives after its frames have played is dropped.
//!
//! **Drift.** The sender sends on its clock, we play on ours, and the two
//! differ by tens of ppm: over an hour, a buffer that drifted would either
//! run dry or add a few hundred milliseconds. How much is buffered when a
//! packet arrives (its end less the playhead) is measured, and the least
//! of each [`DRIFT_WINDOW`] is the buffer's floor. The first floor is the
//! target. A later one more than [`DRIFT_DEADBAND_FRAMES`] off it is
//! corrected one frame a packet: one frame dropped when the sender runs
//! fast, one repeated when it runs slow. That is at most one frame in
//! 352, inaudible, and far more than any real drift.
//!
//! **Notes.** Each of those (a gap played as silence, a jump skipped, the
//! buffer running dry, a frame dropped or repeated) is kept as a [`Note`]
//! until [`Playout::take_notes`], so the connection can report it.

use std::collections::BTreeMap;
use std::time::Duration;

use tokio::time::Instant;

use crate::raop::announce::{CHANNELS, SAMPLE_RATE};

/// A gap longer than this is the sender's timestamps jumping, not lost
/// packets: one second.
pub const MAX_GAP_FRAMES: i64 = SAMPLE_RATE as i64;
/// The most silence played for a gap at once: one packet's worth.
const SILENCE_FRAMES: i64 = 352;
/// The most buffered: ten seconds of 352-frame packets. A sender sends in
/// real time, so more is a sender gone wrong.
const MAX_PACKETS: usize = 1_250;
pub const DRIFT_WINDOW: Duration = Duration::from_secs(2);
/// 10 ms.
pub const DRIFT_DEADBAND_FRAMES: i64 = SAMPLE_RATE as i64 / 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inserted {
    Queued,
    /// The playhead has passed it.
    Late,
    /// Already queued.
    Duplicate,
    /// From before the last flush.
    Stale,
    /// The buffer is full.
    Overflow,
}

/// Something the playout did that the stream's telemetry marks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Note {
    /// A gap played as this many frames of silence.
    Silence(i64),
    /// The timestamps jumped by this many frames, skipped over.
    Jump(i64),
    /// Nothing to play for a latency: idle.
    RanDry,
    /// One frame dropped (1) or repeated (-1) for drift.
    Corrected(i64),
}

/// Interleaved stereo s16 frames, ready to play.
pub type Frames = Vec<i16>;

/// Frames played, and the sender's RTP time of the first, by which the
/// sender's timeline places them ([`crate::raop::clock`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Played {
    pub rtp: u32,
    pub frames: Frames,
}

#[derive(Debug)]
pub struct Playout {
    latency: Duration,
    /// Packets by unwrapped RTP time.
    packets: BTreeMap<i64, Frames>,
    /// The next frame to play, unwrapped, and when the stream started
    /// playing; `None` while idle.
    playhead: Option<Playhead>,
    /// After a flush: packets before this RTP time are dropped, until one
    /// at or after it comes.
    flushed_before: Option<u32>,
    drift: Drift,
    notes: Vec<Note>,
}

#[derive(Debug, Clone, Copy)]
struct Playhead {
    rtp: i64,
    /// When the frame the stream was anchored at plays.
    anchor: Instant,
    /// Frames played since.
    played: u64,
}

impl Playhead {
    fn due(&self) -> Instant {
        self.anchor + frames_duration(self.played)
    }

    /// `ts` unwrapped near the playhead.
    fn unwrap(&self, ts: u32) -> i64 {
        self.rtp + i64::from(ts.wrapping_sub(self.rtp as u32) as i32)
    }
}

#[derive(Debug, Default)]
struct Drift {
    window_start: Option<Instant>,
    window_min: Option<i64>,
    target: Option<i64>,
    /// Frames still to drop (positive) or repeat (negative).
    correction: i64,
}

impl Drift {
    fn measure(&mut self, now: Instant, buffered: i64) {
        let start = *self.window_start.get_or_insert(now);
        self.window_min = Some(self.window_min.map_or(buffered, |m| m.min(buffered)));
        if now.duration_since(start) < DRIFT_WINDOW {
            return;
        }
        let floor = self.window_min.take().expect("set above");
        self.window_start = Some(now);
        match self.target {
            None => self.target = Some(floor),
            Some(target) => {
                let off = floor - target;
                self.correction = if off.abs() > DRIFT_DEADBAND_FRAMES {
                    off
                } else {
                    0
                };
            }
        }
    }

    /// Drops or repeats a frame of `frames`, if a correction is due, and
    /// says which.
    fn correct(&mut self, frames: &mut Frames) -> Option<Note> {
        let n = frames.len() / CHANNELS;
        if self.correction > 0 && n > 1 {
            frames.truncate((n - 1) * CHANNELS);
            self.correction -= 1;
            Some(Note::Corrected(1))
        } else if self.correction < 0 && n > 0 {
            let last = frames[(n - 1) * CHANNELS..].to_vec();
            frames.extend_from_slice(&last);
            self.correction += 1;
            Some(Note::Corrected(-1))
        } else {
            None
        }
    }
}

fn frames_duration(frames: u64) -> Duration {
    Duration::from_nanos(frames * 1_000_000_000 / u64::from(SAMPLE_RATE))
}

impl Playout {
    pub fn new(latency: Duration) -> Self {
        Self {
            latency,
            packets: BTreeMap::new(),
            playhead: None,
            flushed_before: None,
            drift: Drift::default(),
            notes: Vec::new(),
        }
    }

    /// What it did since the last call, in order.
    pub fn take_notes(&mut self) -> std::vec::Drain<'_, Note> {
        self.notes.drain(..)
    }

    /// Frames queued and not yet played: from the playhead to the end of
    /// the newest packet.
    pub fn buffered_frames(&self) -> i64 {
        let Some(head) = self.playhead else {
            return 0;
        };
        self.packets.last_key_value().map_or(0, |(at, f)| {
            (at + (f.len() / CHANNELS) as i64 - head.rtp).max(0)
        })
    }

    pub fn is_idle(&self) -> bool {
        self.playhead.is_none()
    }

    /// Queues a packet of `frames` with RTP time `ts`, arrived at `now`.
    pub fn insert(&mut self, now: Instant, ts: u32, mut frames: Frames) -> Inserted {
        let n = (frames.len() / CHANNELS) as i64;
        if n == 0 {
            return Inserted::Duplicate;
        }
        if let Some(before) = self.flushed_before {
            if (ts.wrapping_sub(before) as i32) < 0 {
                return Inserted::Stale;
            }
            self.flushed_before = None;
        }
        let head = *self.playhead.get_or_insert(Playhead {
            rtp: i64::from(ts),
            anchor: now + self.latency,
            played: 0,
        });
        let mut at = head.unwrap(ts);
        if at + n <= head.rtp {
            return Inserted::Late;
        }
        if at < head.rtp {
            // Its start has played; keep the rest.
            frames.drain(..((head.rtp - at) as usize * CHANNELS));
            at = head.rtp;
        }
        if self.packets.contains_key(&at) {
            return Inserted::Duplicate;
        }
        if self.packets.len() >= MAX_PACKETS {
            return Inserted::Overflow;
        }
        let end = at + (frames.len() / CHANNELS) as i64;
        let newest = self
            .packets
            .last_key_value()
            .is_none_or(|(k, f)| end >= k + (f.len() / CHANNELS) as i64);
        self.packets.insert(at, frames);
        if newest && now >= head.anchor {
            self.drift.measure(now, end - head.rtp);
        }
        Inserted::Queued
    }

    /// Drops what is buffered and goes idle. With `before`, packets from
    /// before that RTP time that are still on their way are dropped too.
    pub fn flush(&mut self, before: Option<u32>) {
        self.packets.clear();
        self.playhead = None;
        self.drift = Drift::default();
        self.flushed_before = before;
    }

    /// When [`Self::pop_due`] next has something to do: the next frame's
    /// time, or, with nothing buffered, when the stream goes idle. `None`
    /// while idle.
    pub fn wake_at(&self) -> Option<Instant> {
        let due = self.playhead?.due();
        Some(if self.packets.is_empty() {
            due + self.latency
        } else {
            due
        })
    }

    /// Everything due by `now`, in order.
    pub fn pop_due(&mut self, now: Instant) -> Vec<Played> {
        let mut out = Vec::new();
        while let Some(head) = self.playhead {
            let due = head.due();
            if now < due {
                break;
            }
            let Some((&at, _)) = self.packets.first_key_value() else {
                if now >= due + self.latency {
                    tracing::debug!("AirPlay audio ran dry; idle until the next packet");
                    self.flush(None);
                    self.notes.push(Note::RanDry);
                }
                break;
            };
            let head = self.playhead.as_mut().expect("matched above");
            if at > head.rtp {
                let gap = at - head.rtp;
                if gap > MAX_GAP_FRAMES {
                    tracing::debug!(gap, "AirPlay timestamps jumped; skipping ahead");
                    head.rtp = at;
                    self.notes.push(Note::Jump(gap));
                    continue;
                }
                let n = gap.min(SILENCE_FRAMES);
                out.push(Played {
                    rtp: head.rtp as u32,
                    frames: vec![0; n as usize * CHANNELS],
                });
                head.rtp += n;
                head.played += n as u64;
                self.notes.push(Note::Silence(n));
                continue;
            }
            let (_, mut frames) = self.packets.pop_first().expect("checked above");
            let n = (frames.len() / CHANNELS) as i64;
            if let Some(note) = self.drift.correct(&mut frames) {
                self.notes.push(note);
            }
            let rtp = head.rtp as u32;
            head.rtp += n;
            head.played += (frames.len() / CHANNELS) as u64;
            out.push(Played { rtp, frames });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LATENCY: Duration = Duration::from_millis(250);
    const PACKET: u32 = 352;

    fn packet(n: u32, value: i16) -> Frames {
        vec![value; n as usize * CHANNELS]
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// The first sample of each chunk, and its frame count.
    fn summary(chunks: &[Played]) -> Vec<(i16, usize)> {
        chunks
            .iter()
            .map(|c| (c.frames[0], c.frames.len() / CHANNELS))
            .collect()
    }

    #[test]
    fn plays_a_latency_after_the_first_packet_then_on_time() {
        let t0 = Instant::now();
        let mut p = Playout::new(LATENCY);
        for i in 0..3u32 {
            assert_eq!(
                p.insert(t0, 1000 + i * PACKET, packet(PACKET, i as i16 + 1)),
                Inserted::Queued
            );
        }
        assert_eq!(p.wake_at(), Some(t0 + LATENCY));
        assert!(p.pop_due(t0 + ms(249)).is_empty());
        assert_eq!(summary(&p.pop_due(t0 + LATENCY)), [(1, 352)]);
        // The second plays 352 frames (7.98 ms) later.
        assert!(p.pop_due(t0 + LATENCY + ms(7)).is_empty());
        assert_eq!(summary(&p.pop_due(t0 + LATENCY + ms(8))), [(2, 352)]);
        assert_eq!(summary(&p.pop_due(t0 + LATENCY + ms(20))), [(3, 352)]);
    }

    #[test]
    fn plays_reordered_packets_in_order_and_drops_late_ones() {
        let t0 = Instant::now();
        let mut p = Playout::new(LATENCY);
        p.insert(t0, 0, packet(PACKET, 1));
        p.insert(t0, 2 * PACKET, packet(PACKET, 3));
        p.insert(t0, PACKET, packet(PACKET, 2));
        assert_eq!(p.insert(t0, PACKET, packet(PACKET, 9)), Inserted::Duplicate);
        assert_eq!(
            summary(&p.pop_due(t0 + ms(300))),
            [(1, 352), (2, 352), (3, 352)]
        );
        assert_eq!(p.insert(t0, PACKET, packet(PACKET, 2)), Inserted::Late);
    }

    #[test]
    fn a_lost_packet_plays_as_silence_when_its_time_comes() {
        let t0 = Instant::now();
        let mut p = Playout::new(LATENCY);
        p.insert(t0, 0, packet(PACKET, 1));
        p.insert(t0, 2 * PACKET, packet(PACKET, 3));
        assert_eq!(p.buffered_frames(), 3 * i64::from(PACKET));
        let played = p.pop_due(t0 + ms(300));
        assert_eq!(summary(&played), [(1, 352), (0, 352), (3, 352)]);
        assert_eq!(
            p.take_notes().collect::<Vec<_>>(),
            [Note::Silence(i64::from(PACKET))]
        );
        assert_eq!(p.buffered_frames(), 0);
        // Each keeps its place on the sender's timeline.
        let rtps: Vec<u32> = played.iter().map(|c| c.rtp).collect();
        assert_eq!(rtps, [0, PACKET, 2 * PACKET]);
    }

    #[test]
    fn a_resend_in_time_fills_the_gap() {
        let t0 = Instant::now();
        let mut p = Playout::new(LATENCY);
        p.insert(t0, 0, packet(PACKET, 1));
        p.insert(t0, 2 * PACKET, packet(PACKET, 3));
        assert_eq!(summary(&p.pop_due(t0 + LATENCY)), [(1, 352)]);
        p.insert(t0 + LATENCY, PACKET, packet(PACKET, 2));
        assert_eq!(summary(&p.pop_due(t0 + ms(300))), [(2, 352), (3, 352)]);
    }

    #[test]
    fn runs_dry_then_starts_afresh() {
        let t0 = Instant::now();
        let mut p = Playout::new(LATENCY);
        p.insert(t0, 0, packet(PACKET, 1));
        assert_eq!(p.pop_due(t0 + LATENCY).len(), 1);
        let due = t0 + LATENCY + frames_duration(352);
        // Nothing to play: wait for a packet, or a latency of nothing.
        assert_eq!(p.wake_at(), Some(due + LATENCY));
        assert!(p.pop_due(due + ms(100)).is_empty());
        assert!(!p.is_idle());
        assert!(p.pop_due(due + LATENCY).is_empty());
        assert!(p.is_idle());
        assert_eq!(p.take_notes().collect::<Vec<_>>(), [Note::RanDry]);
        assert_eq!(p.wake_at(), None);

        // Sent again ten seconds on: anchored to its own arrival.
        let t1 = t0 + Duration::from_secs(10);
        p.insert(t1, 441_000, packet(PACKET, 5));
        assert_eq!(p.wake_at(), Some(t1 + LATENCY));
        assert_eq!(summary(&p.pop_due(t1 + LATENCY)), [(5, 352)]);
    }

    #[test]
    fn a_flush_drops_the_buffer_and_what_was_still_on_its_way() {
        let t0 = Instant::now();
        let mut p = Playout::new(LATENCY);
        p.insert(t0, 0, packet(PACKET, 1));
        p.insert(t0, PACKET, packet(PACKET, 2));
        p.flush(Some(10 * PACKET));
        assert!(p.is_idle());
        assert_eq!(p.insert(t0, 2 * PACKET, packet(PACKET, 3)), Inserted::Stale);
        let t1 = t0 + ms(50);
        assert_eq!(
            p.insert(t1, 10 * PACKET, packet(PACKET, 4)),
            Inserted::Queued
        );
        assert_eq!(summary(&p.pop_due(t1 + LATENCY)), [(4, 352)]);
    }

    #[test]
    fn timestamps_wrap() {
        let t0 = Instant::now();
        let mut p = Playout::new(LATENCY);
        let first = u32::MAX - PACKET + 1;
        p.insert(t0, first, packet(PACKET, 1));
        p.insert(t0, first.wrapping_add(PACKET), packet(PACKET, 2));
        p.insert(t0, first.wrapping_add(2 * PACKET), packet(PACKET, 3));
        let played = p.pop_due(t0 + ms(300));
        assert_eq!(summary(&played), [(1, 352), (2, 352), (3, 352)]);
        assert_eq!(played[1].rtp, 0, "wrapped");
    }

    #[test]
    fn a_jump_in_timestamps_is_skipped_not_played() {
        let t0 = Instant::now();
        let mut p = Playout::new(LATENCY);
        p.insert(t0, 0, packet(PACKET, 1));
        p.insert(t0, 5 * 44_100, packet(PACKET, 2));
        assert_eq!(summary(&p.pop_due(t0 + ms(300))), [(1, 352), (2, 352)]);
        assert_eq!(
            p.take_notes().collect::<Vec<_>>(),
            [Note::Jump(5 * 44_100 - i64::from(PACKET))]
        );
    }

    /// A sender `ppm` fast (positive) or slow, sending a packet every
    /// 352 of its frames for `secs`: how far what is buffered strays from
    /// the drift target over the second half, below and above it, and
    /// the frames dropped (positive) or repeated less those repeated.
    fn drift(ppm: f64, secs: u64) -> (i64, i64, i64) {
        let t0 = Instant::now();
        let mut p = Playout::new(LATENCY);
        let period = Duration::from_secs_f64(f64::from(PACKET) / 44_100.0 / (1.0 + ppm / 1e6));
        let (mut sent, mut below, mut above) = (0i64, 0i64, 0i64);
        let mut i = 0u32;
        loop {
            let now = t0 + period * i;
            if now > t0 + Duration::from_secs(secs) {
                break;
            }
            p.insert(now, i * PACKET, packet(PACKET, 1));
            sent += i64::from(PACKET);
            // Measured as the drift is: as a packet arrives.
            if now > t0 + Duration::from_secs(secs / 2) {
                let buffered = sent - p.playhead.expect("playing").rtp;
                let target = p.drift.target.expect("measured by now");
                below = below.max(target - buffered);
                above = above.max(buffered - target);
            }
            p.pop_due(now);
            i += 1;
        }
        let corrected = p
            .take_notes()
            .map(|n| match n {
                Note::Corrected(f) => f,
                other => panic!("{other:?}"),
            })
            .sum();
        (below, above, corrected)
    }

    #[test]
    fn drift_is_corrected_either_way() {
        // Uncorrected, 300 ppm over 5 minutes is 90 ms: 3 969 frames.
        // Corrected, it stays within the deadband, give or take where in
        // a packet's 352 frames the playhead was.
        let slack = DRIFT_DEADBAND_FRAMES + 2 * i64::from(PACKET);
        for ppm in [-300.0, 300.0] {
            let (below, above, corrected) = drift(ppm, 300);
            assert!(
                below <= slack && above <= slack,
                "{ppm} ppm: {below} below, {above} above"
            );
            // Each correction is noted: most of the 3 969 frames, dropped
            // for a fast sender and repeated for a slow one.
            assert!(
                corrected.signum() == ppm.signum() as i64 && corrected.abs() > 3_000,
                "{ppm} ppm: {corrected} corrected"
            );
        }
        // On time, it stays within a packet.
        let (below, above, _) = drift(0.0, 60);
        assert!(
            below <= i64::from(PACKET) && above <= i64::from(PACKET),
            "{below} {above}"
        );
    }
}
