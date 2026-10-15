//! The reorder buffer: packets in, PCM out in order, as soon as it is.
//!
//! **When audio is handed over.** At once: every chunk carries the time
//! its sender means it to play ([`crate::raop::clock`]), and whatever is
//! downstream places it there. So nothing here waits on our clock for a
//! frame's time to come. What is left is putting packets in order, and
//! deciding how long a missing one is waited for.
//!
//! **What is handed over.** Packets are placed by RTP time, unwrapped
//! across 2^32, so order and duplicates don't matter. The first packet
//! after a start, flush or idle sets the next frame to hand over, and
//! [`Playout::pop_ready`] hands over from there for as long as packets
//! follow on. At a gap (a later packet is here but not the next one):
//!
//! - it waits for the resend for [`Playout::new`]'s window from when the
//!   gap was found ([`RESEND_WINDOW`] for a stream), or until the
//!   **deadline** the caller gives, if that is sooner: when the missing
//!   frame plays less the lead downstream needs. Downstream plays a
//!   phone's audio well before its stamps ([`crate::audio`]), keeping only
//!   so much lead past its send-ahead, and whatever follows a gap is held
//!   back with it: a longer wait would leave that late;
//! - once the deadline passes, the gap is handed over as silence, up to a
//!   packet's worth at a time;
//! - a gap over [`MAX_GAP_FRAMES`] is the sender jumping its timestamps,
//!   and is skipped over at once, not played.
//!
//! With nothing at all for [`IDLE_AFTER`], the stream is **idle** (the
//! sender paused or went away), and the next packet starts it afresh. A
//! packet from before the next frame to hand over is dropped.
//!
//! Drift is not handled here: the sender's stamps follow its clock, and
//! blastcast's rate match ([`blastcast::RateMatch`]) absorbs it downstream.
//!
//! **Notes.** Each of those (a gap played as silence, a jump skipped,
//! going idle) is kept as a [`Note`] until [`Playout::take_notes`], so the
//! connection can report it.

use std::collections::BTreeMap;
use std::time::Duration;

use tokio::time::Instant;

use crate::raop::announce::{CHANNELS, SAMPLE_RATE};

/// A gap longer than this is the sender's timestamps jumping, not lost
/// packets: one second.
pub const MAX_GAP_FRAMES: i64 = SAMPLE_RATE as i64;
/// Nothing arriving for this long is the sender pausing or gone, not a
/// loss: one second, as a gap that long would be skipped anyway.
pub const IDLE_AFTER: Duration = Duration::from_secs(1);
/// How long a gap waits for its resend, from when it was found: the
/// Sendspin stream's tolerance for a late chunk, since downstream keeps
/// none of the sender's lead for it ([`crate::audio`]), so a longer wait
/// adds latency. A quiet Wi-Fi LAN's round trip is a few ms (Music on a
/// Mac: 3 ms median), so most resends still make it.
pub const RESEND_WINDOW: Duration = Duration::from_millis(20);
/// The most silence played for a gap at once: one packet's worth.
const SILENCE_FRAMES: i64 = 352;
/// The most buffered: ten seconds of 352-frame packets. A sender sends in
/// real time, so more is a sender gone wrong.
const MAX_PACKETS: usize = 1_250;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inserted {
    Queued,
    /// It was handed over already, or its time to be was given up on.
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
    /// Nothing came for [`IDLE_AFTER`]: idle.
    RanDry,
}

/// Interleaved stereo s16 frames.
pub type Frames = Vec<i16>;

/// Frames handed over, and the sender's RTP time of the first, by which
/// the sender's timeline places them ([`crate::raop::clock`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Played {
    pub rtp: u32,
    pub frames: Frames,
}

#[derive(Debug)]
pub struct Playout {
    /// How long a gap waits for its resend when no deadline is given.
    window: Duration,
    /// Packets by unwrapped RTP time.
    packets: BTreeMap<i64, Frames>,
    /// The next frame to hand over, unwrapped; `None` while idle.
    head: Option<i64>,
    /// When the latest packet arrived.
    arrived: Option<Instant>,
    /// When the gap at the head was found, while there is one.
    gap_since: Option<Instant>,
    /// After a flush: packets before this RTP time are dropped, until one
    /// at or after it comes.
    flushed_before: Option<u32>,
    notes: Vec<Note>,
}

impl Playout {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            packets: BTreeMap::new(),
            head: None,
            arrived: None,
            gap_since: None,
            flushed_before: None,
            notes: Vec::new(),
        }
    }

    /// What it did since the last call, in order.
    pub fn take_notes(&mut self) -> std::vec::Drain<'_, Note> {
        self.notes.drain(..)
    }

    /// Frames queued and not yet handed over: from the next frame to the
    /// end of the newest packet, a gap included.
    pub fn buffered_frames(&self) -> i64 {
        let Some(head) = self.head else {
            return 0;
        };
        self.packets
            .last_key_value()
            .map_or(0, |(at, f)| (at + frames(f) - head).max(0))
    }

    pub fn is_idle(&self) -> bool {
        self.head.is_none()
    }

    /// `ts` unwrapped near the head.
    fn unwrap(head: i64, ts: u32) -> i64 {
        head + i64::from(ts.wrapping_sub(head as u32) as i32)
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
        self.arrived = Some(now);
        let head = *self.head.get_or_insert(i64::from(ts));
        let mut at = Self::unwrap(head, ts);
        if at + n <= head {
            return Inserted::Late;
        }
        if at < head {
            // Its start went as silence; keep the rest.
            frames.drain(..((head - at) as usize * CHANNELS));
            at = head;
        }
        if self.packets.contains_key(&at) {
            return Inserted::Duplicate;
        }
        if self.packets.len() >= MAX_PACKETS {
            return Inserted::Overflow;
        }
        self.packets.insert(at, frames);
        Inserted::Queued
    }

    /// Drops what is buffered and goes idle. With `before`, packets from
    /// before that RTP time that are still on their way are dropped too.
    pub fn flush(&mut self, before: Option<u32>) {
        self.packets.clear();
        self.head = None;
        self.gap_since = None;
        self.flushed_before = before;
    }

    /// When the gap at the head stops waiting for its resend: the window
    /// from when the gap was found, or `deadline` for the missing frame if
    /// that says sooner.
    fn gap_deadline(&self, head: i64, deadline: &impl Fn(u32) -> Option<Instant>) -> Instant {
        let window = self.gap_since.expect("set when the gap was found") + self.window;
        deadline(head as u32).map_or(window, |d| d.min(window))
    }

    /// When [`Self::pop_ready`] next has something to do, if nothing
    /// arrives before: a gap's deadline, or, with nothing buffered, when
    /// the stream goes idle. `None` while idle.
    pub fn wake_at(&self, deadline: impl Fn(u32) -> Option<Instant>) -> Option<Instant> {
        let head = self.head?;
        Some(match self.packets.first_key_value() {
            None => self.arrived.expect("set with the head") + IDLE_AFTER,
            Some(_) if self.gap_since.is_some() => self.gap_deadline(head, &deadline),
            // Ready now: the caller pops as it inserts.
            Some(_) => Instant::now(),
        })
    }

    /// Everything ready by `now`, in order: the packets that follow on,
    /// and any gap whose `deadline` (when its first frame must be handed
    /// over by, if the sender's timeline says) has passed, as silence.
    pub fn pop_ready(
        &mut self,
        now: Instant,
        deadline: impl Fn(u32) -> Option<Instant>,
    ) -> Vec<Played> {
        let mut out = Vec::new();
        while let Some(head) = self.head {
            let Some((&at, _)) = self.packets.first_key_value() else {
                if now >= self.arrived.expect("set with the head") + IDLE_AFTER {
                    tracing::debug!("AirPlay audio ran dry; idle until the next packet");
                    self.flush(None);
                    self.notes.push(Note::RanDry);
                }
                break;
            };
            if at > head {
                let gap = at - head;
                if gap > MAX_GAP_FRAMES {
                    tracing::debug!(gap, "AirPlay timestamps jumped; skipping ahead");
                    self.head = Some(at);
                    self.gap_since = None;
                    self.notes.push(Note::Jump(gap));
                    continue;
                }
                self.gap_since.get_or_insert(now);
                if now < self.gap_deadline(head, &deadline) {
                    break;
                }
                let n = gap.min(SILENCE_FRAMES);
                out.push(Played {
                    rtp: head as u32,
                    frames: vec![0; n as usize * CHANNELS],
                });
                self.head = Some(head + n);
                self.notes.push(Note::Silence(n));
                continue;
            }
            let (_, frames) = self.packets.pop_first().expect("checked above");
            self.head = Some(head + self::frames(&frames));
            self.gap_since = None;
            out.push(Played {
                rtp: head as u32,
                frames,
            });
        }
        out
    }
}

fn frames(f: &Frames) -> i64 {
    (f.len() / CHANNELS) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: Duration = Duration::from_millis(250);
    const PACKET: u32 = 352;

    fn packet(n: u32, value: i16) -> Frames {
        vec![value; n as usize * CHANNELS]
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn no_deadline(_: u32) -> Option<Instant> {
        None
    }

    /// The first sample of each chunk, and its frame count.
    fn summary(chunks: &[Played]) -> Vec<(i16, usize)> {
        chunks
            .iter()
            .map(|c| (c.frames[0], c.frames.len() / CHANNELS))
            .collect()
    }

    #[test]
    fn hands_over_each_packet_as_it_comes() {
        let t0 = Instant::now();
        let mut p = Playout::new(WINDOW);
        for i in 0..3u32 {
            let now = t0 + ms(8) * i;
            assert_eq!(
                p.insert(now, 1000 + i * PACKET, packet(PACKET, i as i16 + 1)),
                Inserted::Queued
            );
            let out = p.pop_ready(now, no_deadline);
            assert_eq!(summary(&out), [(i as i16 + 1, 352)]);
            assert_eq!(out[0].rtp, 1000 + i * PACKET);
        }
        assert_eq!(p.buffered_frames(), 0);
        // Nothing waiting but the idle check.
        assert_eq!(p.wake_at(no_deadline), Some(t0 + ms(16) + IDLE_AFTER));
    }

    #[test]
    fn puts_reordered_packets_in_order_and_drops_late_ones() {
        let t0 = Instant::now();
        let mut p = Playout::new(WINDOW);
        p.insert(t0, 0, packet(PACKET, 1));
        p.insert(t0, 2 * PACKET, packet(PACKET, 3));
        assert_eq!(summary(&p.pop_ready(t0, no_deadline)), [(1, 352)]);
        p.insert(t0, PACKET, packet(PACKET, 2));
        assert_eq!(p.insert(t0, PACKET, packet(PACKET, 9)), Inserted::Duplicate);
        assert_eq!(summary(&p.pop_ready(t0, no_deadline)), [(2, 352), (3, 352)]);
        assert_eq!(p.insert(t0, PACKET, packet(PACKET, 2)), Inserted::Late);
    }

    /// Without the sender's timeline, a gap waits the window from when
    /// it was found, then goes as silence.
    #[test]
    fn a_lost_packet_goes_as_silence_after_the_window() {
        let t0 = Instant::now();
        let mut p = Playout::new(WINDOW);
        p.insert(t0, 0, packet(PACKET, 1));
        p.insert(t0, 2 * PACKET, packet(PACKET, 3));
        assert_eq!(summary(&p.pop_ready(t0, no_deadline)), [(1, 352)]);
        assert_eq!(p.buffered_frames(), 2 * i64::from(PACKET));
        assert_eq!(p.wake_at(no_deadline), Some(t0 + WINDOW));
        assert!(p.pop_ready(t0 + ms(249), no_deadline).is_empty());
        let played = p.pop_ready(t0 + WINDOW, no_deadline);
        assert_eq!(summary(&played), [(0, 352), (3, 352)]);
        assert_eq!(
            p.take_notes().collect::<Vec<_>>(),
            [Note::Silence(i64::from(PACKET))]
        );
        // Each keeps its place on the sender's timeline.
        let rtps: Vec<u32> = played.iter().map(|c| c.rtp).collect();
        assert_eq!(rtps, [PACKET, 2 * PACKET]);
        // Its resend comes too late.
        assert_eq!(
            p.insert(t0 + WINDOW, PACKET, packet(PACKET, 2)),
            Inserted::Late
        );
    }

    /// With the sender's timeline, a gap still waits only the window: a
    /// phone's two seconds of lead are trimmed downstream, and what
    /// follows the gap waits with it. A deadline sooner than the window
    /// cuts the wait short.
    #[test]
    fn a_gap_waits_the_window_or_its_deadline_if_sooner() {
        let t0 = Instant::now();
        let mut p = Playout::new(WINDOW);
        // Frame r must be handed over by 1.9 s plus its own time.
        let far = |rtp: u32| {
            Some(t0 + ms(1_900) + Duration::from_micros(u64::from(rtp) * 1_000_000 / 44_100))
        };
        p.insert(t0, 0, packet(PACKET, 1));
        p.insert(t0, 2 * PACKET, packet(PACKET, 3));
        assert_eq!(summary(&p.pop_ready(t0, far)), [(1, 352)]);
        assert_eq!(p.wake_at(far), Some(t0 + WINDOW));
        // The resend comes within the window.
        assert!(p.pop_ready(t0 + ms(100), far).is_empty());
        p.insert(t0 + ms(100), PACKET, packet(PACKET, 2));
        assert_eq!(
            summary(&p.pop_ready(t0 + ms(100), far)),
            [(2, 352), (3, 352)]
        );
        // Another gap, whose resend never comes: silence once the window
        // is out, nowhere near its deadline.
        let t1 = t0 + ms(200);
        p.insert(t1, 4 * PACKET, packet(PACKET, 5));
        assert!(p.pop_ready(t1, far).is_empty());
        assert_eq!(p.wake_at(far), Some(t1 + WINDOW));
        assert!(p.pop_ready(t1 + WINDOW - ms(1), far).is_empty());
        assert_eq!(
            summary(&p.pop_ready(t1 + WINDOW, far)),
            [(0, 352), (5, 352)]
        );
        // A gap whose frame is due sooner waits only until then.
        let t2 = t0 + ms(400);
        let near = |_: u32| Some(t2 + ms(30));
        p.insert(t2, 6 * PACKET, packet(PACKET, 7));
        assert!(p.pop_ready(t2, near).is_empty());
        assert_eq!(p.wake_at(near), Some(t2 + ms(30)));
        assert_eq!(
            summary(&p.pop_ready(t2 + ms(30), near)),
            [(0, 352), (7, 352)]
        );
    }

    #[test]
    fn runs_dry_then_starts_afresh() {
        let t0 = Instant::now();
        let mut p = Playout::new(WINDOW);
        p.insert(t0, 0, packet(PACKET, 1));
        assert_eq!(p.pop_ready(t0, no_deadline).len(), 1);
        assert_eq!(p.wake_at(no_deadline), Some(t0 + IDLE_AFTER));
        assert!(p.pop_ready(t0 + ms(500), no_deadline).is_empty());
        assert!(!p.is_idle());
        assert!(p.pop_ready(t0 + IDLE_AFTER, no_deadline).is_empty());
        assert!(p.is_idle());
        assert_eq!(p.take_notes().collect::<Vec<_>>(), [Note::RanDry]);
        assert_eq!(p.wake_at(no_deadline), None);

        // Sent again from further back: started afresh, not dropped.
        let t1 = t0 + Duration::from_secs(10);
        assert_eq!(p.insert(t1, 0, packet(PACKET, 5)), Inserted::Queued);
        assert_eq!(summary(&p.pop_ready(t1, no_deadline)), [(5, 352)]);
    }

    #[test]
    fn a_flush_drops_the_buffer_and_what_was_still_on_its_way() {
        let t0 = Instant::now();
        let mut p = Playout::new(WINDOW);
        p.insert(t0, 0, packet(PACKET, 1));
        p.insert(t0, 2 * PACKET, packet(PACKET, 3));
        p.flush(Some(10 * PACKET));
        assert!(p.is_idle());
        assert_eq!(p.buffered_frames(), 0);
        assert_eq!(p.insert(t0, PACKET, packet(PACKET, 2)), Inserted::Stale);
        let t1 = t0 + ms(50);
        assert_eq!(
            p.insert(t1, 10 * PACKET, packet(PACKET, 4)),
            Inserted::Queued
        );
        assert_eq!(summary(&p.pop_ready(t1, no_deadline)), [(4, 352)]);
    }

    #[test]
    fn timestamps_wrap() {
        let t0 = Instant::now();
        let mut p = Playout::new(WINDOW);
        let first = u32::MAX - PACKET + 1;
        p.insert(t0, first.wrapping_add(PACKET), packet(PACKET, 2));
        p.insert(t0, first, packet(PACKET, 1));
        p.insert(t0, first.wrapping_add(2 * PACKET), packet(PACKET, 3));
        // The first to come set the head; the one before it is late.
        let played = p.pop_ready(t0, no_deadline);
        assert_eq!(summary(&played), [(2, 352), (3, 352)]);
        assert_eq!(played[0].rtp, 0, "wrapped");
    }

    #[test]
    fn a_jump_in_timestamps_is_skipped_not_played() {
        let t0 = Instant::now();
        let mut p = Playout::new(WINDOW);
        p.insert(t0, 0, packet(PACKET, 1));
        p.insert(t0, 5 * 44_100, packet(PACKET, 2));
        assert_eq!(summary(&p.pop_ready(t0, no_deadline)), [(1, 352), (2, 352)]);
        assert_eq!(
            p.take_notes().collect::<Vec<_>>(),
            [Note::Jump(5 * 44_100 - i64::from(PACKET))]
        );
    }
}
