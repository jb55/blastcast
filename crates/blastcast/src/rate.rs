//! Rate matching: a sender's clock against ours, absorbed in one place.
//!
//! A timed source stamps each chunk with when its sender means it to play,
//! mapped onto the server clock. The sender counts its frames on its own
//! clock, though, and ours differs by tens of ppm: at 20 ppm its stamps
//! stray from the chunks' nominal durations by about 20 µs a second. The
//! sink plays frames at the nominal rate, so something must make up the
//! difference, or the stamps and the audio walk apart.
//!
//! [`RateMatch`] does, once for every input. It keeps a contiguous
//! nominal-rate [`Timeline`] from the first stamp, and yields each chunk at
//! the timeline's cursor rather than at its own stamp, so what comes out is
//! always contiguous. The **error** is a chunk's stamp less the cursor.
//! While it stays within [`DEADBAND_US`] nothing is touched, so a sender on
//! time passes through bit for bit. Once it leaves the deadband, one frame
//! of each chunk is dropped (the error is negative: the sender runs fast)
//! or its last frame repeated (positive: it runs slow) until the error
//! crosses zero. That is at most one frame in a chunk (in 352 for RAOP),
//! inaudible, and far more than any real drift.
//!
//! An error past [`REBASE_US`] is not drift but a discontinuity (the sender
//! skipped, or re-anchored), and the timeline moves to the stamp at once.
//! A flush, an unstamped chunk or the end starts the timeline afresh.
//!
//! Because the error is measured against stamps, not how much is buffered
//! or when a packet arrived, Wi-Fi jitter doesn't reach it: only the clock
//! map does, and a map that wanders within the deadband changes nothing.
//! A later stage can swap the drop and repeat for fractional resampling
//! behind the same interface.

use async_trait::async_trait;

use crate::AudioError;
use crate::format::AudioFormat;
use crate::source::{Item, TimedSource};
use crate::timeline::Timeline;

/// How far a stamp may stray from the timeline before frames are dropped
/// or repeated: 10 ms.
pub const DEADBAND_US: i64 = 10_000;
/// How far a stamp may stray before the timeline moves to it rather than
/// catching up a frame at a time: 100 ms, which at one frame in 352 would
/// take half a minute.
pub const REBASE_US: i64 = 100_000;

/// What a [`RateMatch`] has done so far.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RateStats {
    /// Frames dropped because the sender ran fast.
    pub dropped: u64,
    /// Frames repeated because it ran slow.
    pub repeated: u64,
    /// Times the timeline moved to a stamp past [`REBASE_US`].
    pub rebased: u64,
    /// The latest stamped chunk's stamp less the cursor, before its
    /// correction, in µs.
    pub error_us: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Correcting {
    No,
    /// One frame a chunk dropped.
    Drop,
    /// One frame a chunk repeated.
    Repeat,
}

/// A [`TimedSource`] whose stamps stay within [`DEADBAND_US`] of a
/// contiguous nominal-rate timeline. See the [module docs](self).
pub struct RateMatch<S> {
    source: S,
    timeline: Option<Timeline>,
    correcting: Correcting,
    stats: RateStats,
}

impl<S: TimedSource> RateMatch<S> {
    pub fn new(source: S) -> Self {
        Self {
            source,
            timeline: None,
            correcting: Correcting::No,
            stats: RateStats::default(),
        }
    }

    pub fn stats(&self) -> RateStats {
        self.stats
    }

    fn restart(&mut self) {
        self.timeline = None;
        self.correcting = Correcting::No;
    }

    /// `pcm`, stamped `stamp`, corrected and placed on the timeline.
    fn place(&mut self, pcm: &mut crate::PcmChunk, stamp: i64) -> i64 {
        let rate = self.source.format().sample_rate;
        let timeline = match &mut self.timeline {
            Some(t) if (stamp - t.cursor_us()).abs() <= REBASE_US => t,
            other => {
                if other.is_some() {
                    self.stats.rebased += 1;
                }
                self.correcting = Correcting::No;
                other.insert(Timeline::new(stamp, rate))
            }
        };
        let error = stamp - timeline.cursor_us();
        self.stats.error_us = error;
        self.correcting = match self.correcting {
            _ if error < -DEADBAND_US => Correcting::Drop,
            _ if error > DEADBAND_US => Correcting::Repeat,
            Correcting::Drop if error >= 0 => Correcting::No,
            Correcting::Repeat if error <= 0 => Correcting::No,
            c => c,
        };
        let channels = usize::from(pcm.format.channels);
        let n = pcm.frames();
        match self.correcting {
            Correcting::Drop if n > 1 => {
                pcm.samples.truncate((n - 1) * channels);
                self.stats.dropped += 1;
            }
            Correcting::Repeat if n > 0 => {
                let last = pcm.samples[(n - 1) * channels..].to_vec();
                pcm.samples.extend_from_slice(&last);
                self.stats.repeated += 1;
            }
            _ => {}
        }
        timeline.advance(pcm.frames() as u64).start_us
    }
}

#[async_trait]
impl<S: TimedSource> TimedSource for RateMatch<S> {
    fn format(&self) -> AudioFormat {
        self.source.format()
    }

    fn source_format(&self) -> AudioFormat {
        self.source.source_format()
    }

    fn is_live(&self) -> bool {
        self.source.is_live()
    }

    fn kind(&self) -> &'static str {
        self.source.kind()
    }

    fn trim_lead(&self) -> Option<i64> {
        self.source.trim_lead()
    }

    fn on_time(&self) -> bool {
        self.source.on_time()
    }

    async fn next(&mut self) -> Result<Option<Item>, AudioError> {
        let item = self.source.next().await?;
        Ok(match item {
            Some(Item::Chunk {
                mut pcm,
                play_at_us: Some(stamp),
            }) => {
                let at = self.place(&mut pcm, stamp);
                Some(Item::Chunk {
                    pcm,
                    play_at_us: Some(at),
                })
            }
            other => {
                self.restart();
                other
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PcmChunk;

    const RATE: u32 = 44_100;
    const FORMAT: AudioFormat = AudioFormat::new(RATE, 2);
    const PACKET: usize = 352;
    const T0: i64 = 2_000_000;

    /// A sender `ppm` fast (positive) or slow, sending RAOP's 352-frame
    /// packets for `secs`, each stamped with when its first frame plays on
    /// our clock, plus `wander(chunk)`: clock-map noise, or a jump. Each
    /// packet's frames count up from its first frame's number, so a
    /// dropped or repeated frame can be told.
    struct Sender {
        ppm: f64,
        chunks: u64,
        sent: u64,
        wander: fn(u64) -> i64,
    }

    impl Sender {
        fn new(ppm: f64, secs: u64) -> Self {
            Self {
                ppm,
                chunks: secs * u64::from(RATE) / PACKET as u64,
                sent: 0,
                wander: |_| 0,
            }
        }

        fn stamp(&self, frame: u64) -> i64 {
            let us = frame as f64 * 1e6 / f64::from(RATE) / (1.0 + self.ppm * 1e-6);
            T0 + us as i64 + (self.wander)(self.sent)
        }
    }

    #[async_trait]
    impl TimedSource for Sender {
        fn format(&self) -> AudioFormat {
            FORMAT
        }

        async fn next(&mut self) -> Result<Option<Item>, AudioError> {
            if self.sent == self.chunks {
                return Ok(Some(Item::End));
            }
            let first = self.sent * PACKET as u64;
            let samples = (0..PACKET as u64)
                .flat_map(|i| {
                    let v = (first + i) as f32;
                    [v, -v]
                })
                .collect();
            let item = Item::Chunk {
                pcm: PcmChunk::new(FORMAT, samples),
                play_at_us: Some(self.stamp(first)),
            };
            self.sent += 1;
            Ok(Some(item))
        }
    }

    /// Every chunk out, with the input stamp it came for.
    struct Run {
        /// (stamp in, stamp out, pcm out)
        chunks: Vec<(i64, i64, PcmChunk)>,
        stats: RateStats,
    }

    async fn matched(sender: Sender) -> Run {
        let mut rm = RateMatch::new(sender);
        let mut chunks = Vec::new();
        loop {
            let stamp_in = rm.source.stamp(rm.source.sent * PACKET as u64);
            match rm.next().await.unwrap() {
                Some(Item::Chunk {
                    pcm,
                    play_at_us: Some(at),
                }) => chunks.push((stamp_in, at, pcm)),
                Some(Item::End) => break,
                other => panic!("{other:?}"),
            }
        }
        Run {
            chunks,
            stats: rm.stats(),
        }
    }

    /// Each chunk out is contiguous with the last, stays within the
    /// deadband of the stamp it came with, and has at most one frame
    /// dropped or repeated, at its end.
    fn check(run: &Run, ppm: f64) {
        let mut timeline = Timeline::new(run.chunks[0].1, RATE);
        for (i, (stamp, at, pcm)) in run.chunks.iter().enumerate() {
            assert_eq!(*at, timeline.advance(pcm.frames() as u64).start_us);
            let error = stamp - at;
            // A packet's worth of drift past the edge before it's seen.
            assert!(
                error.abs() <= DEADBAND_US + 1,
                "{ppm} ppm, chunk {i}: {error} µs"
            );
            let n = pcm.frames();
            assert!((PACKET - 1..=PACKET + 1).contains(&n), "{n} frames");
            let first = (i * PACKET) as f32;
            let want = |f: usize| first + f.min(PACKET - 1) as f32;
            for f in 0..n {
                assert_eq!(pcm.samples[2 * f], want(f), "chunk {i}, frame {f}");
            }
        }
    }

    #[tokio::test]
    async fn drift_either_way_stays_in_the_deadband() {
        // Uncorrected, 50 ppm over 10 minutes is 30 ms: 1323 frames.
        for ppm in [50.0, -50.0] {
            let run = matched(Sender::new(ppm, 600)).await;
            check(&run, ppm);
            let s = run.stats;
            let (fixed, other) = if ppm > 0.0 {
                (s.dropped, s.repeated)
            } else {
                (s.repeated, s.dropped)
            };
            // All of it, less what is left inside the deadband.
            assert!(
                (1323 - 441..=1323).contains(&fixed) && other == 0,
                "{ppm} ppm: {s:?}"
            );
            assert_eq!(s.rebased, 0);
        }
    }

    #[tokio::test]
    async fn a_sender_on_time_passes_through_bit_for_bit() {
        let mut sender = Sender::new(0.0, 60);
        let mut rm = RateMatch::new(Sender::new(0.0, 60));
        loop {
            let want = sender.next().await.unwrap();
            let got = rm.next().await.unwrap();
            assert_eq!(got, want);
            if want == Some(Item::End) {
                break;
            }
        }
        assert_eq!(rm.stats().dropped + rm.stats().repeated, 0);
    }

    /// A clock map that wanders by 8 ms, as a noisy Wi-Fi round trip
    /// leaves it, isn't drift: nothing is touched.
    #[tokio::test]
    async fn clock_map_noise_inside_the_deadband_is_left_alone() {
        let mut sender = Sender::new(0.0, 120);
        sender.wander = |chunk| [0, 8_000, -6_000, 3_000][(chunk / 125 % 4) as usize];
        let run = matched(sender).await;
        assert_eq!(run.stats.dropped + run.stats.repeated, 0, "{:?}", run.stats);
        assert!(run.chunks.iter().all(|(_, _, pcm)| pcm.frames() == PACKET));
    }

    /// A jump past [`REBASE_US`] moves the timeline at once; a smaller step
    /// is caught up a frame at a time.
    #[tokio::test]
    async fn a_jump_moves_the_timeline_and_a_step_is_caught_up() {
        let mut sender = Sender::new(0.0, 60);
        sender.wander = |chunk| if chunk >= 1000 { 500_000 } else { 0 };
        let run = matched(sender).await;
        assert_eq!(run.stats.rebased, 1);
        assert_eq!(run.stats.dropped + run.stats.repeated, 0);
        let (stamp, at, _) = &run.chunks[1000];
        assert_eq!(stamp, at);

        let mut sender = Sender::new(0.0, 60);
        sender.wander = |chunk| if chunk >= 1000 { 30_000 } else { 0 };
        let run = matched(sender).await;
        assert_eq!(run.stats.rebased, 0);
        // 30 ms is 1323 frames, one a packet.
        assert!(
            (1322..=1324).contains(&run.stats.repeated),
            "{:?}",
            run.stats
        );
        let (stamp, at, _) = run.chunks.last().unwrap();
        assert!((stamp - at).abs() < 50, "{stamp} {at}");
    }

    /// A flush or an unstamped chunk passes through, and the next stamp
    /// starts the timeline afresh.
    #[tokio::test]
    async fn a_flush_starts_afresh() {
        struct Script(Vec<Item>);
        #[async_trait]
        impl TimedSource for Script {
            fn format(&self) -> AudioFormat {
                FORMAT
            }
            async fn next(&mut self) -> Result<Option<Item>, AudioError> {
                Ok((!self.0.is_empty()).then(|| self.0.remove(0)))
            }
        }
        let chunk = |at: Option<i64>| Item::Chunk {
            pcm: PcmChunk::new(FORMAT, vec![0.0; PACKET * 2]),
            play_at_us: at,
        };
        let mut rm = RateMatch::new(Script(vec![
            chunk(Some(0)),
            Item::Flush,
            chunk(Some(50_000)),
            chunk(None),
            chunk(Some(80_000)),
        ]));
        let mut out = Vec::new();
        while let Some(item) = rm.next().await.unwrap() {
            out.push(match item {
                Item::Chunk { play_at_us, .. } => format!("{play_at_us:?}"),
                other => format!("{other:?}"),
            });
        }
        assert_eq!(
            out,
            ["Some(0)", "Flush", "Some(50000)", "None", "Some(80000)"]
        );
        assert_eq!(rm.stats().rebased, 0);
    }
}
