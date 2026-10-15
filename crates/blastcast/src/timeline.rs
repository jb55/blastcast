//! A stream's timeline: the server-clock time of each chunk, derived from
//! how many frames came before it.
//!
//! A chunk of 1024 frames at 44.1 kHz lasts 23 219.95… µs. Rounding each
//! chunk's duration to whole microseconds and adding them up drifts by up to
//! a microsecond per chunk, which is seconds over a long session. Instead
//! the remainder of each division is carried into the next (the residue
//! accumulator, ported from aiosendspin's `_advance_channel_timing` in
//! `push_stream.py`), so after any run of chunks totalling N frames the
//! cursor is exactly `start + floor(N * 1e6 / rate)`.

/// When one chunk plays on the server clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start_us: i64,
    pub duration_us: i64,
}

impl Span {
    pub fn end_us(&self) -> i64 {
        self.start_us + self.duration_us
    }
}

#[derive(Debug, Clone)]
pub struct Timeline {
    cursor_us: i64,
    sample_rate: u32,
    /// `frames * 1e6 mod sample_rate` carried from earlier chunks: the
    /// fraction of a microsecond, in units of 1/sample_rate µs, not yet
    /// added to the cursor.
    residue: u64,
}

impl Timeline {
    pub fn new(start_us: i64, sample_rate: u32) -> Self {
        assert!(sample_rate > 0, "sample rate must be positive");
        Self {
            cursor_us: start_us,
            sample_rate,
            residue: 0,
        }
    }

    /// Where the next chunk starts.
    pub fn cursor_us(&self) -> i64 {
        self.cursor_us
    }

    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    /// Place the next `frames` frames on the timeline and move past them.
    pub fn advance(&mut self, frames: u64) -> Span {
        let rate = u64::from(self.sample_rate);
        let numerator = self.residue + frames * 1_000_000;
        let duration_us = (numerator / rate) as i64;
        self.residue = numerator % rate;
        let span = Span {
            start_us: self.cursor_us,
            duration_us,
        };
        self.cursor_us += duration_us;
        span
    }

    /// Change the rate of the frames that follow. The residue is in units of
    /// the old rate, so it is dropped: at most a microsecond is lost.
    pub fn set_sample_rate(&mut self, sample_rate: u32) {
        assert!(sample_rate > 0, "sample rate must be positive");
        if sample_rate != self.sample_rate {
            self.sample_rate = sample_rate;
            self.residue = 0;
        }
    }

    /// Restart the timeline at `start_us`, e.g. after a gap or a resync.
    pub fn rebase(&mut self, start_us: i64) {
        self.cursor_us = start_us;
        self.residue = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn does_not_drift_over_many_uneven_chunks() {
        let start = 5_000_000;
        let mut timeline = Timeline::new(start, 44_100);
        let mut total: u64 = 0;
        let mut last_end = start;
        // About 40 minutes of audio in chunks of 352..=1375 frames.
        for i in 0..100_000u64 {
            let frames = 352 + (i * 7919) % 1024;
            let span = timeline.advance(frames);
            assert_eq!(span.start_us, last_end, "chunks are contiguous");
            last_end = span.end_us();
            total += frames;
            let exact = start + (total * 1_000_000 / 44_100) as i64;
            assert_eq!(timeline.cursor_us(), exact, "after chunk {i}");
        }
    }

    #[test]
    fn whole_seconds_at_any_rate() {
        for rate in [44_100, 48_000, 96_000, 22_050] {
            let mut timeline = Timeline::new(0, rate);
            for _ in 0..rate {
                timeline.advance(1);
            }
            assert_eq!(timeline.cursor_us(), 1_000_000, "rate {rate}");
        }
    }

    #[test]
    fn rate_change_and_rebase_reset_the_residue() {
        let mut timeline = Timeline::new(0, 44_100);
        assert_eq!(timeline.advance(1).duration_us, 22);
        timeline.set_sample_rate(48_000);
        assert_eq!(timeline.advance(48).duration_us, 1000);
        assert_eq!(timeline.cursor_us(), 1022);
        timeline.rebase(10);
        assert_eq!(timeline.advance(48_000).start_us, 10);
        assert_eq!(timeline.cursor_us(), 1_000_010);
    }
}
