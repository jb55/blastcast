//! One grandmaster's clock against the server clock, from its Syncs.
//!
//! Each Sync, with its Follow_Up, is one sample: the time it left, on the
//! grandmaster's clock, against the time we read it, on the server clock.
//! Their difference is the clock offset less the packet's one-way delay,
//! and delay is never negative, so the least-delayed samples read highest.
//! A follower that sends nothing (no Delay_Req, as nqptp) can't measure
//! the delay itself; it keeps the upper envelope of the samples and counts
//! the least delay on the network as part of the offset. Every receiver on
//! a LAN sees about the same, and the sender's own latency covers it.
//!
//! The envelope is kept as the highest sample of each second, for the
//! last minute. Once those span a few seconds, a least-squares line
//! through them gives the drift, and the line is lifted to the highest of
//! them, so the map sits on the envelope. Until then the map is the
//! highest recent sample, with no drift.
//!
//! A sample far off the map is a delayed packet, or the grandmaster
//! stepping its clock. A few in a row are a step: the envelope starts
//! again.

use std::collections::VecDeque;

use blastcast::ClockMap;

/// The envelope's resolution: the highest sample of each second is kept.
const BUCKET_US: i64 = 1_000_000;
/// The seconds of envelope the drift is fitted over.
const BUCKETS: usize = 60;
/// How far apart the envelope's ends must be before a drift is fitted.
/// Apple's Sync every 125 ms gives 32 samples in that time.
const MIN_DRIFT_SPAN_US: i64 = 4_000_000;
/// A sample this far off the map is an outlier, not added.
pub const STEP_US: i64 = 50_000;
/// This many outliers in a row are a step, and the envelope restarts.
const STEP_AFTER: u32 = 4;

/// What one sample did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sample {
    /// Grandmaster time less server time, in µs: the offset less this
    /// packet's delay.
    pub offset_us: i64,
    /// Whether it went into the map; an outlier doesn't.
    pub kept: bool,
    /// Whether it was the one that made the envelope start again.
    pub stepped: bool,
}

#[derive(Debug, Default)]
pub struct Estimator {
    /// `(second, at_us, offset_us)`: each second's highest sample, with
    /// the server time it was read at. Oldest first.
    envelope: VecDeque<(i64, i64, i64)>,
    map: Option<ClockMap>,
    outliers: u32,
}

impl Estimator {
    /// The map as of the latest sample; `None` until the first.
    pub fn map(&self) -> Option<ClockMap> {
        self.map
    }

    /// A Sync that left at grandmaster time `remote_us` and was read at
    /// server time `server_us`.
    pub fn sample(&mut self, remote_us: i64, server_us: i64) -> Sample {
        let offset_us = remote_us - server_us;
        let mut stepped = false;
        if let Some(map) = self.map
            && (remote_us - map.to_remote_us(server_us)).abs() > STEP_US
        {
            self.outliers += 1;
            if self.outliers < STEP_AFTER {
                return Sample {
                    offset_us,
                    kept: false,
                    stepped: false,
                };
            }
            self.envelope.clear();
            stepped = true;
        }
        self.outliers = 0;
        let second = server_us.div_euclid(BUCKET_US);
        match self.envelope.back_mut() {
            Some(b) if b.0 == second => {
                if offset_us > b.2 {
                    (b.1, b.2) = (server_us, offset_us);
                }
            }
            _ => {
                if self.envelope.len() == BUCKETS {
                    self.envelope.pop_front();
                }
                self.envelope.push_back((second, server_us, offset_us));
            }
        }
        self.map = Some(self.fit());
        Sample {
            offset_us,
            kept: true,
            stepped,
        }
    }

    fn fit(&self) -> ClockMap {
        let &(_, at_us, newest) = self.envelope.back().expect("a sample was just added");
        let first_us = self.envelope.front().expect("not empty").1;
        if self.envelope.len() < 3 || at_us - first_us < MIN_DRIFT_SPAN_US {
            // No drift yet: the highest of the last two seconds, which a
            // drift of 100 ppm moves by 100 µs at most.
            let recent = self.envelope.iter().rev().take(2);
            let offsets = recent.map(|b| b.2);
            let (high, low) = offsets.fold((i64::MIN, i64::MAX), |(h, l), o| (h.max(o), l.min(o)));
            return ClockMap {
                offset_us: high,
                drift_ppm: 0.0,
                at_us,
                error_us: (high - low).max(1),
            };
        }
        // Relative to the newest, so f64 keeps every µs.
        let points: Vec<(f64, f64)> = self
            .envelope
            .iter()
            .map(|&(_, at, offset)| ((at - at_us) as f64, (offset - newest) as f64))
            .collect();
        let n = points.len() as f64;
        let (mx, my) = points
            .iter()
            .fold((0.0, 0.0), |(x, y), p| (x + p.0 / n, y + p.1 / n));
        let (sxy, sxx) = points.iter().fold((0.0, 0.0), |(xy, xx), p| {
            (xy + (p.0 - mx) * (p.1 - my), xx + (p.0 - mx) * (p.0 - mx))
        });
        let slope = sxy / sxx;
        let line = |x: f64| my + slope * (x - mx);
        let (high, low) = points.iter().fold((f64::MIN, f64::MAX), |(h, l), p| {
            let r = p.1 - line(p.0);
            (h.max(r), l.min(r))
        });
        ClockMap {
            offset_us: newest + (line(0.0) + high).round() as i64,
            drift_ppm: slope * 1e6,
            at_us,
            error_us: ((high - low).round() as i64).max(1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A grandmaster `offset` µs ahead at server time 0, gaining `ppm`.
    fn remote(offset: i64, ppm: f64, server_us: i64) -> i64 {
        server_us + offset + (server_us as f64 * ppm * 1e-6).round() as i64
    }

    /// A delay of 0 to 400 µs, from a cheap generator; every fifth packet
    /// is quick, at 0 to 2 µs, as the least-loaded moment on a LAN is.
    fn delay(i: u64) -> i64 {
        let x = i.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 40;
        if i.is_multiple_of(5) {
            (x % 3) as i64
        } else {
            (x % 400) as i64
        }
    }

    /// Syncs every 125 ms for `secs` from server time `start`; returns the
    /// estimator.
    fn follow(offset: i64, ppm: f64, start: i64, secs: i64) -> Estimator {
        let mut e = Estimator::default();
        let mut sent = start;
        let mut i = 0;
        while sent < start + secs * 1_000_000 {
            let s = e.sample(remote(offset, ppm, sent), sent + delay(i));
            assert!(s.kept, "sample {i}");
            sent += 125_000;
            i += 1;
        }
        e
    }

    #[test]
    fn a_known_offset_and_drift_map_within_a_few_microseconds() {
        // An Apple PTP clock: seconds since the phone booted.
        let offset = 412_345_678_901;
        for ppm in [-80.0, -3.5, 0.0, 12.25, 80.0] {
            let e = follow(offset, ppm, 10_000_000, 40);
            let map = e.map().unwrap();
            assert!((map.drift_ppm - ppm).abs() < 0.2, "{ppm} ppm: {map:?}");
            assert!(map.error_us < 10, "{ppm} ppm: {map:?}");
            // Over the window and ten seconds past it.
            for server in (10_000_000..60_000_000).step_by(1_000_000) {
                let want = remote(offset, ppm, server);
                let got = map.to_remote_us(server);
                assert!(
                    (got - want).abs() <= 5,
                    "{ppm} ppm at {server}: {}",
                    got - want
                );
            }
        }
    }

    #[test]
    fn before_the_drift_is_known_the_map_is_the_quickest_sample() {
        let mut e = Estimator::default();
        assert_eq!(e.map(), None);
        e.sample(5_000_000 + 1_000 - 300, 1_000);
        e.sample(5_000_000 + 2_000 - 2, 2_000);
        e.sample(5_000_000 + 3_000 - 150, 3_000);
        let map = e.map().unwrap();
        assert_eq!(map.offset_us, 5_000_000 - 2);
        assert_eq!(map.drift_ppm, 0.0);
    }

    #[test]
    fn one_late_packet_is_left_out_and_a_step_starts_again() {
        let mut e = follow(1_000_000, 0.0, 0, 10);
        let before = e.map().unwrap();
        let late = e.sample(10_000_000 + 1_000_000 - 200_000, 10_000_000);
        assert!(!late.kept, "{late:?}");
        assert_eq!(e.map(), Some(before));
        assert!(e.sample(10_050_000 + 1_000_000, 10_050_000).kept);
        // The grandmaster's clock jumps a second forward and stays there.
        let mut last = None;
        for k in 0..4 {
            let t = 10_100_000 + k * 125_000;
            last = Some(e.sample(t + 2_000_000, t));
        }
        let last = last.unwrap();
        assert!(last.kept && last.stepped, "{last:?}");
        assert_eq!(e.map().unwrap().offset_us, 2_000_000);
    }
}
