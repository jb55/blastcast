//! A remote clock against the server clock, and when a remote frame plays.
//!
//! Each protocol's sender keeps its own clock: RAOP's NTP, AirPlay 2's
//! PTP, Sendspin's time sync. A [`ClockMap`] says what that clock reads
//! against the [`ServerClock`](crate::ServerClock), and an [`Anchor`] says
//! which frame plays at which remote time. Together they put any sender's
//! frames on the server clock.
//!
//! The map is plain data. How it is estimated (an NTP exchange, a PTP
//! follower, a Kalman filter over Sendspin time messages) stays with the
//! protocol, which hands blastcast a fresh map whenever it has one.

/// A remote clock against the server clock:
/// remote = server + offset + drift·(server − at).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClockMap {
    /// Remote time less server time at server time `at_us`, in µs.
    pub offset_us: i64,
    /// How much faster the remote clock runs, in parts per million.
    pub drift_ppm: f64,
    /// The server time the offset was measured at, which drift counts from.
    pub at_us: i64,
    /// How far off the map may be, in µs: the estimator's own bound.
    pub error_us: i64,
}

impl ClockMap {
    /// The server clock itself, for an output whose clock *is* ours.
    pub fn identity() -> Self {
        Self {
            offset_us: 0,
            drift_ppm: 0.0,
            at_us: 0,
            error_us: 0,
        }
    }

    /// The server time of remote time `remote_us`, to the nearest µs.
    pub fn to_server_us(&self, remote_us: i64) -> i64 {
        let since = remote_us - self.offset_us - self.at_us;
        if self.drift_ppm == 0.0 {
            return self.at_us + since;
        }
        self.at_us + (since as f64 / (1.0 + self.drift_ppm * 1e-6)).round() as i64
    }

    /// The remote time of server time `server_us`, to the nearest µs.
    pub fn to_remote_us(&self, server_us: i64) -> i64 {
        let since = server_us - self.at_us;
        let drift = (since as f64 * self.drift_ppm * 1e-6).round() as i64;
        server_us + self.offset_us + drift
    }
}

/// "frame `frame` plays at remote time `remote_us`, at `rate` frames a
/// second": a RAOP sync packet, or AirPlay 2's SETRATEANCHORTIME.
///
/// Frames count up from wherever the protocol likes; RTP's 32-bit
/// timestamps are unwrapped to these by the protocol. A frame before the
/// anchor plays before it, and frame numbers wrap like `u64` arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Anchor {
    pub frame: u64,
    pub remote_us: i64,
    pub rate: u32,
}

impl Anchor {
    /// When `frame` plays on the server clock, through `map`.
    pub fn play_at_us(&self, frame: u64, map: &ClockMap) -> i64 {
        let frames = i128::from(frame.wrapping_sub(self.frame) as i64);
        let since_us = (frames * 1_000_000 / i128::from(self.rate)) as i64;
        map.to_server_us(self.remote_us + since_us)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR_US: i64 = 3_600_000_000;

    #[test]
    fn identity_is_the_server_clock() {
        let map = ClockMap::identity();
        for us in [-HOUR_US, -1, 0, 1, 123_456_789] {
            assert_eq!(map.to_remote_us(us), us);
            assert_eq!(map.to_server_us(us), us);
        }
    }

    #[test]
    fn drift_counts_from_at() {
        let map = ClockMap {
            offset_us: 5_000,
            drift_ppm: 100.0,
            at_us: 10_000_000,
            error_us: 0,
        };
        assert_eq!(map.to_remote_us(10_000_000), 10_005_000);
        // An hour on, a 100 ppm clock has gained 360 ms.
        assert_eq!(
            map.to_remote_us(10_000_000 + HOUR_US),
            10_005_000 + HOUR_US + 360_000
        );
        assert_eq!(
            map.to_remote_us(10_000_000 - HOUR_US),
            10_005_000 - HOUR_US - 360_000
        );
    }

    #[test]
    fn round_trips_within_a_microsecond_over_an_hour() {
        for drift_ppm in [-100.0, -12.5, 0.0, 0.3, 100.0] {
            let map = ClockMap {
                // An NTP-epoch sender: about 124 years ahead of us.
                offset_us: 3_912_345_678_901_234,
                drift_ppm,
                at_us: 42_000_000,
                error_us: 1_000,
            };
            let mut server = map.at_us - HOUR_US;
            while server <= map.at_us + HOUR_US {
                let back = map.to_server_us(map.to_remote_us(server));
                assert!(
                    (back - server).abs() <= 1,
                    "{drift_ppm} ppm at {server}: {back}"
                );
                let remote = server + map.offset_us;
                let there = map.to_remote_us(map.to_server_us(remote));
                assert!(
                    (there - remote).abs() <= 1,
                    "{drift_ppm} ppm at remote {remote}"
                );
                server += 7_919_993;
            }
        }
    }

    /// raop/clock.rs's `a_sync_packet_puts_frames_on_the_server_clock`,
    /// through an anchor: a sender 7 s ahead says frame 1000 plays at our
    /// 2 s.
    #[test]
    fn an_anchor_puts_frames_on_the_server_clock() {
        let offset = 7_000_000;
        let map = ClockMap {
            offset_us: offset,
            ..ClockMap::identity()
        };
        let anchor = Anchor {
            frame: 1000,
            remote_us: 2_000_000 + offset,
            rate: 44_100,
        };
        assert_eq!(anchor.play_at_us(1000, &map), 2_000_000);
        assert_eq!(anchor.play_at_us(1000 + 44_100, &map), 3_000_000);
        assert_eq!(anchor.play_at_us(1000 - 441, &map), 1_990_000);
    }

    #[test]
    fn frames_before_a_low_anchor_wrap() {
        let anchor = Anchor {
            frame: 5,
            remote_us: 1_000_000,
            rate: 48_000,
        };
        let map = ClockMap::identity();
        assert_eq!(anchor.play_at_us(5u64.wrapping_sub(48_000), &map), 0);
    }
}
