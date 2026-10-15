//! The server clock: one monotonic microsecond count, shared by everything
//! that puts a time on the wire.
//!
//! Sendspin chunk timestamps and `server/time` replies must come from the
//! same clock, or a client's clock sync would map chunks to the wrong local
//! time. It counts from when the server started, never from the epoch, so a
//! wall-clock step (NTP, a suspended laptop) can't move it.
//!
//! It is built on `tokio::time::Instant`, which is `std::time::Instant` at
//! runtime but follows a paused test clock, so pacing can be tested without
//! sleeping.

use std::time::Duration;

use tokio::time::Instant;

/// Microseconds since the server started. Cheap to copy; every copy reads
/// the same clock.
#[derive(Debug, Clone, Copy)]
pub struct ServerClock {
    epoch: Instant,
}

impl ServerClock {
    /// A clock that reads zero now.
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }

    /// The current time in microseconds.
    pub fn now_us(&self) -> i64 {
        self.us_at(Instant::now())
    }

    /// The clock's reading at `instant`. Instants before the clock started
    /// read negative.
    pub fn us_at(&self, instant: Instant) -> i64 {
        if instant >= self.epoch {
            duration_us(instant - self.epoch)
        } else {
            -duration_us(self.epoch - instant)
        }
    }

    /// The instant at which the clock reads `us`, for sleeping until a
    /// timestamp.
    pub fn instant_at(&self, us: i64) -> Instant {
        let offset = Duration::from_micros(us.unsigned_abs());
        if us >= 0 {
            self.epoch + offset
        } else {
            self.epoch - offset
        }
    }
}

impl Default for ServerClock {
    fn default() -> Self {
        Self::new()
    }
}

fn duration_us(d: Duration) -> i64 {
    i64::try_from(d.as_micros()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn counts_microseconds_from_start() {
        let clock = ServerClock::new();
        assert_eq!(clock.now_us(), 0);
        tokio::time::advance(Duration::from_millis(1500)).await;
        assert_eq!(clock.now_us(), 1_500_000);
    }

    #[tokio::test(start_paused = true)]
    async fn instant_and_us_round_trip() {
        tokio::time::advance(Duration::from_secs(10)).await;
        let clock = ServerClock::new();
        for us in [-2_000_000, -1, 0, 1, 123_456_789] {
            assert_eq!(clock.us_at(clock.instant_at(us)), us);
        }
    }

    #[test]
    fn copies_share_the_clock() {
        let a = ServerClock::new();
        let b = a;
        let (x, y) = (a.now_us(), b.now_us());
        assert!(y >= x);
        assert!(y - x < 1_000_000);
    }
}
