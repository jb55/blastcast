//! How much audio a player holds that it hasn't played yet, as far as the
//! server can tell from what it sent.
//!
//! Ported from sendspin-go's `buffer_tracker.go`, which follows aiosendspin's
//! `BufferTracker`. Each chunk sent is remembered until its play time has
//! passed; until then its bytes count against the player's
//! `buffer_capacity` and its duration against a 30 s cap.

use std::collections::VecDeque;

/// The most audio a player is sent ahead, whatever its byte capacity:
/// aiosendspin's `max_duration_us`.
pub const MAX_BUFFERED_US: i64 = 30_000_000;

/// The capacity assumed for a player whose hello didn't give one,
/// sendspin-go's default.
pub const DEFAULT_CAPACITY_BYTES: u64 = 1 << 20;

#[derive(Debug, Clone, Copy)]
struct Sent {
    end_us: i64,
    bytes: u64,
    duration_us: i64,
}

#[derive(Debug, Clone)]
pub struct BufferTracker {
    sent: VecDeque<Sent>,
    bytes: u64,
    duration_us: i64,
    capacity_bytes: u64,
    max_duration_us: i64,
}

impl BufferTracker {
    /// A tracker for a player that holds `capacity_bytes`; 0 means it
    /// didn't say, and gets [`DEFAULT_CAPACITY_BYTES`].
    pub fn new(capacity_bytes: u64) -> Self {
        Self {
            sent: VecDeque::new(),
            bytes: 0,
            duration_us: 0,
            capacity_bytes: if capacity_bytes == 0 {
                DEFAULT_CAPACITY_BYTES
            } else {
                capacity_bytes
            },
            max_duration_us: MAX_BUFFERED_US,
        }
    }

    /// Forget chunks that have finished playing by `now_us`.
    pub fn prune(&mut self, now_us: i64) {
        while let Some(front) = self.sent.front().filter(|s| s.end_us <= now_us) {
            self.bytes -= front.bytes;
            self.duration_us -= front.duration_us;
            self.sent.pop_front();
        }
    }

    /// Whether a chunk of `bytes` lasting `duration_us` fits. A chunk
    /// bigger than the whole capacity fits only into an empty buffer, so
    /// an undersized capacity slows the stream rather than stopping it.
    pub fn can_send(&self, bytes: u64, duration_us: i64) -> bool {
        if self.sent.is_empty() {
            return true;
        }
        self.bytes + bytes <= self.capacity_bytes
            && self.duration_us + duration_us <= self.max_duration_us
    }

    /// Remember a chunk that was sent.
    pub fn register(&mut self, end_us: i64, bytes: u64, duration_us: i64) {
        self.sent.push_back(Sent {
            end_us,
            bytes,
            duration_us,
        });
        self.bytes += bytes;
        self.duration_us += duration_us;
    }

    /// When the oldest chunk finishes playing and frees its room.
    pub fn next_release_us(&self) -> Option<i64> {
        self.sent.front().map(|s| s.end_us)
    }

    /// Forget everything, after a `stream/clear` empties the player.
    pub fn clear(&mut self) {
        self.sent.clear();
        self.bytes = 0;
        self.duration_us = 0;
    }

    pub fn buffered_bytes(&self) -> u64 {
        self.bytes
    }

    pub fn buffered_us(&self) -> i64 {
        self.duration_us
    }

    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn holds_bytes_until_their_play_time_passes() {
        let mut t = BufferTracker::new(100);
        t.register(1_000, 40, 20);
        t.register(2_000, 40, 20);
        assert!(t.can_send(20, 20));
        assert!(!t.can_send(21, 20));
        assert_eq!(t.next_release_us(), Some(1_000));
        t.prune(999);
        assert_eq!(t.buffered_bytes(), 80);
        t.prune(1_000);
        assert_eq!((t.buffered_bytes(), t.buffered_us()), (40, 20));
        assert!(t.can_send(60, 20));
        t.prune(5_000);
        assert_eq!(t.buffered_bytes(), 0);
        assert_eq!(t.next_release_us(), None);
    }

    #[test]
    fn caps_duration_at_thirty_seconds() {
        let mut t = BufferTracker::new(u64::MAX);
        for i in 1..=1500 {
            assert!(t.can_send(1, 20_000), "chunk {i}");
            t.register(i * 20_000, 1, 20_000);
        }
        assert_eq!(t.buffered_us(), MAX_BUFFERED_US);
        assert!(!t.can_send(1, 20_000));
    }

    #[test]
    fn unknown_capacity_defaults_to_a_megabyte() {
        assert_eq!(BufferTracker::new(0).capacity_bytes(), 1 << 20);
    }

    #[test]
    fn an_oversized_chunk_waits_for_an_empty_buffer() {
        let mut t = BufferTracker::new(10);
        assert!(t.can_send(50, 20));
        t.register(100, 50, 20);
        assert!(!t.can_send(1, 20));
        t.clear();
        assert!(t.can_send(50, 20));
    }
}
