//! When the sender means each frame to play, on the server clock.
//!
//! Two things say it, and the receiver keeps both:
//!
//! - **The clock map** ([`ClockMap`]): the sender's NTP clock against
//!   [`ServerClock`], from our timing requests to its timing port. Each
//!   request carries our send time as an NTP-format reading of the server
//!   clock; the reply echoes it beside the sender's receive and transmit
//!   times, which is one NTP sample. The sample with the shortest round
//!   trip of the last few is kept, as its offset is the least skewed by
//!   one-way delay.
//! - **The anchor** ([`Anchor`]): the latest sync packet's "frame `rtp`
//!   plays at NTP `ntp`", which the sender sends each second.
//!
//! Together they give [`Timeline::play_at_us`]: frame `r` plays at the
//! anchor's time, mapped to the server clock, plus `r - rtp` frames. That
//! is the sender's own timeline, latency included (an iPhone's is 2 s), so
//! audio stamped with it can be played in step with the phone's picture.

use std::collections::VecDeque;

use tas_audio::ServerClock;

use crate::raop::announce::SAMPLE_RATE;

/// Timing samples kept; the shortest round trip among them wins.
const SAMPLES: usize = 8;

/// An NTP timestamp (32 bits of seconds, 32 of fraction) as microseconds.
pub fn ntp_us(ntp: u64) -> i64 {
    let secs = (ntp >> 32) as i64;
    let micros = (((ntp & 0xffff_ffff) * 1_000_000 + (1 << 31)) >> 32) as i64;
    secs * 1_000_000 + micros
}

/// Microseconds as an NTP timestamp, for a server-clock reading the
/// sender will echo back.
pub fn us_ntp(us: i64) -> u64 {
    let secs = us.div_euclid(1_000_000) as u64;
    let micros = us.rem_euclid(1_000_000) as u64;
    (secs << 32) | ((micros << 32) / 1_000_000)
}

/// What the sender's NTP clock reads against the server clock.
#[derive(Debug, Default)]
pub struct ClockMap {
    /// `(round trip, offset)` of the latest samples, in µs; the offset is
    /// sender NTP time less server-clock time.
    samples: VecDeque<(i64, i64)>,
}

impl ClockMap {
    /// A timing reply read at server-clock `now_us`. Returns whether it
    /// was one.
    pub fn reply(&mut self, packet: &[u8], now_us: i64) -> bool {
        if packet.len() < 32 || crate::raop::rtp::payload_type(packet) != Some(0x53) {
            return false;
        }
        let read = |at: usize| u64::from_be_bytes(packet[at..at + 8].try_into().expect("8 bytes"));
        // Our send time, their receive and transmit, our receive.
        let t1 = ntp_us(read(8));
        let (t2, t3) = (ntp_us(read(16)), ntp_us(read(24)));
        let t4 = now_us;
        let round_trip = (t4 - t1) - (t3 - t2);
        if round_trip < 0 {
            return false;
        }
        let offset = ((t2 - t1) + (t3 - t4)) / 2;
        if self.samples.len() == SAMPLES {
            self.samples.pop_front();
        }
        self.samples.push_back((round_trip, offset));
        true
    }

    /// Sender NTP time less server-clock time, in µs, once a reply came.
    pub fn offset_us(&self) -> Option<i64> {
        self.samples.iter().min_by_key(|(rtt, _)| *rtt).map(|s| s.1)
    }

    /// The server-clock time of the sender's NTP time `ntp`.
    pub fn server_us(&self, ntp: u64) -> Option<i64> {
        Some(ntp_us(ntp) - self.offset_us()?)
    }
}

/// A sync packet: frame `rtp` plays at the sender's NTP time `ntp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Anchor {
    pub rtp: u32,
    pub ntp: u64,
}

impl Anchor {
    /// From a sync packet on the control port.
    pub fn parse(packet: &[u8]) -> Option<Self> {
        if packet.len() < 20
            || crate::raop::rtp::payload_type(packet)? != crate::raop::rtp::PAYLOAD_SYNC
        {
            return None;
        }
        Some(Self {
            rtp: u32::from_be_bytes(packet[4..8].try_into().ok()?),
            ntp: u64::from_be_bytes(packet[8..16].try_into().ok()?),
        })
    }
}

/// The sender's timeline: its clock map and latest anchor.
#[derive(Debug)]
pub struct Timeline {
    pub clock: ServerClock,
    pub map: ClockMap,
    pub anchor: Option<Anchor>,
}

impl Timeline {
    pub fn new(clock: ServerClock) -> Self {
        Self {
            clock,
            map: ClockMap::default(),
            anchor: None,
        }
    }

    /// A timing request to send now: our clock, as NTP, to be echoed.
    pub fn request(&self) -> [u8; 32] {
        crate::raop::rtp::timing_request(us_ntp(self.clock.now_us()))
    }

    /// When the sender means frame `rtp` to play, on the server clock;
    /// `None` until both a sync packet and a timing reply have come.
    pub fn play_at_us(&self, rtp: u32) -> Option<i64> {
        let anchor = self.anchor?;
        let at = self.map.server_us(anchor.ntp)?;
        let frames = i64::from(rtp.wrapping_sub(anchor.rtp) as i32);
        Some(at + frames * 1_000_000 / i64::from(SAMPLE_RATE))
    }
}

#[cfg(test)]
mod tests {
    use tas_airplay_out::rtp as sender;

    use super::*;

    #[test]
    fn ntp_round_trips() {
        for us in [0, 1, 999_999, 1_000_000, 3_912_345_678_901_234] {
            assert_eq!(ntp_us(us_ntp(us)), us);
        }
    }

    /// A sender whose clock reads `offset` µs ahead of ours, `delay` µs
    /// away each way.
    fn sample(map: &mut ClockMap, sent_us: i64, offset: i64, delay: i64) {
        let request = crate::raop::rtp::timing_request(us_ntp(sent_us));
        let theirs = sent_us + delay + offset;
        let reply = sender::timing_reply(&request, us_ntp(theirs)).unwrap();
        assert!(map.reply(&reply, sent_us + 2 * delay));
    }

    #[test]
    fn the_shortest_round_trip_sets_the_offset() {
        let mut map = ClockMap::default();
        assert_eq!(map.offset_us(), None);
        let offset = 1_000_000_000;
        sample(&mut map, 0, offset, 5_000);
        assert_eq!(map.offset_us(), Some(offset));
        // A slow reply skewed by queueing on the way back only.
        let request = crate::raop::rtp::timing_request(us_ntp(10_000));
        let reply = sender::timing_reply(&request, us_ntp(10_000 + 1_000 + offset)).unwrap();
        assert!(map.reply(&reply, 10_000 + 1_000 + 40_000));
        assert_eq!(map.offset_us(), Some(offset), "the quick one still wins");
        assert!(!map.reply(&request, 0), "a request isn't a reply");
    }

    #[test]
    fn a_sync_packet_puts_frames_on_the_server_clock() {
        let mut t = Timeline::new(ServerClock::new());
        let offset = 7_000_000;
        sample(&mut t.map, 0, offset, 100);
        assert_eq!(t.play_at_us(0), None, "no anchor yet");
        // Frame 1000 plays at our 2 s, as the sender writes it.
        let sync = sender::sync_packet(true, 1000, us_ntp(2_000_000 + offset), 88_200);
        t.anchor = Anchor::parse(&sync);
        assert_eq!(t.play_at_us(1000), Some(2_000_000));
        assert_eq!(t.play_at_us(1000 + 44_100), Some(3_000_000));
        assert_eq!(t.play_at_us(1000 - 441), Some(1_990_000));
    }
}
