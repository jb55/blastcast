//! RAOP's UDP packets, and the NTP clock they carry.
//!
//! An AirPlay 1 stream uses three UDP channels besides the RTSP connection:
//!
//! - **Audio** goes to the receiver's `server_port`: RTP, payload type 96,
//!   one ALAC frame of [`FRAMES_PER_PACKET`](crate::alac::FRAMES_PER_PACKET)
//!   frames a packet, the first with the marker bit.
//! - **Control** is ours and the receiver's `control_port`. We send a
//!   **sync** packet each second, which ties the stream to the clock: the
//!   frame with RTP time `playing` plays at NTP time `now`, and `playing +
//!   latency` is the frame being sent. The receiver asks for lost packets
//!   on it (**resend**), and gets them back wrapped.
//! - **Timing** is NTP in miniature: the receiver asks our timing port for
//!   our clock, and we answer with it, so it can map our NTP times to its
//!   own.
//!
//! Our NTP clock is the server clock, offset to the Unix time it started
//! at: its clock map is [`ClockMap::identity`], and only the epoch differs.
//! A Sendspin chunk's timestamp is server-clock time, so the time it plays
//! at is also an NTP time the receiver can follow: that is what puts
//! AirPlay speakers on a Sendspin group's timeline.
//!
//! The layouts are libraop's (`raop_client.c`), which Music Assistant's
//! AirPlay CLI is built on.

use std::time::{SystemTime, UNIX_EPOCH};

use blastcast::ClockMap;
use blastcast::ServerClock;

/// Seconds from the NTP epoch (1900) to the Unix one.
const NTP_UNIX_OFFSET_S: i64 = 2_208_988_800;

pub const PAYLOAD_AUDIO: u8 = 0x60;
pub const PAYLOAD_SYNC: u8 = 0x54;
pub const PAYLOAD_TIMING_REQUEST: u8 = 0x52;
pub const PAYLOAD_TIMING_REPLY: u8 = 0x53;
pub const PAYLOAD_RESEND_REQUEST: u8 = 0x55;
pub const PAYLOAD_RESEND_REPLY: u8 = 0x56;

/// The marker bit, set on the first audio packet after a start or flush.
const MARKER: u8 = 0x80;
/// The extension bit, set on the first sync packet of a stream.
const FIRST_SYNC: u8 = 0x10;

/// NTP time on the server clock: 32 bits of seconds and 32 of fraction.
///
/// It isn't an estimate of a remote clock but ours, written on another
/// epoch: the map is the identity, and every RAOP speaker we send to
/// follows the server clock itself.
#[derive(Debug, Clone, Copy)]
pub struct NtpClock {
    clock: ServerClock,
    /// Always [`ClockMap::identity`]: this clock is the server clock.
    map: ClockMap,
    /// Unix microseconds at server-clock zero.
    unix_at_zero_us: i64,
}

impl NtpClock {
    pub fn new(clock: ServerClock) -> Self {
        let unix_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() as i64);
        Self {
            clock,
            map: ClockMap::identity(),
            unix_at_zero_us: unix_us - clock.now_us(),
        }
    }

    pub fn server_clock(&self) -> ServerClock {
        self.clock
    }

    pub fn now(&self) -> u64 {
        self.at_us(self.clock.now_us())
    }

    /// The NTP time of server-clock microsecond `us`.
    pub fn at_us(&self, us: i64) -> u64 {
        let t = self.map.to_remote_us(us) + self.epoch_us();
        let secs = t.div_euclid(1_000_000) as u64;
        let micros = t.rem_euclid(1_000_000) as u64;
        (secs << 32) | ((micros << 32) / 1_000_000)
    }

    /// The server-clock microsecond of NTP time `ntp`, to the nearest one.
    pub fn us_at(&self, ntp: u64) -> i64 {
        let secs = (ntp >> 32) as i64;
        let micros = (((ntp & 0xffff_ffff) * 1_000_000 + (1 << 31)) >> 32) as i64;
        self.map
            .to_server_us(secs * 1_000_000 + micros - self.epoch_us())
    }

    /// NTP microseconds at server-clock zero.
    fn epoch_us(&self) -> i64 {
        self.unix_at_zero_us + NTP_UNIX_OFFSET_S * 1_000_000
    }
}

/// An audio packet: the RTP header and one ALAC frame.
pub fn audio_packet(marker: bool, seq: u16, timestamp: u32, ssrc: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + payload.len());
    out.push(0x80);
    out.push(PAYLOAD_AUDIO | if marker { MARKER } else { 0 });
    out.extend_from_slice(&seq.to_be_bytes());
    out.extend_from_slice(&timestamp.to_be_bytes());
    out.extend_from_slice(&ssrc.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// A sync packet: frame `playing` plays at `ntp`, and the stream runs
/// `latency` frames ahead of it.
pub fn sync_packet(first: bool, playing: u32, ntp: u64, latency: u32) -> [u8; 20] {
    let mut out = [0u8; 20];
    out[0] = 0x80 | if first { FIRST_SYNC } else { 0 };
    out[1] = MARKER | PAYLOAD_SYNC;
    // libraop: "seems that seq=7 shall be forced".
    out[2..4].copy_from_slice(&7u16.to_be_bytes());
    out[4..8].copy_from_slice(&playing.to_be_bytes());
    out[8..16].copy_from_slice(&ntp.to_be_bytes());
    out[16..20].copy_from_slice(&playing.wrapping_add(latency).to_be_bytes());
    out
}

/// What a sync packet says, read back. For tests and the fake receiver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sync {
    pub first: bool,
    pub playing: u32,
    pub ntp: u64,
    pub sending: u32,
}

pub fn parse_sync(packet: &[u8]) -> Option<Sync> {
    if packet.len() < 20 || packet[1] & 0x7f != PAYLOAD_SYNC {
        return None;
    }
    Some(Sync {
        first: packet[0] & FIRST_SYNC != 0,
        playing: u32::from_be_bytes(packet[4..8].try_into().ok()?),
        ntp: u64::from_be_bytes(packet[8..16].try_into().ok()?),
        sending: u32::from_be_bytes(packet[16..20].try_into().ok()?),
    })
}

/// The answer to a receiver's timing request, read and sent at `now`, or
/// `None` if `request` isn't one.
pub fn timing_reply(request: &[u8], now: u64) -> Option<[u8; 32]> {
    if request.len() < 32 || request[1] & 0x7f != PAYLOAD_TIMING_REQUEST {
        return None;
    }
    let mut out = [0u8; 32];
    out[0] = 0x80;
    out[1] = MARKER | PAYLOAD_TIMING_REPLY;
    out[2..4].copy_from_slice(&request[2..4]);
    // Their transmit time is our reply's origin time.
    out[8..16].copy_from_slice(&request[24..32]);
    out[16..24].copy_from_slice(&now.to_be_bytes());
    out[24..32].copy_from_slice(&now.to_be_bytes());
    Some(out)
}

/// A timing request sent at `now`. For the fake receiver.
pub fn timing_request(now: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[0] = 0x80;
    out[1] = MARKER | PAYLOAD_TIMING_REQUEST;
    out[2..4].copy_from_slice(&7u16.to_be_bytes());
    out[24..32].copy_from_slice(&now.to_be_bytes());
    out
}

/// The first sequence number and count of a resend request.
pub fn parse_resend_request(packet: &[u8]) -> Option<(u16, u16)> {
    if packet.len() < 8 || packet[1] & 0x7f != PAYLOAD_RESEND_REQUEST {
        return None;
    }
    let seq = u16::from_be_bytes([packet[4], packet[5]]);
    let count = u16::from_be_bytes([packet[6], packet[7]]);
    Some((seq, count))
}

/// A resend request for `count` packets from `seq`. For the fake receiver.
pub fn resend_request(seq: u16, count: u16) -> [u8; 8] {
    let mut out = [0x80, MARKER | PAYLOAD_RESEND_REQUEST, 0, 1, 0, 0, 0, 0];
    out[4..6].copy_from_slice(&seq.to_be_bytes());
    out[6..8].copy_from_slice(&count.to_be_bytes());
    out
}

/// An audio packet sent again, as the answer to a resend request.
pub fn resend_reply(original: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + original.len());
    out.extend_from_slice(&[0x80, MARKER | PAYLOAD_RESEND_REPLY, 0x00, 0x01]);
    out.extend_from_slice(original);
    out
}

/// An audio packet's header, read back. For tests and the fake receiver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AudioHeader {
    pub marker: bool,
    pub seq: u16,
    pub timestamp: u32,
    pub ssrc: u32,
}

pub fn parse_audio(packet: &[u8]) -> Option<(AudioHeader, &[u8])> {
    if packet.len() < 12 || packet[1] & 0x7f != PAYLOAD_AUDIO {
        return None;
    }
    let header = AudioHeader {
        marker: packet[1] & MARKER != 0,
        seq: u16::from_be_bytes([packet[2], packet[3]]),
        timestamp: u32::from_be_bytes(packet[4..8].try_into().ok()?),
        ssrc: u32::from_be_bytes(packet[8..12].try_into().ok()?),
    };
    Some((header, &packet[12..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ntp_round_trips_the_server_clock() {
        let ntp = NtpClock::new(ServerClock::new());
        for us in [-5_000_000, 0, 1, 999_999, 1_000_000, 123_456_789_012] {
            assert_eq!(ntp.us_at(ntp.at_us(us)), us, "{us}");
        }
        let a = ntp.at_us(1_000_000);
        let b = ntp.at_us(1_500_000);
        assert_eq!(b - a, 1 << 31, "half a second is half the fraction");
        // It is the Unix time, not a count from zero.
        assert!(ntp.now() >> 32 > NTP_UNIX_OFFSET_S as u64);
    }

    #[test]
    fn sync_packets_read_back() {
        let p = sync_packet(true, 1000, 0x0102_0304_0506_0708, 88_200);
        assert_eq!(&p[..4], [0x90, 0xd4, 0x00, 0x07]);
        let s = parse_sync(&p).unwrap();
        assert_eq!(
            s,
            Sync {
                first: true,
                playing: 1000,
                ntp: 0x0102_0304_0506_0708,
                sending: 89_200
            }
        );
        let wrapped = parse_sync(&sync_packet(false, u32::MAX, 0, 2)).unwrap();
        assert!(!wrapped.first);
        assert_eq!(wrapped.sending, 1);
    }

    #[test]
    fn timing_replies_echo_the_request() {
        let request = timing_request(42);
        let reply = timing_reply(&request, 99).unwrap();
        assert_eq!(&reply[..4], [0x80, 0xd3, 0x00, 0x07]);
        assert_eq!(u64::from_be_bytes(reply[8..16].try_into().unwrap()), 42);
        assert_eq!(u64::from_be_bytes(reply[16..24].try_into().unwrap()), 99);
        assert_eq!(u64::from_be_bytes(reply[24..32].try_into().unwrap()), 99);
        assert_eq!(timing_reply(&reply, 1), None, "a reply isn't a request");
    }

    #[test]
    fn audio_and_resend_packets() {
        let p = audio_packet(true, 65_535, 7, 9, &[1, 2, 3]);
        let (h, payload) = parse_audio(&p).unwrap();
        assert_eq!(
            h,
            AudioHeader {
                marker: true,
                seq: 65_535,
                timestamp: 7,
                ssrc: 9
            }
        );
        assert_eq!(payload, [1, 2, 3]);
        assert!(
            !parse_audio(&audio_packet(false, 0, 0, 0, &[]))
                .unwrap()
                .0
                .marker
        );

        assert_eq!(
            parse_resend_request(&resend_request(300, 4)),
            Some((300, 4))
        );
        let r = resend_reply(&p);
        assert_eq!(&r[..4], [0x80, 0xd6, 0x00, 0x01]);
        assert_eq!(&r[4..], p);
    }
}
