//! The PTP (IEEE 1588-2008) messages a follower reads: Sync, Follow_Up and
//! Announce, from the 34-byte common header and the body after it.
//!
//! Apple's senders send two-step Syncs: the Sync marks the moment, on the
//! event port (319), and the Follow_Up after it, on the general port
//! (320), carries the time it left, its `preciseOriginTimestamp`. Both
//! carry the same `sequenceId`. Announce, on the general port, says which
//! clock is grandmaster and how good it claims to be.
//!
//! Field offsets are the standard's (clause 13), as nqptp and shairplay
//! 0.10's net/ptp.rs read them. Apple's Follow_Up carries TLVs after the
//! timestamp; they are ignored.

/// The common header's length; every body starts here.
pub const HEADER_LEN: usize = 34;

/// A Sync's or Follow_Up's length: the header and one timestamp.
const TIMESTAMPED_LEN: usize = HEADER_LEN + 10;

/// An Announce's length.
const ANNOUNCE_LEN: usize = 64;

/// `flagField` bit 1 of its first octet: a Follow_Up carries the time.
const TWO_STEP: u8 = 0x02;

/// A PTP timestamp: 48 bits of seconds and 32 of nanoseconds, on the
/// sending clock's own timescale. Apple's is not TAI: nothing but the
/// offset to it means anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timestamp {
    pub secs: u64,
    pub nanos: u32,
}

impl Timestamp {
    fn read(b: &[u8]) -> Self {
        let mut secs = [0u8; 8];
        secs[2..].copy_from_slice(&b[..6]);
        Self {
            secs: u64::from_be_bytes(secs),
            nanos: u32::from_be_bytes(b[6..10].try_into().expect("4 bytes")),
        }
    }

    /// In nanoseconds.
    pub fn ns(self) -> i128 {
        i128::from(self.secs) * 1_000_000_000 + i128::from(self.nanos)
    }

    fn write(self, b: &mut [u8]) {
        b[..6].copy_from_slice(&self.secs.to_be_bytes()[2..]);
        b[6..10].copy_from_slice(&self.nanos.to_be_bytes());
    }
}

/// One port of one clock: `sourcePortIdentity`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PortIdentity {
    /// The clock's EUI-64. For a grandmaster, AirPlay 2 calls it the
    /// timeline id: SETRATEANCHORTIME's `networkTimeTimelineID`.
    pub clock: u64,
    pub port: u16,
}

/// What an Announce says of its grandmaster, in the order the best master
/// clock algorithm compares it: lower wins at each field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Announce {
    pub priority1: u8,
    pub class: u8,
    pub accuracy: u8,
    pub variance: u16,
    pub priority2: u8,
    pub grandmaster: u64,
    pub steps_removed: u16,
}

impl Announce {
    /// Its place in the best master clock algorithm's order: the lowest
    /// is the best.
    pub fn rank(&self) -> (u8, u8, u8, u16, u8, u64) {
        (
            self.priority1,
            self.class,
            self.accuracy,
            self.variance,
            self.priority2,
            self.grandmaster,
        )
    }
}

/// A message's body, as far as a follower cares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Body {
    /// `originTimestamp`: the time a one-step Sync left; only roughly so
    /// for a two-step one, whose Follow_Up has it.
    Sync(Timestamp),
    /// `preciseOriginTimestamp`: when the Sync of the same sequence left.
    FollowUp(Timestamp),
    Announce(Announce),
    /// Delay_Req, Pdelay, Signaling, Management: nothing a passive
    /// follower reads.
    Other(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Message {
    pub source: PortIdentity,
    pub sequence: u16,
    pub domain: u8,
    /// A Sync whose time comes in a Follow_Up.
    pub two_step: bool,
    /// `correctionField`, in ns: what transparent clocks on the way added.
    /// The time a Sync left is its Follow_Up's timestamp plus both
    /// corrections.
    pub correction_ns: i64,
    pub body: Body,
}

impl Message {
    /// A PTP version 2 message, or `None` for anything else or too short.
    pub fn parse(b: &[u8]) -> Option<Self> {
        if b.len() < HEADER_LEN || b[1] & 0x0f != 2 {
            return None;
        }
        let kind = b[0] & 0x0f;
        let body = match kind {
            0x0 if b.len() >= TIMESTAMPED_LEN => Body::Sync(Timestamp::read(&b[34..])),
            0x8 if b.len() >= TIMESTAMPED_LEN => Body::FollowUp(Timestamp::read(&b[34..])),
            0xb if b.len() >= ANNOUNCE_LEN => Body::Announce(Announce {
                priority1: b[47],
                class: b[48],
                accuracy: b[49],
                variance: u16::from_be_bytes([b[50], b[51]]),
                priority2: b[52],
                grandmaster: u64::from_be_bytes(b[53..61].try_into().ok()?),
                steps_removed: u16::from_be_bytes([b[61], b[62]]),
            }),
            0x0 | 0x8 | 0xb => return None,
            other => Body::Other(other),
        };
        Some(Self {
            source: PortIdentity {
                clock: u64::from_be_bytes(b[20..28].try_into().ok()?),
                port: u16::from_be_bytes([b[28], b[29]]),
            },
            sequence: u16::from_be_bytes([b[30], b[31]]),
            domain: b[4],
            two_step: b[6] & TWO_STEP != 0,
            // Scaled nanoseconds: ns times 2^16.
            correction_ns: i64::from_be_bytes(b[8..16].try_into().ok()?) >> 16,
            body,
        })
    }

    /// The message on the wire, as a grandmaster sends it: for tests and
    /// [`crate::ap2::testing`]'s grandmaster.
    pub fn to_bytes(&self) -> Vec<u8> {
        let (kind, len, control) = match self.body {
            Body::Sync(_) => (0x0, TIMESTAMPED_LEN, 0),
            Body::FollowUp(_) => (0x8, TIMESTAMPED_LEN, 2),
            Body::Announce(_) => (0xb, ANNOUNCE_LEN, 5),
            Body::Other(kind) => (kind, HEADER_LEN, 5),
        };
        let mut b = vec![0u8; len];
        b[0] = kind;
        b[1] = 2;
        b[2..4].copy_from_slice(&(len as u16).to_be_bytes());
        b[4] = self.domain;
        if self.two_step {
            b[6] |= TWO_STEP;
        }
        b[8..16].copy_from_slice(&(self.correction_ns << 16).to_be_bytes());
        b[20..28].copy_from_slice(&self.source.clock.to_be_bytes());
        b[28..30].copy_from_slice(&self.source.port.to_be_bytes());
        b[30..32].copy_from_slice(&self.sequence.to_be_bytes());
        b[32] = control;
        // logMessageInterval: Apple's Sync is every 125 ms, 2^-3 s.
        b[33] = (-3i8) as u8;
        match self.body {
            Body::Sync(t) | Body::FollowUp(t) => t.write(&mut b[34..]),
            Body::Announce(a) => {
                b[47] = a.priority1;
                b[48] = a.class;
                b[49] = a.accuracy;
                b[50..52].copy_from_slice(&a.variance.to_be_bytes());
                b[52] = a.priority2;
                b[53..61].copy_from_slice(&a.grandmaster.to_be_bytes());
                b[61..63].copy_from_slice(&a.steps_removed.to_be_bytes());
                // timeSource: internal oscillator.
                b[63] = 0xa0;
            }
            Body::Other(_) => {}
        }
        b
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLOCK: u64 = 0x0011_22ff_fe33_4455;

    fn message(body: Body) -> Message {
        Message {
            source: PortIdentity {
                clock: CLOCK,
                port: 0x8005,
            },
            sequence: 0xbeef,
            domain: 0,
            two_step: matches!(body, Body::Sync(_)),
            correction_ns: -1_234,
            body,
        }
    }

    #[test]
    fn messages_round_trip() {
        let t = Timestamp {
            secs: 0x0001_2345_6789,
            nanos: 999_999_999,
        };
        let announce = Announce {
            priority1: 248,
            class: 248,
            accuracy: 0xfe,
            variance: 0xffff,
            priority2: 248,
            grandmaster: CLOCK,
            steps_removed: 0,
        };
        for body in [
            Body::Sync(t),
            Body::FollowUp(t),
            Body::Announce(announce),
            Body::Other(0xc),
        ] {
            let m = message(body);
            assert_eq!(Message::parse(&m.to_bytes()), Some(m), "{body:?}");
        }
    }

    /// A Follow_Up as the standard lays it out, by hand: the offsets are
    /// what matter, not our own writer.
    #[test]
    fn a_follow_up_from_the_standards_offsets() {
        let mut b = [0u8; 44];
        b[0] = 0x18; // transportSpecific 1, Follow_Up
        b[1] = 0x02;
        b[8..16].copy_from_slice(&(500i64 << 16).to_be_bytes());
        b[20..28].copy_from_slice(&CLOCK.to_be_bytes());
        b[30..32].copy_from_slice(&7u16.to_be_bytes());
        b[38..40].copy_from_slice(&1000u16.to_be_bytes()); // seconds, low bytes
        b[40..44].copy_from_slice(&500_000_000u32.to_be_bytes());
        let m = Message::parse(&b).unwrap();
        assert_eq!(m.source.clock, CLOCK);
        assert_eq!(m.sequence, 7);
        assert_eq!(m.correction_ns, 500);
        let Body::FollowUp(t) = m.body else {
            panic!("{m:?}")
        };
        assert_eq!(t.ns(), 1000 * 1_000_000_000 + 500_000_000);
    }

    #[test]
    fn short_or_foreign_packets_are_not_messages() {
        let sync = message(Body::Sync(Timestamp { secs: 1, nanos: 0 })).to_bytes();
        assert!(Message::parse(&sync[..40]).is_none(), "a cut-off Sync");
        assert!(Message::parse(&sync[..10]).is_none());
        let mut v1 = sync.clone();
        v1[1] = 1;
        assert!(Message::parse(&v1).is_none(), "PTP version 1");
    }

    #[test]
    fn the_lowest_announce_ranks_best() {
        let phone = Announce {
            priority1: 248,
            class: 248,
            accuracy: 0xfe,
            variance: 0xffff,
            priority2: 248,
            grandmaster: 2,
            steps_removed: 0,
        };
        let homepod = Announce {
            priority1: 128,
            grandmaster: 9,
            ..phone
        };
        assert!(homepod.rank() < phone.rank());
        let twin = Announce {
            grandmaster: 1,
            ..phone
        };
        assert!(twin.rank() < phone.rank(), "the identity breaks a tie");
    }
}
