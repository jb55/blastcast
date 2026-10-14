//! The UDP packets of an AirPlay 1 stream, from the receiver's side.
//!
//! - **Audio** comes to our server port: RTP, payload type 96, one codec
//!   frame a packet, the first after a start or flush with the marker bit.
//! - **Control**: the sender's sync packets come to our control port, and
//!   so do the packets it sends again, wrapped in four bytes. We ask for
//!   lost packets from its control port.
//! - **Timing**: we ask the sender's timing port for its clock, and answer
//!   it if it asks ours.
//!
//! The layouts are the ones `tas-airplay-out` sends (`rtp.rs`), from
//! libraop and shairport-sync.

pub const PAYLOAD_AUDIO: u8 = 0x60;
pub const PAYLOAD_SYNC: u8 = 0x54;
pub const PAYLOAD_TIMING_REQUEST: u8 = 0x52;
pub const PAYLOAD_TIMING_REPLY: u8 = 0x53;
pub const PAYLOAD_RESEND_REQUEST: u8 = 0x55;
pub const PAYLOAD_RESEND_REPLY: u8 = 0x56;

const MARKER: u8 = 0x80;

/// An audio packet's header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AudioHeader {
    pub marker: bool,
    pub seq: u16,
    pub timestamp: u32,
}

/// The packet's type, without the marker bit.
pub fn payload_type(packet: &[u8]) -> Option<u8> {
    packet.get(1).map(|b| b & 0x7f)
}

/// An audio packet's header and payload.
pub fn parse_audio(packet: &[u8]) -> Option<(AudioHeader, &[u8])> {
    if packet.len() < 12 || payload_type(packet)? != PAYLOAD_AUDIO {
        return None;
    }
    let header = AudioHeader {
        marker: packet[1] & MARKER != 0,
        seq: u16::from_be_bytes([packet[2], packet[3]]),
        timestamp: u32::from_be_bytes(packet[4..8].try_into().ok()?),
    };
    Some((header, &packet[12..]))
}

/// The audio packet inside a resend reply.
pub fn parse_resend_reply(packet: &[u8]) -> Option<(AudioHeader, &[u8])> {
    if payload_type(packet)? != PAYLOAD_RESEND_REPLY {
        return None;
    }
    parse_audio(packet.get(4..)?)
}

/// Asks the sender for `count` packets from `first`. `seq` numbers our
/// requests.
pub fn resend_request(seq: u16, first: u16, count: u16) -> [u8; 8] {
    let mut out = [0x80, MARKER | PAYLOAD_RESEND_REQUEST, 0, 0, 0, 0, 0, 0];
    out[2..4].copy_from_slice(&seq.to_be_bytes());
    out[4..6].copy_from_slice(&first.to_be_bytes());
    out[6..8].copy_from_slice(&count.to_be_bytes());
    out
}

/// A timing request sent at NTP time `now`.
pub fn timing_request(now: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[0] = 0x80;
    out[1] = MARKER | PAYLOAD_TIMING_REQUEST;
    out[2..4].copy_from_slice(&7u16.to_be_bytes());
    out[24..32].copy_from_slice(&now.to_be_bytes());
    out
}

/// The answer to a timing request, read and sent at NTP time `now`, or
/// `None` if `request` isn't one.
pub fn timing_reply(request: &[u8], now: u64) -> Option<[u8; 32]> {
    if request.len() < 32 || payload_type(request)? != PAYLOAD_TIMING_REQUEST {
        return None;
    }
    let mut out = [0u8; 32];
    out[0] = 0x80;
    out[1] = MARKER | PAYLOAD_TIMING_REPLY;
    out[2..4].copy_from_slice(&request[2..4]);
    out[8..16].copy_from_slice(&request[24..32]);
    out[16..24].copy_from_slice(&now.to_be_bytes());
    out[24..32].copy_from_slice(&now.to_be_bytes());
    Some(out)
}

#[cfg(test)]
mod tests {
    use tas_airplay_out::rtp as sender;

    use super::*;

    /// Our side and the sender's agree on every layout.
    #[test]
    fn packets_match_the_sender() {
        let audio = sender::audio_packet(true, 513, 0xdead_beef, 9, &[1, 2, 3]);
        let (h, payload) = parse_audio(&audio).unwrap();
        assert_eq!(
            h,
            AudioHeader {
                marker: true,
                seq: 513,
                timestamp: 0xdead_beef
            }
        );
        assert_eq!(payload, [1, 2, 3]);

        let resent = sender::resend_reply(&audio);
        assert_eq!(parse_resend_reply(&resent).unwrap().0.seq, 513);
        assert_eq!(parse_audio(&resent), None, "a resend isn't plain audio");

        assert_eq!(
            sender::parse_resend_request(&resend_request(1, 65_535, 3)),
            Some((65_535, 3))
        );

        let reply = sender::timing_reply(&timing_request(42), 99).unwrap();
        assert_eq!(payload_type(&reply), Some(PAYLOAD_TIMING_REPLY));
        assert_eq!(reply[8..16], 42u64.to_be_bytes(), "our time comes back");
        let ours = timing_reply(&sender::timing_request(5), 6).unwrap();
        assert_eq!(ours[8..16], 5u64.to_be_bytes());
        assert_eq!(timing_reply(&ours, 1), None, "a reply isn't a request");
    }
}
