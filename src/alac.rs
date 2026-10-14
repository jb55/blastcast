//! ALAC frames for RAOP, written uncompressed.
//!
//! Every AirPlay 1 receiver decodes Apple Lossless, and ALAC has an escape
//! for frames stored verbatim. Writing those needs no encoder: a frame is a
//! short header and the 16-bit samples, at the 1411 kb/s of the PCM itself,
//! which a LAN carries easily. It is what libraop's `pcm_to_alac_raw` sends.
//!
//! The bitstream, MSB first: a 3-bit element tag (1, a channel pair), a
//! 4-bit instance tag, 12 unused bits, a "partial frame" bit (1: the frame
//! count follows), 2 bits of shift, the escape bit (1: uncompressed), the
//! 32-bit frame count, then each frame's left and right sample as 16 bits,
//! and finally the end tag (7) padded to a byte.

/// Frames in one RAOP packet, as the SDP's `fmtp` line announces.
pub const FRAMES_PER_PACKET: usize = 352;

/// The `fmtp` parameters of a 16-bit stereo 44.1 kHz ALAC stream with
/// [`FRAMES_PER_PACKET`] frames a packet.
pub const FMTP: &str = "352 0 16 40 10 14 2 255 0 0 44100";

const ID_CPE: u32 = 1;
const ID_END: u32 = 7;

/// Encodes interleaved stereo s16 frames as one uncompressed ALAC frame.
pub fn encode_stereo_s16(samples: &[i16]) -> Vec<u8> {
    assert!(
        samples.len().is_multiple_of(2),
        "stereo samples come in pairs"
    );
    let frames = samples.len() / 2;
    let mut w = BitWriter::with_capacity(8 + samples.len() * 2);
    w.put(ID_CPE, 3);
    w.put(0, 4); // instance tag
    w.put(0, 12); // unused
    w.put(1, 1); // the frame count follows
    w.put(0, 2); // no bytes shifted out
    w.put(1, 1); // escape: uncompressed
    w.put(frames as u32, 32);
    for &s in samples {
        w.put(u32::from(s as u16), 16);
    }
    w.put(ID_END, 3);
    w.finish()
}

/// Decodes an uncompressed stereo frame written by [`encode_stereo_s16`],
/// or `None` for anything else. For tests and the fake receiver.
pub fn decode_stereo_s16(frame: &[u8]) -> Option<Vec<i16>> {
    let mut r = BitReader {
        data: frame,
        bit: 0,
    };
    if r.get(3)? != ID_CPE {
        return None;
    }
    r.get(4)?;
    r.get(12)?;
    let header = r.get(4)?;
    if header != 0b1001 {
        return None;
    }
    let frames = r.get(32)? as usize;
    let mut out = Vec::with_capacity(frames * 2);
    for _ in 0..frames * 2 {
        out.push(r.get(16)? as u16 as i16);
    }
    (r.get(3)? == ID_END).then_some(out)
}

struct BitWriter {
    out: Vec<u8>,
    acc: u64,
    bits: u32,
}

impl BitWriter {
    fn with_capacity(bytes: usize) -> Self {
        Self {
            out: Vec::with_capacity(bytes),
            acc: 0,
            bits: 0,
        }
    }

    /// Appends the low `n` bits of `value`, at most 32.
    fn put(&mut self, value: u32, n: u32) {
        debug_assert!(n <= 32);
        let mask = if n == 32 { u32::MAX } else { (1 << n) - 1 };
        self.acc = (self.acc << n) | u64::from(value & mask);
        self.bits += n;
        while self.bits >= 8 {
            self.bits -= 8;
            self.out.push((self.acc >> self.bits) as u8);
        }
        self.acc &= (1 << self.bits) - 1;
    }

    fn finish(mut self) -> Vec<u8> {
        if self.bits > 0 {
            self.out.push((self.acc << (8 - self.bits)) as u8);
        }
        self.out
    }
}

struct BitReader<'a> {
    data: &'a [u8],
    bit: usize,
}

impl BitReader<'_> {
    fn get(&mut self, n: u32) -> Option<u32> {
        let mut v = 0u32;
        for _ in 0..n {
            let byte = *self.data.get(self.bit / 8)?;
            let b = (byte >> (7 - self.bit % 8)) & 1;
            v = (v << 1) | u32::from(b);
            self.bit += 1;
        }
        Some(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_a_packet() {
        let samples: Vec<i16> = (0..FRAMES_PER_PACKET * 2)
            .map(|i| (i as i32 * 187 - 30_000) as i16)
            .collect();
        let frame = encode_stereo_s16(&samples);
        // 55 header bits, the samples, 3 end bits: 1416 bytes.
        assert_eq!(frame.len(), (55 + samples.len() * 16 + 3).div_ceil(8));
        assert_eq!(decode_stereo_s16(&frame).unwrap(), samples);
    }

    #[test]
    fn header_bits_match_libraop() {
        // libraop writes 0x20 0x00 0x12, then the count from bit 23.
        let frame = encode_stereo_s16(&[0x1234, -1]);
        assert_eq!(&frame[..3], [0x20, 0x00, 0x12]);
        // A count of 1 in bits 23-54, so its one set bit is bit 54.
        assert_eq!(&frame[3..7], [0x00, 0x00, 0x00, 0x02]);
        assert_eq!(decode_stereo_s16(&frame).unwrap(), [0x1234, -1]);
        assert_eq!(decode_stereo_s16(&[0xff; 8]), None);
    }
}
