//! Converting between `f32` and the integer PCM formats at the edges:
//! s16le in from AirPlay, s16le or s24le out to Sendspin speakers.
//!
//! Integer samples map to `f32` by dividing by 2^(bits-1), so full-scale
//! negative is exactly -1.0 and every integer sample survives a round trip.
//! Encoding clamps, so a sample past full scale clips rather than wraps.

const S16_SCALE: f32 = 32_768.0;
const S24_SCALE: f32 = 8_388_608.0;

/// Decode s16le samples onto `out`. A trailing odd byte is left unread;
/// returns how many bytes were consumed, so a stream reader can keep the
/// rest for the next read.
pub fn s16le_to_f32(bytes: &[u8], out: &mut Vec<f32>) -> usize {
    let (samples, _) = bytes.as_chunks::<2>();
    out.extend(
        samples
            .iter()
            .map(|&b| f32::from(i16::from_le_bytes(b)) / S16_SCALE),
    );
    samples.len() * 2
}

/// Encode samples as s16le onto `out`.
pub fn f32_to_s16le(samples: &[f32], out: &mut Vec<u8>) {
    out.reserve(samples.len() * 2);
    for &s in samples {
        let v = (s * S16_SCALE).round().clamp(-32_768.0, 32_767.0) as i16;
        out.extend_from_slice(&v.to_le_bytes());
    }
}

/// Encode samples as packed 3-byte s24le onto `out`.
pub fn f32_to_s24le(samples: &[f32], out: &mut Vec<u8>) {
    out.reserve(samples.len() * 3);
    for &s in samples {
        let v = (s * S24_SCALE).round().clamp(-8_388_608.0, 8_388_607.0) as i32;
        out.extend_from_slice(&v.to_le_bytes()[..3]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_s16_sample_round_trips() {
        let bytes: Vec<u8> = (i16::MIN..=i16::MAX).flat_map(i16::to_le_bytes).collect();
        let mut samples = Vec::new();
        assert_eq!(s16le_to_f32(&bytes, &mut samples), bytes.len());
        assert_eq!(samples[0], -1.0);
        let mut back = Vec::new();
        f32_to_s16le(&samples, &mut back);
        assert_eq!(back, bytes);
    }

    #[test]
    fn leaves_a_trailing_odd_byte() {
        let mut out = Vec::new();
        assert_eq!(s16le_to_f32(&[0x00, 0x40, 0x7f], &mut out), 2);
        assert_eq!(out, [0.5]);
    }

    #[test]
    fn encoding_clips_past_full_scale() {
        let mut out = Vec::new();
        f32_to_s16le(&[2.0, -2.0, 1.0], &mut out);
        assert_eq!(out, [0xff, 0x7f, 0x00, 0x80, 0xff, 0x7f]);
    }

    #[test]
    fn encodes_s24le() {
        let mut out = Vec::new();
        f32_to_s24le(&[-1.0, 0.5, 1.0, 0.0], &mut out);
        assert_eq!(
            out,
            [
                0x00, 0x00, 0x80, 0x00, 0x00, 0x40, 0xff, 0xff, 0x7f, 0x00, 0x00, 0x00
            ]
        );
    }
}
