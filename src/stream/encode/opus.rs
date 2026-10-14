//! Opus: each 20 ms chunk is one Opus packet, with no container.
//!
//! The encoder always runs at 48 kHz, the session rate. An Opus packet
//! doesn't carry a rate, so a player that asked for 24 kHz decodes the
//! same packets at 24 kHz; only the frame count on its timeline differs.
//! Encoded in pure Rust (`opus-pure`); the tests decode the packets with
//! libopus, which is what real players (sendspin-go, ESPHome) run.

use opus_pure::Application;
use tas_audio::{AudioError, SESSION_FORMAT};

use super::whole_chunk;
use crate::protocol::{Codec, StreamFormat};
use crate::stream::CHUNK_FRAMES;

/// The rates an Opus decoder runs at.
pub const RATES: [u32; 5] = [8_000, 12_000, 16_000, 24_000, 48_000];

/// 128 kb/s a channel, sendspin-go's choice: transparent for music.
const BITRATE: u32 = 128_000 * SESSION_FORMAT.channels as u32;

/// The largest packet one 20 ms frame can take (RFC 6716 §3.4, R4),
/// plus its TOC byte.
const MAX_PACKET: usize = 1_276;

pub struct OpusEncoder {
    format: StreamFormat,
    encoder: opus_pure::OpusEncoder,
    delay_us: i64,
}

impl OpusEncoder {
    pub fn new(format: StreamFormat) -> Result<Self, AudioError> {
        if format.codec != Codec::Opus
            || !RATES.contains(&format.sample_rate)
            || format.channels != u32::from(SESSION_FORMAT.channels)
        {
            return Err(AudioError::Format(format!(
                "Opus is sent only as stereo at {RATES:?} Hz, not {format:?}"
            )));
        }
        let mut encoder = opus_pure::OpusEncoder::new(
            SESSION_FORMAT.sample_rate as i32,
            usize::from(SESSION_FORMAT.channels),
            Application::Audio,
        )
        .map_err(|e| AudioError::Encoder(e.to_string()))?;
        encoder.bitrate_bps = BITRATE as i32;
        let lookahead = encoder.lookahead() as i64;
        Ok(Self {
            format,
            encoder,
            delay_us: lookahead * 1_000_000 / i64::from(SESSION_FORMAT.sample_rate),
        })
    }

    pub fn format(&self) -> StreamFormat {
        self.format
    }

    /// How much later than its input the decoded audio comes out: the
    /// encoder's lookahead, about 6.5 ms. With no container to carry a
    /// pre-skip, the decoder plays it, so chunks are stamped this much
    /// early to stay in sync with a PCM player.
    pub fn delay_us(&self) -> i64 {
        self.delay_us
    }

    /// The bitrate's share of a chunk, with room for variable bitrate.
    pub fn estimate_bytes(&self, session_frames: usize) -> usize {
        let frames = session_frames.max(CHUNK_FRAMES) as u64;
        let bytes = u64::from(BITRATE) * frames / u64::from(SESSION_FORMAT.sample_rate) / 8;
        (bytes as usize * 2).min(MAX_PACKET)
    }

    /// One packet of [`CHUNK_FRAMES`]; a short last chunk is padded with
    /// silence. The frames are counted at the player's rate.
    pub fn encode(&mut self, samples: &[f32]) -> Result<(Vec<u8>, usize), AudioError> {
        let samples = whole_chunk(samples)?;
        let mut packet = vec![0; MAX_PACKET];
        let len = self
            .encoder
            .encode(&samples, CHUNK_FRAMES, &mut packet)
            .map_err(|e| AudioError::Encoder(e.to_string()))?;
        packet.truncate(len);
        let frames =
            CHUNK_FRAMES * self.format.sample_rate as usize / SESSION_FORMAT.sample_rate as usize;
        Ok((packet, frames))
    }

    pub fn reset(&mut self) {
        if let Err(e) = self.encoder.reset_state() {
            tracing::warn!(error = %e, "Opus encoder reset failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::encode::tests::{fmt, sine};

    /// Encodes `input` a chunk at a time and decodes it with libopus, the
    /// decoder real players run. Returns the SNR in dB of the decoded audio
    /// shifted back by the encoder's reported lookahead, and the packets'
    /// average and largest sizes.
    fn round_trip(input: &[f32]) -> (f64, usize, usize) {
        let mut enc = OpusEncoder::new(fmt(Codec::Opus, 48_000, 16)).unwrap();
        let mut dec = opus::Decoder::new(48_000, opus::Channels::Stereo).unwrap();
        let mut decoded = Vec::new();
        let (mut total, mut max) = (0, 0);
        let chunks = input.chunks(CHUNK_FRAMES * 2);
        let count = chunks.len();
        for chunk in chunks {
            let (packet, frames) = enc.encode(chunk).unwrap();
            assert_eq!(frames, CHUNK_FRAMES);
            assert!(
                packet.len() <= enc.estimate_bytes(CHUNK_FRAMES),
                "{}",
                packet.len()
            );
            total += packet.len();
            max = max.max(packet.len());
            let mut out = vec![0.0; CHUNK_FRAMES * 2];
            assert_eq!(
                dec.decode_float(&packet, &mut out, false).unwrap(),
                CHUNK_FRAMES
            );
            decoded.extend(out);
        }
        // A wrong lookahead misaligns the two and the SNR collapses, so
        // this also checks what `delay_us` reports.
        let lag = (enc.delay_us() * 48 / 1000) as usize * 2;
        let (mut signal, mut noise) = (0.0f64, 0.0f64);
        for (i, &want) in input[..input.len() - lag].iter().enumerate().skip(4_800) {
            signal += f64::from(want).powi(2);
            noise += f64::from(decoded[i + lag] - want).powi(2);
        }
        (10.0 * (signal / noise).log10(), total / count, max)
    }

    #[test]
    fn one_packet_per_chunk_that_decodes_back() {
        let enc = OpusEncoder::new(fmt(Codec::Opus, 48_000, 16)).unwrap();
        assert!(
            (5_000..=7_000).contains(&enc.delay_us()),
            "{}",
            enc.delay_us()
        );
        let (snr_db, ..) = round_trip(&sine(50 * CHUNK_FRAMES));
        assert!(snr_db > 20.0, "SNR {snr_db:.1} dB");
    }

    #[test]
    fn a_chord_decodes_back_at_the_bitrate() {
        // A C major chord over a low A on the left, C minor over a low E on
        // the right: many partials, and channels that differ.
        let chord = |t: f32, freqs: [f32; 5]| -> f32 {
            freqs
                .iter()
                .map(|f| (t * f * std::f32::consts::TAU).sin())
                .sum::<f32>()
                * 0.15
        };
        let input: Vec<f32> = (0..100 * CHUNK_FRAMES)
            .flat_map(|i| {
                let t = i as f32 / 48_000.0;
                [
                    chord(t, [110.0, 261.63, 329.63, 392.0, 523.25]),
                    chord(t, [82.41, 261.63, 311.13, 392.0, 659.25]),
                ]
            })
            .collect();
        let (snr_db, avg, max) = round_trip(&input);
        assert!(snr_db > 20.0, "SNR {snr_db:.1} dB");
        // 256 kb/s is 640 bytes a chunk.
        assert!((560..=720).contains(&avg), "{avg} bytes a packet");
        assert!(max <= MAX_PACKET, "{max}");
    }

    #[test]
    fn a_lower_rate_counts_fewer_frames() {
        let mut enc = OpusEncoder::new(fmt(Codec::Opus, 24_000, 16)).unwrap();
        let (packet, frames) = enc.encode(&sine(100)).unwrap();
        assert_eq!(frames, 480);
        let mut dec = opus::Decoder::new(24_000, opus::Channels::Stereo).unwrap();
        let mut out = vec![0.0; 480 * 2];
        assert_eq!(dec.decode_float(&packet, &mut out, false).unwrap(), 480);
    }

    #[test]
    fn refuses_what_it_cannot_send() {
        assert!(OpusEncoder::new(fmt(Codec::Opus, 44_100, 16)).is_err());
        assert!(OpusEncoder::new(fmt(Codec::Pcm, 48_000, 16)).is_err());
    }
}
