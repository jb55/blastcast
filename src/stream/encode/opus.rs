//! Opus: each 20 ms chunk is one Opus packet, with no container.
//!
//! The encoder always runs at 48 kHz, the session rate. An Opus packet
//! doesn't carry a rate, so a player that asked for 24 kHz decodes the
//! same packets at 24 kHz; only the frame count on its timeline differs.
//! Encoded with libopus (the `opus` crate, which builds it from source
//! with cmake).

use opus::{Application, Bitrate, Channels};
use tas_audio::{AudioError, SESSION_FORMAT};

use super::whole_chunk;
use crate::protocol::{Codec, StreamFormat};
use crate::stream::CHUNK_FRAMES;

/// The rates an Opus decoder runs at.
pub const RATES: [u32; 5] = [8_000, 12_000, 16_000, 24_000, 48_000];

/// 128 kb/s a channel, sendspin-go's choice: transparent for music.
const BITRATE: u32 = 128_000 * SESSION_FORMAT.channels as u32;

/// The largest packet libopus writes.
const MAX_PACKET: usize = 4_000;

pub struct OpusEncoder {
    format: StreamFormat,
    encoder: opus::Encoder,
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
        let err = |e: opus::Error| AudioError::Encoder(e.to_string());
        let mut encoder = opus::Encoder::new(
            SESSION_FORMAT.sample_rate,
            Channels::Stereo,
            Application::Audio,
        )
        .map_err(err)?;
        encoder
            .set_bitrate(Bitrate::Bits(BITRATE as i32))
            .map_err(err)?;
        let lookahead = encoder.get_lookahead().map_err(err)?;
        Ok(Self {
            format,
            encoder,
            delay_us: i64::from(lookahead) * 1_000_000 / i64::from(SESSION_FORMAT.sample_rate),
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
        let packet = self
            .encoder
            .encode_vec_float(&samples, MAX_PACKET)
            .map_err(|e| AudioError::Encoder(e.to_string()))?;
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

    #[test]
    fn one_packet_per_chunk_that_decodes_back() {
        let mut enc = OpusEncoder::new(fmt(Codec::Opus, 48_000, 16)).unwrap();
        assert!(
            (5_000..=7_000).contains(&enc.delay_us()),
            "{}",
            enc.delay_us()
        );
        let mut dec = opus::Decoder::new(48_000, Channels::Stereo).unwrap();
        let input = sine(50 * CHUNK_FRAMES);
        let mut decoded = Vec::new();
        for chunk in input.chunks(CHUNK_FRAMES * 2) {
            let (packet, frames) = enc.encode(chunk).unwrap();
            assert_eq!(frames, CHUNK_FRAMES);
            assert!(
                packet.len() <= enc.estimate_bytes(CHUNK_FRAMES),
                "{}",
                packet.len()
            );
            let mut out = vec![0.0; CHUNK_FRAMES * 2];
            assert_eq!(
                dec.decode_float(&packet, &mut out, false).unwrap(),
                CHUNK_FRAMES
            );
            decoded.extend(out);
        }
        // Shifted back by the lookahead, the decoded sine is the input
        // to within Opus's loss.
        let lag = (enc.delay_us() * 48 / 1000) as usize * 2;
        let (mut signal, mut noise) = (0.0f64, 0.0f64);
        for (i, &want) in input[..input.len() - lag].iter().enumerate().skip(4_800) {
            signal += f64::from(want).powi(2);
            noise += f64::from(decoded[i + lag] - want).powi(2);
        }
        let snr_db = 10.0 * (signal / noise).log10();
        assert!(snr_db > 20.0, "SNR {snr_db:.1} dB");
    }

    #[test]
    fn a_lower_rate_counts_fewer_frames() {
        let mut enc = OpusEncoder::new(fmt(Codec::Opus, 24_000, 16)).unwrap();
        let (packet, frames) = enc.encode(&sine(100)).unwrap();
        assert_eq!(frames, 480);
        let mut dec = opus::Decoder::new(24_000, Channels::Stereo).unwrap();
        let mut out = vec![0.0; 480 * 2];
        assert_eq!(dec.decode_float(&packet, &mut out, false).unwrap(), 480);
    }

    #[test]
    fn refuses_what_it_cannot_send() {
        assert!(OpusEncoder::new(fmt(Codec::Opus, 44_100, 16)).is_err());
        assert!(OpusEncoder::new(fmt(Codec::Pcm, 48_000, 16)).is_err());
    }
}
