//! PCM: session audio resampled to the player's rate if that differs, then
//! packed as s16le or s24le.

use blastcast::{AudioError, SESSION_FORMAT};
use blastcast_audio::{StreamResampler, pcm};

use super::BIT_DEPTHS;
use crate::protocol::{Codec, StreamFormat};

pub struct PcmEncoder {
    format: StreamFormat,
    resampler: Option<StreamResampler>,
    scratch: Vec<f32>,
}

impl PcmEncoder {
    pub fn new(format: StreamFormat) -> Result<Self, AudioError> {
        if format.codec != Codec::Pcm || !BIT_DEPTHS.contains(&format.bit_depth) {
            return Err(AudioError::Format(format!(
                "{:?} at {} bits is not a PCM format this server encodes",
                format.codec, format.bit_depth
            )));
        }
        let resampler = (format.sample_rate != SESSION_FORMAT.sample_rate)
            .then(|| {
                StreamResampler::new(
                    SESSION_FORMAT.sample_rate,
                    format.sample_rate,
                    SESSION_FORMAT.channels,
                )
            })
            .transpose()?;
        Ok(Self {
            format,
            resampler,
            scratch: Vec::new(),
        })
    }

    pub fn format(&self) -> StreamFormat {
        self.format
    }

    /// Bytes per frame on the wire.
    pub fn frame_bytes(&self) -> usize {
        self.format.channels as usize * (self.format.bit_depth as usize / 8)
    }

    /// How many bytes `session_frames` of session audio encode to.
    pub fn estimate_bytes(&self, session_frames: usize) -> usize {
        let frames = (session_frames as u64 * u64::from(self.format.sample_rate))
            .div_ceil(u64::from(SESSION_FORMAT.sample_rate));
        frames as usize * self.frame_bytes()
    }

    /// A resampler may hold input back, so a chunk can encode to nothing;
    /// the frames come out with a later one.
    pub fn encode(&mut self, samples: &[f32]) -> Result<(Vec<u8>, usize), AudioError> {
        let samples = match &mut self.resampler {
            Some(resampler) => {
                self.scratch.clear();
                resampler.process(samples, &mut self.scratch)?;
                &self.scratch[..]
            }
            None => samples,
        };
        let mut out = Vec::new();
        match self.format.bit_depth {
            16 => pcm::f32_to_s16le(samples, &mut out),
            _ => pcm::f32_to_s24le(samples, &mut out),
        }
        Ok((out, samples.len() / self.format.channels as usize))
    }

    pub fn reset(&mut self) {
        if let Some(resampler) = &mut self.resampler {
            resampler.reset();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::encode::tests::fmt;

    #[test]
    fn packs_24_bit_into_three_bytes() {
        let mut enc = PcmEncoder::new(fmt(Codec::Pcm, 48_000, 24)).unwrap();
        assert_eq!(enc.frame_bytes(), 6);
        let (bytes, frames) = enc.encode(&[0.5, -1.0, 0.0, 0.0]).unwrap();
        assert_eq!(frames, 2);
        assert_eq!(
            bytes,
            [0x00, 0x00, 0x40, 0x00, 0x00, 0x80, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(enc.estimate_bytes(960), 960 * 6);
    }

    #[test]
    fn resamples_to_the_players_rate() {
        let mut enc = PcmEncoder::new(fmt(Codec::Pcm, 44_100, 16)).unwrap();
        assert_eq!(enc.estimate_bytes(960), 882 * 4);
        let chunk = vec![0.0; 960 * 2];
        let mut frames = 0;
        for _ in 0..50 {
            let (bytes, n) = enc.encode(&chunk).unwrap();
            assert_eq!(bytes.len(), n * 4);
            frames += n;
        }
        // One second in; the resampler holds back at most a block.
        assert!((44_100 - 1100..=44_100).contains(&frames), "{frames}");
    }

    #[test]
    fn only_pcm_encodes() {
        assert!(PcmEncoder::new(fmt(Codec::Flac, 48_000, 16)).is_err());
        assert!(PcmEncoder::new(fmt(Codec::Pcm, 48_000, 32)).is_err());
    }
}
