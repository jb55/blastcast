//! Picking a player's PCM format from its hello, and encoding session audio
//! into it.

use tas_audio::{AudioError, SESSION_FORMAT, StreamResampler, pcm};

use crate::protocol::{Codec, PlayerSupport, StreamFormat};

/// The PCM bit depths this server encodes. 24-bit samples are packed into
/// three bytes, as the spec has it.
const PCM_BIT_DEPTHS: [u32; 2] = [16, 24];

/// The PCM format to send a player: the first in its preference order at
/// the session's rate, so nothing is resampled, and failing that the first
/// at any rate. Only the session's channel count and 16- or 24-bit depths
/// are considered. `None` when the player lists no such format.
///
/// sendspin-go lists 192 kHz first, so taking its first PCM entry outright
/// would upsample every chunk fourfold for nothing.
pub fn pick_pcm_format(support: &PlayerSupport) -> Option<StreamFormat> {
    let candidates: Vec<StreamFormat> = support
        .formats()
        .into_iter()
        .filter(|f| {
            f.codec == Codec::Pcm
                && f.channels == u32::from(SESSION_FORMAT.channels)
                && PCM_BIT_DEPTHS.contains(&f.bit_depth)
                && f.sample_rate > 0
        })
        .collect();
    candidates
        .iter()
        .find(|f| f.sample_rate == SESSION_FORMAT.sample_rate)
        .or(candidates.first())
        .copied()
}

/// Turns session-format audio into one player's wire bytes: resampled to
/// its rate if that differs, then packed as s16le or s24le.
pub struct PcmEncoder {
    format: StreamFormat,
    resampler: Option<StreamResampler>,
    scratch: Vec<f32>,
}

impl PcmEncoder {
    pub fn new(format: StreamFormat) -> Result<Self, AudioError> {
        if format.codec != Codec::Pcm || !PCM_BIT_DEPTHS.contains(&format.bit_depth) {
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

    /// About how many bytes `session_frames` of session audio encode to,
    /// for checking buffer room before encoding.
    pub fn estimate_bytes(&self, session_frames: usize) -> usize {
        let frames = (session_frames as u64 * u64::from(self.format.sample_rate))
            .div_ceil(u64::from(SESSION_FORMAT.sample_rate));
        frames as usize * self.frame_bytes()
    }

    /// Encode interleaved session-format samples, returning the bytes and
    /// how many wire frames they hold. A resampler may hold input back, so
    /// a chunk can encode to nothing; the frames come out with a later one.
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

    /// Forget held-back input, after the stream jumps.
    pub fn reset(&mut self) {
        if let Some(resampler) = &mut self.resampler {
            resampler.reset();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pcm(sample_rate: u32, bit_depth: u32) -> StreamFormat {
        StreamFormat {
            codec: Codec::Pcm,
            channels: 2,
            sample_rate,
            bit_depth,
        }
    }

    fn support(formats: Vec<StreamFormat>) -> PlayerSupport {
        PlayerSupport {
            supported_formats: formats,
            ..Default::default()
        }
    }

    #[test]
    fn prefers_the_session_rate_in_the_players_order() {
        // sendspin-go's default list.
        let s = support(vec![
            pcm(192_000, 24),
            pcm(96_000, 24),
            pcm(48_000, 16),
            pcm(44_100, 16),
            StreamFormat {
                codec: Codec::Flac,
                ..pcm(48_000, 24)
            },
        ]);
        assert_eq!(pick_pcm_format(&s), Some(pcm(48_000, 16)));
        let s = support(vec![pcm(48_000, 24), pcm(48_000, 16)]);
        assert_eq!(pick_pcm_format(&s), Some(pcm(48_000, 24)));
    }

    #[test]
    fn falls_back_to_another_rate() {
        let s = support(vec![
            StreamFormat {
                codec: Codec::Opus,
                ..pcm(48_000, 16)
            },
            pcm(44_100, 32),
            pcm(44_100, 16),
        ]);
        assert_eq!(pick_pcm_format(&s), Some(pcm(44_100, 16)));
    }

    #[test]
    fn refuses_what_it_cannot_send() {
        let mono = StreamFormat {
            channels: 1,
            ..pcm(48_000, 16)
        };
        assert_eq!(pick_pcm_format(&support(vec![mono, pcm(48_000, 8)])), None);
        assert_eq!(pick_pcm_format(&support(vec![])), None);
    }

    #[test]
    fn reads_the_oldest_per_dimension_lists() {
        let s = PlayerSupport {
            support_codecs: vec![Codec::Pcm],
            support_channels: vec![2],
            support_sample_rates: vec![44_100, 48_000],
            support_bit_depth: vec![16],
            ..Default::default()
        };
        assert_eq!(pick_pcm_format(&s), Some(pcm(48_000, 16)));
    }

    #[test]
    fn packs_24_bit_into_three_bytes() {
        let mut enc = PcmEncoder::new(pcm(48_000, 24)).unwrap();
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
        let mut enc = PcmEncoder::new(pcm(44_100, 16)).unwrap();
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
        let flac = StreamFormat {
            codec: Codec::Flac,
            ..pcm(48_000, 16)
        };
        assert!(PcmEncoder::new(flac).is_err());
    }
}
