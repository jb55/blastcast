//! Turning 20 ms chunks of session audio into a player's wire format:
//! PCM, FLAC or Opus.
//!
//! An [`Encoder`] is made for a format, not a player. The stream keeps one
//! per format in use and sends what it encodes to every player that
//! negotiated that format, so a group of ten FLAC speakers costs one
//! FLAC encode a chunk.

mod flac;
mod opus;
mod pcm;

use std::borrow::Cow;

use base64::Engine;
use blastcast::{AudioError, SESSION_FORMAT};

pub use flac::FlacEncoder;
pub use opus::OpusEncoder;
pub use pcm::PcmEncoder;

use super::CHUNK_FRAMES;
use crate::protocol::{Codec, StreamFormat};

/// The integer bit depths this server encodes, for PCM and FLAC. 24-bit
/// PCM samples are packed into three bytes, as the spec has it.
const BIT_DEPTHS: [u32; 2] = [16, 24];

/// Whether this server can send `format`: the session's channel count,
/// and per codec
///
/// - PCM at 16 or 24 bits, at any rate (it is resampled);
/// - FLAC at 16 or 24 bits, at the session rate only;
/// - Opus at any rate an Opus decoder runs at.
pub fn can_encode(format: &StreamFormat) -> bool {
    if format.channels != u32::from(SESSION_FORMAT.channels) || format.sample_rate == 0 {
        return false;
    }
    match format.codec {
        Codec::Pcm => BIT_DEPTHS.contains(&format.bit_depth),
        Codec::Flac => {
            BIT_DEPTHS.contains(&format.bit_depth)
                && format.sample_rate == SESSION_FORMAT.sample_rate
        }
        Codec::Opus => opus::RATES.contains(&format.sample_rate),
        Codec::Unknown => false,
    }
}

pub enum Encoder {
    Pcm(Box<PcmEncoder>),
    Flac(Box<FlacEncoder>),
    Opus(Box<OpusEncoder>),
}

impl Encoder {
    pub fn new(format: StreamFormat) -> Result<Self, AudioError> {
        Ok(match format.codec {
            Codec::Pcm => Self::Pcm(Box::new(PcmEncoder::new(format)?)),
            Codec::Flac => Self::Flac(Box::new(FlacEncoder::new(format)?)),
            Codec::Opus => Self::Opus(Box::new(OpusEncoder::new(format)?)),
            Codec::Unknown => {
                return Err(AudioError::Format("an unknown codec".into()));
            }
        })
    }

    pub fn format(&self) -> StreamFormat {
        match self {
            Self::Pcm(e) => e.format(),
            Self::Flac(e) => e.format(),
            Self::Opus(e) => e.format(),
        }
    }

    /// The `codec_header` for `stream/start`, in standard Base64: FLAC's
    /// `fLaC` and STREAMINFO, nothing for the others.
    pub fn codec_header(&self) -> Option<String> {
        match self {
            Self::Flac(e) => Some(base64::engine::general_purpose::STANDARD.encode(e.header())),
            Self::Pcm(_) | Self::Opus(_) => None,
        }
    }

    /// How many bytes `session_frames` of session audio encode to, at
    /// most, for checking a player's buffer room before encoding. Exact
    /// for PCM; a bound for FLAC and Opus, whose size depends on the audio.
    pub fn estimate_bytes(&self, session_frames: usize) -> usize {
        match self {
            Self::Pcm(e) => e.estimate_bytes(session_frames),
            Self::Flac(e) => e.estimate_bytes(session_frames),
            Self::Opus(e) => e.estimate_bytes(session_frames),
        }
    }

    /// How far behind its timestamp the decoded audio starts, which the
    /// stream stamps chunks that much early to undo: Opus's lookahead.
    pub fn delay_us(&self) -> i64 {
        match self {
            Self::Opus(e) => e.delay_us(),
            Self::Pcm(_) | Self::Flac(_) => 0,
        }
    }

    /// Encodes one chunk of interleaved session-format samples, at most
    /// [`CHUNK_FRAMES`], returning the bytes and how many frames they hold
    /// at the wire rate. FLAC and Opus always encode a whole chunk,
    /// padding a short last one with silence.
    pub fn encode(&mut self, samples: &[f32]) -> Result<(Vec<u8>, usize), AudioError> {
        match self {
            Self::Pcm(e) => e.encode(samples),
            Self::Flac(e) => e.encode(samples),
            Self::Opus(e) => e.encode(samples),
        }
    }

    /// Forget what the encoder carries from one chunk to the next, after
    /// the stream jumps. FLAC frames carry nothing.
    pub fn reset(&mut self) {
        match self {
            Self::Pcm(e) => e.reset(),
            Self::Opus(e) => e.reset(),
            Self::Flac(_) => {}
        }
    }
}

/// `samples` as a whole chunk, a short one padded with silence.
fn whole_chunk(samples: &[f32]) -> Result<Cow<'_, [f32]>, AudioError> {
    let len = CHUNK_FRAMES * usize::from(SESSION_FORMAT.channels);
    match samples.len() {
        n if n == len => Ok(Cow::Borrowed(samples)),
        n if n < len => {
            let mut padded = samples.to_vec();
            padded.resize(len, 0.0);
            Ok(Cow::Owned(padded))
        }
        n => Err(AudioError::Encoder(format!(
            "{n} samples is more than a chunk"
        ))),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn fmt(codec: Codec, sample_rate: u32, bit_depth: u32) -> StreamFormat {
        StreamFormat {
            codec,
            channels: 2,
            sample_rate,
            bit_depth,
        }
    }

    /// `frames` of a 440 Hz stereo sine at half scale, session rate.
    pub(crate) fn sine(frames: usize) -> Vec<f32> {
        (0..frames)
            .flat_map(|i| {
                let s = 0.5 * (i as f32 * 440.0 * std::f32::consts::TAU / 48_000.0).sin();
                [s, s]
            })
            .collect()
    }

    #[test]
    fn what_it_can_encode() {
        assert!(can_encode(&fmt(Codec::Pcm, 44_100, 24)));
        assert!(!can_encode(&fmt(Codec::Pcm, 48_000, 32)));
        assert!(can_encode(&fmt(Codec::Flac, 48_000, 24)));
        assert!(!can_encode(&fmt(Codec::Flac, 44_100, 16)));
        assert!(can_encode(&fmt(Codec::Opus, 24_000, 16)));
        assert!(!can_encode(&fmt(Codec::Opus, 44_100, 16)));
        assert!(!can_encode(&fmt(Codec::Unknown, 48_000, 16)));
        let mono = StreamFormat {
            channels: 1,
            ..fmt(Codec::Pcm, 48_000, 16)
        };
        assert!(!can_encode(&mono));
        // Every format it says it can encode, it can.
        for codec in [Codec::Pcm, Codec::Flac, Codec::Opus] {
            for rate in [8_000, 24_000, 44_100, 48_000, 96_000] {
                for bits in [16, 24] {
                    let f = fmt(codec, rate, bits);
                    assert_eq!(can_encode(&f), Encoder::new(f).is_ok(), "{f:?}");
                }
            }
        }
    }

    #[test]
    fn only_flac_has_a_codec_header() {
        let flac = Encoder::new(fmt(Codec::Flac, 48_000, 16)).unwrap();
        let header = flac.codec_header().unwrap();
        assert!(header.starts_with("ZkxhQ"), "{header}"); // "fLaC"
        assert_eq!(header.len(), (4 + 4 + 34usize).div_ceil(3) * 4);
        assert!(
            Encoder::new(fmt(Codec::Opus, 48_000, 16))
                .unwrap()
                .codec_header()
                .is_none()
        );
    }
}
