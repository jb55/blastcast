//! Audio formats and the PCM chunk every source yields.

use std::fmt;

/// A sample rate and channel count. Samples are always interleaved `f32`
/// inside the server; the wire's sample format belongs to the codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AudioFormat {
    pub sample_rate: u32,
    pub channels: u16,
}

/// The format of every session: 48 kHz stereo, which is Music Assistant's
/// Sendspin session default.
pub const SESSION_FORMAT: AudioFormat = AudioFormat {
    sample_rate: 48_000,
    channels: 2,
};

impl AudioFormat {
    pub const fn new(sample_rate: u32, channels: u16) -> Self {
        Self {
            sample_rate,
            channels,
        }
    }
}

impl fmt::Display for AudioFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} Hz/{} ch", self.sample_rate, self.channels)
    }
}

/// A run of interleaved `f32` frames, nominally in -1.0..=1.0.
#[derive(Debug, Clone, PartialEq)]
pub struct PcmChunk {
    pub format: AudioFormat,
    pub samples: Vec<f32>,
}

impl PcmChunk {
    pub fn new(format: AudioFormat, samples: Vec<f32>) -> Self {
        debug_assert_eq!(samples.len() % usize::from(format.channels), 0);
        Self { format, samples }
    }

    /// Frames, i.e. samples per channel.
    pub fn frames(&self) -> usize {
        self.samples.len() / usize::from(self.format.channels)
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }
}
