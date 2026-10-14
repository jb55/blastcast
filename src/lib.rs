//! Audio plumbing shared by inputs and outputs: PCM frame types, the
//! server clock, resampling and codecs.
//!
//! Inside the server, audio is always [`SESSION_FORMAT`]: 48 kHz stereo,
//! interleaved `f32`. Inputs convert to it as they read ([`pcm`] for the
//! sample format, [`Resampled`] for the rate) and outputs encode from it as
//! they send, so nothing in between handles more than one format.

pub mod clock;
pub mod format;
pub mod pcm;
pub mod resample;
pub mod source;
pub mod timeline;

pub use clock::ServerClock;
pub use format::{AudioFormat, PcmChunk, SESSION_FORMAT};
pub use resample::StreamResampler;
pub use source::{LiveSource, Resampled, SineSource, to_session_format};
pub use timeline::{Span, Timeline};

/// What can go wrong reading, converting or resampling audio.
#[derive(Debug, thiserror::Error)]
pub enum AudioError {
    #[error("unsupported audio format: {0}")]
    Format(String),
    #[error("resampler: {0}")]
    Resampler(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
