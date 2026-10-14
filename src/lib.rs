//! Audio plumbing shared by inputs and outputs: PCM frame types, the
//! server clock, resampling and codecs.
//!
//! Inside the server, audio is always [`SESSION_FORMAT`]: 48 kHz stereo,
//! interleaved `f32`. Inputs convert to it as they read ([`pcm`] for the
//! sample format, [`Resampled`] for the rate) and outputs encode from it as
//! they send, so nothing in between handles more than one format.

pub mod pcm;
pub mod resample;
pub mod source;

// The clocks, formats and source trait live in blastcast, so protocol
// crates can share them without depending on the server.
pub use blastcast::{AudioError, clock, format, timeline};

pub use clock::ServerClock;
pub use format::{AudioFormat, PcmChunk, SESSION_FORMAT};
pub use resample::StreamResampler;
pub use source::{LiveSource, Resampled, SineSource, to_session_format};
pub use timeline::{Span, Timeline};
