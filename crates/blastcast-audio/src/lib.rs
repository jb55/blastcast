//! Audio plumbing shared by inputs and outputs: PCM sample conversion,
//! resampling and test tones.
//!
//! Between protocols, audio is always blastcast's
//! [`SESSION_FORMAT`](blastcast::SESSION_FORMAT): 48 kHz stereo, interleaved
//! `f32`. Inputs convert to it as they read ([`pcm`] for the sample format,
//! [`Resampled`] for the rate) and outputs encode from it as they send, so
//! nothing in between handles more than one format. The clock, formats,
//! timeline and source traits are blastcast's own; import them from there.

pub mod pcm;
pub mod resample;
pub mod source;

pub use resample::StreamResampler;
pub use source::{
    Resampled, SineSource, TimedResampled, timed_to_session_format, to_session_format,
};
