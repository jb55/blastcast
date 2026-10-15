//! blastcast: one timeline across every audio protocol.
//!
//! Every protocol has its own clock (RAOP's NTP, AirPlay 2's PTP,
//! Sendspin's time sync) and its own way of saying "this frame plays
//! then". blastcast puts them all on one clock, the [`ServerClock`]: each
//! input maps its sender's clock to it with a [`ClockMap`], and says when
//! its frames play with an [`Anchor`]. Any input can then feed any output.
//!
//! It knows nothing of the server around it: it depends on no `tas-*`
//! crate (`scripts/check.sh` enforces it), so protocol crates depend on
//! it and never the reverse.

pub mod clock;
pub mod format;
pub mod map;
pub mod source;
pub mod timeline;

pub use clock::ServerClock;
pub use format::{AudioFormat, PcmChunk, SESSION_FORMAT};
pub use map::{Anchor, ClockMap};
pub use source::{IntoTimed, Item, LiveSource, TimedSource, Untimed};
pub use timeline::{Span, Timeline};

/// What can go wrong reading, converting or resampling audio.
#[derive(Debug, thiserror::Error)]
pub enum AudioError {
    #[error("unsupported audio format: {0}")]
    Format(String),
    #[error("resampler: {0}")]
    Resampler(String),
    #[error("encoder: {0}")]
    Encoder(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
