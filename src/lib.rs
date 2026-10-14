//! AirPlay input: a native AirPlay 1 receiver per target, or a
//! shairport-sync per target whose PCM and metadata fifos are read.
//!
//! - [`raop`] is the native receiver.
//! - [`binary`] finds a working shairport-sync.
//! - [`config`] names each receiver's port, fifos and config file, and
//!   writes the config.
//! - [`supervisor`] runs one receiver per AirPlay target, of either kind,
//!   and keeps it running.
//! - [`audio`] turns each receiver's PCM (handed over, or read off its
//!   fifo) into a live source.
//! - [`metadata`] keeps what each receiver's sender is playing, parsing
//!   shairport's metadata fifo.
//! - [`route`] plays each sender's session on the receiver's target player
//!   or group, with its volume and what it is playing.

pub mod audio;
pub mod binary;
pub mod config;
pub mod metadata;
pub mod raop;
pub mod route;
pub mod supervisor;
#[cfg(test)]
mod testing;

pub use audio::AirplayAudio;
pub use config::{NameTemplate, ReceiverInfo};
pub use metadata::{AirplayEvent, AirplayMetadata, MetadataEvent, NowPlaying};
pub use raop::NativeConfig;
pub use route::AirplayRouter;
pub use supervisor::{Backend, ReceiverHandler, ReceiverSupervisor, SupervisorConfig};
