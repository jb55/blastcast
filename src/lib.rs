//! AirPlay input. For now this wraps shairport-sync and reads its PCM and
//! metadata fifos; a native receiver can live beside it later.
//!
//! - [`binary`] finds a working shairport-sync.
//! - [`config`] names each receiver's port, fifos and config file, and
//!   writes the config.
//! - [`supervisor`] runs one shairport-sync per AirPlay target and keeps it
//!   running.
//! - [`audio`] reads each receiver's PCM fifo into a live source.
//! - [`metadata`] parses each receiver's metadata fifo into what its
//!   sender is playing.
//! - [`route`] plays each sender's session on the receiver's target player
//!   or group, with its volume and what it is playing.

pub mod audio;
pub mod binary;
pub mod config;
pub mod metadata;
pub mod route;
pub mod supervisor;
#[cfg(test)]
mod testing;

pub use audio::AirplayAudio;
pub use config::{NameTemplate, ReceiverInfo};
pub use metadata::{AirplayEvent, AirplayMetadata, MetadataEvent, NowPlaying};
pub use route::AirplayRouter;
pub use supervisor::{ReceiverHandler, ReceiverSupervisor, SupervisorConfig};
