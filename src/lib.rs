//! AirPlay input. For now this wraps shairport-sync and reads its PCM and
//! metadata fifos; a native receiver can live beside it later.
//!
//! - [`binary`] finds a working shairport-sync.
//! - [`config`] names each receiver's port, fifos and config file, and
//!   writes the config.
//! - [`supervisor`] runs one shairport-sync per AirPlay target and keeps it
//!   running.

pub mod binary;
pub mod config;
pub mod supervisor;
#[cfg(test)]
mod testing;

pub use config::{NameTemplate, ReceiverInfo};
pub use supervisor::{ReceiverHandler, ReceiverSupervisor, SupervisorConfig};
