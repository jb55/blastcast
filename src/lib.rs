//! AirPlay input: a native AirPlay receiver per target.
//!
//! - [`raop`] is the receiver.
//! - [`ap2`] is what it adds for AirPlay 2 senders: the advert, `/info`
//!   and each target's identity.
//! - [`config`] names each receiver and its port.
//! - [`supervisor`] runs one receiver per AirPlay target and keeps it
//!   running.
//! - [`audio`] turns each receiver's PCM into a live source.
//! - [`metadata`] keeps what each receiver's sender is playing.
//! - [`route`] plays each sender's session on the receiver's target player
//!   or group, with its volume and what it is playing.

pub mod ap2;
pub mod audio;
pub mod config;
pub mod metadata;
pub mod raop;
pub mod route;
pub mod supervisor;

pub use audio::AirplayAudio;
pub use config::{NameTemplate, ReceiverInfo};
pub use metadata::{AirplayEvent, AirplayMetadata, MetadataEvent, NowPlaying};
pub use raop::NativeConfig;
pub use route::AirplayRouter;
pub use supervisor::{ReceiverHandler, ReceiverSupervisor, SupervisorConfig};
