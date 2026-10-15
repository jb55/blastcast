//! A native AirPlay receiver: AirPlay 1 (RAOP) and AirPlay 2 senders, such
//! as a phone, play into it as a timed source on the blastcast timeline.
//!
//! [`Receiver::spawn`] starts one receiver, named and on a port
//! ([`ReceiverInfo`]), with a [`NativeConfig`] (the server clock, the mDNS
//! responder it advertises on, its AirPlay 2 [`ap2::Identity`]). It tells a
//! [`ReceiverHandler`] what its senders do, and the handlers here make
//! that usable:
//!
//! - [`AirplayAudio`] turns each receiver's PCM into a [`blastcast`]
//!   `TimedSource`, rate matched to the server clock, for any output to
//!   play.
//! - [`AirplayMetadata`] keeps what each sender is playing
//!   ([`NowPlaying`]: track, cover art, progress, volume) and broadcasts
//!   each [`MetadataEvent`].
//!
//! Give it several in a `Vec<Arc<dyn ReceiverHandler>>`.
//!
//! - [`raop`] is the receiver.
//! - [`ap2`] is what it adds for AirPlay 2 senders: the advert, `/info`,
//!   pairing, PTP timing and each target's identity.
//! - [`config`] names each receiver and its port.
//! - [`receiver`] runs one receiver and keeps it running.
//! - [`audio`] turns each receiver's PCM into a live source.
//! - [`metadata`] keeps what each receiver's sender is playing.

pub mod ap2;
pub mod audio;
pub mod config;
pub mod metadata;
pub mod raop;
pub mod receiver;

pub use audio::AirplayAudio;
pub use config::{NameTemplate, ReceiverInfo};
pub use metadata::{AirplayEvent, AirplayMetadata, Artwork, MetadataEvent, NowPlaying};
pub use raop::NativeConfig;
pub use receiver::{Ended, Receiver, ReceiverConfig, ReceiverHandler};
