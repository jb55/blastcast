//! AirPlay output: AirPlay 1 (RAOP) speakers as players, grouped with
//! Sendspin speakers or each other on one timeline.
//!
//! - [`device`] reads a speaker from its `_raop._tcp` advert.
//! - [`session`] is a native RAOP sender: RTSP setup, then uncompressed
//!   ALAC ([`alac`]) over RTP, with sync and timing ([`rtp`]) on the
//!   server clock.
//! - [`bridge`] makes each speaker a Sendspin player inside the server, so
//!   the Sendspin groups, volume and timeline carry it like any other.
//! - [`discovery`] browses for speakers and runs a bridge for each.

pub mod alac;
pub mod bridge;
pub mod device;
pub mod discovery;
pub mod rtp;
pub mod rtsp;
pub mod session;
pub mod testing;

pub use device::{RAOP_SERVICE_TYPE, RaopDevice, Unsupported};
pub use discovery::AirplayOutputs;
pub use rtp::NtpClock;
pub use session::{RaopSession, SessionError};
