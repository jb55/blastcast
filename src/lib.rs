//! The Sendspin server: time sync, audio chunks and group updates to
//! Sendspin speakers.

pub mod artwork;
pub mod connection;
pub mod debug;
pub mod dialer;
pub mod group;
mod membership;
pub mod player;
pub mod protocol;
pub mod server;
pub mod stats;
pub mod status;
pub mod stream;

pub use connection::{Connection, Frame, SendError};
pub use debug::DebugReport;
pub use dialer::{Dialer, DialerConfig};
pub use group::{GroupError, GroupInfo, Groups, TrackMetadata, Transport, TransportControl};
pub use player::SendspinPlayer;
pub use server::{Disconnect, HandshakeError, SendspinServer, ServerConfig};
pub use status::{PlayerStatus, PlayerTiming, StatusChange};
pub use stream::AudioStream;
