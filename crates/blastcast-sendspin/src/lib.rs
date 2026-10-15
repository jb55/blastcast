//! The Sendspin server: time sync, audio chunks and group updates to
//! Sendspin speakers. An app hears about the players through a [`Host`].

pub mod artwork;
pub mod connection;
pub mod debug;
pub mod dialer;
pub mod group;
pub mod host;
pub mod player;
pub mod protocol;
pub mod secure;
pub mod server;
pub mod stats;
pub mod status;
pub mod stream;
#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use connection::{Connection, Frame, SendError};
pub use debug::DebugReport;
pub use dialer::{Dialer, DialerConfig};
pub use group::{
    GroupError, GroupInfo, Groups, StreamInfo, TrackMetadata, Transport, TransportControl,
};
pub use host::{Host, NoHost};
pub use player::{CommandError, SendspinPlayer};
pub use server::{Disconnect, HandshakeError, SendspinServer, ServerConfig};
pub use status::{PlayerStatus, PlayerTiming, StatusChange};
pub use stream::AudioStream;
