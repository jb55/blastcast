//! The Sendspin server: time sync, audio chunks and group updates to
//! Sendspin speakers.

pub mod connection;
pub mod dialer;
pub mod player;
pub mod protocol;
pub mod server;
pub mod status;
pub mod stream;

pub use connection::{Connection, Frame, SendError};
pub use dialer::{Dialer, DialerConfig};
pub use player::SendspinPlayer;
pub use server::{Disconnect, HandshakeError, SendspinServer, ServerConfig};
pub use status::{PlayerStatus, PlayerTiming, StatusChange};
pub use stream::AudioStream;
