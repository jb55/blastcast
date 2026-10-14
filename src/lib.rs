//! The Sendspin server: time sync, audio chunks and group updates to
//! Sendspin speakers.

pub mod connection;
pub mod player;
pub mod protocol;
pub mod server;

pub use connection::{Connection, Frame, SendError};
pub use player::SendspinPlayer;
pub use server::{Disconnect, HandshakeError, SendspinServer, ServerConfig};
