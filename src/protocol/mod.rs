//! The Sendspin wire format. The messages after the hello are the same for
//! legacy plaintext v1 clients and encrypted ones; the encrypted handshake,
//! hello, activation and pairing messages are in [`crate::secure`].
//!
//! Every text frame is a JSON envelope, `{"type": "client/hello", "payload":
//! {...}}`. [`ClientMessage`] holds what a client sends and [`ServerMessage`]
//! what the server sends; both decode with `decode` and encode with
//! `to_json`. Binary frames (audio and artwork) are in [`binary`].
//!
//! The types are ported from sendspin-go (`pkg/protocol/messages.go`) and
//! checked against aiosendspin 9.x, which is what Music Assistant and current
//! devices run. Roles are versioned (`player@v1`), but the objects inside
//! `client/state`, `server/command` and `stream/start` are keyed by the bare
//! family (`player`). A hello's support object is keyed `player@v1_support`,
//! or `player_support` by older clients.
//!
//! Decoding is lenient where clients differ: unknown fields are ignored and
//! enum values this build doesn't know read as `Unknown`, so one new codec or
//! command in a hello doesn't refuse the whole client.

pub mod binary;
mod client;
mod envelope;
mod server;
mod types;

pub use binary::{BinaryFrame, FrameError, HeaderLayout};
pub use client::{
    ArtworkChannelSupport, ArtworkSupport, ClientCommand, ClientGoodbye, ClientHello,
    ClientMessage, ClientState, ClientTime, ControllerCommand, DeviceInfo, DynamicCodeMethod,
    MetadataSupport, PairMethods, PlayerStateReport, PlayerSupport, RequestFormatArtwork,
    RequestFormatPlayer, StreamRequestFormat, UnpairedAccess, VisualizerSupport,
};
pub use envelope::DecodeError;
pub use server::{
    ArtworkStreamChannel, ControllerState, GroupUpdate, MAX_OUTPUT_DELAY_MS, MetadataState,
    PlayerCommand, Progress, ServerCommand, ServerHello, ServerMessage, ServerState, ServerTime,
    StreamClear, StreamEnd, StreamStart, StreamStartArtwork, StreamStartPlayer,
};
pub use types::{
    Codec, ConnectionReason, GoodbyeReason, MediaCommand, PlaybackState, PlayerCommandKind,
    PlayerSyncState, RepeatMode, Role, StreamFormat, roles,
};

/// The core message format version both sides send in their hello.
pub const PROTOCOL_VERSION: u32 = 1;
