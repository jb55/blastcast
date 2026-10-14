//! Values shared by several messages: roles, codecs, formats and the
//! enumerations the spec names.

use std::fmt;

use serde::{Deserialize, Serialize};

/// The versioned role names this server knows.
pub mod roles {
    pub const PLAYER_V1: &str = "player@v1";
    pub const CONTROLLER_V1: &str = "controller@v1";
    pub const METADATA_V1: &str = "metadata@v1";
    pub const ARTWORK_V1: &str = "artwork@v1";
    pub const VISUALIZER_V1: &str = "visualizer@v1";
}

/// A role name split into its family and version: `player@v1` is family
/// `player`, version `v1`. A legacy client may send a bare `player`, which
/// has no version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Role<'a> {
    pub family: &'a str,
    pub version: Option<&'a str>,
}

impl<'a> Role<'a> {
    pub fn parse(role: &'a str) -> Self {
        match role.split_once('@') {
            Some((family, version)) if !family.is_empty() => Self {
                family,
                version: Some(version),
            },
            _ => Self {
                family: role,
                version: None,
            },
        }
    }
}

impl fmt::Display for Role<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.version {
            Some(v) => write!(f, "{}@{v}", self.family),
            None => f.write_str(self.family),
        }
    }
}

/// An audio codec on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Codec {
    Pcm,
    Flac,
    Opus,
    #[serde(other)]
    Unknown,
}

/// One audio format: in a hello's `supported_formats`, and (with an optional
/// codec header) in `stream/start`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamFormat {
    pub codec: Codec,
    pub channels: u32,
    pub sample_rate: u32,
    pub bit_depth: u32,
}

/// Why the server holds this connection (`server/hello`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionReason {
    Discovery,
    Playback,
    #[serde(other)]
    Unknown,
}

/// A group's playback state (`group/update`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlaybackState {
    Playing,
    Paused,
    Stopped,
    #[serde(other)]
    Unknown,
}

/// Why a client is leaving (`client/goodbye`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoodbyeReason {
    AnotherServer,
    Shutdown,
    Restart,
    UserRequest,
    #[serde(other)]
    Unknown,
}

/// A player's own report of whether it can play (`client/state`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlayerSyncState {
    Synchronized,
    Error,
    ExternalSource,
    #[serde(other)]
    Unknown,
}

/// A command the server sends a player (`server/command`), or a player lists
/// in its hello's `supported_commands`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlayerCommandKind {
    Volume,
    Mute,
    /// The current spec's name for setting the output delay.
    SetOutputDelay,
    /// aiosendspin 9.x's name for the same command.
    SetStaticDelay,
    #[serde(other)]
    Unknown,
}

/// A command a controller sends for its group (`client/command`), or the
/// server lists in `server/state`'s controller object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaCommand {
    Play,
    Pause,
    Stop,
    Next,
    Previous,
    Volume,
    Mute,
    RepeatOff,
    RepeatOne,
    RepeatAll,
    Shuffle,
    Unshuffle,
    Switch,
    Seek,
    SeekRelative,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepeatMode {
    Off,
    One,
    All,
    #[serde(other)]
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_parse() {
        let r = Role::parse("player@v1");
        assert_eq!((r.family, r.version), ("player", Some("v1")));
        let r = Role::parse("player");
        assert_eq!((r.family, r.version), ("player", None));
        let r = Role::parse("visualizer@_draft_r1");
        assert_eq!((r.family, r.version), ("visualizer", Some("_draft_r1")));
        assert_eq!(Role::parse("@v1").family, "@v1");
        assert_eq!(Role::parse("player@v1").to_string(), "player@v1");
    }

    #[test]
    fn unknown_enum_values_decode() {
        let c: Codec = serde_json::from_str("\"aac\"").unwrap();
        assert_eq!(c, Codec::Unknown);
        let m: MediaCommand = serde_json::from_str("\"teleport\"").unwrap();
        assert_eq!(m, MediaCommand::Unknown);
    }
}
