//! Server → client messages.

use serde::{Deserialize, Serialize};

use super::envelope::{DecodeError, Envelope, tristate};
use super::types::{
    Codec, ConnectionReason, MediaCommand, PlaybackState, PlayerCommandKind, RepeatMode,
    StreamFormat,
};

/// Everything the server sends as text.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", content = "payload")]
pub enum ServerMessage {
    #[serde(rename = "server/hello")]
    Hello(ServerHello),
    #[serde(rename = "server/time")]
    Time(ServerTime),
    #[serde(rename = "server/state")]
    State(ServerState),
    #[serde(rename = "server/command")]
    Command(ServerCommand),
    #[serde(rename = "group/update")]
    GroupUpdate(GroupUpdate),
    #[serde(rename = "stream/start")]
    StreamStart(StreamStart),
    #[serde(rename = "stream/clear")]
    StreamClear(StreamClear),
    #[serde(rename = "stream/end")]
    StreamEnd(StreamEnd),
}

impl ServerMessage {
    /// Decode a server message, as a test client or the dialing side would.
    pub fn decode(text: &str) -> Result<Self, DecodeError> {
        let env = Envelope::parse(text)?;
        Ok(match env.kind.as_str() {
            "server/hello" => Self::Hello(env.payload()?),
            "server/time" => Self::Time(env.payload()?),
            "server/state" => Self::State(env.payload()?),
            "server/command" => Self::Command(env.payload()?),
            "group/update" => Self::GroupUpdate(env.payload()?),
            "stream/start" => Self::StreamStart(env.payload()?),
            "stream/clear" => Self::StreamClear(env.payload()?),
            "stream/end" => Self::StreamEnd(env.payload()?),
            _ => return Err(DecodeError::UnknownType(env.kind)),
        })
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("server messages always serialize")
    }

    /// Sets `server_transmitted`, on the messages that carry it, to `now_us`.
    /// A connection's writer calls this just before it writes the message,
    /// never when it is queued; whatever was there before is overwritten.
    pub fn stamp_transmitted(&mut self, now_us: i64) {
        match self {
            Self::Time(t) => t.server_transmitted = now_us,
            Self::StreamStart(s) => s.server_transmitted = Some(now_us),
            Self::StreamClear(c) => c.server_transmitted = Some(now_us),
            Self::StreamEnd(e) => e.server_transmitted = Some(now_us),
            Self::Hello(_) | Self::State(_) | Self::Command(_) | Self::GroupUpdate(_) => {}
        }
    }
}

/// `server/hello`, the answer to `client/hello`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerHello {
    pub server_id: String,
    pub name: String,
    pub version: u32,
    /// The client's roles this server took, one per family, as the client
    /// wrote them.
    pub active_roles: Vec<String>,
    pub connection_reason: ConnectionReason,
}

/// `server/time`, the answer to `client/time`. All three in microseconds;
/// the server's two are on the server clock. Queue it with
/// `server_transmitted` 0: the writer stamps it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerTime {
    pub client_transmitted: i64,
    pub server_received: i64,
    pub server_transmitted: i64,
}

/// `server/state`: what metadata and controller clients show.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ServerState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetadataState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller: Option<ControllerState>,
}

/// The `metadata` object in `server/state`. Each field after `timestamp` is
/// a diff: `None` is omitted ("unchanged"), `Some(None)` is `null` ("clear
/// it") and `Some(Some(v))` sets it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MetadataState {
    /// Server clock, in microseconds, at which this is true.
    pub timestamp: i64,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "tristate")]
    pub title: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "tristate")]
    pub artist: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "tristate")]
    pub album_artist: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "tristate")]
    pub album: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "tristate")]
    pub artwork_url: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "tristate")]
    pub year: Option<Option<u32>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "tristate")]
    pub track: Option<Option<u32>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "tristate")]
    pub progress: Option<Option<Progress>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "tristate")]
    pub repeat: Option<Option<RepeatMode>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "tristate")]
    pub shuffle: Option<Option<bool>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    /// Position in milliseconds.
    pub track_progress: u64,
    /// Length in milliseconds, 0 when unknown (a live stream).
    pub track_duration: u64,
    /// Speed times 1000: 1000 is normal, 0 is paused.
    pub playback_speed: u32,
}

/// The `controller` object in `server/state`: the group's volume, mute and
/// the commands a controller may send.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControllerState {
    pub supported_commands: Vec<MediaCommand>,
    pub volume: u32,
    pub muted: bool,
    pub repeat: RepeatMode,
    pub shuffle: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seek_max_ms: Option<u64>,
}

/// `server/command`: the server acting on one player.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ServerCommand {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub player: Option<PlayerCommand>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlayerCommand {
    pub command: PlayerCommandKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mute: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_delay_ms: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub static_delay_ms: Option<u32>,
}

/// The largest output delay the spec allows, in milliseconds.
pub const MAX_OUTPUT_DELAY_MS: u32 = 5000;

impl PlayerCommand {
    fn new(command: PlayerCommandKind) -> Self {
        Self {
            command,
            volume: None,
            mute: None,
            output_delay_ms: None,
            static_delay_ms: None,
        }
    }

    /// Set the volume, 0-100; higher values are clamped.
    pub fn volume(volume: u32) -> Self {
        Self {
            volume: Some(volume.min(100)),
            ..Self::new(PlayerCommandKind::Volume)
        }
    }

    pub fn mute(mute: bool) -> Self {
        Self {
            mute: Some(mute),
            ..Self::new(PlayerCommandKind::Mute)
        }
    }

    /// Set the output delay, 0-5000 ms, as the current spec names it.
    pub fn set_output_delay(ms: u32) -> Self {
        Self {
            output_delay_ms: Some(ms.min(MAX_OUTPUT_DELAY_MS)),
            ..Self::new(PlayerCommandKind::SetOutputDelay)
        }
    }

    /// The same, as aiosendspin 9.x names it.
    pub fn set_static_delay(ms: u32) -> Self {
        Self {
            static_delay_ms: Some(ms.min(MAX_OUTPUT_DELAY_MS)),
            ..Self::new(PlayerCommandKind::SetStaticDelay)
        }
    }
}

/// `group/update`: the group a client is in and whether it plays. Omitted
/// fields are unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GroupUpdate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub playback_state: Option<PlaybackState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_name: Option<String>,
}

/// `stream/start`: the format of the binary frames that follow. Re-sent
/// when the format changes. The player and artwork objects are independent.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StreamStart {
    /// Newer servers stamp this; legacy clients ignore it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_transmitted: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub player: Option<StreamStartPlayer>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artwork: Option<StreamStartArtwork>,
}

/// The player's stream format. Fields are in sendspin-go's order, so the
/// encoding matches its byte for byte.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamStartPlayer {
    pub codec: Codec,
    pub sample_rate: u32,
    pub channels: u32,
    pub bit_depth: u32,
    /// Base64 codec header, for codecs that need one (FLAC's STREAMINFO).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec_header: Option<String>,
}

impl StreamStartPlayer {
    pub fn new(format: StreamFormat, codec_header: Option<String>) -> Self {
        Self {
            codec: format.codec,
            sample_rate: format.sample_rate,
            channels: format.channels,
            bit_depth: format.bit_depth,
            codec_header,
        }
    }

    pub fn format(&self) -> StreamFormat {
        StreamFormat {
            codec: self.codec,
            channels: self.channels,
            sample_rate: self.sample_rate,
            bit_depth: self.bit_depth,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StreamStartArtwork {
    pub channels: Vec<ArtworkStreamChannel>,
}

/// One artwork channel as it will be sent: the image's real size, where
/// the hello's `media_width`/`media_height` were what the client accepts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtworkStreamChannel {
    pub source: String,
    pub format: String,
    pub width: u32,
    pub height: u32,
}

/// `stream/clear`: drop buffered data (a seek), keeping the stream. `roles`
/// are bare families (`player`, `visualizer`); `None` means all of them.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StreamClear {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_transmitted: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub roles: Option<Vec<String>>,
}

/// `stream/end`: stop the streams of `roles` (all when `None`).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StreamEnd {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_transmitted: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub roles: Option<Vec<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamps_only_the_messages_that_carry_a_transmit_time() {
        let mut time = ServerMessage::Time(ServerTime {
            client_transmitted: 1,
            server_received: 2,
            server_transmitted: 0,
        });
        time.stamp_transmitted(3);
        assert_eq!(
            time,
            ServerMessage::Time(ServerTime {
                client_transmitted: 1,
                server_received: 2,
                server_transmitted: 3,
            })
        );

        let mut clear = ServerMessage::StreamClear(StreamClear {
            server_transmitted: Some(-1),
            roles: None,
        });
        clear.stamp_transmitted(7);
        assert!(clear.to_json().contains(r#""server_transmitted":7"#));

        let mut start = ServerMessage::StreamStart(StreamStart::default());
        start.stamp_transmitted(8);
        assert!(start.to_json().contains(r#""server_transmitted":8"#));

        let mut state = ServerMessage::State(ServerState::default());
        state.stamp_transmitted(9);
        assert_eq!(state, ServerMessage::State(ServerState::default()));
    }
}
