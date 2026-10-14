//! Client → server messages.

use serde::{Deserialize, Serialize};

use super::envelope::{DecodeError, Envelope};
use super::types::{
    Codec, GoodbyeReason, MediaCommand, PlayerCommandKind, PlayerSyncState, Role, StreamFormat,
};

/// Everything a client sends as text.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", content = "payload")]
pub enum ClientMessage {
    #[serde(rename = "client/hello")]
    Hello(Box<ClientHello>),
    #[serde(rename = "client/time")]
    Time(ClientTime),
    #[serde(rename = "client/state")]
    State(ClientState),
    #[serde(rename = "client/command")]
    Command(ClientCommand),
    #[serde(rename = "client/goodbye")]
    Goodbye(ClientGoodbye),
    #[serde(rename = "stream/request-format")]
    RequestFormat(StreamRequestFormat),
}

impl ClientMessage {
    pub fn decode(text: &str) -> Result<Self, DecodeError> {
        let env = Envelope::parse(text)?;
        Ok(match env.kind.as_str() {
            "client/hello" => Self::Hello(Box::new(env.payload()?)),
            "client/time" => Self::Time(env.payload()?),
            "client/state" => Self::State(env.payload()?),
            "client/command" => Self::Command(env.payload()?),
            "client/goodbye" => Self::Goodbye(env.payload()?),
            "stream/request-format" => Self::RequestFormat(env.payload()?),
            _ => return Err(DecodeError::UnknownType(env.kind)),
        })
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("client messages always serialize")
    }
}

/// `client/hello`: who the client is and which roles it can take.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ClientHello {
    pub client_id: String,
    pub name: String,
    #[serde(default)]
    pub version: u32,
    /// Versioned roles in the client's order of preference.
    #[serde(default)]
    pub supported_roles: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_info: Option<DeviceInfo>,

    #[serde(
        rename = "player@v1_support",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub player_v1_support: Option<PlayerSupport>,
    #[serde(
        rename = "artwork@v1_support",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub artwork_v1_support: Option<ArtworkSupport>,
    #[serde(
        rename = "visualizer@v1_support",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub visualizer_v1_support: Option<VisualizerSupport>,

    /// Unversioned support keys from older clients (Music Assistant's
    /// earlier aiosendspin). Read them through the accessors below.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub player_support: Option<PlayerSupport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_support: Option<MetadataSupport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artwork_support: Option<ArtworkSupport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visualizer_support: Option<VisualizerSupport>,
}

impl ClientHello {
    /// The player support object, versioned key first.
    pub fn player(&self) -> Option<&PlayerSupport> {
        self.player_v1_support
            .as_ref()
            .or(self.player_support.as_ref())
    }

    pub fn artwork(&self) -> Option<&ArtworkSupport> {
        self.artwork_v1_support
            .as_ref()
            .or(self.artwork_support.as_ref())
    }

    pub fn visualizer(&self) -> Option<&VisualizerSupport> {
        self.visualizer_v1_support
            .as_ref()
            .or(self.visualizer_support.as_ref())
    }

    /// The client's most preferred role of `family`, as it wrote it
    /// (`player@v1`, or a bare `player` from a legacy client).
    pub fn role(&self, family: &str) -> Option<&str> {
        self.supported_roles
            .iter()
            .map(String::as_str)
            .find(|r| Role::parse(r).family == family)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DeviceInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub product_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manufacturer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub software_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mac_address: Option<String>,
}

/// What a player can play: `player@v1_support` (or legacy `player_support`).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PlayerSupport {
    /// In the client's order of preference. Formats with a codec this build
    /// doesn't know read as `Codec::Unknown`.
    #[serde(default)]
    pub supported_formats: Vec<StreamFormat>,
    /// Bytes of encoded audio the player can hold before it plays them.
    #[serde(default)]
    pub buffer_capacity: u64,
    #[serde(default)]
    pub supported_commands: Vec<PlayerCommandKind>,

    /// The oldest format: one list per dimension instead of whole formats.
    /// Read through [`PlayerSupport::formats`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub support_codecs: Vec<Codec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub support_channels: Vec<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub support_sample_rates: Vec<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub support_bit_depth: Vec<u32>,
}

impl PlayerSupport {
    /// The formats the player accepts, in preference order. A client that
    /// sent only the per-dimension lists gets their product, codec first.
    pub fn formats(&self) -> Vec<StreamFormat> {
        if !self.supported_formats.is_empty() {
            return self.supported_formats.clone();
        }
        let mut out = Vec::new();
        for &codec in &self.support_codecs {
            for &sample_rate in &self.support_sample_rates {
                for &bit_depth in &self.support_bit_depth {
                    for &channels in &self.support_channels {
                        out.push(StreamFormat {
                            codec,
                            channels,
                            sample_rate,
                            bit_depth,
                        });
                    }
                }
            }
        }
        out
    }

    pub fn supports(&self, command: PlayerCommandKind) -> bool {
        self.supported_commands.contains(&command)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ArtworkSupport {
    #[serde(default)]
    pub channels: Vec<ArtworkChannelSupport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtworkChannelSupport {
    /// `album`, `artist` or `none`.
    pub source: String,
    /// `jpeg`, `png` or `bmp`.
    pub format: String,
    pub media_width: u32,
    pub media_height: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct VisualizerSupport {
    #[serde(default)]
    pub buffer_capacity: u64,
}

/// Legacy `metadata_support`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MetadataSupport {
    #[serde(default)]
    pub support_picture_formats: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_width: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_height: Option<u32>,
}

/// `client/time`: the client's clock when it sent this, in microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientTime {
    pub client_transmitted: i64,
}

/// `client/state`. Clients disagree on where availability goes: the
/// current spec has `available`, aiosendspin's transition clients a
/// top-level `state`, and sendspin-go `player.state`. Read it with
/// [`ClientState::available`].
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ClientState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<PlayerSyncState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub player: Option<PlayerStateReport>,
}

impl ClientState {
    /// Whether the client says it can take part in playback, or `None` if
    /// this update doesn't say. A sync state counts as available unless it
    /// is `external_source`, as aiosendspin reads it.
    pub fn available(&self) -> Option<bool> {
        self.available.or_else(|| {
            self.state
                .or(self.player.as_ref().and_then(|p| p.state))
                .map(|s| s != PlayerSyncState::ExternalSource)
        })
    }
}

/// The `player` object in `client/state`. Every field is optional: an
/// update carries only what changed. Volume and delay are left unclamped
/// for the handler to clamp, so one out-of-range value doesn't drop the
/// update. The delay is `output_delay_ms` in the current spec and
/// `static_delay_ms` in aiosendspin 9.x; read it with
/// [`PlayerStateReport::output_delay_ms`].
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PlayerStateReport {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<PlayerSyncState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub muted: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_delay_ms: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub static_delay_ms: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_lead_time_ms: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_buffer_ms: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supported_commands: Option<Vec<PlayerCommandKind>>,
}

impl PlayerStateReport {
    /// The output delay in milliseconds, under either name.
    pub fn output_delay_ms(&self) -> Option<u32> {
        self.output_delay_ms.or(self.static_delay_ms)
    }
}

/// `client/command`: a controller acting on its group.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ClientCommand {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller: Option<ControllerCommand>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControllerCommand {
    pub command: MediaCommand,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mute: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset_ms: Option<i64>,
}

/// `client/goodbye`, sent before a clean disconnect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ClientGoodbye {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<GoodbyeReason>,
}

/// `stream/request-format`: the client asks for a different stream. Omitted
/// fields keep their current value; the server answers with `stream/start`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StreamRequestFormat {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub player: Option<RequestFormatPlayer>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artwork: Option<RequestFormatArtwork>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RequestFormatPlayer {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<Codec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channels: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bit_depth: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RequestFormatArtwork {
    pub channel: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_width: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_height: Option<u32>,
}
