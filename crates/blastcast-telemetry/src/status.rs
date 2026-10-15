//! What a protocol has, whole, as state rather than series: the streams
//! going out, the AirPlay receivers and their senders' sessions, and the
//! AirPlay speakers played to.

use serde::{Deserialize, Serialize};

/// One audio stream going out: a source played on one timeline to a set of
/// players.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stream {
    /// New for every stream, even in the same group.
    pub id: String,
    /// The protocol it goes out on: `sendspin`, which also carries AirPlay
    /// speakers.
    pub protocol: String,
    /// The protocol's own group it plays to.
    pub group: String,
    pub name: String,
    /// Where its audio comes from: `airplay`, `tone`, `unknown`.
    pub source: String,
    /// Live audio is sent as it arrives; other audio is read ahead as far
    /// as the players hold.
    pub live: bool,
    /// The source's format, before it is resampled to the session's.
    pub format: Format,
    /// Player ids.
    pub members: Vec<String>,
    /// When it started, in server-clock µs.
    pub started_us: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Format {
    pub sample_rate: u32,
    pub channels: u16,
}

/// A native AirPlay receiver: what a phone sees and plays to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receiver {
    /// Its source id, which the player it plays to names, and the entity its
    /// telemetry is under.
    pub id: String,
    /// The player or group it plays to.
    pub target: String,
    /// The name a phone shows.
    pub name: String,
    pub port: u16,
    /// The [`Session`] playing, the sender that announced last.
    pub session: Option<String>,
    /// Between a sender's first `RECORD` and the receiver going quiet
    /// after the last one left.
    pub playing: bool,
}

/// One sender's stream on a [`Receiver`], from its `ANNOUNCE` until it
/// tears down, is taken over or goes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    /// The [`Receiver::id`] it plays on.
    pub receiver: String,
    /// The sender's address.
    pub sender: String,
    /// What the sender calls itself: its `X-Apple-Client-Name`, or else
    /// its `User-Agent`.
    pub sender_name: Option<String>,
    /// `alac` or `l16` from AirPlay 1, `alac` or `aac` from AirPlay 2.
    pub codec: String,
    pub encrypted: bool,
    /// When it announced, in server-clock µs.
    pub since_us: i64,
}

/// An AirPlay speaker tas plays to, as a player.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Speaker {
    /// Its player id, which its telemetry is under too.
    pub id: String,
    pub name: String,
    /// Its RAOP address, `ip:port`.
    pub address: String,
    pub model: Option<String>,
    /// The RAOP session to it, while there is one.
    pub session: Option<SpeakerSession>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpeakerSession {
    pub state: SpeakerState,
    /// When it entered `state`, in server-clock µs.
    pub since_us: i64,
    /// The speaker's own `Audio-Latency`, in µs, once it said.
    pub audio_latency_us: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpeakerState {
    Connecting,
    Playing,
    /// The stream ended; the session closes unless another starts.
    Lingering,
    #[serde(other)]
    Unknown,
}
