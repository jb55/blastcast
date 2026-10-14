//! The Sendspin half of `/debug`: every connected client with its sync and
//! stream stats, the clients that left and why, and the groups.

use serde::Serialize;

use crate::protocol::{ConnectionReason, GoodbyeReason, PlaybackState};
use crate::server::SendspinServer;
use crate::stats::{ErrorSnapshot, StatsSnapshot};
use crate::status::PlayerTiming;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DebugReport {
    /// The server clock every `_us` time here and on the wire is read from.
    pub clock_us: i64,
    /// Connected clients, by `client_id`.
    pub clients: Vec<ClientDebug>,
    /// Clients whose last connection ended, newest first.
    pub departed: Vec<DepartedDebug>,
    pub groups: Vec<GroupDebug>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ClientDebug {
    pub client_id: String,
    pub name: String,
    pub roles: Vec<String>,
    pub reason: ConnectionReason,
    pub group_id: Option<String>,
    pub available: bool,
    pub volume: Option<u8>,
    pub muted: Option<bool>,
    pub timing: TimingDebug,
    /// The connection's sync, stream and last error. A client with no
    /// error of its own yet shows its previous connection's, so a
    /// reconnect doesn't hide why it dropped.
    #[serde(flatten)]
    pub stats: StatsSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TimingDebug {
    pub output_delay_ms: u32,
    pub min_buffer_ms: Option<u32>,
    pub required_lead_time_ms: Option<u32>,
    pub buffer_capacity: u64,
}

impl From<PlayerTiming> for TimingDebug {
    fn from(t: PlayerTiming) -> Self {
        Self {
            output_delay_ms: t.output_delay_ms,
            min_buffer_ms: t.min_buffer_ms,
            required_lead_time_ms: t.required_lead_time_ms,
            buffer_capacity: t.buffer_capacity,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DepartedDebug {
    pub client_id: String,
    pub name: String,
    pub s_ago: f64,
    pub goodbye: Option<GoodbyeReason>,
    pub last_error: Option<ErrorSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GroupDebug {
    pub id: String,
    pub name: String,
    pub members: Vec<String>,
    pub playback_state: PlaybackState,
}

impl SendspinServer {
    /// What `/debug` shows of the Sendspin side, as of now.
    pub fn debug_report(&self) -> DebugReport {
        let now = self.config().clock.now_us();
        let departed = self.departed().clone();
        let mut clients: Vec<ClientDebug> = self
            .connections()
            .iter()
            .map(|conn| {
                let status = conn.player_status();
                let mut stats = conn.stats().snapshot(now, conn.audio_queued());
                if stats.last_error.is_none()
                    && let Some(e) = departed
                        .get(conn.client_id())
                        .and_then(|d| d.last_error.as_ref())
                {
                    let mut e = ErrorSnapshot::new(e, now);
                    e.message = format!("previous connection: {}", e.message);
                    stats.last_error = Some(e);
                }
                ClientDebug {
                    client_id: conn.client_id().to_owned(),
                    name: conn.name().to_owned(),
                    roles: conn.active_roles().to_vec(),
                    reason: conn.connection_reason(),
                    group_id: self.groups().group_of(conn.client_id()).map(|g| g.id),
                    available: status.available,
                    volume: status.volume,
                    muted: status.muted,
                    timing: status.timing.into(),
                    stats,
                }
            })
            .collect();
        clients.sort_by(|a, b| a.client_id.cmp(&b.client_id));
        let mut departed: Vec<DepartedDebug> = departed
            .into_iter()
            .filter(|(id, _)| !clients.iter().any(|c| &c.client_id == id))
            .map(|(client_id, d)| DepartedDebug {
                client_id,
                name: d.name,
                s_ago: (now - d.at_us) as f64 / 1e6,
                goodbye: d.goodbye,
                last_error: d.last_error.as_ref().map(|e| ErrorSnapshot::new(e, now)),
            })
            .collect();
        departed.sort_by(|a, b| a.s_ago.total_cmp(&b.s_ago));
        let mut groups: Vec<GroupDebug> = self
            .groups()
            .list()
            .into_iter()
            .map(|g| GroupDebug {
                id: g.id,
                name: g.name,
                members: g.members,
                playback_state: g.playback_state,
            })
            .collect();
        groups.sort_by(|a, b| a.id.cmp(&b.id));
        DebugReport {
            clock_us: now,
            clients,
            departed,
            groups,
        }
    }
}
