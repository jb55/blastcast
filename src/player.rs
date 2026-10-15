//! A handle on a connected Sendspin player.

use std::sync::Arc;

use blastcast::TimedSource;
use bytes::Bytes;

use crate::connection::{Connection, SendError};
use crate::group::{GroupError, Groups, TrackMetadata};
use crate::protocol::{PlayerCommand, PlayerCommandKind, ServerCommand, ServerMessage};

/// Why a command to a player failed.
#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    /// The player didn't list the command, so it would ignore it.
    #[error("{command} is not supported by this player")]
    Unsupported { command: &'static str },
    #[error(transparent)]
    Group(#[from] GroupError),
    #[error(transparent)]
    Send(#[from] SendError),
}

/// A connected player, as [`Host::player_connected`] hands it over and
/// [`SendspinServer::player`] finds it: volume, mute and the output delay
/// go out on its connection as `server/command`, and stop, a live input
/// (its audio, metadata and artwork) and grouping go to its [`Groups`],
/// for the group it is in.
///
/// A volume or mute command only asks. The player answers with a
/// `client/state` carrying the value it settled on, which reaches
/// [`Host::player_status`].
///
/// [`Host::player_connected`]: crate::Host::player_connected
/// [`Host::player_status`]: crate::Host::player_status
/// [`SendspinServer::player`]: crate::SendspinServer::player
#[derive(Clone)]
pub struct SendspinPlayer {
    conn: Arc<Connection>,
    groups: Groups,
}

impl std::fmt::Debug for SendspinPlayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendspinPlayer")
            .field("client_id", &self.conn.client_id())
            .finish_non_exhaustive()
    }
}

impl SendspinPlayer {
    pub(crate) fn new(conn: Arc<Connection>, groups: Groups) -> Self {
        Self { conn, groups }
    }

    pub fn connection(&self) -> &Arc<Connection> {
        &self.conn
    }

    pub fn client_id(&self) -> &str {
        self.conn.client_id()
    }

    /// Sets the player's output delay, 0-5000 ms, by whichever name the
    /// player listed: `set_output_delay` (the spec) or `set_static_delay`
    /// (aiosendspin 9.x).
    pub fn set_output_delay(&self, ms: u32) -> Result<(), CommandError> {
        let status = self.conn.player_status();
        let command = if status.supports(PlayerCommandKind::SetOutputDelay) {
            PlayerCommand::set_output_delay(ms)
        } else if status.supports(PlayerCommandKind::SetStaticDelay) {
            PlayerCommand::set_static_delay(ms)
        } else {
            return Err(CommandError::Unsupported {
                command: "set_output_delay",
            });
        };
        self.send(command)
    }

    /// Asks the player for a volume, 0-100.
    pub fn set_volume(&self, level: u8) -> Result<(), CommandError> {
        self.send_supported(PlayerCommand::volume(level.into()), "volume_set")
    }

    /// Asks the player to mute or unmute.
    pub fn set_muted(&self, muted: bool) -> Result<(), CommandError> {
        self.send_supported(PlayerCommand::mute(muted), "volume_mute")
    }

    /// Stops the group the player is in. For a member of a bigger group,
    /// that is the whole group.
    pub async fn stop(&self) -> Result<(), CommandError> {
        let group_id = self.group_id()?;
        Ok(self.groups.stop(&group_id).await?)
    }

    /// Plays `source` to the group the player is in: for a member of a
    /// bigger group, the whole group.
    pub async fn play(&self, source: Box<dyn TimedSource>) -> Result<(), CommandError> {
        let group_id = self.group_id()?;
        Ok(self.groups.play(&group_id, source).await?)
    }

    /// For the metadata clients in the player's group.
    pub async fn set_metadata(&self, metadata: Option<TrackMetadata>) -> Result<(), CommandError> {
        let group_id = self.group_id()?;
        Ok(self.groups.set_metadata(&group_id, metadata).await?)
    }

    /// For the artwork clients in the player's group.
    pub async fn set_artwork(&self, image: Option<Bytes>) -> Result<(), CommandError> {
        let group_id = self.group_id()?;
        Ok(self.groups.set_artwork(&group_id, image).await?)
    }

    /// Each of `remove` leaves this player's group for a stopped one of
    /// its own, and each of `add` joins it, leaving its old group. When
    /// this player leaves, the group, its stream and its timeline stay with
    /// the rest, so they play on without a gap. Stops at the first move
    /// that fails; the moves before it stand.
    pub async fn set_members(&self, add: &[String], remove: &[String]) -> Result<(), CommandError> {
        let group_id = self.group_id()?;
        for id in remove {
            // Only out of this group: the leader itself, last, hands the
            // group on, and it plays on for the rest.
            if self.groups.group_of(id).is_some_and(|g| g.id == group_id) {
                self.groups.leave(id).await?;
            }
        }
        for id in add {
            self.groups.join(id, &group_id).await?;
        }
        Ok(())
    }

    /// Sends `command` if the player listed it; the spec has players ignore
    /// any other, so it would only be lost.
    fn send_supported(
        &self,
        command: PlayerCommand,
        name: &'static str,
    ) -> Result<(), CommandError> {
        if !self.conn.player_status().supports(command.command) {
            return Err(CommandError::Unsupported { command: name });
        }
        self.send(command)
    }

    fn group_id(&self) -> Result<String, CommandError> {
        let client_id = self.conn.client_id();
        self.groups
            .group_of(client_id)
            .map(|g| g.id)
            .ok_or_else(|| GroupError::NoClient(client_id.into()).into())
    }

    fn send(&self, command: PlayerCommand) -> Result<(), CommandError> {
        Ok(self.conn.send(ServerMessage::Command(ServerCommand {
            player: Some(command),
        }))?)
    }
}
