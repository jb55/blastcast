//! The registry's handle on a Sendspin player.

use std::sync::Arc;

use async_trait::async_trait;
use tas_core::{CommandError, PlayerControl};

use crate::connection::Connection;
use crate::group::{GroupError, Groups};
use crate::membership::Membership;
use crate::protocol::{PlayerCommand, PlayerCommandKind, ServerCommand, ServerMessage};

/// What the [`tas_core::PlayerRegistry`] holds for a connected player:
/// volume and mute go out on its connection as `server/command`, and
/// grouping and stop go to its [`Groups`].
///
/// A volume or mute command only asks. The player answers with a
/// `client/state` carrying the value it settled on, and that is what the
/// registry shows. Grouping is mirrored from the groups, once its moves
/// are done.
pub struct SendspinPlayer {
    conn: Arc<Connection>,
    groups: Groups,
    membership: Arc<Membership>,
}

impl std::fmt::Debug for SendspinPlayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendspinPlayer")
            .field("client_id", &self.conn.client_id())
            .finish_non_exhaustive()
    }
}

impl SendspinPlayer {
    pub(crate) fn new(conn: Arc<Connection>, groups: Groups, membership: Arc<Membership>) -> Self {
        Self {
            conn,
            groups,
            membership,
        }
    }

    pub fn connection(&self) -> &Arc<Connection> {
        &self.conn
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
            .ok_or_else(|| failed(GroupError::NoClient(client_id.into())))
    }

    async fn move_members(&self, add: &[String], remove: &[String]) -> Result<(), CommandError> {
        let group_id = self.group_id()?;
        for id in remove {
            // Only out of this group: the leader itself, last, hands the
            // group on, and it plays on for the rest.
            if self.groups.group_of(id).is_some_and(|g| g.id == group_id) {
                self.groups.leave(id).await.map_err(failed)?;
            }
        }
        for id in add {
            self.groups.join(id, &group_id).await.map_err(failed)?;
        }
        Ok(())
    }

    fn send(&self, command: PlayerCommand) -> Result<(), CommandError> {
        self.conn
            .send(ServerMessage::Command(ServerCommand {
                player: Some(command),
            }))
            .map_err(|e| CommandError::Failed(e.to_string()))
    }
}

#[async_trait]
impl PlayerControl for SendspinPlayer {
    async fn volume_set(&self, level: u8) -> Result<(), CommandError> {
        self.send_supported(PlayerCommand::volume(level.into()), "volume_set")
    }

    async fn volume_mute(&self, muted: bool) -> Result<(), CommandError> {
        self.send_supported(PlayerCommand::mute(muted), "volume_mute")
    }

    /// Stops the group the player is in. For a follower that is its
    /// leader's group, as MA sends a member's stop to its leader.
    async fn stop(&self) -> Result<(), CommandError> {
        let group_id = self.group_id()?;
        self.groups.stop(&group_id).await.map_err(failed)?;
        self.membership.sync();
        Ok(())
    }

    /// Each removed member leaves this player's group for a stopped one of
    /// its own, and each added one joins it, leaving its old group. When
    /// this player leaves, the group, its stream and its timeline stay with
    /// the rest, so they play on without a gap.
    async fn set_members(&self, add: &[String], remove: &[String]) -> Result<(), CommandError> {
        let moved = self.move_members(add, remove).await;
        // Whatever got moved before a failure is mirrored too.
        self.membership.sync();
        moved
    }
}

fn failed(e: GroupError) -> CommandError {
    CommandError::Failed(e.to_string())
}
