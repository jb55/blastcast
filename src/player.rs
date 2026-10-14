//! The registry's handle on a Sendspin player.

use std::sync::Arc;

use async_trait::async_trait;
use tas_core::{CommandError, PlayerControl};

use crate::connection::Connection;
use crate::protocol::{PlayerCommand, PlayerCommandKind, ServerCommand, ServerMessage};

/// What the [`tas_core::PlayerRegistry`] holds for a connected player:
/// commands go out on its connection as `server/command`.
///
/// A command only asks. The player answers with a `client/state` carrying
/// the value it settled on, and that is what the registry shows; nothing
/// here writes the registry.
#[derive(Debug)]
pub struct SendspinPlayer {
    conn: Arc<Connection>,
}

impl SendspinPlayer {
    pub fn new(conn: Arc<Connection>) -> Self {
        Self { conn }
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
}
