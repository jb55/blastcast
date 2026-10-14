//! The registry's handle on a Sendspin player.

use std::sync::Arc;

use async_trait::async_trait;
use tas_core::PlayerControl;

use crate::connection::Connection;

/// What the [`tas_core::PlayerRegistry`] holds for a connected player:
/// commands go out on its connection.
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
}

#[async_trait]
impl PlayerControl for SendspinPlayer {}
