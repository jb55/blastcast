//! What the app serving Sendspin hears about its players.
//!
//! A [`SendspinServer`] runs the protocol by itself: handshakes, clock
//! sync, groups and their streams. An app that keeps its own list of
//! speakers (a UI, a player registry, a home automation bridge) gives the
//! server a [`Host`] and is told as players come and go and as they report
//! their state. Every method has a default that does nothing, so an app
//! that only needs the groups implements none of them, or passes
//! [`NoHost`].
//!
//! The calls come from the connection's read task, so a host returns
//! quickly and never waits on the server.
//!
//! [`SendspinServer`]: crate::SendspinServer

use std::fmt;

use crate::player::SendspinPlayer;
use crate::status::PlayerStatus;

pub trait Host: Send + Sync + fmt::Debug {
    /// A client with the player role finished its hello. `player` controls
    /// it until [`Host::player_gone`] names it. A reconnect is a
    /// `player_gone` for the old connection and then a `player_connected`
    /// for the new one.
    ///
    /// It is unavailable until its first `client/state` says otherwise,
    /// which arrives as [`Host::player_status`]. Its group is
    /// [`Groups::group_of`] its client id, and follows [`Groups::watch`].
    ///
    /// [`Groups::group_of`]: crate::Groups::group_of
    /// [`Groups::watch`]: crate::Groups::watch
    fn player_connected(&self, player: SendspinPlayer) {
        let _ = player;
    }

    /// A connected player's `client/state` changed what it reports:
    /// availability, volume, mute, or the commands it takes. The player's
    /// report is the truth; a volume command only asks.
    fn player_status(&self, client_id: &str, status: &PlayerStatus) {
        let _ = (client_id, status);
    }

    /// The player disconnected, or a newer connection replaced it.
    fn player_gone(&self, client_id: &str) {
        let _ = client_id;
    }

    /// The output delay to send a player once its first `client/state`
    /// says which commands it takes, in ms. `None` or 0 leaves the
    /// player's own.
    fn output_delay_ms(&self, client_id: &str) -> Option<u32> {
        let _ = client_id;
        None
    }
}

/// A host that hears nothing.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoHost;

impl Host for NoHost {}
