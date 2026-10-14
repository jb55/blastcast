//! Mirrors the Sendspin groups into the registry the way MA's Sendspin
//! provider does: a group's first player member is its sync leader, whose
//! `group_members` lists every player in it, itself first, and every other
//! player's `synced_to` names it. A player alone in its group has neither.
//! Each player's `playback_state` follows its group's.
//!
//! The groups are the truth, not what `set_members` last asked for: the
//! server moves an unavailable member out by itself, and a leader leaving
//! hands its group to the next member. So the mirror follows
//! [`Groups::watch`], and a command that moves players also syncs as soon
//! as its moves are done, so the grouping layer reads its own change.

use std::sync::{Arc, Mutex, PoisonError, Weak};

use tas_core::PlayerRegistry;
use tas_core::player::PlaybackState as MaPlaybackState;
use tokio::sync::watch;

use crate::group::{GroupInfo, Groups};
use crate::protocol::PlaybackState;
use crate::server::PROVIDER;

pub(crate) struct Membership {
    groups: watch::Receiver<Vec<GroupInfo>>,
    /// Weak: players' controls hold this, and the registry holds them.
    registry: Weak<PlayerRegistry>,
    /// Each sync reads the newest snapshot under it, so an older one is
    /// never written over a newer one.
    lock: Mutex<()>,
}

impl Membership {
    pub(crate) fn new(groups: &Groups, registry: &Arc<PlayerRegistry>) -> Arc<Self> {
        Arc::new(Self {
            groups: groups.watch(),
            registry: Arc::downgrade(registry),
            lock: Mutex::new(()),
        })
    }

    /// Follows the groups until they or the registry are gone.
    pub(crate) async fn run(self: Arc<Self>) {
        let mut changes = self.groups.clone();
        while changes.changed().await.is_ok() {
            if !self.sync() {
                return;
            }
        }
    }

    /// Writes the newest snapshot into every Sendspin player. False once
    /// the registry is gone.
    pub(crate) fn sync(&self) -> bool {
        let Some(registry) = self.registry.upgrade() else {
            return false;
        };
        let _newest_wins = self.lock.lock().unwrap_or_else(PoisonError::into_inner);
        let groups = self.groups.borrow().clone();
        for group in &groups {
            // Controller- and metadata-only clients are members too, but not
            // players.
            let players: Vec<String> = group
                .members
                .iter()
                .filter(|id| registry.get(id).is_some_and(|p| p.provider == PROVIDER))
                .cloned()
                .collect();
            let grouped = players.len() > 1;
            let playback_state = match group.playback_state {
                PlaybackState::Playing => MaPlaybackState::Playing,
                PlaybackState::Paused => MaPlaybackState::Paused,
                _ => MaPlaybackState::Idle,
            };
            for (i, id) in players.iter().enumerate() {
                let _ = registry.update(id, |p| {
                    p.group_members = if grouped && i == 0 {
                        players.clone()
                    } else {
                        Vec::new()
                    };
                    p.synced_to = (grouped && i > 0).then(|| players[0].clone());
                    p.playback_state = playback_state;
                });
            }
        }
        true
    }
}
