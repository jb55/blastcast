//! The controller, metadata and artwork roles on top of the groups.
//!
//! A group holds the state these roles show: the controller object is
//! worked out from its members and its [`Transport`](super::Transport),
//! and the metadata and artwork are whatever its source last set. After
//! every change the actor runs [`Actor::sync_roles`], which compares each
//! member's [`Sent`] record with its group and sends only what differs:
//!
//! - **controller** and **metadata** go in one `server/state`. A metadata
//!   client moved into a group with nothing playing is sent `metadata:
//!   null`, which clears it.
//! - **artwork**: once a client is ready and available it is sent
//!   `stream/start` with its channels as declared (and as
//!   `stream/request-format` changed them), then for each `album` channel
//!   the image scaled to that channel, or an empty frame to clear it.
//!   Images are rendered off the task and cached per size on the image.
//!
//! A controller's `client/command` is checked against the group's
//! `supported_commands`. Volume, mute, `switch` and (without a transport
//! that takes it) `stop` are carried out here; the rest go to the
//! transport.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use super::controller::{self, Offer, SwitchTo};
use super::{Actor, Group, GroupInfo, TrackMetadata};
use crate::artwork::{self, Target};
use crate::connection::Connection;
use crate::protocol::{
    ArtworkChannelSupport, ArtworkStreamChannel, ControllerCommand, ControllerState, MediaCommand,
    MetadataState, PlayerCommand, PlayerCommandKind, RepeatMode, ServerMessage, ServerState,
    StreamStart, StreamStartArtwork, binary,
};

/// An image id no group has: what a channel shows after a `stream/start`,
/// until the server sends it something.
const UNKNOWN: u64 = u64::MAX;

/// A group's metadata as it goes on the wire, numbered.
pub(super) struct Metadata {
    id: u64,
    state: MetadataState,
}

impl Metadata {
    pub(super) fn new(id: u64, now_us: i64, m: TrackMetadata) -> Self {
        Self {
            id,
            state: MetadataState {
                timestamp: now_us,
                title: Some(m.title),
                artist: Some(m.artist),
                album_artist: Some(m.album_artist),
                album: Some(m.album),
                artwork_url: Some(m.artwork_url),
                year: Some(m.year),
                track: Some(m.track),
                progress: Some(m.progress),
                repeat: None,
                shuffle: None,
            },
        }
    }
}

/// A group's artwork, numbered, with its renders by size.
pub(super) struct Artwork {
    id: u64,
    image: Bytes,
    /// `None` for a size that couldn't be made; it isn't tried again.
    renders: HashMap<Target, Option<Bytes>>,
}

impl Artwork {
    pub(super) fn new(id: u64, image: Bytes) -> Self {
        Self {
            id,
            image,
            renders: HashMap::new(),
        }
    }
}

/// What a client was last sent for its roles.
#[derive(Debug, Default)]
pub(super) struct Sent {
    controller: Option<ControllerState>,
    /// The metadata's id; 0 for none.
    metadata: u64,
    /// The channels its artwork stream was started with.
    artwork: Option<Vec<ArtworkStreamChannel>>,
    /// The image id each channel shows; 0 for none.
    images: [u64; binary::ARTWORK_CHANNELS as usize],
}

impl Group {
    /// The members with the player role, and what each last reported.
    fn players(&self) -> impl Iterator<Item = (&Arc<Connection>, crate::PlayerStatus)> {
        self.members
            .iter()
            .filter(|c| c.has_role("player"))
            .map(|c| (c, c.player_status()))
    }

    fn transport_takes(&self, command: MediaCommand) -> bool {
        self.transport
            .as_ref()
            .is_some_and(|t| t.commands.contains(&command))
    }

    /// The `controller` object of `server/state`, by the spec's rules for
    /// reading group volume and mute.
    pub(super) fn controller_state(&self) -> ControllerState {
        let (mut volumes, mut mutes) = (Vec::new(), Vec::new());
        let (mut takes_volume, mut takes_mute) = (false, false);
        for (_, status) in self.players() {
            if status.supports(PlayerCommandKind::Volume) {
                takes_volume = true;
                volumes.extend(status.volume);
            }
            if status.supports(PlayerCommandKind::Mute) {
                takes_mute = true;
                mutes.push(status.muted.unwrap_or(false));
            }
        }
        let transport = self.transport.as_ref();
        let supported_commands = controller::supported_commands(Offer {
            transport: transport.map_or(&[], |t| &t.commands),
            seekable: transport.is_some_and(|t| t.seek_max_ms.is_some()),
            playing: self.stream.is_some(),
            volume: takes_volume,
            mute: takes_mute,
        });
        let seek_max_ms = transport
            .and_then(|t| t.seek_max_ms)
            .filter(|_| supported_commands.contains(&MediaCommand::Seek));
        ControllerState {
            supported_commands,
            volume: controller::group_volume(&volumes).map_or(100, u32::from),
            muted: controller::group_muted(&mutes).unwrap_or(false),
            repeat: transport.map_or(RepeatMode::Off, |t| t.repeat),
            shuffle: transport.is_some_and(|t| t.shuffle),
            seek_max_ms,
        }
    }

    /// Sets the group volume to `requested` the spec's way: one volume
    /// command to each player that takes it, keeping their relative
    /// levels. A player that hasn't reported a volume is set to it as is.
    fn spread_volume(&self, requested: u8) {
        let (mut known, mut unknown) = (Vec::new(), Vec::new());
        for (conn, status) in self.players() {
            if !status.supports(PlayerCommandKind::Volume) {
                continue;
            }
            match status.volume {
                Some(v) => known.push((conn, v)),
                None => unknown.push(conn),
            }
        }
        let current: Vec<u8> = known.iter().map(|&(_, v)| v).collect();
        let spread = controller::redistribute_volume(&current, requested);
        let targets = known
            .into_iter()
            .map(|(c, _)| c)
            .zip(spread)
            .chain(unknown.into_iter().map(|c| (c, requested)));
        for (conn, volume) in targets {
            let msg = ServerMessage::Command(crate::protocol::ServerCommand {
                player: Some(PlayerCommand::volume(volume.into())),
            });
            if let Err(e) = conn.send(msg) {
                tracing::debug!(client_id = conn.client_id(), error = %e, "volume not sent");
            }
        }
    }

    /// Stops the position for metadata clients, where the stream stopped.
    pub(super) fn freeze_progress(&mut self, id: u64, now_us: i64) {
        let Some(meta) = &mut self.metadata else {
            return;
        };
        let Some(Some(p)) = &mut meta.state.progress else {
            return;
        };
        if p.playback_speed == 0 {
            return;
        }
        let elapsed_us = now_us.saturating_sub(meta.state.timestamp).max(0) as u64;
        let mut position = p.track_progress + elapsed_us * u64::from(p.playback_speed) / 1_000_000;
        if p.track_duration != 0 {
            position = position.min(p.track_duration);
        }
        p.track_progress = position;
        p.playback_speed = 0;
        meta.state.timestamp = now_us;
        meta.id = id;
    }
}

/// Whether `conn` is sent artwork now: it has the role, and has said it is
/// ready and available.
fn takes_artwork(conn: &Connection) -> bool {
    conn.has_role("artwork") && conn.is_ready() && conn.is_available()
}

/// The size a channel's image is rendered at, for an album channel in a
/// format this server makes.
fn album_target(channel: &ArtworkChannelSupport) -> Option<Target> {
    (channel.source == "album")
        .then(|| Target::new(&channel.format, channel.media_width, channel.media_height))
        .flatten()
}

impl Actor {
    /// Brings every member's controller, metadata and artwork up to date
    /// with its group.
    pub(super) async fn sync_roles(&mut self) {
        self.render_artwork().await;
        let now = self.clock.now_us();
        for group in self.groups.values() {
            let controller = group.controller_state();
            for conn in &group.members {
                let sent = self.sent.entry(conn.client_id().to_owned()).or_default();
                sync_state(conn, sent, &controller, group.metadata.as_ref());
                sync_artwork(conn, sent, group.artwork.as_ref(), now);
            }
        }
    }

    /// Renders each group's artwork at every size its artwork members need
    /// and it doesn't have yet, off the task.
    async fn render_artwork(&mut self) {
        for group in self.groups.values_mut() {
            let Some(art) = &mut group.artwork else {
                continue;
            };
            let mut wanted: Vec<Target> = Vec::new();
            let channels = group
                .members
                .iter()
                .filter(|c| takes_artwork(c))
                .flat_map(|c| c.artwork_channels());
            for target in channels.filter_map(|c| album_target(&c)) {
                if !art.renders.contains_key(&target) && !wanted.contains(&target) {
                    wanted.push(target);
                }
            }
            if wanted.is_empty() {
                continue;
            }
            let (image, targets) = (art.image.clone(), wanted.clone());
            let rendered = tokio::task::spawn_blocking(move || artwork::render(&image, &targets))
                .await
                .unwrap_or_else(|_| vec![None; wanted.len()]);
            art.renders.extend(wanted.into_iter().zip(rendered));
        }
    }

    /// Carries out a controller's `client/command` on its group.
    pub(super) async fn controller_command(
        &mut self,
        conn: Arc<Connection>,
        command: ControllerCommand,
    ) {
        let client_id = conn.client_id();
        if !conn.has_role("controller") {
            tracing::debug!(client_id, "ignoring a command from a non-controller");
            return;
        }
        let Some(group) = self.current_group(&conn) else {
            return;
        };
        let state = group.controller_state();
        let kind = command.command;
        if !state.supported_commands.contains(&kind) {
            tracing::debug!(
                client_id,
                ?kind,
                "ignoring a command the group doesn't offer"
            );
            return;
        }
        let group_id = group.id.clone();
        tracing::info!(client_id, group_id, ?kind, "controller command");
        match kind {
            MediaCommand::Volume => match command.volume {
                Some(v) => group.spread_volume(v.min(100) as u8),
                None => tracing::debug!(client_id, "volume command without a volume"),
            },
            MediaCommand::Mute => match command.mute {
                Some(mute) => group.command(PlayerCommand::mute(mute)),
                None => tracing::debug!(client_id, "mute command without mute"),
            },
            MediaCommand::Switch => self.switch(&conn).await,
            MediaCommand::Stop if !group.transport_takes(kind) => self.stop(&group_id).await,
            MediaCommand::Seek
                if !command
                    .position_ms
                    .is_some_and(|p| state.seek_max_ms.is_some_and(|max| p <= max)) =>
            {
                tracing::debug!(client_id, ?command.position_ms, "seek out of range");
            }
            MediaCommand::SeekRelative if command.offset_ms.is_none() => {
                tracing::debug!(client_id, "seek_relative without an offset");
            }
            _ => {
                if let Some(transport) = &group.transport {
                    transport.control.command(&group_id, &command);
                }
            }
        }
    }

    /// Moves `conn` to the next group in its switch cycle.
    async fn switch(&mut self, conn: &Arc<Connection>) {
        let client_id = conn.client_id();
        let groups: Vec<GroupInfo> = self.groups.values().map(Group::info).collect();
        let previous = self.previous.get(client_id).map(String::as_str);
        let target =
            controller::switch_target(client_id, conn.has_role("player"), &groups, previous);
        let result = match target {
            Some(SwitchTo::Group(group_id)) => self.join(client_id, &group_id).await,
            Some(SwitchTo::Solo) => self.leave(client_id).await.map(drop),
            None => Ok(()),
        };
        if let Err(e) = result {
            tracing::warn!(client_id, error = %e, "switch failed");
        }
    }
}

/// Sends `conn` the controller and metadata state it hasn't seen.
fn sync_state(
    conn: &Connection,
    sent: &mut Sent,
    controller: &ControllerState,
    metadata: Option<&Metadata>,
) {
    let mut state = ServerState::default();
    if conn.has_role("controller") && sent.controller.as_ref() != Some(controller) {
        state.controller = Some(Some(controller.clone()));
    }
    let metadata_id = metadata.map_or(0, |m| m.id);
    if conn.has_role("metadata") && sent.metadata != metadata_id {
        state.metadata = Some(metadata.map(|m| m.state.clone()));
    }
    if state == ServerState::default() {
        return;
    }
    match conn.send(ServerMessage::State(state.clone())) {
        Ok(()) => {
            if let Some(Some(c)) = state.controller {
                sent.controller = Some(c);
            }
            if state.metadata.is_some() {
                sent.metadata = metadata_id;
            }
        }
        Err(e) => {
            tracing::debug!(client_id = conn.client_id(), error = %e, "server/state not sent")
        }
    }
}

/// Starts `conn`'s artwork stream if its channels changed, and sends each
/// album channel the image it should show.
fn sync_artwork(conn: &Connection, sent: &mut Sent, art: Option<&Artwork>, now_us: i64) {
    if !conn.has_role("artwork") {
        return;
    }
    if !takes_artwork(conn) {
        // No stream/start to a client that isn't available; it gets a new
        // one, and its images again, when it is.
        sent.artwork = None;
        return;
    }
    let channels = conn.artwork_channels();
    if channels.is_empty() {
        return;
    }
    let config: Vec<ArtworkStreamChannel> = channels
        .iter()
        .map(|c| ArtworkStreamChannel {
            source: c.source.clone(),
            format: c.format.clone(),
            width: c.media_width,
            height: c.media_height,
        })
        .collect();
    if sent.artwork.as_ref() != Some(&config) {
        let start = ServerMessage::StreamStart(StreamStart {
            artwork: Some(StreamStartArtwork {
                channels: config.clone(),
            }),
            ..Default::default()
        });
        if let Err(e) = conn.send(start) {
            tracing::debug!(client_id = conn.client_id(), error = %e, "artwork stream/start not sent");
            return;
        }
        sent.artwork = Some(config);
        sent.images = [UNKNOWN; binary::ARTWORK_CHANNELS as usize];
    }
    for (i, channel) in channels.iter().enumerate() {
        if channel.source == "none" {
            sent.images[i] = 0;
            continue;
        }
        let image = album_target(channel)
            .zip(art)
            .and_then(|(target, art)| Some((art.id, art.renders.get(&target)?.clone()?)));
        let id = image.as_ref().map_or(0, |(id, _)| *id);
        if sent.images[i] == id {
            continue;
        }
        let bytes = image.as_ref().map_or(&[][..], |(_, b)| b);
        let frame = binary::artwork(i as u8, now_us, bytes);
        match conn.send_artwork(frame.into()) {
            Ok(()) => sent.images[i] = id,
            Err(e) => tracing::debug!(client_id = conn.client_id(), error = %e, "artwork not sent"),
        }
    }
}
