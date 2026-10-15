//! AirPlay in, Sendspin out, on blastcast alone: a phone plays to every
//! connected Sendspin player at once, in sync.
//!
//! One native AirPlay receiver ([`blastcast_airplay`]) takes the phone,
//! and one Sendspin server ([`blastcast_sendspin`]) the players. The
//! [`Relay`] in between keeps every available player in **one group** and,
//! while a sender's session lasts, plays the receiver's audio there. The
//! group's one stream puts every player on the phone's timeline, so they
//! play each frame at the same instant.
//!
//! - A player that connects, or comes back, joins the group, mid-session
//!   too: the stream catches it up.
//! - A session's start plays the receiver's audio on the group, and its
//!   end stops it. A pause or seek is the receiver's flush, in line with
//!   the audio.
//! - What the phone says is playing (title, artist, album, progress, the
//!   cover art) goes to the group's metadata and artwork clients, and its
//!   volume slider sets the group's volume.
//!
//! The binary (`src/main.rs`) adds what a real network needs: mDNS
//! adverts, AirPlay 2's PTP, dialing players that wait to be dialed, and a
//! [`Store`] for the identities a phone and a paired speaker know it by.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use blastcast_airplay::metadata::{AirplayEvent, MetadataEvent, SenderVolume};
use blastcast_airplay::{AirplayAudio, AirplayMetadata, ReceiverHandler, ReceiverInfo};
use blastcast_sendspin::protocol::Progress;
use blastcast_sendspin::{
    GroupInfo, Host, PlayerStatus, SendspinPlayer, SendspinServer, ServerConfig, TrackMetadata,
};
use bytes::Bytes;
use tokio::sync::Notify;
use tokio::sync::broadcast::error::RecvError;

pub mod store;

pub use store::Store;

/// The one receiver's target id. Its AirPlay device id is made from it
/// and the server id ([`blastcast_airplay::raop::device_id`]).
pub const TARGET: &str = "relay";

/// The receiver the relay plays from, advertised as `name` on `port`.
pub fn receiver_info(name: &str, port: u16) -> ReceiverInfo {
    ReceiverInfo::new(TARGET, name.to_owned(), port)
}

/// Tells the relay when a player comes, goes or changes, such as coming
/// back available, which moves no group.
#[derive(Debug, Default)]
struct Players(Notify);

impl Host for Players {
    fn player_connected(&self, _: SendspinPlayer) {
        self.0.notify_one();
    }
    fn player_status(&self, _: &str, _: &PlayerStatus) {
        self.0.notify_one();
    }
    fn player_gone(&self, _: &str) {
        self.0.notify_one();
    }
}

/// The session as the relay has played it.
#[derive(Debug, Default)]
struct Session {
    /// Between the sender's `PlayBegin` and `PlayEnd`.
    on: bool,
    /// The session's first volume has gone by: the one the receiver told
    /// the phone, which the players don't follow.
    volume_seen: bool,
    /// The group everyone plays in.
    group: Option<String>,
    /// The generation of the stream the relay started there.
    stream: Option<u64>,
}

/// Share it as `Arc<Relay>`.
pub struct Relay {
    server: Arc<SendspinServer>,
    players: Arc<Players>,
    audio: Arc<AirplayAudio>,
    metadata: Arc<AirplayMetadata>,
}

impl Relay {
    /// A relay with a Sendspin server of `config`. Call it inside a Tokio
    /// runtime, as [`SendspinServer::new`].
    pub fn new(config: ServerConfig) -> Arc<Self> {
        let players = Arc::new(Players::default());
        let audio = AirplayAudio::with_telemetry(config.telemetry.clone(), config.clock);
        let server = SendspinServer::new(config, players.clone());
        Arc::new(Self {
            server,
            players,
            audio,
            metadata: AirplayMetadata::new(),
        })
    }

    /// Serve players with it: [`SendspinServer::serve`].
    pub fn server(&self) -> &Arc<SendspinServer> {
        &self.server
    }

    /// What the receiver tells: give it to
    /// [`blastcast_airplay::Receiver::spawn`] for [`receiver_info`].
    pub fn handler(&self) -> Arc<dyn ReceiverHandler> {
        let handlers: Vec<Arc<dyn ReceiverHandler>> =
            vec![self.audio.clone(), self.metadata.clone()];
        Arc::new(handlers)
    }

    /// Follows the sender and the players until the server is gone. Run it
    /// once, in its own task. It subscribes before it returns, so nothing
    /// that happens after this call is missed.
    pub fn run(self: Arc<Self>) -> impl Future<Output = ()> + Send + 'static {
        let mut events = self.metadata.subscribe();
        let mut groups = self.server.groups().watch();
        async move {
            let mut s = Session::default();
            loop {
                tokio::select! {
                    event = events.recv() => match event {
                        Ok(AirplayEvent { target_id, event }) if target_id == TARGET => {
                            self.handle(&mut s, event).await;
                        }
                        Ok(_) => {}
                        // Missed events: take the session as it is now.
                        Err(RecvError::Lagged(_)) => {
                            s.on = self.metadata.now_playing(TARGET).is_some_and(|n| n.playing);
                            s.volume_seen = true;
                        }
                        Err(RecvError::Closed) => return,
                    },
                    changed = groups.changed() => if changed.is_err() {
                        return;
                    },
                    () = self.players.0.notified() => {}
                }
                self.reconcile(&mut s).await;
            }
        }
    }

    async fn handle(&self, s: &mut Session, event: MetadataEvent) {
        match event {
            MetadataEvent::PlayBegin => {
                tracing::info!("AirPlay session starting");
                s.on = true;
                s.volume_seen = false;
            }
            MetadataEvent::PlayEnd => {
                tracing::info!("AirPlay session ended");
                s.on = false;
                if let Some(group) = &s.group {
                    let groups = self.server.groups();
                    let _ = groups.set_metadata(group, None).await;
                    let _ = groups.set_artwork(group, None).await;
                }
            }
            MetadataEvent::Volume(volume) => self.volume(s, volume).await,
            MetadataEvent::Artwork(art) => {
                if let Some(group) = s.group.as_ref().filter(|_| s.stream.is_some()) {
                    let image = Bytes::copy_from_slice(&art.bytes);
                    let _ = self.server.groups().set_artwork(group, Some(image)).await;
                }
            }
            MetadataEvent::Track(_)
            | MetadataEvent::Progress(_)
            | MetadataEvent::Paused
            | MetadataEvent::Resumed => {
                if s.stream.is_some()
                    && let Some(group) = &s.group
                {
                    self.show(group).await;
                }
            }
        }
    }

    /// Every available player in one group, playing the session while
    /// there is one.
    async fn reconcile(&self, s: &mut Session) {
        let groups = self.server.groups();
        let players: Vec<String> = self
            .server
            .connections()
            .into_iter()
            .filter(|c| c.has_role("player") && c.is_available())
            .map(|c| c.client_id().to_owned())
            .collect();
        // The group already playing, while a player is still in it; or the
        // first player's.
        let kept = s
            .group
            .as_deref()
            .and_then(|id| groups.get(id))
            .filter(|g| g.members.iter().any(|m| players.contains(m)));
        let Some(group) = kept.or_else(|| groups.group_of(players.first()?)) else {
            s.group = None;
            s.stream = None;
            return;
        };
        if s.group.as_ref() != Some(&group.id) {
            tracing::info!(group = %group.id, "relaying to group");
            s.group = Some(group.id.clone());
        }
        for player in players.iter().filter(|p| !group.members.contains(p)) {
            match groups.join(player, &group.id).await {
                Ok(()) => tracing::info!(player, group = %group.id, "player joined"),
                Err(e) => tracing::warn!(player, error = %e, "can't add the player"),
            }
        }
        let playing = group.stream.as_ref().map(|st| st.generation);
        let ours = playing.is_some() && playing == s.stream;
        if s.on && !ours {
            self.start(s, &group).await;
        } else if !s.on && ours {
            self.audio.end_stream(TARGET);
            if let Err(e) = groups.stop(&group.id).await {
                tracing::warn!(group = %group.id, error = %e, "can't stop the group");
            }
            s.stream = None;
        }
    }

    /// Plays the receiver's audio on `group`.
    async fn start(&self, s: &mut Session, group: &GroupInfo) {
        let source = match self.audio.source(TARGET) {
            Some(Ok(source)) => source,
            Some(Err(e)) => {
                tracing::warn!(error = %e, "can't read the AirPlay audio");
                return;
            }
            None => {
                tracing::warn!("the AirPlay receiver isn't running");
                return;
            }
        };
        let groups = self.server.groups();
        if let Err(e) = groups.play(&group.id, source).await {
            tracing::warn!(group = %group.id, error = %e, "can't play the AirPlay session");
            return;
        }
        // As it is now, with the players this reconcile added.
        let now = groups.get(&group.id);
        s.stream = now
            .as_ref()
            .and_then(|g| g.stream.as_ref())
            .map(|st| st.generation);
        let members = now.map(|g| g.members).unwrap_or_default();
        tracing::info!(group = %group.id, ?members, "playing the AirPlay session");
        self.show(&group.id).await;
        let art = self.metadata.now_playing(TARGET).and_then(|n| n.artwork);
        if let Some(art) = art {
            let image = Bytes::copy_from_slice(&art.bytes);
            let _ = groups.set_artwork(&group.id, Some(image)).await;
        }
    }

    /// The sender's volume slider, after the session's first.
    async fn volume(&self, s: &mut Session, volume: SenderVolume) {
        if !std::mem::replace(&mut s.volume_seen, true) {
            return;
        }
        let Some(group) = s.group.as_ref().filter(|_| s.on) else {
            return;
        };
        let groups = self.server.groups();
        let set = async {
            groups.set_mute(group, volume.muted).await?;
            if !volume.muted {
                groups.set_volume(group, volume.level.into()).await?;
            }
            Ok::<_, blastcast_sendspin::GroupError>(())
        };
        if let Err(e) = set.await {
            tracing::warn!(group, error = %e, "can't set the group's volume");
        }
    }

    /// What the sender says is playing, to `group`'s metadata clients.
    async fn show(&self, group: &str) {
        let Some(now) = self.metadata.now_playing(TARGET) else {
            return;
        };
        let track = now.track.clone().unwrap_or_default();
        let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
        let duration = track
            .duration
            .or_else(|| now.progress.and_then(|(p, _)| p.duration));
        let progress = now.elapsed(Instant::now()).map(|elapsed| Progress {
            track_progress: ms(elapsed),
            track_duration: duration.map_or(0, ms),
            playback_speed: if now.paused { 0 } else { 1000 },
        });
        let metadata = TrackMetadata {
            title: track.title,
            artist: track.artist,
            album: track.album,
            progress,
            ..Default::default()
        };
        if let Err(e) = self
            .server
            .groups()
            .set_metadata(group, Some(metadata))
            .await
        {
            tracing::debug!(group, error = %e, "can't show what is playing");
        }
    }
}
