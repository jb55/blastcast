//! AirPlay sessions, played on their target player or group: what MA's
//! `airplay_receiver` provider does with a sender's session
//! (`providers/airplay_receiver/provider.py`).
//!
//! [`AirplayRouter`] follows [`AirplayMetadata`]'s events:
//!
//! - **[`MetadataEvent::PlayBegin`]** selects the AirPlay source on the
//!   receiver's target and plays the receiver's audio there: a player plays
//!   it for the group it is in, and a group player forms and plays it on
//!   its leader ([`PlayerRegistry::play_source`]).
//! - **[`MetadataEvent::PlayEnd`]** ends the stream, clears what the source
//!   showed and stops the target.
//! - **The sender's volume** sets the group volume of the player playing
//!   the session, and its mute mutes the group. The first one of each
//!   session is skipped: it is the starting volume the receiver told the
//!   phone, not a choice, and the speakers keep their own level.
//! - **What is playing** (title, artist, album, progress, pause) goes to
//!   the player's `current_media` as a source session
//!   ([`PlayerRegistry::set_source_session`]) and to its protocol's
//!   now-playing (Sendspin's metadata role), and the cover art to the
//!   protocol's artwork. The art's URL on the API's imageproxy goes in
//!   `current_media.image_url`, which the session is re-sent for when
//!   the art changes.
//!
//! Each receiver's source is listed in its target's `source_list` while
//! the receiver runs, passive, with no transport controls: the sender is
//! the only one that can start, pause or skip it.
//!
//! **The source is exclusive.** The player it is selected on claims it,
//! with a session number; selecting it on another player
//! ([`AirplayRouter::select`]) stops the first. The claim is taken when the
//! source is selected, before its stream exists, and
//! [`AirplayRouter::unselect`] only releases the claim it names, so a late
//! release from a session that was replaced can't end the new one.
//!
//! **Events are handled one at a time**, in the order the receivers read
//! them, and each start and stop is finished before the next event: a new
//! session never overtakes the stop of the last, which is what MA's
//! `pending_stop_task` and `pending_start_task` are for. A missed event
//! (the subscription lagging) resyncs every receiver from its
//! [`AirplayMetadata::now_playing`].

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use async_trait::async_trait;
use tas_core::images::image_url;
use tas_core::player::{MediaType, PlayerMedia, PlayerSource, unix_now};
use tas_core::{
    CommandError, PlayerCommand, PlayerRegistry, SourceMetadata, SourceSelect, SourceSession,
};
use tokio::sync::broadcast::error::RecvError;
use tracing::Instrument;

use crate::audio::AirplayAudio;
use crate::config::ReceiverInfo;
use crate::metadata::{AirplayEvent, AirplayMetadata, MetadataEvent, NowPlaying, SenderVolume};
use crate::supervisor::ReceiverHandler;

/// MA's provider instance id, the scheme of every AirPlay source's uri.
pub const SOURCE_PROVIDER: &str = "airplay_receiver";

/// The source id (MA's `AudioSource` uri) of `target_id`'s receiver: its
/// entry in the source list and the `active_source` while it plays.
pub fn source_id(target_id: &str) -> String {
    format!("{SOURCE_PROVIDER}://audio_source/{target_id}")
}

/// The receiver whose source id is `source_id`.
pub fn target_of(source_id: &str) -> Option<&str> {
    source_id
        .strip_prefix(SOURCE_PROVIDER)?
        .strip_prefix("://audio_source/")
        .filter(|t| !t.is_empty())
}

/// The source's entry in a player's source list.
fn player_source(receiver: &ReceiverInfo) -> PlayerSource {
    PlayerSource {
        id: source_id(&receiver.target_id),
        name: format!("AirPlay Receiver ({})", receiver.airplay_name),
        passive: true,
        can_play_pause: false,
        can_seek: false,
        can_next_previous: false,
    }
}

/// Who a receiver's source is playing on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    pub player_id: String,
    /// Changes with every selection, so a release can name the one it
    /// means.
    pub session: u64,
}

/// One receiver's session.
#[derive(Debug, Default)]
struct Target {
    claim: Option<Claim>,
    /// This session's first sender volume has gone by.
    volume_seen: bool,
    /// The sender muted the player, so its next volume unmutes it.
    muted: bool,
}

/// Plays each receiver's sessions on its target. Share it as
/// `Arc<AirplayRouter>`, and give it to the supervisor as a handler, after
/// the [`AirplayAudio`] and [`AirplayMetadata`] it reads.
pub struct AirplayRouter {
    registry: Arc<PlayerRegistry>,
    audio: Arc<AirplayAudio>,
    metadata: Arc<AirplayMetadata>,
    /// The API's base URL, which serves the cover art (as
    /// [`AirplayMetadata`]'s [`tas_core::ImageSource`]). Without it no
    /// `image_url` is set.
    base_url: Option<String>,
    /// Held across a whole start or stop, so they never interleave.
    targets: tokio::sync::Mutex<HashMap<String, Target>>,
    next_session: AtomicU64,
}

impl AirplayRouter {
    pub fn new(
        registry: Arc<PlayerRegistry>,
        audio: Arc<AirplayAudio>,
        metadata: Arc<AirplayMetadata>,
        base_url: Option<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            registry,
            audio,
            metadata,
            base_url,
            targets: tokio::sync::Mutex::default(),
            next_session: AtomicU64::new(1),
        })
    }

    /// Follows the receivers' events until the metadata is gone. It
    /// subscribes before it returns, so no event sent after this call is
    /// missed.
    pub fn run(self: Arc<Self>) -> impl Future<Output = ()> + Send + 'static {
        let mut events = self.metadata.subscribe();
        async move {
            loop {
                match events.recv().await {
                    Ok(event) => {
                        let span = tracing::info_span!("airplay", target_id = %event.target_id);
                        self.handle(event).instrument(span).await
                    }
                    Err(RecvError::Lagged(missed)) => {
                        tracing::warn!(missed, "missed AirPlay events; resyncing");
                        self.resync().await;
                    }
                    Err(RecvError::Closed) => return,
                }
            }
        }
    }

    /// Who `target_id`'s source is playing on, if anyone.
    pub async fn claim(&self, target_id: &str) -> Option<Claim> {
        let targets = self.targets.lock().await;
        targets.get(target_id).and_then(|t| t.claim.clone())
    }

    /// MA's `select_source` of the AirPlay source on `player_id`: plays
    /// `target_id`'s session there instead, stopping the player it was on.
    /// Only while a sender is playing to that receiver. Returns the new
    /// claim's session.
    pub async fn select(&self, target_id: &str, player_id: &str) -> Result<u64, CommandError> {
        if !self
            .metadata
            .now_playing(target_id)
            .is_some_and(|n| n.playing)
        {
            return Err(CommandError::Failed(format!(
                "no AirPlay sender is playing to {target_id}; start playback from the sender first"
            )));
        }
        let mut targets = self.targets.lock().await;
        let t = targets.entry(target_id.to_owned()).or_default();
        if let Some(claim) = &t.claim {
            if claim.player_id == player_id {
                return Ok(claim.session);
            }
            tracing::info!(target_id, from = %claim.player_id, to = player_id, "AirPlay source moves");
            let old = t.claim.take().expect("just matched");
            self.release(target_id, &old.player_id).await;
            self.stop(&old.player_id).await;
        }
        self.start(t, target_id, player_id).await
    }

    /// The player `player_id` let go of `target_id`'s source (it stopped,
    /// or plays something else). Only the claim `session` names is
    /// released; returns whether it was. The sender plays on, to nobody,
    /// until it starts a new session.
    pub async fn unselect(&self, target_id: &str, player_id: &str, session: u64) -> bool {
        let mut targets = self.targets.lock().await;
        let Some(t) = targets.get_mut(target_id) else {
            return false;
        };
        let wanted = Claim {
            player_id: player_id.to_owned(),
            session,
        };
        if t.claim.as_ref() != Some(&wanted) {
            tracing::debug!(
                target_id,
                player_id,
                session,
                "ignoring a stale AirPlay unselect"
            );
            return false;
        }
        t.claim = None;
        self.release(target_id, player_id).await;
        true
    }

    async fn handle(&self, AirplayEvent { target_id, event }: AirplayEvent) {
        let id = target_id.as_str();
        match event {
            MetadataEvent::PlayBegin => self.begin(id, false).await,
            MetadataEvent::PlayEnd => self.end(id).await,
            MetadataEvent::Volume(volume) => self.sender_volume(id, volume).await,
            MetadataEvent::Artwork(art) => {
                if let Some(player) = self.claimed(id).await {
                    self.artwork(&player, Some(art.bytes)).await;
                    self.session(id, &player, Instant::now());
                }
            }
            MetadataEvent::Track(_)
            | MetadataEvent::Progress(_)
            | MetadataEvent::Paused
            | MetadataEvent::Resumed => {
                if let Some(player) = self.claimed(id).await {
                    self.now_playing(id, &player).await;
                }
            }
        }
    }

    /// A sender started a session: play it on the target, unless it is
    /// already playing somewhere. `volume_seen` skips no volume, for a
    /// session whose first one may have been missed.
    async fn begin(&self, target_id: &str, volume_seen: bool) {
        let mut targets = self.targets.lock().await;
        let t = targets.entry(target_id.to_owned()).or_default();
        t.volume_seen = volume_seen;
        t.muted = false;
        if t.claim.is_some() {
            return;
        }
        tracing::info!(target_id, "AirPlay session starting");
        let _ = self.start(t, target_id, target_id).await;
    }

    /// The session ended: end its stream, clear what it showed and stop
    /// the player it played on.
    async fn end(&self, target_id: &str) {
        let mut targets = self.targets.lock().await;
        let Some(t) = targets.get_mut(target_id) else {
            return;
        };
        t.volume_seen = false;
        t.muted = false;
        let Some(claim) = t.claim.take() else {
            return;
        };
        tracing::info!(target_id, player_id = %claim.player_id, "AirPlay session ended");
        self.release(target_id, &claim.player_id).await;
        self.stop(&claim.player_id).await;
    }

    /// Claims the source for `player_id` and plays it there.
    async fn start(
        &self,
        t: &mut Target,
        target_id: &str,
        player_id: &str,
    ) -> Result<u64, CommandError> {
        let session = self.next_session.fetch_add(1, Ordering::Relaxed);
        tracing::info!(target_id, player_id, session, "AirPlay source claimed");
        t.claim = Some(Claim {
            player_id: player_id.to_owned(),
            session,
        });
        let played = async {
            let source = self
                .audio
                .source(target_id)
                .ok_or_else(|| {
                    CommandError::Failed(format!("{target_id}'s AirPlay receiver isn't running"))
                })?
                .map_err(|e| CommandError::Failed(e.to_string()))?;
            self.registry.play_source(player_id, source).await
        }
        .await;
        if let Err(e) = played {
            tracing::warn!(target_id, player_id, error = %e, "can't play the AirPlay session");
            t.claim = None;
            self.release(target_id, player_id).await;
            return Err(e);
        }
        self.now_playing(target_id, player_id).await;
        let art = self.metadata.now_playing(target_id).and_then(|n| n.artwork);
        if let Some(art) = art {
            self.artwork(player_id, Some(art.bytes)).await;
        }
        Ok(session)
    }

    /// Ends the stream and clears what the source showed on `player_id`.
    async fn release(&self, target_id: &str, player_id: &str) {
        self.audio.end_stream(target_id);
        self.registry.set_source_session(player_id, None);
        if let Err(e) = self.registry.set_source_metadata(player_id, None).await {
            log_unshown(player_id, &e);
        }
        self.artwork(player_id, None).await;
    }

    async fn stop(&self, player_id: &str) {
        if let Err(e) = self.registry.execute(player_id, PlayerCommand::Stop).await {
            tracing::warn!(player_id, error = %e, "can't stop the AirPlay session's player");
        }
    }

    /// The sender's volume slider, for the group playing its session.
    async fn sender_volume(&self, target_id: &str, volume: SenderVolume) {
        let mut targets = self.targets.lock().await;
        let Some(t) = targets.get_mut(target_id) else {
            return;
        };
        let Some(claim) = &t.claim else {
            return;
        };
        if !t.volume_seen {
            t.volume_seen = true;
            tracing::debug!(
                target_id,
                ?volume,
                "skipping the session's starting AirPlay volume"
            );
            return;
        }
        let player_id = claim.player_id.clone();
        let result = if volume.muted {
            t.muted = true;
            self.registry.group_volume_mute(&player_id, true).await
        } else {
            let unmuted = if t.muted {
                t.muted = false;
                self.registry.group_volume_mute(&player_id, false).await
            } else {
                Ok(())
            };
            match unmuted {
                Ok(()) => self.registry.group_volume(&player_id, volume.level).await,
                e => e,
            }
        };
        if let Err(e) = result {
            tracing::debug!(target_id, player_id, error = %e, "can't apply the AirPlay volume");
        }
    }

    /// Shows what `target_id`'s sender is playing on `player_id`.
    async fn now_playing(&self, target_id: &str, player_id: &str) {
        let at = Instant::now();
        let now = self.session(target_id, player_id, at);
        if let Err(e) = self
            .registry
            .set_source_metadata(player_id, Some(metadata(&now, at)))
            .await
        {
            log_unshown(player_id, &e);
        }
    }

    /// Sets `player_id`'s `current_media` to what `target_id`'s sender is
    /// playing, as of `at`, and returns that.
    fn session(&self, target_id: &str, player_id: &str, at: Instant) -> NowPlaying {
        let now = self.metadata.now_playing(target_id).unwrap_or_default();
        self.registry.set_source_session(
            player_id,
            Some(SourceSession {
                source_id: source_id(target_id),
                media: media(target_id, &now, at, self.base_url.as_deref()),
            }),
        );
        now
    }

    async fn artwork(&self, player_id: &str, image: Option<Arc<[u8]>>) {
        if let Err(e) = self.registry.set_source_artwork(player_id, image).await {
            log_unshown(player_id, &e);
        }
    }

    /// The player `target_id`'s source is playing on.
    async fn claimed(&self, target_id: &str) -> Option<String> {
        self.claim(target_id).await.map(|c| c.player_id)
    }

    /// Brings every receiver's session in line with what its sender is
    /// doing, after events were missed.
    async fn resync(&self) {
        let mut ids: BTreeSet<String> = self.metadata.targets().into_iter().collect();
        ids.extend(self.targets.lock().await.keys().cloned());
        for id in ids {
            let now = self.metadata.now_playing(&id);
            let playing = now.as_ref().is_some_and(|n| n.playing);
            match self.claimed(&id).await {
                None if playing => self.begin(&id, true).await,
                Some(_) if !playing => self.end(&id).await,
                Some(player) => {
                    self.now_playing(&id, &player).await;
                    let art = now.and_then(|n| n.artwork).map(|a| a.bytes);
                    self.artwork(&player, art).await;
                }
                None => {}
            }
        }
    }
}

/// `players/cmd/select_source` of an AirPlay source, and the release when
/// the player playing it stops.
#[async_trait]
impl SourceSelect for AirplayRouter {
    fn owns(&self, source_id: &str) -> bool {
        target_of(source_id).is_some()
    }

    async fn select(&self, source_id: &str, player_id: &str) -> Result<(), CommandError> {
        let target = target_of(source_id)
            .ok_or_else(|| CommandError::Failed(format!("{source_id} is not an AirPlay source")))?;
        AirplayRouter::select(self, target, player_id)
            .await
            .map(drop)
    }

    async fn release(&self, source_id: &str, player_id: &str) {
        let Some(target) = target_of(source_id) else {
            return;
        };
        if let Some(claim) = self.claim(target).await
            && claim.player_id == player_id
        {
            self.unselect(target, player_id, claim.session).await;
        }
    }
}

impl ReceiverHandler for AirplayRouter {
    /// Lists the receiver's source on its target.
    fn started(&self, receiver: &ReceiverInfo) {
        self.registry
            .bind_source(&receiver.target_id, player_source(receiver));
    }

    /// Takes it off again. A session it was playing ends with the
    /// [`MetadataEvent::PlayEnd`] the metadata sends for it.
    fn stopped(&self, receiver: &ReceiverInfo) {
        self.registry
            .unbind_source(&receiver.target_id, &source_id(&receiver.target_id));
    }
}

/// A protocol that shows no now-playing is fine; anything else is worth a
/// line.
fn log_unshown(player_id: &str, e: &CommandError) {
    if !matches!(e, CommandError::Unsupported { .. }) {
        tracing::debug!(player_id, error = %e, "can't show the AirPlay session");
    }
}

/// The player's `current_media` for `now`, as of `at`, with its art
/// served under `base_url`.
fn media(target_id: &str, now: &NowPlaying, at: Instant, base_url: Option<&str>) -> PlayerMedia {
    let track = now.track.clone().unwrap_or_default();
    let secs = |d: std::time::Duration| u32::try_from(d.as_secs()).unwrap_or(u32::MAX);
    let elapsed = now.elapsed(at);
    PlayerMedia {
        uri: source_id(target_id),
        media_type: MediaType::AudioSource,
        title: track.title,
        artist: track.artist,
        album: track.album,
        duration: track.duration.map(secs),
        source_id: Some(source_id(target_id)),
        elapsed_time: elapsed.map(secs),
        elapsed_time_last_updated: elapsed.map(|_| unix_now()),
        image_url: base_url
            .zip(now.artwork.as_ref())
            .map(|(base, art)| image_url(base, &art.hash, art.image().content_type)),
        ..Default::default()
    }
}

/// What the protocol shows for `now`, as of `at`.
fn metadata(now: &NowPlaying, at: Instant) -> SourceMetadata {
    let track = now.track.clone().unwrap_or_default();
    SourceMetadata {
        title: track.title,
        artist: track.artist,
        album: track.album,
        duration: track.duration,
        elapsed: now.elapsed(at),
        paused: now.paused,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;
    use tas_audio::LiveSource;
    use tas_core::player::PlayerState;
    use tas_core::{ImageSource, PlayerControl, PlayerFeature};
    use tokio::io::AsyncWriteExt;
    use tokio::net::unix::pipe;

    use super::*;
    use crate::config::{PLAY_BEGIN, PLAY_END};
    use crate::testing::{item, mkfifo};

    /// Where a fake player keeps the source it was handed.
    type Slot = Arc<Mutex<Option<Box<dyn LiveSource>>>>;

    /// A player that records what it is asked and keeps the source it was
    /// given. Its stop takes a while, as a real one can.
    struct Fake {
        id: String,
        log: Arc<Mutex<Vec<String>>>,
        source: Slot,
    }

    impl Fake {
        fn record(&self, what: String) {
            self.log.lock().unwrap().push(format!("{} {what}", self.id));
        }
    }

    #[async_trait]
    impl PlayerControl for Fake {
        async fn play_source(&self, source: Box<dyn LiveSource>) -> Result<(), CommandError> {
            self.record("play_source".into());
            *self.source.lock().unwrap() = Some(source);
            Ok(())
        }

        async fn stop(&self) -> Result<(), CommandError> {
            tokio::time::sleep(Duration::from_millis(30)).await;
            self.record("stop".into());
            Ok(())
        }

        async fn volume_set(&self, level: u8) -> Result<(), CommandError> {
            self.record(format!("volume {level}"));
            Ok(())
        }

        async fn volume_mute(&self, muted: bool) -> Result<(), CommandError> {
            self.record(format!("mute {muted}"));
            Ok(())
        }

        async fn set_source_metadata(
            &self,
            metadata: Option<SourceMetadata>,
        ) -> Result<(), CommandError> {
            self.record(match metadata {
                None => "metadata cleared".into(),
                Some(m) => format!("metadata {}", m.title.as_deref().unwrap_or("-")),
            });
            Ok(())
        }

        async fn set_source_artwork(&self, image: Option<Arc<[u8]>>) -> Result<(), CommandError> {
            self.record(match image {
                None => "artwork cleared".into(),
                Some(i) => format!("artwork {}", i.len()),
            });
            Ok(())
        }
    }

    /// A running receiver for "kitchen", a "den" player beside it, and
    /// the router, handed the run as the supervisor hands it.
    struct Rig {
        _dir: tempfile::TempDir,
        info: ReceiverInfo,
        registry: Arc<PlayerRegistry>,
        handlers: Vec<Arc<dyn ReceiverHandler>>,
        metadata: Arc<AirplayMetadata>,
        router: Arc<AirplayRouter>,
        log: Arc<Mutex<Vec<String>>>,
        sources: HashMap<String, Slot>,
    }

    impl Rig {
        /// `run`: whether the router follows the events from the start.
        fn new(run: bool) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let info = ReceiverInfo::new(dir.path(), "kitchen", "Kitchen".into(), 7000, None);
            mkfifo(&info.audio_pipe);
            let registry = Arc::new(PlayerRegistry::default());
            let log = Arc::default();
            let mut sources = HashMap::new();
            for id in ["kitchen", "den"] {
                let mut p = PlayerState::new(id, "fake", id.to_uppercase());
                p.supported_features = [PlayerFeature::VolumeSet, PlayerFeature::VolumeMute].into();
                p.volume_level = Some(40);
                let source: Slot = Arc::default();
                sources.insert(id.to_owned(), source.clone());
                let fake = Fake {
                    id: id.into(),
                    log: Arc::clone(&log),
                    source,
                };
                registry.add(p, Arc::new(fake)).unwrap();
            }
            let audio = AirplayAudio::new();
            let metadata = AirplayMetadata::new();
            let router = AirplayRouter::new(
                registry.clone(),
                audio.clone(),
                metadata.clone(),
                Some(BASE_URL.into()),
            );
            if run {
                tokio::spawn(router.clone().run());
            }
            let handlers: Vec<Arc<dyn ReceiverHandler>> =
                vec![audio, metadata.clone(), router.clone()];
            handlers.started(&info);
            Self {
                _dir: dir,
                info,
                registry,
                handlers,
                metadata,
                router,
                log,
                sources,
            }
        }

        /// What shairport and its hooks write to the metadata fifo.
        fn say(&self, bytes: &[u8]) {
            self.handlers.metadata(&self.info, bytes);
        }

        fn marker(&self, marker: &str) {
            self.say(format!("{marker}\n").as_bytes());
        }

        fn pvol(&self, db: f64) {
            self.say(&item(
                "ssnc",
                "pvol",
                format!("{db:.2},0.00,0.00,0.00").as_bytes(),
            ));
        }

        fn track(&self, title: &str) {
            let mut bytes = item("ssnc", "mdst", b"");
            bytes.extend(item("core", "minm", title.as_bytes()));
            bytes.extend(item("core", "asar", b"Massive Attack"));
            bytes.extend(item("ssnc", "mden", b""));
            self.say(&bytes);
        }

        /// The calls so far, and forgets them.
        fn calls(&self) -> Vec<String> {
            std::mem::take(&mut self.log.lock().unwrap())
        }

        /// Waits for `want` calls, then returns them all.
        async fn wait(&self, want: usize) -> Vec<String> {
            for _ in 0..500 {
                if self.log.lock().unwrap().len() >= want {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            // Room for any that shouldn't come.
            tokio::time::sleep(Duration::from_millis(10)).await;
            self.calls()
        }

        fn get(&self, id: &str) -> PlayerState {
            self.registry.get(id).unwrap()
        }

        fn source(&self, id: &str) -> Option<Box<dyn LiveSource>> {
            self.sources[id].lock().unwrap().take()
        }
    }

    const KITCHEN: &str = "airplay_receiver://audio_source/kitchen";
    const BASE_URL: &str = "http://10.0.0.5:8095";

    #[tokio::test]
    async fn a_session_plays_on_its_target_and_stops_at_the_end() {
        let rig = Rig::new(true);
        let listed = rig.get("kitchen").source_list;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, KITCHEN);
        assert_eq!(listed[0].name, "AirPlay Receiver (Kitchen)");
        assert!(listed[0].passive && !listed[0].can_play_pause && !listed[0].can_seek);
        assert!(rig.get("den").source_list.is_empty());

        rig.marker(PLAY_BEGIN);
        assert_eq!(
            rig.wait(2).await,
            ["kitchen play_source", "kitchen metadata -"]
        );
        let p = rig.get("kitchen");
        assert_eq!(p.active_source.as_deref(), Some(KITCHEN));
        let media = p.current_media.unwrap();
        assert_eq!(media.uri, KITCHEN);
        assert_eq!(media.media_type, MediaType::AudioSource);
        assert_eq!(media.image_url, None);
        let claim = rig.router.claim("kitchen").await.unwrap();
        assert_eq!(claim.player_id, "kitchen");

        // The receiver's audio is what the player was handed, in the
        // session format.
        let mut source = rig.source("kitchen").unwrap();
        let mut fifo = pipe::OpenOptions::new()
            .open_sender(&rig.info.audio_pipe)
            .unwrap();
        fifo.write_all(&[0x00, 0x40, 0x00, 0x40].repeat(1408))
            .await
            .unwrap();
        let chunk = tokio::time::timeout(Duration::from_secs(5), source.next_chunk())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(chunk.format, tas_audio::SESSION_FORMAT);

        // The first volume is the receiver's own starting level: skipped.
        rig.pvol(-24.0);
        rig.track("Teardrop");
        let art = [0xFF, 0xD8, 0xFF, 0xE0, 1, 2, 3, 0xFF, 0xD9];
        rig.say(&item("ssnc", "PICT", &art));
        assert_eq!(
            rig.wait(2).await,
            ["kitchen metadata Teardrop", "kitchen artwork 9"]
        );
        // The art alone re-sends the session, with its URL.
        let media = rig.get("kitchen").current_media.unwrap();
        assert_eq!(media.title.as_deref(), Some("Teardrop"));
        assert_eq!(media.artist.as_deref(), Some("Massive Attack"));
        let hash = crate::metadata::Artwork::new(art.to_vec()).hash;
        assert_eq!(
            media.image_url,
            Some(format!("{BASE_URL}/imageproxy/{hash}?size=0&fmt=jpg"))
        );
        assert!(rig.metadata.image(&hash).is_some());

        // Later ones set the volume; a mute mutes, and the next volume
        // unmutes first.
        rig.pvol(-15.0);
        rig.pvol(-144.0);
        rig.pvol(0.0);
        assert_eq!(
            rig.wait(4).await,
            [
                "kitchen volume 50",
                "kitchen mute true",
                "kitchen mute false",
                "kitchen volume 100"
            ]
        );

        rig.marker(PLAY_END);
        assert_eq!(
            rig.wait(3).await,
            [
                "kitchen metadata cleared",
                "kitchen artwork cleared",
                "kitchen stop"
            ]
        );
        let p = rig.get("kitchen");
        assert_eq!(p.active_source, None);
        assert_eq!(p.current_media, None);
        assert_eq!(rig.router.claim("kitchen").await, None);
        // The stream ended with the session.
        let ended = tokio::time::timeout(Duration::from_secs(5), async {
            while source.next_chunk().await.unwrap().is_some() {}
        })
        .await;
        assert!(ended.is_ok(), "the stream never ended");

        // Volume outside a session goes nowhere.
        rig.pvol(-3.0);
        assert!(rig.wait(1).await.is_empty());
    }

    #[tokio::test]
    async fn a_new_session_waits_for_the_last_to_stop() {
        let rig = Rig::new(true);
        rig.marker(PLAY_BEGIN);
        rig.wait(2).await;
        // The end and the next begin in one read; the stop is slow.
        rig.say(format!("{PLAY_END}\n{PLAY_BEGIN}\n").as_bytes());
        assert_eq!(
            rig.wait(5).await,
            [
                "kitchen metadata cleared",
                "kitchen artwork cleared",
                "kitchen stop",
                "kitchen play_source",
                "kitchen metadata -"
            ]
        );
        // A second begin while it plays changes nothing, but its first
        // volume is skipped again.
        let claim = rig.router.claim("kitchen").await;
        rig.marker(PLAY_BEGIN);
        rig.pvol(-15.0);
        assert!(rig.wait(1).await.is_empty());
        assert_eq!(rig.router.claim("kitchen").await, claim);
    }

    #[tokio::test]
    async fn selecting_moves_the_source_and_a_stale_unselect_is_ignored() {
        let rig = Rig::new(true);
        assert!(
            rig.router.select("kitchen", "den").await.is_err(),
            "no sender yet"
        );
        rig.marker(PLAY_BEGIN);
        rig.wait(2).await;
        let first = rig.router.claim("kitchen").await.unwrap().session;

        let second = rig.router.select("kitchen", "den").await.unwrap();
        assert_ne!(first, second);
        assert_eq!(
            rig.calls(),
            [
                "kitchen metadata cleared",
                "kitchen artwork cleared",
                "kitchen stop",
                "den play_source",
                "den metadata -"
            ]
        );
        assert_eq!(rig.get("kitchen").active_source, None);
        assert_eq!(rig.get("den").active_source.as_deref(), Some(KITCHEN));
        // Selecting it where it already plays keeps the claim.
        assert_eq!(rig.router.select("kitchen", "den").await, Ok(second));
        assert!(rig.calls().is_empty());

        // The volume follows it.
        rig.pvol(-24.0);
        rig.pvol(-15.0);
        assert_eq!(rig.wait(1).await, ["den volume 50"]);

        // A release of the replaced session, or by the wrong player, is
        // stale.
        assert!(!rig.router.unselect("kitchen", "kitchen", first).await);
        assert!(!rig.router.unselect("kitchen", "den", first).await);
        assert_eq!(rig.router.claim("kitchen").await.unwrap().session, second);
        assert!(rig.calls().is_empty());

        // The live one releases it, without a stop: the player let go.
        assert!(rig.router.unselect("kitchen", "den", second).await);
        assert_eq!(rig.calls(), ["den metadata cleared", "den artwork cleared"]);
        assert_eq!(rig.get("den").active_source, None);
        assert_eq!(rig.router.claim("kitchen").await, None);
        // The sender plays on to nobody: its metadata goes nowhere.
        rig.track("Angel");
        assert!(rig.wait(1).await.is_empty());
    }

    #[tokio::test]
    async fn source_select_selects_and_releases_by_source_id() {
        let rig = Rig::new(true);
        let router: &dyn SourceSelect = &*rig.router;
        assert!(router.owns(KITCHEN));
        assert!(!router.owns("airplay_receiver://audio_source/"));
        assert!(!router.owns("spotify://kitchen"));
        rig.marker(PLAY_BEGIN);
        rig.wait(2).await;

        router.select(KITCHEN, "den").await.unwrap();
        rig.calls();
        assert_eq!(rig.router.claim("kitchen").await.unwrap().player_id, "den");
        // A player that isn't playing it can't release it.
        router.release(KITCHEN, "kitchen").await;
        assert!(rig.router.claim("kitchen").await.is_some());
        router.release(KITCHEN, "den").await;
        assert_eq!(rig.router.claim("kitchen").await, None);
        assert_eq!(rig.get("den").active_source, None);
    }

    #[tokio::test]
    async fn an_unplayable_target_leaves_the_source_unclaimed() {
        let rig = Rig::new(true);
        rig.registry
            .update("kitchen", |p| p.available = false)
            .unwrap();
        rig.marker(PLAY_BEGIN);
        // Nothing reaches a player that isn't there.
        assert!(rig.wait(1).await.is_empty());
        assert_eq!(rig.router.claim("kitchen").await, None);
        assert_eq!(rig.registry.source_session("kitchen"), None);
    }

    #[tokio::test]
    async fn a_receiver_stopping_mid_session_ends_it() {
        let rig = Rig::new(true);
        rig.marker(PLAY_BEGIN);
        rig.wait(2).await;
        rig.handlers.stopped(&rig.info);
        assert_eq!(
            rig.wait(3).await,
            [
                "kitchen metadata cleared",
                "kitchen artwork cleared",
                "kitchen stop"
            ]
        );
        assert!(rig.get("kitchen").source_list.is_empty());
        assert_eq!(rig.router.claim("kitchen").await, None);
    }

    #[tokio::test]
    async fn a_resync_starts_a_missed_session_without_skipping_its_volume() {
        let rig = Rig::new(false);
        rig.marker(PLAY_BEGIN);
        rig.track("Teardrop");
        assert!(rig.metadata.now_playing("kitchen").unwrap().playing);
        rig.router.resync().await;
        assert_eq!(
            rig.calls(),
            ["kitchen play_source", "kitchen metadata Teardrop"]
        );
        rig.router
            .sender_volume("kitchen", SenderVolume::from_db(-15.0))
            .await;
        assert_eq!(rig.calls(), ["kitchen volume 50"]);

        // Its end missed too.
        rig.marker(PLAY_END);
        rig.router.resync().await;
        assert_eq!(
            rig.calls(),
            [
                "kitchen metadata cleared",
                "kitchen artwork cleared",
                "kitchen stop"
            ]
        );
    }
}
