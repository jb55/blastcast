//! What each receiver's sender is playing.
//!
//! A receiver ([`crate::raop`]) hands over what its sender says as
//! [`MetadataEvent`]s, already parsed. The vocabulary is MA's, which
//! named it after shairport-sync's metadata
//! (`providers/airplay_receiver/metadata.py`):
//!
//! - [`MetadataEvent::PlayBegin`] and [`MetadataEvent::PlayEnd`] bracket
//!   a session.
//! - A track's title, artist, album and duration come as one
//!   [`MetadataEvent::Track`].
//! - Cover art comes as [`MetadataEvent::Artwork`].
//! - The volume is AirPlay's dB: -30..0 dB maps to 0..100, -144 is mute.
//! - Progress is `start/current/end` in 44.1 kHz RTP frames.
//! - Pause and resume.
//!
//! [`AirplayMetadata`] is the [`ReceiverHandler`] that keeps what each
//! receiver is playing as a [`NowPlaying`], and broadcasts the events.
//!
//! Like the audio, nothing here reaches the disk: the latest cover art is
//! held in memory until the session ends.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use tas_core::{Image, ImageSource};
use tokio::sync::broadcast;

use crate::config::ReceiverInfo;
use crate::supervisor::ReceiverHandler;

/// AirPlay 1's RTP clock: progress counts these.
const RTP_RATE: u64 = 44_100;

/// Events queued for a subscriber that isn't keeping up.
const EVENT_QUEUE: usize = 64;

/// One thing a sender said.
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataEvent {
    /// A sender started a session.
    PlayBegin,
    /// The session ended.
    PlayEnd,
    /// A track's details.
    Track(TrackInfo),
    Artwork(Artwork),
    /// The sender's volume slider.
    Volume(SenderVolume),
    Progress(Progress),
    Paused,
    Resumed,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrackInfo {
    pub title: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub duration: Option<Duration>,
}

/// Cover art as the sender sent it, usually a JPEG.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Artwork {
    pub bytes: Arc<[u8]>,
    /// 16 hex digits of the image's SHA-256: changes when the image does,
    /// so it can go in a URL that clients cache.
    pub hash: String,
}

impl Artwork {
    pub fn new(bytes: Vec<u8>) -> Self {
        let digest = Sha256::digest(&bytes);
        let hash = digest[..8].iter().map(|b| format!("{b:02x}")).collect();
        Self {
            bytes: bytes.into(),
            hash,
        }
    }

    /// The image's media type, from its magic bytes.
    pub fn content_type(&self) -> Option<&'static str> {
        match &self.bytes[..] {
            [0xFF, 0xD8, 0xFF, ..] => Some("image/jpeg"),
            [0x89, b'P', b'N', b'G', ..] => Some("image/png"),
            _ => None,
        }
    }

    /// The image to serve: of [`Self::content_type`], or untyped bytes
    /// for the client to sniff.
    pub fn image(&self) -> Image {
        Image {
            bytes: self.bytes.clone(),
            content_type: self.content_type().unwrap_or("application/octet-stream"),
        }
    }
}

/// The sender's volume, as MA maps it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SenderVolume {
    /// 0..=100: -30 dB and below is 0, 0 dB is 100.
    pub level: u8,
    /// The sender muted (-144 dB). `level` is 0 then.
    pub muted: bool,
}

impl SenderVolume {
    /// From the AirPlay volume in dB.
    pub fn from_db(db: f64) -> Self {
        if db <= -144.0 {
            return Self {
                level: 0,
                muted: true,
            };
        }
        // MA truncates.
        let level = ((db + 30.0) / 30.0 * 100.0).clamp(0.0, 100.0) as u8;
        Self {
            level,
            muted: false,
        }
    }
}

/// Where the sender is in the track.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    pub elapsed: Duration,
    /// The track's length; `None` for a stream with no end.
    pub duration: Option<Duration>,
}

impl Progress {
    /// From `start/current/end`, RTP frames that wrap at 2^32.
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let mut parts = text.trim().split('/').map(|p| p.trim().parse::<u32>());
        let (start, current, end) = (
            parts.next()?.ok()?,
            parts.next()?.ok()?,
            parts.next()?.ok()?,
        );
        // A current a little behind start (the track hasn't begun) is 0,
        // not a wrap of 27 hours.
        let elapsed = (current.wrapping_sub(start) as i32).max(0) as u64;
        let length = end.wrapping_sub(start);
        Some(Self {
            elapsed: frames(elapsed),
            duration: (length > 0).then(|| frames(length.into())),
        })
    }
}

fn frames(n: u64) -> Duration {
    Duration::from_nanos(n * 1_000_000_000 / RTP_RATE)
}

/// What one receiver's sender is playing, as of the last event.
#[derive(Debug, Clone, Default)]
pub struct NowPlaying {
    /// Between [`MetadataEvent::PlayBegin`] and [`MetadataEvent::PlayEnd`].
    pub playing: bool,
    pub paused: bool,
    pub track: Option<TrackInfo>,
    pub artwork: Option<Artwork>,
    /// The sender's last volume. Kept across sessions, since it is the
    /// receiver's, not the track's.
    pub volume: Option<SenderVolume>,
    /// The last progress, and when it was heard (or, once paused, when the
    /// pause was).
    pub progress: Option<(Progress, Instant)>,
}

impl NowPlaying {
    /// How far into the track the sender is at `now`: the last progress
    /// moved on by the time since, unless paused.
    pub fn elapsed(&self, now: Instant) -> Option<Duration> {
        let (progress, at) = self.progress?;
        let mut elapsed = progress.elapsed;
        if !self.paused {
            elapsed += now.saturating_duration_since(at);
        }
        Some(progress.duration.map_or(elapsed, |d| elapsed.min(d)))
    }

    fn apply(&mut self, event: &MetadataEvent, now: Instant) {
        match event {
            MetadataEvent::PlayBegin => self.playing = true,
            MetadataEvent::PlayEnd => {
                *self = Self {
                    volume: self.volume,
                    ..Self::default()
                };
            }
            MetadataEvent::Track(track) => self.track = Some(track.clone()),
            MetadataEvent::Artwork(art) => self.artwork = Some(art.clone()),
            MetadataEvent::Volume(volume) => self.volume = Some(*volume),
            MetadataEvent::Progress(progress) => self.progress = Some((*progress, now)),
            MetadataEvent::Paused => {
                // Freeze it where the pause found it.
                if let Some(elapsed) = self.elapsed(now)
                    && let Some((progress, at)) = &mut self.progress
                {
                    progress.elapsed = elapsed;
                    *at = now;
                }
                self.paused = true;
            }
            MetadataEvent::Resumed => {
                if self.paused
                    && let Some((_, at)) = &mut self.progress
                {
                    *at = now;
                }
                self.paused = false;
            }
        }
    }
}

/// An event from one receiver.
#[derive(Debug, Clone, PartialEq)]
pub struct AirplayEvent {
    pub target_id: String,
    pub event: MetadataEvent,
}

/// Every running receiver's metadata, by target.
pub struct AirplayMetadata {
    receivers: Mutex<HashMap<String, NowPlaying>>,
    events: broadcast::Sender<AirplayEvent>,
}

impl AirplayMetadata {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            receivers: Mutex::default(),
            events: broadcast::channel(EVENT_QUEUE).0,
        })
    }

    /// Every receiver's events from now on, in the order each said them.
    pub fn subscribe(&self) -> broadcast::Receiver<AirplayEvent> {
        self.events.subscribe()
    }

    /// What `target_id`'s sender is playing; `None` when its receiver
    /// isn't running.
    pub fn now_playing(&self, target_id: &str) -> Option<NowPlaying> {
        lock(&self.receivers).get(target_id).cloned()
    }

    /// The targets whose receivers are running.
    pub fn targets(&self) -> Vec<String> {
        lock(&self.receivers).keys().cloned().collect()
    }
}

/// Serves each receiver's current cover art by its [`Artwork::hash`], for
/// as long as it is the current one.
impl ImageSource for AirplayMetadata {
    fn image(&self, id: &str) -> Option<Image> {
        lock(&self.receivers)
            .values()
            .filter_map(|now| now.artwork.as_ref())
            .find(|art| art.hash == id)
            .map(Artwork::image)
    }
}

impl ReceiverHandler for AirplayMetadata {
    fn started(&self, receiver: &ReceiverInfo) {
        lock(&self.receivers).insert(receiver.target_id.clone(), NowPlaying::default());
    }

    /// Applies `event` to what the receiver is playing, then broadcasts
    /// it. Only while it runs: an event after it stopped is dropped.
    fn event(&self, receiver: &ReceiverInfo, event: &MetadataEvent) {
        let id = &receiver.target_id;
        match lock(&self.receivers).get_mut(id) {
            Some(playing) => playing.apply(event, Instant::now()),
            None => return,
        }
        match event {
            MetadataEvent::Artwork(art) => {
                tracing::debug!(target_id = %id, bytes = art.bytes.len(), hash = %art.hash, "AirPlay artwork");
            }
            event => tracing::debug!(target_id = %id, ?event, "AirPlay metadata"),
        }
        // Nobody listening is fine.
        let _ = self.events.send(AirplayEvent {
            target_id: id.clone(),
            event: event.clone(),
        });
    }

    /// A receiver that stops mid-session ends it: it never will.
    fn stopped(&self, receiver: &ReceiverInfo) {
        let removed = lock(&self.receivers).remove(&receiver.target_id);
        if removed.is_some_and(|now| now.playing) {
            tracing::debug!(target_id = %receiver.target_id, "AirPlay receiver stopped mid-session");
            let _ = self.events.send(AirplayEvent {
                target_id: receiver.target_id.clone(),
                event: MetadataEvent::PlayEnd,
            });
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every critical section leaves the value consistent.
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn art() -> Vec<u8> {
        let mut art = vec![0xFF, 0xD8, 0xFF, 0xE0];
        art.extend((0..=255u8).take(294));
        art.extend([0xFF, 0xD9]);
        art
    }

    /// A session from an iPhone: the begin, a starting volume, the track,
    /// the art, then progress, volume, pause, resume, mute and the end.
    fn session_events() -> Vec<MetadataEvent> {
        use MetadataEvent::*;
        vec![
            PlayBegin,
            Volume(SenderVolume {
                level: 20,
                muted: false,
            }),
            Track(TrackInfo {
                title: Some("Unfinished Sympathy".into()),
                artist: Some("Massive Attack".into()),
                album: Some("Blue Lines".into()),
                duration: Some(Duration::from_secs(308)),
            }),
            Artwork(super::Artwork::new(art())),
            Progress(super::Progress {
                elapsed: frames(2027),
                duration: Some(Duration::from_secs(308)),
            }),
            Volume(SenderVolume {
                level: 50,
                muted: false,
            }),
            Paused,
            Progress(super::Progress {
                elapsed: frames(441_027),
                duration: Some(Duration::from_secs(308)),
            }),
            Resumed,
            Volume(SenderVolume {
                level: 0,
                muted: true,
            }),
            PlayEnd,
        ]
    }

    #[test]
    fn artwork_is_typed_and_hashed() {
        let got = super::Artwork::new(art());
        assert_eq!(got.content_type(), Some("image/jpeg"));
        assert_eq!(got.hash.len(), 16);
        assert_ne!(got.hash, super::Artwork::new(vec![1, 2, 3]).hash);
        assert_eq!(super::Artwork::new(vec![1, 2, 3]).content_type(), None);
    }

    #[test]
    fn volume_maps_minus_30_to_0_db_onto_0_to_100() {
        for (db, level) in [
            (0.0, 100),
            (-15.0, 50),
            (-29.9, 0),
            (-30.0, 0),
            (-80.0, 0),
            (5.0, 100),
        ] {
            assert_eq!(
                SenderVolume::from_db(db),
                SenderVolume {
                    level,
                    muted: false
                },
                "{db} dB"
            );
        }
        assert!(SenderVolume::from_db(-144.0).muted);
    }

    #[test]
    fn progress_handles_wrapping_and_streams() {
        // Start just below 2^32, current past the wrap.
        let p = Progress::parse("4294963296/40100/88200").unwrap();
        assert_eq!(p.elapsed, Duration::from_secs(1));
        assert_eq!(p.duration, Some(frames(92_200)));
        // The track hasn't begun.
        assert_eq!(
            Progress::parse("1000/900/2000").unwrap().elapsed,
            Duration::ZERO
        );
        // No end.
        assert_eq!(Progress::parse("5/5/5").unwrap().duration, None);
        assert!(Progress::parse("1/2").is_none());
    }

    #[test]
    fn elapsed_moves_on_while_playing_and_freezes_while_paused() {
        let t0 = Instant::now();
        let mut now = NowPlaying::default();
        now.apply(
            &MetadataEvent::Progress(Progress {
                elapsed: Duration::from_secs(10),
                duration: Some(Duration::from_secs(12)),
            }),
            t0,
        );
        let at = |s| t0 + Duration::from_secs(s);
        assert_eq!(now.elapsed(at(1)), Some(Duration::from_secs(11)));
        now.apply(&MetadataEvent::Paused, at(1));
        assert_eq!(now.elapsed(at(5)), Some(Duration::from_secs(11)));
        now.apply(&MetadataEvent::Resumed, at(5));
        assert_eq!(now.elapsed(at(6)), Some(Duration::from_secs(12)));
        // Never past the end.
        assert_eq!(now.elapsed(at(60)), Some(Duration::from_secs(12)));
    }

    fn receiver() -> ReceiverInfo {
        ReceiverInfo::new("ss-kitchen", "Kitchen".into(), 7000)
    }

    #[test]
    fn the_handler_tracks_now_playing_and_broadcasts() {
        let meta = AirplayMetadata::new();
        let info = receiver();
        let mut events = meta.subscribe();
        assert!(meta.now_playing("ss-kitchen").is_none());
        meta.started(&info);

        // Everything up to the pause.
        let session = session_events();
        let paus = session
            .iter()
            .position(|e| *e == MetadataEvent::Paused)
            .unwrap();
        for event in &session[..paus] {
            meta.event(&info, event);
        }
        let now = meta.now_playing("ss-kitchen").unwrap();
        assert!(now.playing && !now.paused);
        assert_eq!(
            now.track.unwrap().title.as_deref(),
            Some("Unfinished Sympathy")
        );
        assert_eq!(&now.artwork.unwrap().bytes[..], &art()[..]);
        assert_eq!(now.volume.unwrap().level, 50);
        assert!(now.progress.is_some());

        // The rest: the session ends, and only the volume outlives it.
        for event in &session[paus..] {
            meta.event(&info, event);
        }
        let now = meta.now_playing("ss-kitchen").unwrap();
        assert!(!now.playing);
        assert!(now.track.is_none() && now.artwork.is_none() && now.progress.is_none());
        assert!(now.volume.unwrap().muted);

        let mut got = Vec::new();
        while let Ok(e) = events.try_recv() {
            assert_eq!(e.target_id, "ss-kitchen");
            got.push(e.event);
        }
        assert_eq!(got, session);

        meta.stopped(&info);
        assert!(meta.now_playing("ss-kitchen").is_none());
    }

    #[test]
    fn an_event_from_a_receiver_not_running_is_dropped() {
        let meta = AirplayMetadata::new();
        let mut events = meta.subscribe();
        meta.event(&receiver(), &MetadataEvent::PlayBegin);
        assert!(events.try_recv().is_err());
        assert!(meta.now_playing("ss-kitchen").is_none());
    }

    #[test]
    fn a_receiver_stopping_mid_session_ends_it() {
        let meta = AirplayMetadata::new();
        let info = receiver();
        let mut events = meta.subscribe();
        meta.started(&info);
        meta.stopped(&info);
        assert!(events.try_recv().is_err(), "an idle one ends nothing");

        meta.started(&info);
        assert_eq!(meta.targets(), ["ss-kitchen"]);
        meta.event(&info, &MetadataEvent::PlayBegin);
        assert_eq!(events.try_recv().unwrap().event, MetadataEvent::PlayBegin);
        meta.stopped(&info);
        let end = events.try_recv().unwrap();
        assert_eq!(end.target_id, "ss-kitchen");
        assert_eq!(end.event, MetadataEvent::PlayEnd);
        assert!(meta.targets().is_empty());
    }

    #[test]
    fn the_current_art_is_served_by_its_hash_until_the_session_ends() {
        let meta = AirplayMetadata::new();
        let info = receiver();
        meta.started(&info);
        let hash = super::Artwork::new(art()).hash;
        assert_eq!(meta.image(&hash), None);

        meta.event(&info, &MetadataEvent::PlayBegin);
        meta.event(&info, &MetadataEvent::Artwork(super::Artwork::new(art())));
        let image = meta.image(&hash).unwrap();
        assert_eq!(&image.bytes[..], &art()[..]);
        assert_eq!(image.content_type, "image/jpeg");
        assert_eq!(meta.image("0000000000000000"), None);

        // A new image replaces it, at a new id.
        let png = b"\x89PNG\r\n\x1a\n....".to_vec();
        let png_hash = super::Artwork::new(png.clone()).hash;
        meta.event(&info, &MetadataEvent::Artwork(super::Artwork::new(png)));
        assert_eq!(meta.image(&hash), None);
        assert_eq!(meta.image(&png_hash).unwrap().content_type, "image/png");

        meta.event(&info, &MetadataEvent::PlayEnd);
        assert_eq!(meta.image(&png_hash), None);
    }
}
