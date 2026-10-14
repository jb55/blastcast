//! shairport-sync's metadata fifo, parsed into what the sender is playing.
//!
//! The fifo carries two kinds of writes, mixed:
//!
//! - shairport's items, written by `metadata/pipe.c` in several `write`s
//!   each: `<item><type>hex</type><code>hex</code><length>N</length>`,
//!   then, when `N > 0`, `\n<data encoding="base64">\n`, the base64 and
//!   `</data>`, then `</item>\n`. The type and code are four ASCII bytes
//!   printed with `%x`.
//! - the session hooks' marker lines, [`PLAY_BEGIN`] and [`PLAY_END`].
//!   A hook's `echo` can land between two of an item's writes; a marker
//!   found inside an item is still a marker.
//!
//! [`MetadataParser`] turns the bytes, split wherever the reads fell, into
//! [`MetadataEvent`]s. It mirrors MA's `MetadataReader`
//! (`providers/airplay_receiver/metadata.py`):
//!
//! - `mdst` … `mden` bracket a batch of `core` items: `minm` (title),
//!   `asar` (artist), `asal` (album) and `astm` (duration, ms as a u32
//!   BE). The batch comes out as one [`MetadataEvent::Track`] at `mden`.
//! - `PICT` is cover art. It usually arrives outside the batch, so it comes
//!   out as soon as it is read.
//! - `pvol` is `"airplay_db,…"`: -30..0 dB maps to 0..100, -144 is mute.
//! - `prgr` is `start/current/end` in 44.1 kHz RTP frames.
//! - `paus` and `prsm` are pause and resume.
//!
//! Everything else is ignored. [`AirplayMetadata`] is the
//! [`ReceiverHandler`] that runs a parser per receiver, keeps what each
//! one is playing as a [`NowPlaying`], and broadcasts the events.
//!
//! Like the audio, nothing here reaches the disk: the latest cover art is
//! held in memory until the session ends.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use base64::Engine;
use sha2::{Digest, Sha256};
use tokio::sync::broadcast;

use crate::config::{PLAY_BEGIN, PLAY_END, ReceiverInfo};
use crate::supervisor::ReceiverHandler;

/// AirPlay 1's RTP clock: `prgr` counts these.
const RTP_RATE: u64 = 44_100;

/// The most an incomplete item may buffer. A `PICT` is the largest item,
/// base64 of an image a phone has already scaled; past this the buffer is
/// garbage, not an item, and is dropped.
const MAX_BUFFER: usize = 16 << 20;

/// Events queued for a subscriber that isn't keeping up.
const EVENT_QUEUE: usize = 64;

const ITEM_OPEN: &[u8] = b"<item>";
const ITEM_CLOSE: &[u8] = b"</item>";

/// One thing the fifo said.
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataEvent {
    /// [`PLAY_BEGIN`]: a sender started a session.
    PlayBegin,
    /// [`PLAY_END`]: the session ended.
    PlayEnd,
    /// A track's details, from one `mdst` … `mden` batch.
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

impl TrackInfo {
    fn is_empty(&self) -> bool {
        *self == Self::default()
    }
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
    /// From `pvol`'s first field, the AirPlay volume in dB.
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
    /// From `prgr`'s `start/current/end`, RTP frames that wrap at 2^32.
    fn parse(text: &str) -> Option<Self> {
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

/// Turns a metadata fifo's bytes into [`MetadataEvent`]s.
#[derive(Debug, Default)]
pub struct MetadataParser {
    /// Bytes not yet parsed: a partial line or item.
    buf: Vec<u8>,
    /// The `core` items since `mdst`.
    batch: TrackInfo,
}

impl MetadataParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Parses `bytes` after whatever came before, returning the events
    /// they completed.
    pub fn push(&mut self, bytes: &[u8]) -> Vec<MetadataEvent> {
        self.buf.extend_from_slice(bytes);
        let mut events = Vec::new();
        let mut done = 0;
        loop {
            let rest = &self.buf[done..];
            let open = find(rest, ITEM_OPEN);
            let newline = rest.iter().position(|&b| b == b'\n');
            match (newline, open) {
                // A whole line before any item: a marker, or noise such
                // as the newline after `</item>`.
                (Some(nl), open) if open.is_none_or(|o| nl < o) => {
                    if let Some(marker) = marker(rest[..nl].trim_ascii()) {
                        events.push(marker);
                    }
                    done += nl + 1;
                }
                (_, Some(o)) => {
                    let Some(len) = find(&rest[o..], ITEM_CLOSE) else {
                        // Wait for the rest of the item.
                        break;
                    };
                    let end = o + len + ITEM_CLOSE.len();
                    let item = &rest[o..end];
                    // A hook's echo that landed inside the item.
                    for (text, event) in [
                        (PLAY_BEGIN, MetadataEvent::PlayBegin),
                        (PLAY_END, MetadataEvent::PlayEnd),
                    ] {
                        let line = [text.as_bytes(), b"\n"].concat();
                        for _ in 0..count(item, &line) {
                            events.push(event.clone());
                        }
                    }
                    let item = Item::parse(item);
                    done += end;
                    if let Some(item) = item
                        && let Some(event) = self.item(item)
                    {
                        events.push(event);
                    }
                }
                _ => break,
            }
        }
        self.buf.drain(..done);
        if self.buf.len() > MAX_BUFFER {
            tracing::warn!(
                bytes = self.buf.len(),
                "AirPlay metadata isn't parsing; dropping what is buffered"
            );
            self.buf.clear();
        }
        events
    }

    fn item(&mut self, item: Item) -> Option<MetadataEvent> {
        let Item { kind, code, data } = item;
        let text = || {
            let s = String::from_utf8_lossy(&data);
            s.trim_end_matches('\0').to_owned()
        };
        match (&kind, &code) {
            (b"ssnc", b"mdst") => self.batch = TrackInfo::default(),
            (b"ssnc", b"mden") => {
                let track = std::mem::take(&mut self.batch);
                return (!track.is_empty()).then_some(MetadataEvent::Track(track));
            }
            // shairport sends it as `ssnc`; MA takes it as `core` too.
            (_, b"PICT") if !data.is_empty() => {
                return Some(MetadataEvent::Artwork(Artwork::new(data)));
            }
            (b"core", b"minm") => self.batch.title = Some(text()),
            (b"core", b"asar") => self.batch.artist = Some(text()),
            (b"core", b"asal") => self.batch.album = Some(text()),
            (b"core", b"astm") => {
                if let Some(ms) = data.first_chunk::<4>() {
                    self.batch.duration =
                        Some(Duration::from_millis(u32::from_be_bytes(*ms).into()));
                }
            }
            (b"ssnc", b"pvol") => {
                let db = text().split(',').next()?.trim().parse::<f64>().ok()?;
                return Some(MetadataEvent::Volume(SenderVolume::from_db(db)));
            }
            (b"ssnc", b"prgr") => return Progress::parse(&text()).map(MetadataEvent::Progress),
            (b"ssnc", b"paus") => return Some(MetadataEvent::Paused),
            (b"ssnc", b"prsm") => return Some(MetadataEvent::Resumed),
            _ => {}
        }
        None
    }
}

fn marker(line: &[u8]) -> Option<MetadataEvent> {
    if line == PLAY_BEGIN.as_bytes() {
        Some(MetadataEvent::PlayBegin)
    } else if line == PLAY_END.as_bytes() {
        Some(MetadataEvent::PlayEnd)
    } else {
        None
    }
}

/// One `<item>`, decoded.
struct Item {
    kind: [u8; 4],
    code: [u8; 4],
    data: Vec<u8>,
}

impl Item {
    fn parse(item: &[u8]) -> Option<Self> {
        let hex = |tag| {
            let field = std::str::from_utf8(element(item, tag)?).ok()?;
            u32::from_str_radix(field.trim(), 16)
                .ok()
                .map(u32::to_be_bytes)
        };
        let (kind, code) = (hex("type")?, hex("code")?);
        let length: usize = std::str::from_utf8(element(item, "length")?)
            .ok()?
            .trim()
            .parse()
            .ok()?;
        let data = if length == 0 {
            Vec::new()
        } else {
            let start = find(item, b"<data encoding=\"base64\">")? + 24;
            let len = find(&item[start..], b"</data>")?;
            let b64: Vec<u8> = item[start..start + len]
                .iter()
                .copied()
                .filter(|b| !b.is_ascii_whitespace())
                .collect();
            match base64::engine::general_purpose::STANDARD.decode(b64) {
                Ok(data) => data,
                Err(e) => {
                    tracing::debug!(
                        code = %String::from_utf8_lossy(&code),
                        error = %e,
                        "bad base64 in an AirPlay metadata item"
                    );
                    return None;
                }
            }
        };
        Some(Self { kind, code, data })
    }
}

/// The text of `<tag>…</tag>` in `item`.
fn element<'a>(item: &'a [u8], tag: &str) -> Option<&'a [u8]> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = find(item, open.as_bytes())? + open.len();
    let len = find(&item[start..], close.as_bytes())?;
    Some(&item[start..start + len])
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn count(haystack: &[u8], needle: &[u8]) -> usize {
    haystack
        .windows(needle.len())
        .filter(|w| *w == needle)
        .count()
}

/// What one receiver's sender is playing, as of the last event.
#[derive(Debug, Clone, Default)]
pub struct NowPlaying {
    /// Between [`PLAY_BEGIN`] and [`PLAY_END`].
    pub playing: bool,
    pub paused: bool,
    pub track: Option<TrackInfo>,
    pub artwork: Option<Artwork>,
    /// The sender's last volume. Kept across sessions, since it is the
    /// receiver's, not the track's.
    pub volume: Option<SenderVolume>,
    /// The last `prgr`, and when it was read (or, once paused, when the
    /// pause was).
    pub progress: Option<(Progress, Instant)>,
}

impl NowPlaying {
    /// How far into the track the sender is at `now`: the last `prgr`
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
    receivers: Mutex<HashMap<String, Receiver>>,
    events: broadcast::Sender<AirplayEvent>,
}

#[derive(Default)]
struct Receiver {
    parser: MetadataParser,
    now: NowPlaying,
}

impl AirplayMetadata {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            receivers: Mutex::default(),
            events: broadcast::channel(EVENT_QUEUE).0,
        })
    }

    /// Every receiver's events from now on, in the order each read them.
    pub fn subscribe(&self) -> broadcast::Receiver<AirplayEvent> {
        self.events.subscribe()
    }

    /// What `target_id`'s sender is playing; `None` when its receiver
    /// isn't running.
    pub fn now_playing(&self, target_id: &str) -> Option<NowPlaying> {
        lock(&self.receivers).get(target_id).map(|r| r.now.clone())
    }
}

impl ReceiverHandler for AirplayMetadata {
    fn started(&self, receiver: &ReceiverInfo) {
        lock(&self.receivers).insert(receiver.target_id.clone(), Receiver::default());
    }

    fn metadata(&self, receiver: &ReceiverInfo, bytes: &[u8]) {
        let id = &receiver.target_id;
        let events = {
            let mut receivers = lock(&self.receivers);
            let r = receivers.entry(id.clone()).or_default();
            let events = r.parser.push(bytes);
            let now = Instant::now();
            for event in &events {
                r.now.apply(event, now);
            }
            events
        };
        for event in events {
            match &event {
                MetadataEvent::Artwork(art) => {
                    tracing::debug!(target_id = %id, bytes = art.bytes.len(), hash = %art.hash, "AirPlay artwork");
                }
                event => tracing::debug!(target_id = %id, ?event, "AirPlay metadata"),
            }
            // Nobody listening is fine.
            let _ = self.events.send(AirplayEvent {
                target_id: id.clone(),
                event,
            });
        }
    }

    fn stopped(&self, receiver: &ReceiverInfo) {
        lock(&self.receivers).remove(&receiver.target_id);
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every critical section leaves the value consistent.
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A session from an iPhone, written byte for byte as shairport's
    /// `metadata/pipe.c` writes it: two empty batches, the begin marker,
    /// a starting `pvol`, a batch with unknown `core` items mixed in, the
    /// art, then progress, volume, pause, resume, mute and the end marker.
    const SESSION: &[u8] = include_bytes!("../testdata/session.metadata");

    fn art() -> Vec<u8> {
        let mut art = vec![0xFF, 0xD8, 0xFF, 0xE0];
        art.extend((0..=255u8).take(294));
        art.extend([0xFF, 0xD9]);
        art
    }

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

    /// Writes one item the way `metadata/pipe.c` does.
    fn item(kind: &str, code: &str, data: &[u8]) -> Vec<u8> {
        let hex = |s: &str| u32::from_be_bytes(s.as_bytes().try_into().unwrap());
        let mut out = format!(
            "<item><type>{:x}</type><code>{:x}</code><length>{}</length>",
            hex(kind),
            hex(code),
            data.len()
        )
        .into_bytes();
        if !data.is_empty() {
            out.extend(b"\n<data encoding=\"base64\">\n");
            for group in data.chunks(57) {
                out.extend(
                    base64::engine::general_purpose::STANDARD
                        .encode(group)
                        .bytes(),
                );
            }
            out.extend(b"</data>");
        }
        out.extend(b"</item>\n");
        out
    }

    #[test]
    fn a_session_dump_parses_whole() {
        assert_eq!(MetadataParser::new().push(SESSION), session_events());
    }

    #[test]
    fn a_session_dump_parses_the_same_however_the_reads_split_it() {
        for size in [1, 2, 3, 7, 64, 100, 4096] {
            let mut parser = MetadataParser::new();
            let events: Vec<_> = SESSION.chunks(size).flat_map(|c| parser.push(c)).collect();
            assert_eq!(events, session_events(), "reads of {size} bytes");
            assert!(parser.buf.is_empty());
        }
    }

    #[test]
    fn artwork_comes_out_without_waiting_for_a_batch() {
        let mut parser = MetadataParser::new();
        let events = parser.push(&item("ssnc", "PICT", &art()));
        let [MetadataEvent::Artwork(got)] = &events[..] else {
            panic!("{events:?}");
        };
        assert_eq!(&got.bytes[..], &art()[..]);
        assert_eq!(got.content_type(), Some("image/jpeg"));
        assert_eq!(got.hash.len(), 16);
        assert_ne!(got.hash, super::Artwork::new(vec![1, 2, 3]).hash);
    }

    #[test]
    fn a_marker_inside_an_item_is_still_a_marker() {
        // The hook's echo landed between the header and the data writes.
        let pvol = item("ssnc", "pvol", b"-15.00,0.00,0.00,0.00");
        let split = find(&pvol, b"\n<data").unwrap();
        let mut bytes = pvol[..split].to_vec();
        bytes.extend(b"MA_PLAY_BEGIN\n");
        bytes.extend(&pvol[split..]);
        assert_eq!(
            MetadataParser::new().push(&bytes),
            [
                MetadataEvent::PlayBegin,
                MetadataEvent::Volume(SenderVolume {
                    level: 50,
                    muted: false
                })
            ]
        );
    }

    #[test]
    fn other_lines_and_broken_items_are_skipped() {
        let mut bytes = b"hello\n".to_vec();
        bytes.extend(b"<item><type>zz</type><code>1</code><length>0</length></item>\n");
        bytes.extend(b"<item><type>73736e63</type><code>70766f6c</code><length>4</length>\n<data encoding=\"base64\">\n!!!!</data></item>\n");
        bytes.extend(item("ssnc", "paus", b""));
        assert_eq!(MetadataParser::new().push(&bytes), [MetadataEvent::Paused]);
    }

    #[test]
    fn a_batch_without_track_items_says_nothing() {
        let mut bytes = item("ssnc", "mdst", b"1");
        bytes.extend(item("core", "caps", b"\x01"));
        bytes.extend(item("ssnc", "mden", b"1"));
        assert!(MetadataParser::new().push(&bytes).is_empty());
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
    fn an_unending_item_is_dropped_past_the_limit() {
        let mut parser = MetadataParser::new();
        parser.push(b"<item><type>73736e63</type><code>50494354</code><length>1</length>\n<data encoding=\"base64\">\n");
        let junk = vec![b'A'; MAX_BUFFER];
        parser.push(&junk);
        assert!(parser.buf.is_empty());
        assert_eq!(
            parser.push(&item("ssnc", "paus", b"")),
            [MetadataEvent::Paused]
        );
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
        ReceiverInfo::new(
            std::path::Path::new("/run/tas"),
            "ss-kitchen",
            "Kitchen".into(),
            7000,
            None,
        )
    }

    #[test]
    fn the_handler_tracks_now_playing_and_broadcasts() {
        let meta = AirplayMetadata::new();
        let info = receiver();
        let mut events = meta.subscribe();
        assert!(meta.now_playing("ss-kitchen").is_none());
        meta.started(&info);

        // Everything up to the pause.
        let paus = find(SESSION, &item("ssnc", "paus", b"")).unwrap();
        meta.metadata(&info, &SESSION[..paus]);
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
        meta.metadata(&info, &SESSION[paus..]);
        let now = meta.now_playing("ss-kitchen").unwrap();
        assert!(!now.playing);
        assert!(now.track.is_none() && now.artwork.is_none() && now.progress.is_none());
        assert!(now.volume.unwrap().muted);

        let mut got = Vec::new();
        while let Ok(e) = events.try_recv() {
            assert_eq!(e.target_id, "ss-kitchen");
            got.push(e.event);
        }
        assert_eq!(got, session_events());

        meta.stopped(&info);
        assert!(meta.now_playing("ss-kitchen").is_none());
    }

    #[test]
    fn a_new_run_starts_a_new_parser() {
        let meta = AirplayMetadata::new();
        let info = receiver();
        meta.started(&info);
        // A run that died mid-item.
        meta.metadata(&info, b"<item><type>73736e63</type>");
        meta.started(&info);
        let mut events = meta.subscribe();
        meta.metadata(&info, &item("ssnc", "paus", b""));
        assert_eq!(events.try_recv().unwrap().event, MetadataEvent::Paused);
    }
}
