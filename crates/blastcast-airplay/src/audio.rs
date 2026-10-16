//! Each receiver's PCM, as it hands it over ([`ReceiverHandler::audio`]),
//! handed to whoever plays it as a [`TimedSource`].
//!
//! **The audio keeps its play time**: each chunk is stamped with when the
//! sender means it to play, on the server clock, so the stream places it
//! on the sender's timeline rather than when it got here. A `FLUSH`
//! ([`ReceiverHandler::flush`]) is an [`Item::Flush`] in line with the
//! audio, so whatever the stream holds of what came before is dropped at
//! once rather than played out.
//!
//! **Less the lead nothing plays to.** A phone stamps its audio about two
//! seconds ahead, a buffer for a receiver that waits to play it, and plays
//! none of it itself. So the stream may trim it ([`TimedSource::trim_lead`]):
//! each timeline (a start, or after a flush) starts the players'
//! send-ahead (plus [`KEEP_LEAD`]) after its first chunk arrives, and the
//! rest keep the stamps' spacing. A jump in the stamps, such as the clock
//! map settling as a session starts, trims again rather than adding the
//! jump to the lead. Play and pause on the phone take about a quarter
//! second to be heard, not two seconds. That is AirPlay 1's: an AirPlay 2
//! phone's stamps are when it plays its own picture and its other
//! speakers, so its audio ([`ReceiverHandler::audio_on_time`]) keeps them
//! ([`TimedSource::on_time`]): what comes too late for the players is cut,
//! never played late.
//!
//! **The sender's clock is matched to ours** by blastcast's [`RateMatch`]:
//! a phone's stamps follow its own clock, tens of ppm off ours, and the
//! stage drops or repeats a frame now and then to keep the audio on them.
//! What it does goes to the telemetry hub under the receiver's source id:
//! each chunk's stamp less where it plays ([`Series::RateError`]), and the
//! frames dropped and repeated so far ([`Series::FramesDropped`],
//! [`Series::FramesRepeated`]).
//!
//! [`AirplayAudio`] is a [`ReceiverHandler`]: it keeps a slot per running
//! receiver, and what is handed over while nobody listens is dropped.
//!
//! [`AirplayAudio::source`] hands out one stream per receiver at a time: a
//! new one ends the last. It yields audio handed over after it was taken,
//! not a backlog, so a live session starts at "now" with no extra lead.
//! [`AirplayAudio::end_stream`] ends it, which is what the session's end
//! calls for: it closes the stream's channel, and the session's pending
//! `next` returns `None` at once.
//!
//! Only audio passes through: nothing is kept past the chunk in flight.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use blastcast::{AudioError, AudioFormat, Item, PcmChunk, ServerClock, TimedSource};
use blastcast::{RateMatch, RateStats};
use blastcast_audio::timed_to_session_format;
use blastcast_telemetry::{Probe, Series, Telemetry};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use crate::config::ReceiverInfo;
use crate::receiver::ReceiverHandler;

/// What a receiver hands over: 44.1 kHz, stereo.
pub const AIRPLAY_FORMAT: AudioFormat = AudioFormat::new(44_100, 2);

/// What a stream keeps of the sender's lead past the players' send-ahead:
/// none, since the send-ahead is already the players' buffer. The rest of
/// a phone's two seconds is trimmed: nothing plays to it. A chunk handed
/// over later than the first by more than the stream's tolerance (Wi-Fi,
/// or a resend waited for, [`crate::raop::playout::RESEND_WINDOW`]) adds that much latency until
/// the next pause; keeping more would trade latency for fewer of those.
pub(crate) const KEEP_LEAD: Duration = Duration::ZERO;

/// Items queued for a stream that isn't keeping up: about 1 s of AirPlay
/// 1's 352-frame packets. Past it, chunks are dropped rather than letting
/// the receiver wait.
const STREAM_QUEUE: usize = 128;

/// The audio of every running receiver, by target.
#[derive(Default)]
pub struct AirplayAudio {
    receivers: Mutex<HashMap<String, Arc<Slot>>>,
    /// Where each stream's rate matching goes, timed on `clock`.
    telemetry: Telemetry,
    clock: ServerClock,
}

/// Where a receiver's audio goes: the stream handed out, if any.
#[derive(Default)]
struct Slot {
    stream: Mutex<Option<Stream>>,
    /// Chunks are being dropped, and that has been logged.
    overflowing: AtomicBool,
    /// The latest audio came on time, from AirPlay 2: its stream keeps its
    /// stamps rather than trimming their lead.
    on_time: Arc<AtomicBool>,
}

/// The stream handed out.
struct Stream {
    tx: mpsc::Sender<Item>,
    /// A flush that found the queue full, owed ahead of the next item.
    flush_owed: bool,
}

impl Stream {
    /// Queues `item`, after any flush owed. A flush is never dropped: one
    /// that doesn't fit is owed instead, so nothing that came after it can
    /// overtake it.
    fn try_send(&mut self, item: Item) -> Result<(), TrySendError<Item>> {
        if matches!(item, Item::Flush) {
            self.flush_owed = true;
        }
        if self.flush_owed {
            self.tx.try_send(Item::Flush)?;
            self.flush_owed = false;
        }
        match item {
            Item::Flush => Ok(()),
            item => self.tx.try_send(item),
        }
    }
}

impl Slot {
    /// Passes `item` to the stream, or drops it when nobody is listening
    /// or, for audio, when the listener is behind.
    fn send(&self, target_id: &str, item: Item) {
        let mut stream = lock(&self.stream);
        let Some(s) = stream.as_mut() else { return };
        let flush = matches!(item, Item::Flush);
        match s.try_send(item) {
            Ok(()) => self.overflowing.store(false, Ordering::Relaxed),
            // An owed flush goes later.
            Err(TrySendError::Full(_)) if flush => {}
            Err(TrySendError::Full(_)) => {
                if !self.overflowing.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        target_id,
                        "AirPlay audio isn't being played fast enough; dropping some"
                    );
                }
            }
            Err(TrySendError::Closed(_)) => *stream = None,
        }
    }

    /// `target_id`'s receiver, if it is running.
    fn of(audio: &AirplayAudio, target_id: &str) -> Option<Arc<Slot>> {
        lock(&audio.receivers).get(target_id).cloned()
    }
}

impl AirplayAudio {
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    /// As [`Self::new`], with each stream's rate matching sent to
    /// `telemetry`, timed on `clock`.
    pub fn with_telemetry(telemetry: Telemetry, clock: ServerClock) -> Arc<Self> {
        Arc::new(Self {
            telemetry,
            clock,
            ..Self::default()
        })
    }

    /// `target_id`'s AirPlay audio from now on, rate matched to the
    /// server clock and in the session format, with each chunk's play time
    /// kept through the resampling. `None` when its receiver isn't
    /// running. Ends any stream handed out before for the same target.
    pub fn source(&self, target_id: &str) -> Option<Result<Box<dyn TimedSource>, AudioError>> {
        let raw = self.subscribe(target_id)?;
        let matched = Matched {
            source: RateMatch::new(raw),
            probe: self.telemetry.probe(&crate::config::source_id(target_id)),
            clock: self.clock,
            reported: None,
        };
        Some(timed_to_session_format(Box::new(matched)))
    }

    /// Ends `target_id`'s stream, once what was already read has been
    /// played. Returns whether there was one.
    pub fn end_stream(&self, target_id: &str) -> bool {
        let receivers = lock(&self.receivers);
        receivers
            .get(target_id)
            .is_some_and(|slot| lock(&slot.stream).take().is_some())
    }

    /// Whether `target_id`'s receiver is running.
    pub fn is_reading(&self, target_id: &str) -> bool {
        lock(&self.receivers).contains_key(target_id)
    }

    /// Queues a receiver's `chunk`, noting whether its stamps are
    /// to be kept as they are.
    fn handed_over(
        &self,
        receiver: &ReceiverInfo,
        chunk: &PcmChunk,
        play_at_us: Option<i64>,
        on_time: bool,
    ) {
        if let Some(slot) = Slot::of(self, &receiver.target_id) {
            slot.on_time.store(on_time, Ordering::Relaxed);
            let item = Item::Chunk {
                pcm: chunk.clone(),
                play_at_us,
            };
            slot.send(&receiver.target_id, item);
        }
    }

    fn subscribe(&self, target_id: &str) -> Option<Received> {
        let receivers = lock(&self.receivers);
        let slot = receivers.get(target_id)?;
        let (tx, rx) = mpsc::channel(STREAM_QUEUE);
        *lock(&slot.stream) = Some(Stream {
            tx,
            flush_owed: false,
        });
        Some(Received {
            items: rx,
            on_time: Arc::clone(&slot.on_time),
        })
    }
}

impl ReceiverHandler for AirplayAudio {
    fn started(&self, receiver: &ReceiverInfo) {
        // A run that never reported stopping is replaced, and its stream
        // ended.
        lock(&self.receivers).insert(receiver.target_id.clone(), Arc::default());
    }

    fn audio(&self, receiver: &ReceiverInfo, chunk: &PcmChunk, play_at_us: Option<i64>) {
        self.handed_over(receiver, chunk, play_at_us, false);
    }

    fn audio_on_time(&self, receiver: &ReceiverInfo, chunk: &PcmChunk, play_at_us: Option<i64>) {
        self.handed_over(receiver, chunk, play_at_us, true);
    }

    fn flush(&self, receiver: &ReceiverInfo) {
        if let Some(slot) = Slot::of(self, &receiver.target_id) {
            slot.send(&receiver.target_id, Item::Flush);
        }
    }

    fn stopped(&self, receiver: &ReceiverInfo) {
        // Dropping the slot ends its stream.
        lock(&self.receivers).remove(&receiver.target_id);
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every critical section leaves the value consistent.
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One stream of a receiver's audio, as it was handed over.
struct Received {
    items: mpsc::Receiver<Item>,
    /// Its slot's `on_time`: whether the stamps are kept, or the lead may
    /// be trimmed. Not the
    /// slot itself, whose sender would keep this open.
    on_time: Arc<AtomicBool>,
}

#[async_trait]
impl TimedSource for Received {
    fn format(&self) -> AudioFormat {
        AIRPLAY_FORMAT
    }

    fn kind(&self) -> &'static str {
        "airplay"
    }

    fn trim_lead(&self) -> Option<i64> {
        (!self.on_time()).then_some(KEEP_LEAD.as_micros() as i64)
    }

    fn on_time(&self) -> bool {
        self.on_time.load(Ordering::Relaxed)
    }

    async fn next(&mut self) -> Result<Option<Item>, AudioError> {
        Ok(self.items.recv().await)
    }
}

/// A stream rate matched, with what the matching does sent to the probe:
/// the error as of each stamped chunk, and the counts as they change.
struct Matched {
    source: RateMatch<Received>,
    probe: Probe,
    clock: ServerClock,
    /// The counts last sent.
    reported: Option<RateStats>,
}

#[async_trait]
impl TimedSource for Matched {
    fn format(&self) -> AudioFormat {
        self.source.format()
    }

    fn kind(&self) -> &'static str {
        self.source.kind()
    }

    fn trim_lead(&self) -> Option<i64> {
        self.source.trim_lead()
    }

    fn on_time(&self) -> bool {
        self.source.on_time()
    }

    async fn next(&mut self) -> Result<Option<Item>, AudioError> {
        let item = self.source.next().await?;
        if let Some(Item::Chunk {
            play_at_us: Some(_),
            ..
        }) = &item
        {
            let (now, stats) = (self.clock.now_us(), self.source.stats());
            self.probe
                .sample(Series::RateError, now, stats.error_us as f64);
            let last = self.reported.unwrap_or(RateStats {
                dropped: u64::MAX,
                repeated: u64::MAX,
                ..stats
            });
            if stats.dropped != last.dropped {
                self.probe
                    .sample(Series::FramesDropped, now, stats.dropped as f64);
            }
            if stats.repeated != last.repeated {
                self.probe
                    .sample(Series::FramesRepeated, now, stats.repeated as f64);
            }
            self.reported = Some(stats);
        }
        Ok(item)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use blastcast::SESSION_FORMAT;

    use super::*;

    struct Rig {
        info: ReceiverInfo,
        audio: Arc<AirplayAudio>,
    }

    impl Rig {
        /// A running receiver, as a [`crate::Receiver`] leaves it after calling
        /// `started`.
        fn start() -> Self {
            let info = ReceiverInfo::new("ss-kitchen", "Kitchen".into(), 7000);
            let audio = AirplayAudio::new();
            audio.started(&info);
            assert!(audio.is_reading("ss-kitchen"));
            Self { info, audio }
        }

        /// Hands over `samples`, unstamped, as the receiver does.
        fn hand(&self, samples: &[f32]) {
            let chunk = PcmChunk::new(AIRPLAY_FORMAT, samples.to_vec());
            self.audio.audio(&self.info, &chunk, None);
        }

        fn raw(&self) -> Received {
            self.audio.subscribe("ss-kitchen").unwrap()
        }
    }

    /// The next item, which must come within 5 s.
    async fn next(source: &mut dyn TimedSource) -> Option<Item> {
        tokio::time::timeout(Duration::from_secs(5), source.next())
            .await
            .expect("nothing came")
            .unwrap()
    }

    /// The next chunk and its play time.
    async fn chunk_of(source: &mut dyn TimedSource) -> (PcmChunk, Option<i64>) {
        match next(source).await {
            Some(Item::Chunk { pcm, play_at_us }) => {
                assert_eq!(pcm.format, source.format());
                assert_eq!(pcm.samples.len() % 2, 0, "a chunk split a frame");
                (pcm, play_at_us)
            }
            other => panic!("expected audio, got {other:?}"),
        }
    }

    /// Pulls chunks until `want` samples have arrived.
    async fn samples(source: &mut dyn TimedSource, want: usize) -> Vec<f32> {
        let mut out = Vec::new();
        while out.len() < want {
            out.extend(chunk_of(source).await.0.samples);
        }
        out
    }

    /// Whether the stream ends next: `None`, or the resampler's
    /// [`Item::End`].
    async fn ended(source: &mut dyn TimedSource) -> bool {
        matches!(
            tokio::time::timeout(Duration::from_secs(5), source.next()).await,
            Ok(Ok(None | Some(Item::End)))
        )
    }

    #[tokio::test]
    async fn end_stream_ends_it_after_what_was_read() {
        let rig = Rig::start();
        let mut source = rig.raw();
        rig.hand(&[0.1, 0.2]);
        assert!(rig.audio.end_stream("ss-kitchen"));
        assert!(!rig.audio.end_stream("ss-kitchen"));
        assert_eq!(samples(&mut source, 2).await.len(), 2);
        assert!(ended(&mut source).await);

        // The receiver still runs, and the next stream gets what follows.
        let mut next = rig.raw();
        rig.hand(&[0.3, 0.4]);
        assert_eq!(samples(&mut next, 2).await, [0.3, 0.4]);
    }

    #[tokio::test]
    async fn a_new_stream_ends_the_last() {
        let rig = Rig::start();
        let mut first = rig.raw();
        let mut second = rig.raw();
        assert!(ended(&mut first).await);
        rig.hand(&[0.1, 0.2]);
        assert_eq!(samples(&mut second, 2).await.len(), 2);
    }

    #[tokio::test]
    async fn stopping_the_run_ends_the_stream() {
        let rig = Rig::start();
        let mut source = rig.audio.source("ss-kitchen").unwrap().unwrap();
        rig.audio.stopped(&rig.info);
        assert!(!rig.audio.is_reading("ss-kitchen"));
        assert!(ended(&mut *source).await);
        assert!(rig.audio.source("ss-kitchen").is_none());
        assert!(!rig.audio.end_stream("ss-kitchen"));
    }

    /// A receiver's audio is handed over, each chunk with its play time.
    #[tokio::test]
    async fn a_receivers_audio_is_handed_over() {
        let info = ReceiverInfo::new("ss-kitchen", "Kitchen".into(), 7000);
        let audio = AirplayAudio::new();
        let chunk = |v: f32| PcmChunk::new(AIRPLAY_FORMAT, vec![v; 352 * 2]);
        // Not running: nowhere to go.
        audio.audio(&info, &chunk(0.1), None);
        audio.started(&info);
        assert!(audio.is_reading("ss-kitchen"));
        // Nobody listening yet: dropped.
        audio.audio(&info, &chunk(0.2), None);
        let mut source = audio.subscribe("ss-kitchen").unwrap();
        audio.audio(&info, &chunk(0.3), Some(1_000));
        audio.audio(&info, &chunk(0.4), None);
        // Each with the play time it came with.
        let (pcm, play_at_us) = chunk_of(&mut source).await;
        assert_eq!((pcm.samples, play_at_us), (vec![0.3; 704], Some(1_000)));
        let (pcm, play_at_us) = chunk_of(&mut source).await;
        assert_eq!((pcm.samples, play_at_us), (vec![0.4; 704], None));
        assert!(audio.end_stream("ss-kitchen"));
        assert!(ended(&mut source).await);
        audio.stopped(&info);
        assert!(!audio.is_reading("ss-kitchen"));
    }

    /// A FLUSH is an item in line with the audio, between what came before
    /// it and what came after.
    #[tokio::test]
    async fn a_flush_comes_in_line_with_the_audio() {
        let info = ReceiverInfo::new("ss-kitchen", "Kitchen".into(), 7000);
        let audio = AirplayAudio::new();
        let chunk = |v: f32| PcmChunk::new(AIRPLAY_FORMAT, vec![v; 352 * 2]);
        // Nobody listening: nothing to flush.
        audio.started(&info);
        audio.flush(&info);
        let mut source = audio.subscribe("ss-kitchen").unwrap();
        audio.audio(&info, &chunk(0.1), Some(1_000));
        audio.flush(&info);
        audio.audio(&info, &chunk(0.2), Some(9_000));
        assert_eq!(chunk_of(&mut source).await.1, Some(1_000));
        assert_eq!(next(&mut source).await, Some(Item::Flush));
        assert_eq!(chunk_of(&mut source).await.1, Some(9_000));
    }

    /// A stream that has fallen behind loses audio, never a flush: one that
    /// finds the queue full goes ahead of the next item that fits.
    #[tokio::test]
    async fn a_flush_is_never_dropped() {
        let info = ReceiverInfo::new("ss-kitchen", "Kitchen".into(), 7000);
        let audio = AirplayAudio::new();
        let chunk = |v: f32| PcmChunk::new(AIRPLAY_FORMAT, vec![v; 4]);
        audio.started(&info);
        let mut source = audio.subscribe("ss-kitchen").unwrap();
        for _ in 0..STREAM_QUEUE {
            audio.audio(&info, &chunk(0.1), None);
        }
        audio.flush(&info);
        // Still full: dropped, and the flush still owed.
        audio.audio(&info, &chunk(0.2), None);
        for _ in 0..STREAM_QUEUE {
            assert_eq!(chunk_of(&mut source).await.0.samples, [0.1; 4]);
        }
        audio.audio(&info, &chunk(0.3), Some(5));
        assert_eq!(next(&mut source).await, Some(Item::Flush));
        assert_eq!(chunk_of(&mut source).await.1, Some(5));
    }

    /// A receiver's stream is rate matched, and what the matching
    /// does goes to the hub under the receiver's source id: a sender whose
    /// stamps step 20 ms late gets frames repeated until it is caught up.
    #[tokio::test]
    async fn rate_matching_is_telemetry() {
        let info = ReceiverInfo::new("ss-kitchen", "Kitchen".into(), 7000);
        let (hub, clock) = (Telemetry::new(), ServerClock::new());
        let audio = AirplayAudio::with_telemetry(hub.clone(), clock);
        audio.started(&info);
        let mut source = audio.source("ss-kitchen").unwrap().unwrap();
        let chunk = PcmChunk::new(AIRPLAY_FORMAT, vec![0.1; 352 * 2]);
        for i in 0..20i64 {
            let late = if i >= 10 { 20_000 } else { 0 };
            let at = 1_000_000 + i * 352 * 1_000_000 / 44_100 + late;
            audio.audio(&info, &chunk, Some(at));
        }
        // Most of it through the resampler.
        samples(&mut *source, 6000 * 2).await;
        let entity = crate::config::source_id("ss-kitchen");
        let values = |series| -> Vec<f64> {
            hub.subscribe(clock.now_us())
                .history
                .into_iter()
                .filter(|t| t.series == series && *t.entity == *entity)
                .flat_map(|t| t.points)
                .map(|p| p.value)
                .collect()
        };
        let errors = values(Series::RateError);
        assert_eq!(errors[..10], [0.0; 10]);
        // 20 ms late, then 22.7 µs less each frame repeated.
        assert!((19_990.0..=20_000.0).contains(&errors[10]), "{errors:?}");
        assert!(errors[11] < errors[10], "{errors:?}");
        let repeated = values(Series::FramesRepeated);
        assert_eq!(repeated[..2], [0.0, 1.0], "{repeated:?}");
        assert_eq!(values(Series::FramesDropped), [0.0]);
    }

    #[tokio::test]
    async fn the_source_is_live_and_in_the_session_format() {
        let rig = Rig::start();
        let mut source = rig.audio.source("ss-kitchen").unwrap().unwrap();
        assert_eq!(source.format(), SESSION_FORMAT);
        assert!(source.is_live());
        // 0.1 s at 44.1 kHz is at least most of 0.1 s at 48 kHz: the
        // resampler holds back at most one block.
        rig.hand(&vec![0.03; 4410 * 2]);
        let got = samples(&mut *source, 3000 * 2).await;
        assert!(got.len() <= 4800 * 2);
    }

    /// What the stream and the resampler add to the latency, with AirPlay
    /// 1's 352-frame packets handed over in real time. Run with
    /// `--nocapture` to see it. The rest of the phone-to-speaker latency is
    /// the sender's lead and the player's; see docs/manual-tests.md.
    #[tokio::test]
    async fn added_latency_is_about_one_resampler_block() {
        const WRITE_FRAMES: usize = 352;
        const WRITES: usize = 125; // 1 s
        let rig = Rig::start();
        let mut source = rig.audio.source("ss-kitchen").unwrap().unwrap();
        let began = Instant::now();
        let writer = tokio::spawn(async move {
            let period = Duration::from_secs_f64(WRITE_FRAMES as f64 / 44_100.0);
            let mut written = Vec::new();
            for i in 0..WRITES {
                tokio::time::sleep_until((began + period * i as u32).into()).await;
                rig.hand(&[0.03; WRITE_FRAMES * 2]);
                written.push(began.elapsed());
            }
            written
        });
        // When each output frame came out, against when its input frame
        // went in.
        let mut out_frames = 0usize;
        let mut worst = Duration::ZERO;
        let mut sum = Duration::ZERO;
        let mut n = 0u32;
        let total_out = WRITES * WRITE_FRAMES * 48_000 / 44_100 - 1200;
        let mut arrivals = Vec::new();
        while out_frames < total_out {
            let (chunk, _) = chunk_of(&mut *source).await;
            out_frames += chunk.frames();
            arrivals.push((out_frames, began.elapsed()));
        }
        let written = writer.await.unwrap();
        for (frames, at) in arrivals {
            // The write that carried the last input frame of this output.
            let input = frames * 44_100 / 48_000;
            let write = (input / WRITE_FRAMES).min(WRITES - 1);
            let delay = at.saturating_sub(written[write]);
            worst = worst.max(delay);
            sum += delay;
            n += 1;
        }
        println!(
            "stream + resampler latency: mean {:?}, worst {:?} over {n} chunks",
            sum / n,
            worst
        );
        // One 1024-frame block is 23 ms; the rest is scheduling slack.
        assert!(worst < Duration::from_millis(100), "worst {worst:?}");
    }
}
