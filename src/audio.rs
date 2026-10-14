//! Each receiver's PCM, handed to whoever plays it as a [`LiveSource`]: a
//! native receiver's as it hands it over ([`ReceiverHandler::audio`]), or
//! shairport-sync's, read off its audio fifo.
//!
//! [`AirplayAudio`] is a [`ReceiverHandler`]: when a shairport run starts
//! it opens that run's audio fifo and reads it until the run stops, **whether or not
//! anything is playing it**. A reader that only read while a session was
//! playing would leave shairport's blocking write stuck on a full pipe in
//! between, stalling its player thread and the sender's session. What is
//! read while nobody listens is dropped.
//!
//! The fifo is opened **read-write**, as the metadata fifo is
//! ([`crate::supervisor`]). Holding a write end ourselves means a read
//! never sees EOF, so a writer going away and coming back (shairport
//! closing the pipe, or a new writer after a restart) costs nothing: the
//! next write just arrives on the same descriptor. There is no reopen loop
//! because there is nothing to reopen.
//!
//! [`AirplayAudio::source`] hands out one stream per receiver at a time: a
//! new one ends the last. It yields audio written after it was taken, not
//! a backlog, so a live session starts at "now" with no extra lead.
//! [`AirplayAudio::end_stream`] ends it, which is what `MA_PLAY_END` calls
//! for. MA writes a second of silence into the fifo there, to wake an
//! ffmpeg blocked reading it; nothing here blocks on the fifo, so ending
//! the stream closes its channel instead, and the session's pending
//! `next_chunk` returns `None` at once.
//!
//! Only audio passes through: nothing is kept past the chunk in flight.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use tas_audio::{AudioError, AudioFormat, LiveSource, PcmChunk, pcm, to_session_format};
use tokio::io::AsyncReadExt;
use tokio::net::unix::pipe;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::JoinHandle;

use crate::config::ReceiverInfo;
use crate::supervisor::ReceiverHandler;

/// What shairport-sync's pipe backend writes, and a native receiver hands
/// over: s16, 44.1 kHz, stereo.
pub const AIRPLAY_FORMAT: AudioFormat = AudioFormat::new(44_100, 2);

/// One s16le stereo frame.
const FRAME_BYTES: usize = 4;

/// Bytes per read. shairport writes 352 frames (1408 bytes) at a time, so a
/// read normally returns one write; this only bounds a backlog.
const READ_BYTES: usize = 16 * 1024;

/// Chunks queued for a stream that isn't keeping up: about 1 s of
/// shairport's 352-frame writes. Past it, chunks are dropped rather than
/// letting the reader, and so shairport, wait.
const STREAM_QUEUE: usize = 128;

/// The audio of every running receiver, by target.
#[derive(Default)]
pub struct AirplayAudio {
    receivers: Mutex<HashMap<String, Reader>>,
}

/// One run's stream, and the task reading its fifo, if it has one.
struct Reader {
    slot: Arc<Slot>,
    task: Option<JoinHandle<()>>,
}

impl Drop for Reader {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

/// Where a receiver's audio goes: the stream handed out, if any.
#[derive(Default)]
struct Slot {
    stream: Mutex<Option<mpsc::Sender<PcmChunk>>>,
    /// Chunks are being dropped, and that has been logged.
    overflowing: AtomicBool,
}

impl Slot {
    /// Passes `chunk` to the stream, or drops it when nobody is listening
    /// or the listener is behind.
    fn send(&self, target_id: &str, chunk: PcmChunk) {
        let mut stream = lock(&self.stream);
        let Some(tx) = stream.as_ref() else { return };
        match tx.try_send(chunk) {
            Ok(()) => self.overflowing.store(false, Ordering::Relaxed),
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
}

impl AirplayAudio {
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    /// `target_id`'s AirPlay audio from now on, in the session format.
    /// `None` when its receiver isn't running. Ends any stream handed out
    /// before for the same target.
    pub fn source(&self, target_id: &str) -> Option<Result<Box<dyn LiveSource>, AudioError>> {
        let raw = self.subscribe(target_id)?;
        Some(to_session_format(Box::new(raw)))
    }

    /// Ends `target_id`'s stream, once what was already read has been
    /// played. Returns whether there was one.
    pub fn end_stream(&self, target_id: &str) -> bool {
        let receivers = lock(&self.receivers);
        receivers
            .get(target_id)
            .is_some_and(|r| lock(&r.slot.stream).take().is_some())
    }

    /// Whether `target_id`'s receiver is running (and its fifo, if it has
    /// one, open).
    pub fn is_reading(&self, target_id: &str) -> bool {
        lock(&self.receivers).contains_key(target_id)
    }

    fn subscribe(&self, target_id: &str) -> Option<FifoSource> {
        let receivers = lock(&self.receivers);
        let reader = receivers.get(target_id)?;
        let (tx, rx) = mpsc::channel(STREAM_QUEUE);
        *lock(&reader.slot.stream) = Some(tx);
        Some(FifoSource { chunks: rx })
    }
}

impl ReceiverHandler for AirplayAudio {
    fn started(&self, receiver: &ReceiverInfo) {
        let id = &receiver.target_id;
        let slot = Arc::<Slot>::default();
        let task = match &receiver.fifos {
            None => None,
            Some(fifos) => {
                // Opened here, before shairport starts, so its first open
                // for writing finds a reader rather than failing with
                // ENXIO.
                let fifo = match pipe::OpenOptions::new()
                    .read_write(true)
                    .open_receiver(&fifos.audio_pipe)
                {
                    Ok(fifo) => fifo,
                    Err(e) => {
                        tracing::error!(
                            target_id = %id,
                            path = %fifos.audio_pipe.display(),
                            error = %e,
                            "can't open the AirPlay audio fifo; this receiver will be silent"
                        );
                        lock(&self.receivers).remove(id);
                        return;
                    }
                };
                Some(tokio::spawn(read(id.clone(), fifo, Arc::clone(&slot))))
            }
        };
        // A run that never reported stopping is replaced, and its stream
        // ended.
        lock(&self.receivers).insert(id.clone(), Reader { slot, task });
    }

    /// The play time is dropped here: [`LiveSource`] has no timestamps,
    /// so the stream plays the audio as it arrives (taking it is
    /// headway:tune-assistant/pact-jeans-tube).
    fn audio(&self, receiver: &ReceiverInfo, chunk: &PcmChunk, _play_at_us: Option<i64>) {
        let slot = lock(&self.receivers)
            .get(&receiver.target_id)
            .map(|r| r.slot.clone());
        if let Some(slot) = slot {
            slot.send(&receiver.target_id, chunk.clone());
        }
    }

    fn stopped(&self, receiver: &ReceiverInfo) {
        // Dropping the reader stops it and ends its stream.
        lock(&self.receivers).remove(&receiver.target_id);
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every critical section leaves the value consistent.
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Reads `fifo` until the task is aborted, sending whole frames to the
/// current stream.
async fn read(target_id: String, mut fifo: pipe::Receiver, slot: Arc<Slot>) {
    let mut buf = vec![0u8; READ_BYTES];
    // Bytes of a frame split across reads, kept at the front of `buf`.
    let mut carry = 0;
    loop {
        let n = match fifo.read(&mut buf[carry..]).await {
            // We hold a write end, so this is never EOF, but don't spin.
            Ok(0) => return,
            Ok(n) => n,
            Err(e) => {
                tracing::error!(target_id, error = %e, "reading the AirPlay audio fifo");
                return;
            }
        };
        let len = carry + n;
        let whole = len - len % FRAME_BYTES;
        carry = len - whole;
        if whole > 0 && lock(&slot.stream).is_some() {
            let mut samples = Vec::with_capacity(whole / 2);
            pcm::s16le_to_f32(&buf[..whole], &mut samples);
            slot.send(&target_id, PcmChunk::new(AIRPLAY_FORMAT, samples));
        }
        buf.copy_within(whole..len, 0);
    }
}

/// One stream of a receiver's audio, as it was handed over.
struct FifoSource {
    chunks: mpsc::Receiver<PcmChunk>,
}

#[async_trait]
impl LiveSource for FifoSource {
    fn format(&self) -> AudioFormat {
        AIRPLAY_FORMAT
    }

    fn is_live(&self) -> bool {
        true
    }

    fn kind(&self) -> &'static str {
        "airplay"
    }

    async fn next_chunk(&mut self) -> Result<Option<PcmChunk>, AudioError> {
        Ok(self.chunks.recv().await)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use tas_audio::SESSION_FORMAT;
    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::testing::mkfifo;

    struct Rig {
        _dir: tempfile::TempDir,
        info: ReceiverInfo,
        audio: Arc<AirplayAudio>,
    }

    impl Rig {
        /// A receiver whose audio fifo exists, as the supervisor leaves it
        /// before calling `started`.
        fn start() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let info = ReceiverInfo::new(dir.path(), "ss-kitchen", "Kitchen".into(), 7000, None);
            mkfifo(&info.files().audio_pipe);
            let audio = AirplayAudio::new();
            audio.started(&info);
            assert!(audio.is_reading("ss-kitchen"));
            Self {
                _dir: dir,
                info,
                audio,
            }
        }

        /// Opens the fifo for writing, as shairport does. It never waits:
        /// the reader is already there.
        fn writer(&self) -> pipe::Sender {
            pipe::OpenOptions::new()
                .open_sender(&self.info.files().audio_pipe)
                .unwrap()
        }

        fn raw(&self) -> FifoSource {
            self.audio.subscribe("ss-kitchen").unwrap()
        }
    }

    fn s16(samples: &[i16]) -> Vec<u8> {
        samples.iter().flat_map(|s| s.to_le_bytes()).collect()
    }

    /// Pulls chunks until `want` samples have arrived.
    async fn samples(source: &mut dyn LiveSource, want: usize) -> Vec<f32> {
        let mut out = Vec::new();
        while out.len() < want {
            let chunk = tokio::time::timeout(Duration::from_secs(5), source.next_chunk())
                .await
                .expect("no audio")
                .unwrap()
                .expect("stream ended early");
            assert_eq!(chunk.format, source.format());
            assert_eq!(chunk.samples.len() % 2, 0, "a chunk split a frame");
            out.extend(chunk.samples);
        }
        out
    }

    async fn ended(source: &mut dyn LiveSource) -> bool {
        matches!(
            tokio::time::timeout(Duration::from_secs(5), source.next_chunk()).await,
            Ok(Ok(None))
        )
    }

    /// Lets the reader drain what is in the pipe.
    async fn settle() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn frames_split_across_writes_come_out_whole_and_exact() {
        let rig = Rig::start();
        let mut source = rig.raw();
        assert_eq!(source.format(), AIRPLAY_FORMAT);
        assert!(source.is_live());
        let pcm: Vec<i16> = vec![i16::MIN, -1, 0, 1, 16_384, i16::MAX, -16_384, 7];
        let bytes = s16(&pcm);
        let mut w = rig.writer();
        // Cut mid-sample and mid-frame.
        for piece in [&bytes[..3], &bytes[3..6], &bytes[6..13], &bytes[13..]] {
            w.write_all(piece).await.unwrap();
            settle().await;
        }
        let got = samples(&mut source, pcm.len()).await;
        let want: Vec<f32> = pcm.iter().map(|&s| f32::from(s) / 32_768.0).collect();
        assert_eq!(got, want);
    }

    #[tokio::test]
    async fn a_writer_that_goes_away_and_comes_back_is_still_read() {
        let rig = Rig::start();
        let mut source = rig.raw();
        let mut w = rig.writer();
        w.write_all(&s16(&[1, 2])).await.unwrap();
        drop(w);
        settle().await;
        // No EOF: the stream is still open, and a new writer carries on.
        let mut w = rig.writer();
        w.write_all(&s16(&[3, 4])).await.unwrap();
        let got = samples(&mut source, 4).await;
        assert_eq!(got, [1.0, 2.0, 3.0, 4.0].map(|s| s / 32_768.0));
    }

    #[tokio::test]
    async fn audio_from_before_the_stream_is_dropped_not_queued() {
        let rig = Rig::start();
        let mut w = rig.writer();
        // More than a pipe holds: shairport must never block on us.
        let backlog = s16(&vec![9; 200_000]);
        tokio::time::timeout(Duration::from_secs(5), w.write_all(&backlog))
            .await
            .expect("the writer blocked with nobody playing")
            .unwrap();
        settle().await;
        let mut source = rig.raw();
        w.write_all(&s16(&[5, 6])).await.unwrap();
        assert_eq!(
            samples(&mut source, 2).await,
            [5.0, 6.0].map(|s| s / 32_768.0)
        );
    }

    #[tokio::test]
    async fn end_stream_ends_it_after_what_was_read() {
        let rig = Rig::start();
        let mut source = rig.raw();
        let mut w = rig.writer();
        w.write_all(&s16(&[1, 2])).await.unwrap();
        settle().await;
        assert!(rig.audio.end_stream("ss-kitchen"));
        assert!(!rig.audio.end_stream("ss-kitchen"));
        assert_eq!(samples(&mut source, 2).await.len(), 2);
        assert!(ended(&mut source).await);

        // The fifo is still read, and the next stream gets what follows.
        let mut next = rig.raw();
        w.write_all(&s16(&[3, 4])).await.unwrap();
        assert_eq!(
            samples(&mut next, 2).await,
            [3.0, 4.0].map(|s| s / 32_768.0)
        );
    }

    #[tokio::test]
    async fn a_new_stream_ends_the_last() {
        let rig = Rig::start();
        let mut first = rig.raw();
        let mut second = rig.raw();
        assert!(ended(&mut first).await);
        rig.writer().write_all(&s16(&[1, 2])).await.unwrap();
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

    #[tokio::test]
    async fn a_missing_fifo_leaves_the_receiver_silent() {
        let dir = tempfile::tempdir().unwrap();
        let info = ReceiverInfo::new(dir.path(), "ss-kitchen", "Kitchen".into(), 7000, None);
        let audio = AirplayAudio::new();
        audio.started(&info);
        assert!(!audio.is_reading("ss-kitchen"));
        assert!(audio.source("ss-kitchen").is_none());
    }

    /// A native receiver has no fifo: its audio is handed over.
    #[tokio::test]
    async fn a_native_receivers_audio_is_handed_over() {
        let info = ReceiverInfo::native("ss-kitchen", "Kitchen".into(), 7000, None);
        let audio = AirplayAudio::new();
        let chunk = |v: f32| PcmChunk::new(AIRPLAY_FORMAT, vec![v; 352 * 2]);
        // Not running: nowhere to go.
        audio.audio(&info, &chunk(0.1), None);
        audio.started(&info);
        assert!(audio.is_reading("ss-kitchen"));
        // Nobody listening yet: dropped.
        audio.audio(&info, &chunk(0.2), None);
        let mut source = audio.subscribe("ss-kitchen").unwrap();
        audio.audio(&info, &chunk(0.3), Some(1));
        assert_eq!(samples(&mut source, 704).await, vec![0.3; 704]);
        assert!(audio.end_stream("ss-kitchen"));
        assert!(ended(&mut source).await);
        audio.stopped(&info);
        assert!(!audio.is_reading("ss-kitchen"));
    }

    #[tokio::test]
    async fn the_source_is_live_and_in_the_session_format() {
        let rig = Rig::start();
        let mut source = rig.audio.source("ss-kitchen").unwrap().unwrap();
        assert_eq!(source.format(), SESSION_FORMAT);
        assert!(source.is_live());
        // 0.1 s at 44.1 kHz is at least most of 0.1 s at 48 kHz: the
        // resampler holds back at most one block.
        rig.writer()
            .write_all(&s16(&vec![1000; 4410 * 2]))
            .await
            .unwrap();
        let got = samples(&mut *source, 3000 * 2).await;
        assert!(got.len() <= 4800 * 2);
    }

    /// What the fifo and the resampler add to the latency, with shairport's
    /// 352-frame writes paced in real time. Run with `--nocapture` to see
    /// it. The rest of the phone-to-speaker latency is shairport's buffer
    /// (about 2 s on AirPlay 1) and the player's; see docs/manual-tests.md.
    #[tokio::test]
    async fn added_latency_is_about_one_resampler_block() {
        const WRITE_FRAMES: usize = 352;
        const WRITES: usize = 125; // 1 s
        let rig = Rig::start();
        let mut source = rig.audio.source("ss-kitchen").unwrap().unwrap();
        let mut w = rig.writer();
        let began = Instant::now();
        let writer = tokio::spawn(async move {
            let period = Duration::from_secs_f64(WRITE_FRAMES as f64 / 44_100.0);
            let mut written = Vec::new();
            for i in 0..WRITES {
                tokio::time::sleep_until((began + period * i as u32).into()).await;
                w.write_all(&s16(&vec![1000; WRITE_FRAMES * 2]))
                    .await
                    .unwrap();
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
            let chunk = source.next_chunk().await.unwrap().unwrap();
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
            "fifo + resampler latency: mean {:?}, worst {:?} over {n} chunks",
            sum / n,
            worst
        );
        // One 1024-frame block is 23 ms; the rest is scheduling slack.
        assert!(worst < Duration::from_millis(100), "worst {worst:?}");
    }
}
