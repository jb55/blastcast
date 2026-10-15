//! One AirPlay receiver ([`crate::raop`]), kept running.
//!
//! [`Receiver::spawn`] starts one: it binds the receiver's port, advertises
//! it, and serves senders until it is stopped, telling the
//! [`ReceiverHandler`] what they do. A run that fails, such as a bind
//! refused, is restarted after [`ReceiverConfig::restart_delay`]; after
//! [`ReceiverConfig::max_restarts`] restarts in a row that each ended
//! within [`ReceiverConfig::stable_after`], it is given up
//! ([`Ended::GaveUp`]).
//!
//! An app that runs several, such as one per speaker, starts one per
//! [`ReceiverInfo`] and stops and restarts them when a name or port
//! changes. [`run`] is the same restart loop as a future, for an app that
//! wants to run it in a task of its own.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use blastcast::PcmChunk;

use crate::config::ReceiverInfo;
use crate::metadata::MetadataEvent;
use crate::raop::{self, NativeConfig};

/// What each receiver runs with, and how it is restarted.
#[derive(Debug, Clone)]
pub struct ReceiverConfig {
    pub native: NativeConfig,
    pub restart_delay: Duration,
    pub max_restarts: u32,
    /// A run that lasts this long resets the restart count.
    pub stable_after: Duration,
}

impl ReceiverConfig {
    pub fn new(native: NativeConfig) -> Self {
        // MA restarts after 2 s, up to 5 times.
        Self {
            native,
            restart_delay: Duration::from_secs(2),
            max_restarts: 5,
            stable_after: Duration::from_secs(60),
        }
    }
}

/// Told about each run of each receiver, in order: `started`, any number
/// of `event`, `audio` and `flush`, then `stopped`. Called from the
/// receiver's tasks, so it must not block.
pub trait ReceiverHandler: Send + Sync + 'static {
    /// The receiver is about to serve senders.
    fn started(&self, _receiver: &ReceiverInfo) {}
    /// What the sender said, parsed.
    fn event(&self, _receiver: &ReceiverInfo, _event: &MetadataEvent) {}
    /// The sender's audio, in real time: 44.1 kHz stereo
    /// ([`crate::audio::AIRPLAY_FORMAT`]). `play_at_us` is when the sender
    /// means its first frame to play, on the server clock, once the
    /// receiver knows ([`crate::raop::clock`]).
    fn audio(&self, _receiver: &ReceiverInfo, _chunk: &PcmChunk, _play_at_us: Option<i64>) {}
    /// As [`audio`](Self::audio), from a sender whose play times are when
    /// the audio plays everywhere it plays: an AirPlay 2 phone's, on the
    /// PTP clock it keeps its own picture and other speakers to. Unlike an
    /// AirPlay 1 sender's surplus lead, nothing may trim them.
    fn audio_on_time(&self, receiver: &ReceiverInfo, chunk: &PcmChunk, play_at_us: Option<i64>) {
        self.audio(receiver, chunk, play_at_us);
    }
    /// The sender flushed (`FLUSH`, a pause or a seek):
    /// drop what was handed over and hasn't played. It comes in line with
    /// [`audio`](Self::audio): every chunk after it is from after the
    /// flush.
    fn flush(&self, _receiver: &ReceiverInfo) {}
    /// The run ended.
    fn stopped(&self, _receiver: &ReceiverInfo) {}
}

/// Discards everything.
impl ReceiverHandler for () {}

/// Tells each handler in turn, such as the audio and the metadata.
impl ReceiverHandler for Vec<Arc<dyn ReceiverHandler>> {
    fn started(&self, receiver: &ReceiverInfo) {
        self.iter().for_each(|h| h.started(receiver));
    }
    fn event(&self, receiver: &ReceiverInfo, event: &MetadataEvent) {
        self.iter().for_each(|h| h.event(receiver, event));
    }
    fn audio(&self, receiver: &ReceiverInfo, chunk: &PcmChunk, play_at_us: Option<i64>) {
        self.iter()
            .for_each(|h| h.audio(receiver, chunk, play_at_us));
    }
    fn audio_on_time(&self, receiver: &ReceiverInfo, chunk: &PcmChunk, play_at_us: Option<i64>) {
        self.iter()
            .for_each(|h| h.audio_on_time(receiver, chunk, play_at_us));
    }
    fn flush(&self, receiver: &ReceiverInfo) {
        self.iter().for_each(|h| h.flush(receiver));
    }
    fn stopped(&self, receiver: &ReceiverInfo) {
        self.iter().for_each(|h| h.stopped(receiver));
    }
}

/// How a receiver's restart loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ended {
    /// It was asked to stop.
    Stopped,
    /// It failed [`ReceiverConfig::max_restarts`] times in a row.
    GaveUp,
}

/// A running receiver, in its own task. Dropping it leaves the task
/// running until it gives up; [`Receiver::stop`] ends it.
pub struct Receiver {
    info: ReceiverInfo,
    stop: watch::Sender<bool>,
    task: JoinHandle<Ended>,
}

impl Receiver {
    /// Starts serving senders as `info`, telling `handler`. `volume`
    /// (0..=100) is what it tells a sender the volume is, until one sets
    /// it.
    pub fn spawn(
        config: ReceiverConfig,
        info: ReceiverInfo,
        handler: Arc<dyn ReceiverHandler>,
        volume: Option<u8>,
    ) -> Self {
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(run(config, info.clone(), handler, volume, stopped));
        Self { info, stop, task }
    }

    pub fn info(&self) -> &ReceiverInfo {
        &self.info
    }

    /// It was given up on (or its task panicked).
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    /// Stops it and waits for the run to end: the handler's `stopped` has
    /// been called and the port is free again.
    pub async fn stop(self) -> Ended {
        let _ = self.stop.send(true);
        match self.task.await {
            Ok(ended) => ended,
            Err(e) => {
                if e.is_panic() {
                    tracing::error!(target_id = %self.info.target_id, "AirPlay receiver task panicked");
                }
                Ended::Stopped
            }
        }
    }
}

/// Runs the receiver `info`, restarting it, until `stop` is set (or its
/// sender dropped) or it is given up on.
pub async fn run(
    config: ReceiverConfig,
    info: ReceiverInfo,
    handler: Arc<dyn ReceiverHandler>,
    volume: Option<u8>,
    stop: watch::Receiver<bool>,
) -> Ended {
    Daemon {
        config,
        handler,
        info,
        volume,
    }
    .supervise(stop)
    .await
}

/// How one run ended.
enum Exit {
    Stopped,
    Failed(io::Error),
}

/// One receiver's restart loop.
struct Daemon {
    config: ReceiverConfig,
    handler: Arc<dyn ReceiverHandler>,
    info: ReceiverInfo,
    volume: Option<u8>,
}

impl Daemon {
    async fn supervise(&self, mut stop: watch::Receiver<bool>) -> Ended {
        let id = &self.info.target_id;
        let mut restarts = 0;
        loop {
            let began = Instant::now();
            let why = match self.run_once(&mut stop).await {
                Exit::Stopped => return Ended::Stopped,
                Exit::Failed(e) => e.to_string(),
            };
            if began.elapsed() >= self.config.stable_after {
                restarts = 0;
            }
            if restarts >= self.config.max_restarts {
                tracing::error!(
                    target_id = %id,
                    name = %self.info.airplay_name,
                    restarts,
                    "AirPlay receiver {why}; giving up on it"
                );
                return Ended::GaveUp;
            }
            restarts += 1;
            tracing::warn!(
                target_id = %id,
                name = %self.info.airplay_name,
                delay = ?self.config.restart_delay,
                "AirPlay receiver {why}; restarting"
            );
            tokio::select! {
                _ = tokio::time::sleep(self.config.restart_delay) => {}
                () = stopped(&mut stop) => return Ended::Stopped,
            }
        }
    }

    async fn run_once(&self, stop: &mut watch::Receiver<bool>) -> Exit {
        if *stop.borrow() {
            return Exit::Stopped;
        }
        let listener = match raop::bind(self.info.port).await {
            Ok(l) => l,
            Err(e) => {
                return Exit::Failed(io::Error::new(
                    e.kind(),
                    format!("binding port {}: {e}", self.info.port),
                ));
            }
        };
        tracing::info!(
            target_id = %self.info.target_id,
            name = %self.info.airplay_name,
            port = self.info.port,
            "AirPlay receiver listening"
        );
        self.handler.started(&self.info);
        let served = raop::serve(
            self.config.native.clone(),
            self.info.clone(),
            self.handler.clone(),
            listener,
            self.volume,
            stop,
        )
        .await;
        self.handler.stopped(&self.info);
        match served {
            Ok(()) => Exit::Stopped,
            Err(e) => Exit::Failed(e),
        }
    }
}

/// Resolves once a stop is asked for, or the stopper is gone.
async fn stopped(stop: &mut watch::Receiver<bool>) {
    // The guard `wait_for` returns isn't Send; drop it here.
    let _ = stop.wait_for(|s| *s).await.is_ok();
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, MutexGuard, PoisonError};

    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    enum Ev {
        Started(String),
        Event(String, MetadataEvent),
        Stopped(String),
    }

    #[derive(Default)]
    struct Recorder(Mutex<Vec<Ev>>);

    fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
        m.lock().unwrap_or_else(PoisonError::into_inner)
    }

    impl Recorder {
        fn events(&self) -> Vec<Ev> {
            lock(&self.0).clone()
        }
    }

    impl ReceiverHandler for Recorder {
        fn started(&self, r: &ReceiverInfo) {
            lock(&self.0).push(Ev::Started(r.target_id.clone()));
        }
        fn event(&self, r: &ReceiverInfo, event: &MetadataEvent) {
            lock(&self.0).push(Ev::Event(r.target_id.clone(), event.clone()));
        }
        fn stopped(&self, r: &ReceiverInfo) {
            lock(&self.0).push(Ev::Stopped(r.target_id.clone()));
        }
    }

    fn config(tweak: impl FnOnce(&mut ReceiverConfig)) -> ReceiverConfig {
        let mut config = ReceiverConfig::new(NativeConfig::new(
            "server",
            blastcast::ServerClock::new(),
            None,
        ));
        config.restart_delay = Duration::from_millis(10);
        tweak(&mut config);
        config
    }

    /// A free port, by binding and letting go of one.
    async fn free_port() -> u16 {
        let l = raop::bind(0).await.unwrap();
        l.local_addr().unwrap().port()
    }

    #[test]
    fn a_vec_of_handlers_tells_each_one() {
        let (a, b) = (Arc::new(Recorder::default()), Arc::new(Recorder::default()));
        let both: Vec<Arc<dyn ReceiverHandler>> = vec![a.clone(), b.clone()];
        let info = ReceiverInfo::new("ss-k", "K".into(), 7000);
        both.started(&info);
        both.event(&info, &MetadataEvent::Paused);
        both.stopped(&info);
        let id = || "ss-k".to_owned();
        for r in [a, b] {
            assert_eq!(
                r.events(),
                [
                    Ev::Started(id()),
                    Ev::Event(id(), MetadataEvent::Paused),
                    Ev::Stopped(id())
                ]
            );
        }
    }

    /// A receiver runs in-process: it listens on its port, answers there,
    /// and lets the port go when it stops.
    #[tokio::test]
    async fn a_receiver_answers_on_its_port_until_stopped() {
        let port = free_port().await;
        let recorder = Arc::new(Recorder::default());
        let info = ReceiverInfo::new("kitchen", "Kitchen".into(), port);
        let receiver = Receiver::spawn(config(|_| {}), info, recorder.clone(), None);

        let mut tcp = loop {
            match tokio::net::TcpStream::connect(("127.0.0.1", port)).await {
                Ok(tcp) => break tcp,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        };
        tcp.write_all(b"OPTIONS * RTSP/1.0\r\nCSeq: 1\r\n\r\n")
            .await
            .unwrap();
        let mut line = String::new();
        tokio::io::BufReader::new(&mut tcp)
            .read_line(&mut line)
            .await
            .unwrap();
        assert_eq!(line, "RTSP/1.0 200 OK\r\n");

        assert!(!receiver.is_finished());
        assert_eq!(receiver.stop().await, Ended::Stopped);
        assert_eq!(
            recorder.events(),
            [Ev::Started("kitchen".into()), Ev::Stopped("kitchen".into())]
        );
        tokio::net::TcpListener::bind(("0.0.0.0", port))
            .await
            .expect("the port is free again");
    }

    /// A port someone else holds fails each run's bind.
    #[tokio::test]
    async fn a_failing_receiver_is_restarted_then_given_up() {
        let held = raop::bind(0).await.unwrap();
        let port = held.local_addr().unwrap().port();
        let recorder = Arc::new(Recorder::default());
        let info = ReceiverInfo::new("kitchen", "Kitchen".into(), port);
        let (_stop, stopped) = watch::channel(false);
        let ended = run(
            config(|c| c.max_restarts = 2),
            info,
            recorder.clone(),
            None,
            stopped,
        )
        .await;
        assert_eq!(ended, Ended::GaveUp);
        assert!(recorder.events().is_empty(), "no run started");
    }
}
