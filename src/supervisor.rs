//! One AirPlay receiver ([`crate::raop`]) per AirPlay target, kept
//! running.
//!
//! A **target** is a player or group player whose config has
//! `airplay_target` set (group players are configured by their
//! `syncgroup_*` id). The [`ReceiverSupervisor`] reconciles the running
//! receivers (called daemons here) with the targets:
//!
//! - A target gets a daemon once its player has been seen in the registry,
//!   since the advertised name comes from the player's name.
//! - The daemon is **kept while the player is gone** (a Sendspin speaker
//!   that disconnected is removed from the registry), so the phone keeps
//!   seeing the same AirPlay device with the same name and port.
//! - A rename, or a port change, restarts it with the new config.
//! - A target that is no longer configured is stopped. A config saved
//!   through the registry ([`PlayerRegistry::configure`], which the API's
//!   `config/players/save` calls) publishes `PlayerConfigUpdated`, and the
//!   supervisor reconciles on it. Nothing watches the state file, so
//!   anything else that changes `airplay_target` calls
//!   [`ReceiverSupervisor::reconcile`].
//!
//! A receiver's run binds the target's port and serves senders until it
//! is stopped, telling the [`ReceiverHandler`] what they do. A run that
//! fails, such as a bind refused, is restarted after
//! [`SupervisorConfig::restart_delay`]; after
//! [`SupervisorConfig::max_restarts`] restarts in a row that each ended
//! within [`SupervisorConfig::stable_after`], it is given up until the
//! player is added again or the target is reconfigured.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tas_core::state::StateStore;
use tas_core::{Event, PlayerRegistry};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use blastcast::PcmChunk;

use crate::config::{self, NameTemplate, ReceiverInfo};
use crate::metadata::MetadataEvent;
use crate::raop::{self, NativeConfig};

#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    /// What each receiver runs with.
    pub native: NativeConfig,
    pub name_template: NameTemplate,
    /// `{server}` in the name template.
    pub server_name: String,
    pub restart_delay: Duration,
    pub max_restarts: u32,
    /// A run that lasts this long resets the restart count.
    pub stable_after: Duration,
}

impl SupervisorConfig {
    pub fn new(native: NativeConfig) -> Self {
        // MA restarts after 2 s, up to 5 times.
        Self {
            native,
            name_template: NameTemplate::default(),
            server_name: String::new(),
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

struct Running {
    info: ReceiverInfo,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

pub struct ReceiverSupervisor {
    config: SupervisorConfig,
    registry: Arc<PlayerRegistry>,
    state: Arc<StateStore>,
    handler: Arc<dyn ReceiverHandler>,
    /// Held across a whole reconcile, so two can't race their stops and
    /// starts.
    running: tokio::sync::Mutex<HashMap<String, Running>>,
    /// Targets whose daemon was given up on; the daemon task adds itself.
    failed: Arc<Mutex<HashSet<String>>>,
}

impl ReceiverSupervisor {
    pub fn new(
        config: SupervisorConfig,
        registry: Arc<PlayerRegistry>,
        state: Arc<StateStore>,
        handler: Arc<dyn ReceiverHandler>,
    ) -> Arc<Self> {
        Arc::new(Self {
            config,
            registry,
            state,
            handler,
            running: tokio::sync::Mutex::new(HashMap::new()),
            failed: Arc::default(),
        })
    }

    /// Follows the registry, reconciling when a target's player is added or
    /// changes. Run it once, in its own task.
    pub fn run(self: Arc<Self>) -> impl Future<Output = ()> + Send + 'static {
        let mut events = self.registry.subscribe();
        async move {
            self.reconcile().await;
            loop {
                match events.recv().await {
                    Ok(Event::PlayerAdded(p)) => {
                        if self.is_target(&p.player_id) {
                            // A player that comes back earns a given-up
                            // daemon a fresh start, as in MA.
                            self.lock_failed().remove(&p.player_id);
                            self.reconcile().await;
                        }
                    }
                    Ok(Event::PlayerUpdated(p)) => {
                        if self.is_target(&p.player_id) {
                            self.reconcile().await;
                        }
                    }
                    // Targeted, untargeted or renamed. A reconfigured
                    // target that was given up gets a fresh start.
                    Ok(Event::PlayerConfigUpdated { player_id }) => {
                        self.lock_failed().remove(&player_id);
                        self.reconcile().await;
                    }
                    Ok(_) => {}
                    Err(RecvError::Lagged(_)) => self.reconcile().await,
                    Err(RecvError::Closed) => return,
                }
            }
        }
    }

    /// The configured targets, sorted.
    pub fn targets(&self) -> Vec<String> {
        self.state.read(|s| {
            s.players
                .iter()
                .filter(|(_, c)| c.airplay_target)
                .map(|(id, _)| id.clone())
                .collect()
        })
    }

    fn is_target(&self, player_id: &str) -> bool {
        self.state.read(|s| s.player(player_id).airplay_target)
    }

    /// What is running now, by target.
    pub async fn receivers(&self) -> Vec<ReceiverInfo> {
        let running = self.running.lock().await;
        let mut out: Vec<_> = running
            .values()
            .filter(|r| !r.task.is_finished())
            .map(|r| r.info.clone())
            .collect();
        out.sort_by(|a, b| a.target_id.cmp(&b.target_id));
        out
    }

    /// Starts, restarts and stops daemons to match the targets and their
    /// players' names.
    pub async fn reconcile(&self) {
        let targets = self.targets();
        let server_id = self.state.server_id();
        let ports = config::receiver_ports(&server_id, targets.iter().map(String::as_str));
        let mut running = self.running.lock().await;

        // Untargeted, or given up and finished.
        let gone: Vec<String> = running
            .iter()
            .filter(|(id, r)| !ports.contains_key(*id) || r.task.is_finished())
            .map(|(id, _)| id.clone())
            .collect();
        for id in gone {
            if let Some(r) = running.remove(&id) {
                stop(r).await;
            }
        }
        self.lock_failed().retain(|id| ports.contains_key(id));

        for (id, &port) in &ports {
            if self.lock_failed().contains(id) {
                continue;
            }
            let player = self.registry.get(id);
            let name = match (&player, running.get(id)) {
                (Some(p), _) => self
                    .config
                    .name_template
                    .render(&p.name, &self.config.server_name),
                // Gone from the registry: keep what is advertised.
                (None, Some(r)) => r.info.airplay_name.clone(),
                // Never seen: no name to advertise yet.
                (None, None) => continue,
            };
            let info = ReceiverInfo::new(id, name, port);
            if let Some(r) = running.get(id) {
                if r.info == info {
                    continue;
                }
                tracing::info!(
                    target_id = %id,
                    from = %r.info.airplay_name,
                    to = %info.airplay_name,
                    port,
                    "restarting the AirPlay receiver with a new name or port"
                );
                if let Some(r) = running.remove(id) {
                    stop(r).await;
                }
            }
            let volume = player.and_then(|p| p.group_volume.or(p.volume_level));
            running.insert(id.clone(), self.start(info, volume));
        }
    }

    /// Stops every daemon.
    pub async fn shutdown(&self) {
        let mut running = self.running.lock().await;
        for (_, r) in running.drain() {
            stop(r).await;
        }
    }

    fn start(&self, info: ReceiverInfo, volume: Option<u8>) -> Running {
        let (stop, stopped) = watch::channel(false);
        let daemon = Daemon {
            config: self.config.clone(),
            handler: self.handler.clone(),
            info: info.clone(),
            volume,
        };
        let failed = self.failed.clone();
        let task = tokio::spawn(async move {
            if daemon.supervise(stopped).await == Ended::GaveUp {
                lock(&failed).insert(daemon.info.target_id.clone());
            }
        });
        Running { info, stop, task }
    }

    fn lock_failed(&self) -> MutexGuard<'_, HashSet<String>> {
        lock(&self.failed)
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every critical section leaves the value consistent.
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

async fn stop(r: Running) {
    let _ = r.stop.send(true);
    if let Err(e) = r.task.await
        && e.is_panic()
    {
        tracing::error!(target_id = %r.info.target_id, "AirPlay receiver task panicked");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ended {
    Stopped,
    GaveUp,
}

/// How one run ended.
enum Exit {
    Stopped,
    Failed(io::Error),
}

/// One receiver's restart loop.
struct Daemon {
    config: SupervisorConfig,
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

/// Resolves once a stop is asked for, or the supervisor is gone.
async fn stopped(stop: &mut watch::Receiver<bool>) {
    // The guard `wait_for` returns isn't Send; drop it here.
    let _ = stop.wait_for(|s| *s).await.is_ok();
}

#[cfg(test)]
mod tests {
    use tas_core::state::PlayerConfig;
    use tas_core::{PlayerControl, PlayerState};
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

    impl Recorder {
        fn events(&self) -> Vec<Ev> {
            lock(&self.0).clone()
        }
        fn starts(&self) -> usize {
            self.events()
                .iter()
                .filter(|e| matches!(e, Ev::Started(_)))
                .count()
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

    struct Inert;

    #[async_trait::async_trait]
    impl PlayerControl for Inert {}

    struct Rig {
        _dir: tempfile::TempDir,
        registry: Arc<PlayerRegistry>,
        state: Arc<StateStore>,
        recorder: Arc<Recorder>,
        sup: Arc<ReceiverSupervisor>,
    }

    impl Rig {
        fn new(tweak: impl FnOnce(&mut SupervisorConfig)) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let state = Arc::new(StateStore::open(dir.path()).unwrap());
            let registry = Arc::new(PlayerRegistry::default());
            let recorder = Arc::new(Recorder::default());
            let mut config = SupervisorConfig::new(NativeConfig::new(
                state.server_id(),
                blastcast::ServerClock::new(),
                None,
            ));
            config.restart_delay = Duration::from_millis(10);
            tweak(&mut config);
            let sup =
                ReceiverSupervisor::new(config, registry.clone(), state.clone(), recorder.clone());
            tokio::spawn(sup.clone().run());
            Self {
                _dir: dir,
                registry,
                state,
                recorder,
                sup,
            }
        }

        fn target(&self, id: &str, on: bool) {
            self.state
                .update(|s| {
                    s.players.insert(
                        id.to_owned(),
                        PlayerConfig {
                            airplay_target: on,
                            ..Default::default()
                        },
                    )
                })
                .unwrap();
        }

        fn add(&self, id: &str, name: &str) {
            let mut p = PlayerState::new(id, "sendspin", name);
            p.volume_level = Some(40);
            self.registry.add(p, Arc::new(Inert)).unwrap();
        }

        /// The port `id` gets as the only target.
        fn port(&self, id: &str) -> u16 {
            config::receiver_ports(&self.state.server_id(), [id])[id]
        }

        async fn running(&self) -> Vec<ReceiverInfo> {
            self.sup.receivers().await
        }
    }

    async fn until(what: &str, mut f: impl AsyncFnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !f().await {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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

    /// A receiver runs in-process: it listens on the target's port,
    /// answers there, and lets the port go when it stops.
    #[tokio::test]
    async fn a_target_starts_once_its_player_is_seen() {
        let rig = Rig::new(|_| {});
        rig.target("ss-kitchen", true);
        rig.target("ss-other", false);
        rig.sup.reconcile().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(rig.running().await.is_empty());

        rig.add("ss-kitchen", "Kitchen");
        rig.add("ss-other", "Other");
        until("the receiver", async || rig.recorder.starts() == 1).await;
        let r = rig.running().await[0].clone();
        assert_eq!(r.target_id, "ss-kitchen");
        assert_eq!(r.airplay_name, "Kitchen");
        assert_eq!(r.port, rig.port("ss-kitchen"));

        let mut tcp = tokio::net::TcpStream::connect(("127.0.0.1", r.port))
            .await
            .unwrap();
        tcp.write_all(b"OPTIONS * RTSP/1.0\r\nCSeq: 1\r\n\r\n")
            .await
            .unwrap();
        let mut line = String::new();
        tokio::io::BufReader::new(&mut tcp)
            .read_line(&mut line)
            .await
            .unwrap();
        assert_eq!(line, "RTSP/1.0 200 OK\r\n");

        rig.sup.shutdown().await;
        assert_eq!(
            rig.recorder.events().last(),
            Some(&Ev::Stopped("ss-kitchen".into()))
        );
        tokio::net::TcpListener::bind(("0.0.0.0", r.port))
            .await
            .expect("the port is free again");
    }

    #[tokio::test]
    async fn a_receiver_outlives_its_player_and_follows_a_rename() {
        let rig = Rig::new(|_| {});
        rig.target("ss-kitchen", true);
        rig.add("ss-kitchen", "Kitchen");
        until("the receiver", async || rig.running().await.len() == 1).await;
        let before = rig.running().await[0].clone();

        // Disconnected: the AirPlay device stays.
        rig.registry.remove("ss-kitchen");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(rig.running().await, std::slice::from_ref(&before));
        assert_eq!(rig.recorder.starts(), 1);

        // Back under the same name: nothing to do.
        rig.add("ss-kitchen", "Kitchen");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(rig.recorder.starts(), 1);

        // Renamed: restarted under the new name, on the same port.
        rig.registry
            .update("ss-kitchen", |p| p.name = "Den".into())
            .unwrap();
        until("the rename", async || {
            rig.running().await.first().map(|r| r.airplay_name.as_str()) == Some("Den")
        })
        .await;
        let after = rig.running().await[0].clone();
        assert_eq!(after.port, before.port);
        until("the restart", async || rig.recorder.starts() == 2).await;
        let events = rig.recorder.events();
        let stop = events
            .iter()
            .position(|e| *e == Ev::Stopped("ss-kitchen".into()));
        let restart = events
            .iter()
            .rposition(|e| *e == Ev::Started("ss-kitchen".into()));
        assert!(stop < restart, "{events:?}");
    }

    /// A port someone else holds fails each run's bind.
    #[tokio::test]
    async fn a_failing_receiver_is_restarted_then_given_up() {
        let rig = Rig::new(|c| c.max_restarts = 2);
        let held = raop::bind(rig.port("ss-kitchen")).await.unwrap();
        rig.target("ss-kitchen", true);
        rig.add("ss-kitchen", "Kitchen");
        // The first run and two restarts, none of which started.
        until("giving up", async || {
            rig.sup.lock_failed().contains("ss-kitchen")
        })
        .await;
        assert!(rig.running().await.is_empty());
        rig.sup.reconcile().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(rig.recorder.starts(), 0);

        // The player coming back earns it another try, which binds now.
        drop(held);
        rig.registry.remove("ss-kitchen");
        rig.add("ss-kitchen", "Kitchen");
        until("the retry", async || rig.recorder.starts() == 1).await;
    }

    #[tokio::test]
    async fn untargeting_stops_it() {
        let rig = Rig::new(|_| {});
        rig.target("ss-kitchen", true);
        rig.add("ss-kitchen", "Kitchen");
        until("the receiver", async || rig.recorder.starts() == 1).await;

        rig.target("ss-kitchen", false);
        rig.sup.reconcile().await;
        assert!(rig.running().await.is_empty());
        assert_eq!(
            rig.recorder.events().last(),
            Some(&Ev::Stopped("ss-kitchen".into()))
        );
    }

    /// A config saved through the registry, as `config/players/save`
    /// saves one, reconciles without anyone calling it.
    #[tokio::test]
    async fn a_config_saved_through_the_registry_reconciles() {
        let rig = Rig::new(|_| {});
        rig.add("ss-kitchen", "Kitchen");
        let save = |on| {
            rig.target("ss-kitchen", on);
            let config = rig.state.read(|s| s.player("ss-kitchen"));
            rig.registry.configure("ss-kitchen", config);
        };
        save(true);
        until("the receiver", async || rig.running().await.len() == 1).await;
        save(false);
        until("it stops", async || rig.running().await.is_empty()).await;
    }
}
