//! One AirPlay receiver per AirPlay target, kept running: a native one
//! ([`crate::raop`]), or a shairport-sync.
//!
//! A **target** is a player or group player whose config has
//! `airplay_target` set (group players are configured by their
//! `syncgroup_*` id). The [`ReceiverSupervisor`] reconciles the running
//! receivers (called daemons here, whichever [`Backend`] runs them) with
//! the targets:
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
//! A native receiver's run binds the target's port and serves senders
//! until it is stopped; a bind that fails is restarted like a daemon that
//! exited. A shairport-sync run creates the audio and metadata fifos and the config file in the
//! run dir, opens the metadata fifo **read-write before shairport starts**
//! (a fifo with no writer reads EOF in a tight loop, and one with no
//! reader blocks the session hooks' `echo`), and passes what it reads to
//! the [`ReceiverHandler`]. A daemon that exits is restarted after
//! [`SupervisorConfig::restart_delay`]; after
//! [`SupervisorConfig::max_restarts`] restarts in a row that each ended
//! within [`SupervisorConfig::stable_after`], it is given up until the
//! player is added again or the target is reconfigured.
//!
//! The fifos only ever carry audio through; nothing is written to disk.

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tas_core::state::StateStore;
use tas_core::{Event, PlayerRegistry};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::net::unix::pipe;
use tokio::process::{Child, Command};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use tas_audio::PcmChunk;

use crate::config::{self, NameTemplate, ReceiverInfo};
use crate::metadata::MetadataEvent;
use crate::raop::{self, NativeConfig};

/// What runs each receiver.
#[derive(Debug, Clone)]
pub enum Backend {
    /// [`crate::raop`], in this process.
    Native(NativeConfig),
    Shairport {
        /// A shairport-sync that [`crate::binary::find`] accepted.
        binary: PathBuf,
        /// Where the fifos and config files go. It must be a
        /// [plain path](config::is_plain_path).
        run_dir: PathBuf,
    },
}

#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    pub backend: Backend,
    pub name_template: NameTemplate,
    /// `{server}` in the name template.
    pub server_name: String,
    /// The network interface to advertise on; every one when `None`.
    pub interface: Option<String>,
    pub restart_delay: Duration,
    pub max_restarts: u32,
    /// A run that lasts this long resets the restart count.
    pub stable_after: Duration,
    /// How long a stopped shairport-sync gets to exit after SIGTERM (and
    /// send its mDNS goodbye) before it is killed.
    pub stop_timeout: Duration,
}

impl SupervisorConfig {
    /// Native receivers.
    pub fn native(native: NativeConfig) -> Self {
        Self::with(Backend::Native(native))
    }

    /// shairport-sync receivers.
    pub fn new(binary: PathBuf, run_dir: PathBuf) -> Self {
        Self::with(Backend::Shairport { binary, run_dir })
    }

    fn with(backend: Backend) -> Self {
        // MA restarts after 2 s, up to 5 times.
        Self {
            backend,
            name_template: NameTemplate::default(),
            server_name: String::new(),
            interface: None,
            restart_delay: Duration::from_secs(2),
            max_restarts: 5,
            stable_after: Duration::from_secs(60),
            stop_timeout: Duration::from_secs(3),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    #[error(
        "run dir {0:?} can't be put in a shairport-sync hook; use a path of letters, digits and /._-+,@ only"
    )]
    RunDir(PathBuf),
}

/// Told about each run of each receiver, in order: `started`, any number
/// of `metadata`, `event` and `audio`, then `stopped`. Called from the
/// receiver's tasks, so it must not block.
pub trait ReceiverHandler: Send + Sync + 'static {
    /// The receiver is about to start: shairport-sync with these fifos, or
    /// a native one.
    fn started(&self, _receiver: &ReceiverInfo) {}
    /// Bytes read from shairport-sync's metadata fifo: its `<item>`s and
    /// the hooks' marker lines, split wherever the reads fell.
    fn metadata(&self, _receiver: &ReceiverInfo, _bytes: &[u8]) {}
    /// What a native receiver's sender said, already parsed.
    fn event(&self, _receiver: &ReceiverInfo, _event: &MetadataEvent) {}
    /// A native receiver's audio, in real time: s16 44.1 kHz stereo, as
    /// shairport-sync writes its fifo.
    fn audio(&self, _receiver: &ReceiverInfo, _chunk: &PcmChunk) {}
    /// The run ended; its fifos are about to be removed.
    fn stopped(&self, _receiver: &ReceiverInfo) {}
}

/// Discards everything.
impl ReceiverHandler for () {}

/// Tells each handler in turn, such as the audio reader and the metadata
/// parser.
impl ReceiverHandler for Vec<Arc<dyn ReceiverHandler>> {
    fn started(&self, receiver: &ReceiverInfo) {
        self.iter().for_each(|h| h.started(receiver));
    }
    fn metadata(&self, receiver: &ReceiverInfo, bytes: &[u8]) {
        self.iter().for_each(|h| h.metadata(receiver, bytes));
    }
    fn event(&self, receiver: &ReceiverInfo, event: &MetadataEvent) {
        self.iter().for_each(|h| h.event(receiver, event));
    }
    fn audio(&self, receiver: &ReceiverInfo, chunk: &PcmChunk) {
        self.iter().for_each(|h| h.audio(receiver, chunk));
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
    ) -> Result<Arc<Self>, SupervisorError> {
        if let Backend::Shairport { run_dir, .. } = &config.backend
            && !config::is_plain_path(run_dir)
        {
            return Err(SupervisorError::RunDir(run_dir.clone()));
        }
        Ok(Arc::new(Self {
            config,
            registry,
            state,
            handler,
            running: tokio::sync::Mutex::new(HashMap::new()),
            failed: Arc::default(),
        }))
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

    /// `native` or `shairport`, for logs and `/debug`.
    pub fn backend_name(&self) -> &'static str {
        match self.config.backend {
            Backend::Native(_) => "native",
            Backend::Shairport { .. } => "shairport",
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
            let interface = self.config.interface.clone();
            let info = match &self.config.backend {
                Backend::Native(_) => ReceiverInfo::native(id, name, port, interface),
                Backend::Shairport { run_dir, .. } => {
                    ReceiverInfo::new(run_dir, id, name, port, interface)
                }
            };
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
    Exited(ExitStatus),
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
                Exit::Exited(status) => format!("exited with {status}"),
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
        match &self.config.backend {
            Backend::Native(native) => self.run_native(native, stop).await,
            Backend::Shairport { binary, run_dir } => {
                self.run_shairport(binary, run_dir, stop).await
            }
        }
    }

    async fn run_native(&self, native: &NativeConfig, stop: &mut watch::Receiver<bool>) -> Exit {
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
            native.clone(),
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

    async fn run_shairport(
        &self,
        binary: &Path,
        run_dir: &Path,
        stop: &mut watch::Receiver<bool>,
    ) -> Exit {
        let metadata = match self.prepare(run_dir) {
            Ok(m) => m,
            Err(e) => {
                self.cleanup();
                return Exit::Failed(io::Error::new(
                    e.kind(),
                    format!("setting up {}: {e}", run_dir.display()),
                ));
            }
        };
        self.handler.started(&self.info);
        let exit = match self.spawn(binary) {
            Ok(child) => self.watch(child, metadata, stop).await,
            Err(e) => Exit::Failed(io::Error::new(
                e.kind(),
                format!("starting {}: {e}", binary.display()),
            )),
        };
        self.handler.stopped(&self.info);
        self.cleanup();
        exit
    }

    /// Makes the fifos and config, and opens the metadata fifo.
    fn prepare(&self, run_dir: &Path) -> io::Result<pipe::Receiver> {
        create_run_dir(run_dir)?;
        self.cleanup();
        let files = self.info.files();
        mkfifo(&files.audio_pipe)?;
        mkfifo(&files.metadata_pipe)?;
        write_private(&files.config_file, &self.info.render(self.volume))?;
        // Read-write: we are a writer too, so a read never sees EOF while
        // shairport is between writes, and a hook's echo never blocks.
        pipe::OpenOptions::new()
            .read_write(true)
            .open_receiver(&files.metadata_pipe)
    }

    fn cleanup(&self) {
        let files = self.info.files();
        for path in [&files.audio_pipe, &files.metadata_pipe, &files.config_file] {
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => tracing::warn!(path = %path.display(), error = %e, "can't remove"),
            }
        }
    }

    fn spawn(&self, binary: &Path) -> io::Result<Child> {
        tracing::info!(
            target_id = %self.info.target_id,
            name = %self.info.airplay_name,
            port = self.info.port,
            "starting shairport-sync"
        );
        Command::new(binary)
            .arg("--configfile")
            .arg(&self.info.files().config_file)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
    }

    /// Logs the daemon's output and forwards its metadata until it exits or
    /// is stopped.
    async fn watch(
        &self,
        mut child: Child,
        mut metadata: pipe::Receiver,
        stop: &mut watch::Receiver<bool>,
    ) -> Exit {
        let mut stderr = child.stderr.take().map(|s| BufReader::new(s).lines());
        let mut stdout = child.stdout.take().map(|s| BufReader::new(s).lines());
        let mut buf = vec![0; 16 * 1024];
        let mut metadata_open = true;
        loop {
            tokio::select! {
                status = child.wait() => {
                    return match status {
                        Ok(status) => Exit::Exited(status),
                        Err(e) => Exit::Failed(e),
                    };
                }
                () = stopped(stop) => {
                    self.terminate(&mut child).await;
                    return Exit::Stopped;
                }
                line = next_line(&mut stderr) => self.log_line(line),
                line = next_line(&mut stdout) => self.log_line(line),
                n = metadata.read(&mut buf), if metadata_open => match n {
                    Ok(0) => metadata_open = false,
                    Ok(n) => self.handler.metadata(&self.info, &buf[..n]),
                    Err(e) => {
                        tracing::warn!(target_id = %self.info.target_id, error = %e, "reading the metadata fifo");
                        metadata_open = false;
                    }
                },
            }
        }
    }

    /// SIGTERM, so it can send its mDNS goodbye, then SIGKILL.
    async fn terminate(&self, child: &mut Child) {
        let id = &self.info.target_id;
        if let Some(pid) = child.id().and_then(|p| i32::try_from(p).ok())
            && let Some(pid) = rustix::process::Pid::from_raw(pid)
        {
            let _ = rustix::process::kill_process(pid, rustix::process::Signal::TERM);
            if let Ok(status) = tokio::time::timeout(self.config.stop_timeout, child.wait()).await {
                tracing::info!(target_id = %id, status = ?status.ok(), "shairport-sync stopped");
                return;
            }
            tracing::warn!(target_id = %id, "shairport-sync ignored SIGTERM; killing it");
        }
        let _ = child.kill().await;
    }

    fn log_line(&self, line: Option<String>) {
        let Some(line) = line else { return };
        let line = line.trim();
        if line.is_empty() {
            return;
        }
        let id = &self.info.target_id;
        let lower = line.to_ascii_lowercase();
        if lower.contains("could not establish a service on port")
            && !line.contains(&format!("port {} ", self.info.port))
        {
            // shairport-sync 5.x resets the port to 5000 after reading its
            // config (shairport.c, "config.port = 5000"), so a second
            // receiver can never bind.
            tracing::error!(
                target_id = %id,
                port = self.info.port,
                "shairport-sync ignored the configured port: {line} \
                 (5.x does; use 4.3.x, as Music Assistant does)"
            );
        } else if lower.contains("fatal error") || lower.contains("unknown option") {
            tracing::error!(target_id = %id, "shairport-sync: {line}");
        } else if lower.contains("connection from") {
            tracing::info!(target_id = %id, "AirPlay sender connected: {line}");
        } else {
            tracing::debug!(target_id = %id, "shairport-sync: {line}");
        }
    }
}

/// Resolves once a stop is asked for, or the supervisor is gone.
async fn stopped(stop: &mut watch::Receiver<bool>) {
    // The guard `wait_for` returns isn't Send; drop it here.
    let _ = stop.wait_for(|s| *s).await.is_ok();
}

/// The next line of a child's output; pends forever once it has ended, so
/// a closed stream drops out of the select.
async fn next_line<R: tokio::io::AsyncBufRead + Unpin>(
    lines: &mut Option<tokio::io::Lines<R>>,
) -> Option<String> {
    let Some(reader) = lines else {
        return std::future::pending().await;
    };
    match reader.next_line().await {
        Ok(Some(line)) => Some(line),
        Ok(None) | Err(_) => {
            *lines = None;
            None
        }
    }
}

fn create_run_dir(dir: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
}

fn mkfifo(path: &Path) -> io::Result<()> {
    rustix::fs::mkfifoat(
        rustix::fs::CWD,
        path,
        rustix::fs::Mode::from_raw_mode(0o600),
    )
    .map_err(io::Error::from)
}

fn write_private(path: &Path, text: &str) -> io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(text.as_bytes())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::FileTypeExt;

    use tas_core::state::PlayerConfig;
    use tas_core::{PlayerControl, PlayerState};

    use super::*;
    use crate::testing::script;

    /// Answers `--version`; otherwise writes to the metadata fifo, runs the
    /// config's play-begin hook as shairport would, then does `then`.
    fn fake_shairport(dir: &Path, then: &str) -> PathBuf {
        script(
            dir,
            "shairport-sync",
            &format!(
                r#"[ "$1" = --version ] && {{ echo 4.3.7-fake; exit 0; }}
conf="$2"
meta=$(sed -n 's/^[[:space:]]*pipe_name = "\(.*\)";/\1/p' "$conf")
printf 'hello ' > "$meta"
begin=$(sed -n 's/.*run_this_before_entering_active_state = "\(.*\)";/\1/p' "$conf")
eval "$begin"
echo "connection from 10.0.0.9" >&2
{then}"#
            ),
        )
    }

    const RUNS: &str = "exec sleep 1000";
    const CRASHES: &str = "exit 1";
    const IGNORES_TERM: &str = "trap '' TERM; exec sleep 1000";

    #[derive(Debug, Clone, PartialEq)]
    enum Ev {
        Started(String),
        Metadata(String, Vec<u8>),
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
        fn metadata(&self) -> String {
            let bytes: Vec<u8> = self
                .events()
                .into_iter()
                .filter_map(|e| match e {
                    Ev::Metadata(_, b) => Some(b),
                    _ => None,
                })
                .flatten()
                .collect();
            String::from_utf8(bytes).unwrap()
        }
    }

    impl ReceiverHandler for Recorder {
        fn started(&self, r: &ReceiverInfo) {
            lock(&self.0).push(Ev::Started(r.target_id.clone()));
        }
        fn metadata(&self, r: &ReceiverInfo, bytes: &[u8]) {
            lock(&self.0).push(Ev::Metadata(r.target_id.clone(), bytes.to_vec()));
        }
        fn stopped(&self, r: &ReceiverInfo) {
            lock(&self.0).push(Ev::Stopped(r.target_id.clone()));
        }
    }

    struct Inert;

    #[async_trait::async_trait]
    impl PlayerControl for Inert {}

    struct Rig {
        dir: tempfile::TempDir,
        registry: Arc<PlayerRegistry>,
        state: Arc<StateStore>,
        recorder: Arc<Recorder>,
        sup: Arc<ReceiverSupervisor>,
    }

    impl Rig {
        fn new(then: &str, tweak: impl FnOnce(&mut SupervisorConfig)) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let bin = fake_shairport(dir.path(), then);
            let state = Arc::new(StateStore::open(&dir.path().join("data")).unwrap());
            let registry = Arc::new(PlayerRegistry::default());
            let recorder = Arc::new(Recorder::default());
            let mut config = SupervisorConfig::new(bin, dir.path().join("run"));
            config.restart_delay = Duration::from_millis(10);
            config.stop_timeout = Duration::from_millis(200);
            tweak(&mut config);
            let sup =
                ReceiverSupervisor::new(config, registry.clone(), state.clone(), recorder.clone())
                    .unwrap();
            tokio::spawn(sup.clone().run());
            Self {
                dir,
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

    fn is_fifo(path: &Path) -> bool {
        std::fs::metadata(path).is_ok_and(|m| m.file_type().is_fifo())
    }

    #[test]
    fn a_vec_of_handlers_tells_each_one() {
        let (a, b) = (Arc::new(Recorder::default()), Arc::new(Recorder::default()));
        let both: Vec<Arc<dyn ReceiverHandler>> = vec![a.clone(), b.clone()];
        let info = ReceiverInfo::new(Path::new("/run/tas"), "ss-k", "K".into(), 7000, None);
        both.started(&info);
        both.metadata(&info, b"x");
        both.stopped(&info);
        let id = || "ss-k".to_owned();
        for r in [a, b] {
            assert_eq!(
                r.events(),
                [
                    Ev::Started(id()),
                    Ev::Metadata(id(), b"x".to_vec()),
                    Ev::Stopped(id())
                ]
            );
        }
    }

    #[tokio::test]
    async fn a_target_starts_once_its_player_is_seen() {
        let rig = Rig::new(RUNS, |_| {});
        rig.target("ss-kitchen", true);
        rig.target("ss-other", false);
        rig.sup.reconcile().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(rig.running().await.is_empty());

        rig.add("ss-kitchen", "Kitchen");
        rig.add("ss-other", "Other");
        until("the receiver", async || rig.running().await.len() == 1).await;
        let r = &rig.running().await[0];
        assert_eq!(r.target_id, "ss-kitchen");
        assert_eq!(r.airplay_name, "Kitchen");
        let server_id = rig.state.server_id();
        assert_eq!(
            r.port,
            config::receiver_ports(&server_id, ["ss-kitchen"])["ss-kitchen"]
        );
        let files = r.files();
        assert!(is_fifo(&files.audio_pipe) && is_fifo(&files.metadata_pipe));
        let text = std::fs::read_to_string(&files.config_file).unwrap();
        assert!(text.contains("name = \"Kitchen\";"), "{text}");
        // The player's volume, 40, as AirPlay dB.
        assert!(text.contains("default_airplay_volume = -18.0;"), "{text}");

        // What shairport and the hook wrote reaches the handler.
        until("the metadata", async || {
            rig.recorder.metadata() == "hello MA_PLAY_BEGIN\n"
        })
        .await;
        assert_eq!(rig.recorder.starts(), 1);
    }

    #[tokio::test]
    async fn a_receiver_outlives_its_player_and_follows_a_rename() {
        let rig = Rig::new(RUNS, |_| {});
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
        let text = std::fs::read_to_string(&after.files().config_file).unwrap();
        assert!(text.contains("name = \"Den\";"), "{text}");
        let events = rig.recorder.events();
        let stop = events
            .iter()
            .position(|e| *e == Ev::Stopped("ss-kitchen".into()));
        let restart = events
            .iter()
            .rposition(|e| *e == Ev::Started("ss-kitchen".into()));
        assert!(stop < restart, "{events:?}");
    }

    #[tokio::test]
    async fn a_crashing_daemon_is_restarted_then_given_up() {
        let rig = Rig::new(CRASHES, |c| c.max_restarts = 2);
        rig.target("ss-kitchen", true);
        rig.add("ss-kitchen", "Kitchen");
        // The first run and two restarts.
        until("giving up", async || {
            rig.recorder.starts() == 3 && rig.running().await.is_empty()
        })
        .await;
        rig.sup.reconcile().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(rig.recorder.starts(), 3);

        // The player coming back earns it another try.
        rig.registry.remove("ss-kitchen");
        rig.add("ss-kitchen", "Kitchen");
        until("the retry", async || rig.recorder.starts() > 3).await;
    }

    #[tokio::test]
    async fn untargeting_stops_it_and_removes_its_files() {
        let rig = Rig::new(RUNS, |_| {});
        rig.target("ss-kitchen", true);
        rig.add("ss-kitchen", "Kitchen");
        until("the receiver", async || rig.running().await.len() == 1).await;
        let r = rig.running().await[0].clone();

        rig.target("ss-kitchen", false);
        rig.sup.reconcile().await;
        assert!(rig.running().await.is_empty());
        assert_eq!(
            rig.recorder.events().last(),
            Some(&Ev::Stopped("ss-kitchen".into()))
        );
        let files = r.files();
        for path in [&files.audio_pipe, &files.metadata_pipe, &files.config_file] {
            assert!(!path.exists(), "{} left behind", path.display());
        }
    }

    /// A config saved through the registry, as `config/players/save`
    /// saves one, reconciles without anyone calling it.
    #[tokio::test]
    async fn a_config_saved_through_the_registry_reconciles() {
        let rig = Rig::new(RUNS, |_| {});
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

    #[tokio::test]
    async fn a_daemon_that_ignores_sigterm_is_killed() {
        let rig = Rig::new(IGNORES_TERM, |c| {
            c.stop_timeout = Duration::from_millis(100)
        });
        rig.target("a", true);
        rig.target("b", true);
        rig.add("a", "A");
        rig.add("b", "B");
        until("both receivers", async || rig.running().await.len() == 2).await;
        let ports: HashSet<u16> = rig.running().await.iter().map(|r| r.port).collect();
        assert_eq!(ports.len(), 2);
        // Both scripts are past their hooks, so about to set the trap.
        until("both hooks", async || {
            rig.recorder.metadata().matches("MA_PLAY_BEGIN").count() == 2
        })
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;

        let began = Instant::now();
        tokio::time::timeout(Duration::from_secs(3), rig.sup.shutdown())
            .await
            .expect("shutdown hung");
        assert!(began.elapsed() >= Duration::from_millis(100));
        assert!(rig.running().await.is_empty());
        let run_dir = rig.dir.path().join("run");
        assert_eq!(std::fs::read_dir(run_dir).unwrap().count(), 0);
    }

    /// A native receiver runs in-process: it listens on the target's port,
    /// answers there, and lets the port go when it stops.
    #[tokio::test]
    async fn a_native_receiver_listens_on_the_targets_port() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(StateStore::open(dir.path()).unwrap());
        let registry = Arc::new(PlayerRegistry::default());
        let recorder = Arc::new(Recorder::default());
        let config = SupervisorConfig::native(NativeConfig::new(state.server_id(), None));
        let sup =
            ReceiverSupervisor::new(config, registry.clone(), state.clone(), recorder.clone())
                .unwrap();
        assert_eq!(sup.backend_name(), "native");
        tokio::spawn(sup.clone().run());
        state
            .update(|s| {
                s.players.insert(
                    "ss-kitchen".into(),
                    PlayerConfig {
                        airplay_target: true,
                        ..Default::default()
                    },
                )
            })
            .unwrap();
        let mut p = PlayerState::new("ss-kitchen", "sendspin", "Kitchen");
        p.volume_level = Some(40);
        registry.add(p, Arc::new(Inert)).unwrap();
        until("the receiver", async || recorder.starts() == 1).await;
        let r = sup.receivers().await[0].clone();
        assert_eq!(r.fifos, None);

        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
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

        sup.shutdown().await;
        assert_eq!(
            recorder.events().last(),
            Some(&Ev::Stopped("ss-kitchen".into()))
        );
        tokio::net::TcpListener::bind(("0.0.0.0", r.port))
            .await
            .expect("the port is free again");
    }

    #[tokio::test]
    async fn a_run_dir_that_needs_quoting_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let state = Arc::new(StateStore::open(dir.path()).unwrap());
        let config = SupervisorConfig::new("x".into(), dir.path().join("my run"));
        let err = ReceiverSupervisor::new(config, Arc::default(), state, Arc::new(()));
        assert!(matches!(err, Err(SupervisorError::RunDir(_))));
    }
}
