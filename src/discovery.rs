//! Finding AirPlay speakers and running a [`bridge`] for each.
//!
//! [`discover`] browses `_raop._tcp` and hands what it finds to
//! [`AirplayOutputs`], which keeps one bridge per speaker while it is
//! advertised, and stops it when the speaker says goodbye. A bridge the
//! server drops (a second connection with its id) is started again after
//! a pause.
//!
//! Speakers on this machine are skipped: they are our own AirPlay
//! receivers, and streaming to them would feed a player its own output.

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use blastcast_mdns::{BrowseEvent, Discovered, Mdns};
use blastcast_sendspin::SendspinServer;
use blastcast_telemetry::{Roster, Speaker};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::Instrument;

use crate::bridge::{self, Volume};
use crate::device::{RAOP_SERVICE_TYPE, RaopDevice};
use crate::rtp::NtpClock;

/// The pause before a bridge the server dropped is started again.
const RESTART_AFTER: Duration = Duration::from_secs(2);

struct Running {
    device: RaopDevice,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

/// Every AirPlay speaker's bridge, by advert name. Share it as
/// `Arc<AirplayOutputs>`.
pub struct AirplayOutputs {
    server: Arc<SendspinServer>,
    ntp: NtpClock,
    local: Vec<IpAddr>,
    running: Mutex<BTreeMap<String, Running>>,
    /// Kept by player id, so a speaker that comes back keeps its volume.
    volumes: Mutex<BTreeMap<String, Arc<Mutex<Volume>>>>,
    /// Adverts already logged as unusable, so a rebrowse doesn't repeat it.
    skipped: Mutex<BTreeSet<String>>,
    /// Every speaker with a bridge, for `/v1/ws`.
    roster: Roster<Speaker>,
}

impl std::fmt::Debug for AirplayOutputs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AirplayOutputs")
            .field("speakers", &self.speakers())
            .finish_non_exhaustive()
    }
}

impl AirplayOutputs {
    pub fn new(server: Arc<SendspinServer>) -> Arc<Self> {
        let local = if_addrs::get_if_addrs()
            .map(|addrs| addrs.into_iter().map(|a| a.ip()).collect())
            .unwrap_or_default();
        Self::with_local_addrs(server, local)
    }

    /// As [`AirplayOutputs::new`], with the addresses counted as this
    /// machine's given, for tests that run speakers on loopback.
    pub fn with_local_addrs(server: Arc<SendspinServer>, local: Vec<IpAddr>) -> Arc<Self> {
        let ntp = NtpClock::new(server.config().clock);
        Arc::new(Self {
            server,
            ntp,
            local,
            running: Mutex::new(BTreeMap::new()),
            volumes: Mutex::new(BTreeMap::new()),
            skipped: Mutex::new(BTreeSet::new()),
            roster: Roster::default(),
        })
    }

    /// Every speaker with a bridge running, and its RAOP session's state.
    pub fn roster(&self) -> &Roster<Speaker> {
        &self.roster
    }

    /// The NTP clock every bridge's sessions send: the server clock.
    pub fn ntp(&self) -> NtpClock {
        self.ntp
    }

    /// The speakers with a bridge running.
    pub fn speakers(&self) -> Vec<RaopDevice> {
        lock(&self.running)
            .values()
            .map(|r| r.device.clone())
            .collect()
    }

    /// A speaker was resolved, or its advert changed.
    pub fn resolved(self: &Arc<Self>, found: &Discovered) {
        let device = match RaopDevice::from_discovered(found) {
            Ok(device) => device,
            Err(e) => {
                if lock(&self.skipped).insert(found.fullname.clone()) {
                    tracing::info!(name = found.instance, reason = %e, "skipping airplay speaker");
                }
                return;
            }
        };
        if self.local.contains(&device.ip()) {
            if lock(&self.skipped).insert(found.fullname.clone()) {
                tracing::debug!(
                    name = found.instance,
                    "skipping an airplay receiver on this machine"
                );
            }
            return;
        }
        self.add(device);
    }

    /// Runs a bridge for `device`, replacing one for the same advert that
    /// differs.
    pub fn add(self: &Arc<Self>, device: RaopDevice) {
        let mut running = lock(&self.running);
        if let Some(old) = running.get(&device.fullname) {
            if old.device == device {
                return;
            }
            tracing::info!(
                name = device.name,
                "airplay speaker changed; restarting its bridge"
            );
            let _ = old.stop.send(true);
        }
        tracing::info!(
            player_id = device.player_id,
            name = device.name,
            addr = %device.addr,
            model = device.model,
            "airplay speaker"
        );
        self.roster.set(Speaker {
            id: device.player_id.clone(),
            name: device.name.clone(),
            address: device.addr.to_string(),
            model: device.model.clone(),
            session: None,
        });
        let (stop, stopped) = watch::channel(false);
        let volume = lock(&self.volumes)
            .entry(device.player_id.clone())
            .or_default()
            .clone();
        let span =
            tracing::info_span!("airplay_out", player_id = %device.player_id, name = %device.name);
        let task = tokio::spawn(
            keep_running(
                self.server.clone(),
                device.clone(),
                self.ntp,
                volume,
                self.roster.clone(),
                stopped,
            )
            .instrument(span),
        );
        running.insert(device.fullname.clone(), Running { device, stop, task });
    }

    /// A speaker's advert went away: its bridge stops, and its player goes.
    pub fn removed(&self, fullname: &str) {
        lock(&self.skipped).remove(fullname);
        if let Some(gone) = lock(&self.running).remove(fullname) {
            tracing::info!(name = gone.device.name, "airplay speaker gone");
            self.roster.remove(&gone.device.player_id);
            let _ = gone.stop.send(true);
        }
    }

    /// Stops every bridge, tearing down their sessions, and waits.
    pub async fn shutdown(&self) {
        let all: Vec<Running> = std::mem::take(&mut *lock(&self.running))
            .into_values()
            .collect();
        for r in &all {
            let _ = r.stop.send(true);
        }
        for r in all {
            let _ = r.task.await;
        }
        self.roster.retain(|_| false);
    }
}

/// Runs the bridge until it is stopped, starting it again whenever the
/// server ends it.
async fn keep_running(
    server: Arc<SendspinServer>,
    device: RaopDevice,
    ntp: NtpClock,
    volume: Arc<Mutex<Volume>>,
    roster: Roster<Speaker>,
    mut stopped: watch::Receiver<bool>,
) {
    loop {
        let end = bridge::run(
            server.clone(),
            device.clone(),
            ntp,
            volume.clone(),
            roster.clone(),
            stopped.clone(),
        )
        .await;
        if *stopped.borrow() {
            return;
        }
        tracing::info!(?end, "the bridge's connection ended; starting it again");
        tokio::select! {
            _ = stopped.wait_for(|s| *s) => return,
            () = sleep(RESTART_AFTER) => {}
        }
    }
}

/// Browses `_raop._tcp` and feeds what it finds to `outputs`, for as long
/// as it runs. The browse restarts every `rebrowse`, as the Sendspin one
/// does ([`blastcast_sendspin::dialer::discover`]).
pub async fn discover(outputs: Arc<AirplayOutputs>, mdns: Mdns, rebrowse: Duration) {
    loop {
        match mdns.browse(RAOP_SERVICE_TYPE) {
            Ok(mut browser) => {
                let deadline = sleep(rebrowse);
                tokio::pin!(deadline);
                loop {
                    let event = tokio::select! {
                        _ = &mut deadline => break,
                        event = browser.next() => event,
                    };
                    match event {
                        Some(BrowseEvent::Resolved(found)) => outputs.resolved(&found),
                        Some(BrowseEvent::Removed { fullname }) => outputs.removed(&fullname),
                        None => {
                            tracing::warn!("airplay browse stopped; restarting");
                            break;
                        }
                    }
                }
                drop(browser);
            }
            Err(e) => tracing::warn!(error = %e, "can't browse for airplay speakers"),
        }
        sleep(Duration::from_secs(1)).await;
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}
