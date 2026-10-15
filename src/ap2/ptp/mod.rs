//! AirPlay 2's timing: a passive PTP follower, one per process, that turns
//! each grandmaster's clock into a blastcast [`ClockMap`].
//!
//! The phone is the PTP grandmaster for every device it plays to, and
//! says when audio plays on its PTP clock (SETRATEANCHORTIME, with the
//! grandmaster's clock id as `networkTimeTimelineID`). The follower reads
//! its Sync, Follow_Up and Announce messages, pairs each Sync with when we
//! read it on the server clock, and keeps an [`Estimator`] per clock, so
//! [`Ptp::map`] puts any timeline id on the server clock. It sends
//! nothing, as shairport-sync's nqptp doesn't.
//!
//! **Ports.** PTP is UDP 319 (event: Sync) and 320 (general: Follow_Up,
//! Announce), and only one socket on a host can own them, so a [`Ptp`] is
//! bound once, at start, and shared by every AirPlay 2 target. Both are
//! privileged: without `CAP_NET_BIND_SERVICE` the bind fails, that is
//! logged once, and [`Ptp::follow`] says why, so SETUP can refuse buffered
//! audio rather than the server not starting. nqptp can't run beside it.
//!
//! **Peers.** Each AirPlay 2 session [`follow`](Ptp::follow)s the
//! addresses SETUP's `timingPeerInfo` names, updated by SETPEERS
//! ([`peer_addresses`] reads both). Messages from anybody else are read
//! and dropped. A session's grandmaster is the best its peers announce,
//! by the best master clock algorithm's order; when the phone hands
//! timing to another device the session's [`Follow::map`] switches to
//! the new grandmaster's clock, which is logged, kept for `/debug`
//! ([`Ptp::report`]) and marked in telemetry.
//!
//! **Telemetry.** Each clock is an entity, `ptp:` and its id in hex, with
//! a [`Series::PtpOffset`], [`Series::PtpDrift`] and [`Series::PtpError`]
//! per Sync and a [`Series::Grandmaster`] mark when a session takes it.

pub mod estimator;
pub mod message;

use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use blastcast::ClockMap;
use tas_audio::ServerClock;
use tas_proto::Series;
use tas_telemetry::{Probe, Telemetry};
use tokio::net::UdpSocket;
use tokio::sync::oneshot;

pub use estimator::Estimator;
pub use message::{Announce, Body, Message, PortIdentity, Timestamp};

/// PTP's event port: Sync.
pub const EVENT_PORT: u16 = 319;
/// PTP's general port: Follow_Up and Announce.
pub const GENERAL_PORT: u16 = 320;

/// How long an Announce stands. Apple's come every second or so.
const ANNOUNCE_FRESH_US: i64 = 5_000_000;
/// A clock not heard from for this long is forgotten when another
/// arrives.
const FORGET_US: i64 = 600_000_000;
/// Grandmaster changes kept for `/debug`.
const CHANGES: usize = 16;

/// The telemetry entity of the PTP timeline `clock`.
pub fn timeline_entity(clock: u64) -> String {
    format!("ptp:{clock:016x}")
}

/// Every address in a SETUP's `timingPeerInfo` (`{Addresses: [...]}`) or
/// a SETPEERS body (an array of them, or SETPEERSX's array of
/// `timingPeerInfo`s). An IPv6 address's `%scope` is dropped, as packets
/// carry none.
pub fn peer_addresses(value: &plist::Value) -> Vec<IpAddr> {
    let mut out = Vec::new();
    collect_addresses(value, &mut out);
    out
}

fn collect_addresses(value: &plist::Value, out: &mut Vec<IpAddr>) {
    match value {
        plist::Value::String(s) => {
            let bare = s.split('%').next().unwrap_or(s);
            if let Ok(ip) = bare.parse::<IpAddr>()
                && !out.contains(&ip.to_canonical())
            {
                out.push(ip.to_canonical());
            }
        }
        plist::Value::Array(items) => items.iter().for_each(|v| collect_addresses(v, out)),
        plist::Value::Dictionary(d) => {
            if let Some(v) = d.get("Addresses") {
                collect_addresses(v, out);
            }
        }
        _ => {}
    }
}

/// Whether the follower is running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    /// Not started: AirPlay 2 is off.
    Off,
    /// The ports couldn't be bound; why.
    Unavailable(String),
    Running {
        event: SocketAddr,
        general: SocketAddr,
    },
}

/// The process' PTP follower. A cheap clone: clones share it, and the
/// sockets close when the last is dropped.
#[derive(Clone)]
pub struct Ptp(Arc<Inner>);

impl std::fmt::Debug for Ptp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Ptp").field(&self.0.status).finish()
    }
}

impl Default for Ptp {
    fn default() -> Self {
        Self::off()
    }
}

struct Inner {
    clock: ServerClock,
    telemetry: Telemetry,
    status: Status,
    state: Mutex<State>,
    /// Dropped with the last [`Ptp`], which ends the socket task.
    _stop: Option<oneshot::Sender<()>>,
}

#[derive(Default)]
struct State {
    clocks: HashMap<u64, Clock>,
    /// Each peer's latest Announce, and when it came.
    announces: HashMap<IpAddr, (Announce, i64)>,
    follows: HashMap<u64, Following>,
    next_follow: u64,
    /// Messages from no peer anybody follows.
    ignored: u64,
    changes: VecDeque<Change>,
}

struct Clock {
    peer: IpAddr,
    estimator: Estimator,
    pending: Pending,
    probe: Probe,
    samples: u64,
    steps: u64,
    heard_us: i64,
}

/// A two-step Sync's halves, which come on two sockets in either order.
#[derive(Default)]
struct Pending {
    sequence: u16,
    /// When the Sync was read, and its correction.
    sync: Option<(i64, i64)>,
    /// The Follow_Up's time, in ns, with its correction.
    follow_up: Option<i128>,
}

struct Following {
    peers: Vec<IpAddr>,
    grandmaster: Option<u64>,
}

/// A session's grandmaster changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Change {
    /// Server time.
    pub at_us: i64,
    pub from: Option<u64>,
    pub to: Option<u64>,
}

/// What `/debug` shows of the follower.
#[derive(Debug, Clone)]
pub struct Report {
    pub status: Status,
    /// Each session following, its peers and grandmaster.
    pub follows: Vec<(Vec<IpAddr>, Option<u64>)>,
    pub clocks: Vec<ClockReport>,
    /// The latest grandmaster changes, oldest first.
    pub changes: Vec<Change>,
    pub ignored: u64,
}

#[derive(Debug, Clone)]
pub struct ClockReport {
    pub id: u64,
    pub peer: IpAddr,
    pub samples: u64,
    /// How often its clock stepped and the map started again.
    pub steps: u64,
    pub heard_us: i64,
    pub map: Option<ClockMap>,
}

impl Ptp {
    /// A follower that isn't running, for when AirPlay 2 is off.
    pub fn off() -> Self {
        Self::with_status(ServerClock::new(), Telemetry::default(), Status::Off, None)
    }

    fn with_status(
        clock: ServerClock,
        telemetry: Telemetry,
        status: Status,
        stop: Option<oneshot::Sender<()>>,
    ) -> Self {
        Self(Arc::new(Inner {
            clock,
            telemetry,
            status,
            state: Mutex::default(),
            _stop: stop,
        }))
    }

    /// The follower on PTP's own ports, 319 and 320, on every address.
    /// Without the privilege to bind them it logs why, once, and runs
    /// without timing.
    pub async fn bind(clock: ServerClock, telemetry: Telemetry) -> Self {
        let any = |port| SocketAddr::from((Ipv6Addr::UNSPECIFIED, port));
        let mut ptp =
            Self::bind_to(clock, telemetry.clone(), any(EVENT_PORT), any(GENERAL_PORT)).await;
        if let Status::Unavailable(why) = &ptp.0.status
            && why.to_lowercase().contains("address family")
        {
            // No IPv6 on this host.
            let any = |port| SocketAddr::from((Ipv4Addr::UNSPECIFIED, port));
            ptp = Self::bind_to(clock, telemetry, any(EVENT_PORT), any(GENERAL_PORT)).await;
        }
        match &ptp.0.status {
            Status::Running { event, general } => {
                tracing::info!(%event, %general, "AirPlay 2: following PTP");
            }
            Status::Unavailable(why) => tracing::warn!(
                "AirPlay 2 runs without timing, so it can't play buffered audio: {why}. \
                 Grant tas CAP_NET_BIND_SERVICE, and stop nqptp if it runs"
            ),
            Status::Off => {}
        }
        ptp
    }

    /// The follower on `event` and `general`, which tests bind to any
    /// free port. Logs nothing.
    pub async fn bind_to(
        clock: ServerClock,
        telemetry: Telemetry,
        event: SocketAddr,
        general: SocketAddr,
    ) -> Self {
        let bound = async {
            let e = UdpSocket::bind(event).await.map_err(|e| (event, e))?;
            let g = UdpSocket::bind(general).await.map_err(|e| (general, e))?;
            Ok::<_, (SocketAddr, std::io::Error)>((e, g))
        };
        let (event, general) = match bound.await {
            Ok(sockets) => sockets,
            Err((addr, e)) => {
                let why = format!("can't bind PTP's UDP port {}: {e}", addr.port());
                return Self::with_status(clock, telemetry, Status::Unavailable(why), None);
            }
        };
        let status = match (event.local_addr(), general.local_addr()) {
            (Ok(event), Ok(general)) => Status::Running { event, general },
            (Err(e), _) | (_, Err(e)) => Status::Unavailable(format!("PTP socket: {e}")),
        };
        let (stop, stopped) = oneshot::channel();
        let ptp = Self::with_status(clock, telemetry, status, Some(stop));
        tokio::spawn(receive(Arc::downgrade(&ptp.0), event, general, stopped));
        ptp
    }

    pub fn status(&self) -> &Status {
        &self.0.status
    }

    /// Follow the grandmaster `peers` choose, for one session; or why
    /// there is no timing, for SETUP to refuse buffered audio with.
    pub fn follow(&self, peers: impl IntoIterator<Item = IpAddr>) -> Result<Follow, String> {
        match &self.0.status {
            Status::Running { .. } => {}
            Status::Off => return Err("AirPlay 2 timing is off".into()),
            Status::Unavailable(why) => return Err(why.clone()),
        }
        let mut state = self.0.lock();
        let id = state.next_follow;
        state.next_follow += 1;
        state.follows.insert(
            id,
            Following {
                peers: canonical(peers),
                grandmaster: None,
            },
        );
        drop(state);
        let follow = Follow {
            ptp: self.clone(),
            id,
        };
        self.0.refresh(id, self.0.clock.now_us());
        Ok(follow)
    }

    /// The server clock against timeline `timeline_id`: a grandmaster's
    /// clock id, SETRATEANCHORTIME's `networkTimeTimelineID`. `None` until
    /// one of its Syncs came.
    pub fn map(&self, timeline_id: u64) -> Option<ClockMap> {
        self.0.lock().clocks.get(&timeline_id)?.estimator.map()
    }

    /// What `/debug` shows.
    pub fn report(&self) -> Report {
        let state = self.0.lock();
        let mut clocks: Vec<ClockReport> = state
            .clocks
            .iter()
            .map(|(&id, c)| ClockReport {
                id,
                peer: c.peer,
                samples: c.samples,
                steps: c.steps,
                heard_us: c.heard_us,
                map: c.estimator.map(),
            })
            .collect();
        clocks.sort_by_key(|c| c.id);
        let mut follows: Vec<_> = state.follows.iter().collect();
        follows.sort_by_key(|(id, _)| **id);
        Report {
            status: self.0.status.clone(),
            follows: follows
                .into_iter()
                .map(|(_, f)| (f.peers.clone(), f.grandmaster))
                .collect(),
            clocks,
            changes: state.changes.iter().copied().collect(),
            ignored: state.ignored,
        }
    }

    /// A message from `from`, read at server time `now_us`.
    #[cfg(test)]
    fn receive(&self, packet: &[u8], from: IpAddr, now_us: i64) {
        self.0.receive(packet, from.to_canonical(), now_us);
    }
}

fn canonical(peers: impl IntoIterator<Item = IpAddr>) -> Vec<IpAddr> {
    let mut out: Vec<IpAddr> = Vec::new();
    for ip in peers.into_iter().map(|ip| ip.to_canonical()) {
        if !out.contains(&ip) {
            out.push(ip);
        }
    }
    out
}

/// One session following its peers' grandmaster. Dropping it stops.
pub struct Follow {
    ptp: Ptp,
    id: u64,
}

impl std::fmt::Debug for Follow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Follow").field("id", &self.id).finish()
    }
}

impl Follow {
    /// SETPEERS: the peers are these now.
    pub fn set_peers(&self, peers: impl IntoIterator<Item = IpAddr>) {
        let peers = canonical(peers);
        if let Some(f) = self.ptp.0.lock().follows.get_mut(&self.id) {
            f.peers = peers;
        }
        self.ptp.0.refresh(self.id, self.ptp.0.clock.now_us());
    }

    /// The timeline id of the grandmaster the peers chose, once one
    /// announced itself or sent a Sync.
    pub fn grandmaster(&self) -> Option<u64> {
        self.ptp.0.refresh(self.id, self.ptp.0.clock.now_us())
    }

    /// The grandmaster's timeline id and clock map, once both are known.
    pub fn map(&self) -> Option<(u64, ClockMap)> {
        let id = self.grandmaster()?;
        Some((id, self.ptp.map(id)?))
    }
}

impl Drop for Follow {
    fn drop(&mut self) {
        self.ptp.0.lock().follows.remove(&self.id);
    }
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn receive(&self, packet: &[u8], from: IpAddr, now_us: i64) {
        let Some(m) = Message::parse(packet) else {
            return;
        };
        let mut state = self.lock();
        if !state.follows.values().any(|f| f.peers.contains(&from)) {
            state.ignored += 1;
            return;
        }
        match m.body {
            Body::Announce(a) => {
                state.announces.insert(from, (a, now_us));
            }
            Body::Sync(t) if !m.two_step => {
                self.sample(
                    &mut state,
                    m.source.clock,
                    from,
                    t.ns() + i128::from(m.correction_ns),
                    now_us,
                );
            }
            Body::Sync(_) => {
                let clock = self.clock_for(&mut state, m.source.clock, from, now_us);
                let pending = &mut clock.pending;
                let sync = (now_us, m.correction_ns);
                match pending.follow_up {
                    Some(ns) if pending.sequence == m.sequence => {
                        *pending = Pending::default();
                        let at = ns + i128::from(sync.1);
                        self.sample(&mut state, m.source.clock, from, at, now_us);
                    }
                    _ => {
                        *pending = Pending {
                            sequence: m.sequence,
                            sync: Some(sync),
                            follow_up: None,
                        };
                    }
                }
            }
            Body::FollowUp(t) => {
                let clock = self.clock_for(&mut state, m.source.clock, from, now_us);
                let pending = &mut clock.pending;
                let ns = t.ns() + i128::from(m.correction_ns);
                match pending.sync {
                    Some((read_us, correction)) if pending.sequence == m.sequence => {
                        *pending = Pending::default();
                        let at = ns + i128::from(correction);
                        self.sample(&mut state, m.source.clock, from, at, read_us);
                    }
                    _ => {
                        *pending = Pending {
                            sequence: m.sequence,
                            sync: None,
                            follow_up: Some(ns),
                        };
                    }
                }
            }
            Body::Other(_) => return,
        }
        let ids: Vec<u64> = state
            .follows
            .iter()
            .filter(|(_, f)| f.peers.contains(&from))
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.refresh_locked(&mut state, id, now_us);
        }
    }

    fn clock_for<'a>(
        &self,
        state: &'a mut State,
        id: u64,
        peer: IpAddr,
        now_us: i64,
    ) -> &'a mut Clock {
        if !state.clocks.contains_key(&id) {
            state.clocks.retain(|_, c| now_us - c.heard_us < FORGET_US);
            state.clocks.insert(
                id,
                Clock {
                    peer,
                    estimator: Estimator::default(),
                    pending: Pending::default(),
                    probe: self.telemetry.probe(&timeline_entity(id)),
                    samples: 0,
                    steps: 0,
                    heard_us: now_us,
                },
            );
        }
        let clock = state.clocks.get_mut(&id).expect("inserted above");
        clock.peer = peer;
        clock.heard_us = now_us;
        clock
    }

    /// A Sync of `id`'s that left at `sent_ns` on its clock and was read
    /// at server time `read_us`.
    fn sample(&self, state: &mut State, id: u64, peer: IpAddr, sent_ns: i128, read_us: i64) {
        let clock = self.clock_for(state, id, peer, read_us);
        let remote_us = (sent_ns.div_euclid(1000)) as i64;
        let s = clock.estimator.sample(remote_us, read_us);
        clock.samples += 1;
        if s.stepped {
            clock.steps += 1;
            tracing::info!(clock = %format!("{id:016x}"), "PTP: the grandmaster's clock stepped");
        }
        clock
            .probe
            .sample(Series::PtpOffset, read_us, s.offset_us as f64);
        if let Some(map) = clock.estimator.map() {
            clock.probe.sample(Series::PtpDrift, read_us, map.drift_ppm);
            clock
                .probe
                .sample(Series::PtpError, read_us, map.error_us as f64);
        }
    }

    fn refresh(&self, follow: u64, now_us: i64) -> Option<u64> {
        let mut state = self.lock();
        self.refresh_locked(&mut state, follow, now_us)
    }

    /// Chooses `follow`'s grandmaster again, and notes a change.
    fn refresh_locked(&self, state: &mut State, follow: u64, now_us: i64) -> Option<u64> {
        let f = state.follows.get(&follow)?;
        let was = f.grandmaster;
        let best = f
            .peers
            .iter()
            .filter_map(|p| state.announces.get(p))
            .filter(|(_, at)| now_us - at <= ANNOUNCE_FRESH_US)
            .min_by_key(|(a, _)| a.rank())
            .map(|(a, _)| a.grandmaster);
        // Nobody announced: a clock of the peers' sending Syncs, the one
        // already chosen if it still is.
        let now = best.or_else(|| {
            let syncing = state
                .clocks
                .iter()
                .filter(|(_, c)| f.peers.contains(&c.peer) && c.samples > 0)
                .filter(|(_, c)| now_us - c.heard_us <= ANNOUNCE_FRESH_US);
            let mut newest: Option<(u64, i64)> = None;
            for (&id, c) in syncing {
                if Some(id) == was {
                    return Some(id);
                }
                if newest.is_none_or(|(_, heard)| c.heard_us > heard) {
                    newest = Some((id, c.heard_us));
                }
            }
            newest.map(|(id, _)| id)
        });
        if now != was {
            state
                .follows
                .get_mut(&follow)
                .expect("got above")
                .grandmaster = now;
            let hex = |id: Option<u64>| id.map_or_else(|| "none".into(), |id| format!("{id:016x}"));
            tracing::info!(from = %hex(was), to = %hex(now), "AirPlay 2: the PTP grandmaster changed");
            if state.changes.len() == CHANGES {
                state.changes.pop_front();
            }
            state.changes.push_back(Change {
                at_us: now_us,
                from: was,
                to: now,
            });
            if let Some(id) = now {
                self.telemetry
                    .mark(Series::Grandmaster, &timeline_entity(id), now_us, 0.0);
            }
        }
        now
    }
}

async fn receive(
    inner: Weak<Inner>,
    event: UdpSocket,
    general: UdpSocket,
    mut stop: oneshot::Receiver<()>,
) {
    let (mut a, mut b) = ([0u8; 512], [0u8; 512]);
    loop {
        let (n, from, packet) = tokio::select! {
            _ = &mut stop => return,
            r = event.recv_from(&mut a) => match r {
                Ok((n, from)) => (n, from, &a[..]),
                Err(e) => { tracing::debug!("PTP event socket: {e}"); continue }
            },
            r = general.recv_from(&mut b) => match r {
                Ok((n, from)) => (n, from, &b[..]),
                Err(e) => { tracing::debug!("PTP general socket: {e}"); continue }
            },
        };
        let Some(inner) = inner.upgrade() else {
            return;
        };
        let now = inner.clock.now_us();
        inner.receive(&packet[..n], from.ip().to_canonical(), now);
    }
}

#[cfg(test)]
mod tests;
