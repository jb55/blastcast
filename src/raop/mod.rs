//! A native AirPlay 1 (RAOP) receiver, in place of shairport-sync.
//!
//! One receiver per target, run by the [`crate::supervisor`] as it runs a
//! shairport-sync: it listens on the target's port, advertises
//! `_raop._tcp` on the shared mDNS responder, and tells the same
//! [`ReceiverHandler`] what happens, so the audio, metadata and router
//! downstream don't know which one they are fed by. The difference is
//! that nothing goes through a fifo: the audio arrives as
//! [`ReceiverHandler::audio`] and what the sender says as
//! [`ReceiverHandler::event`], already parsed.
//!
//! A sender's connection runs iTunes' sequence ([`connection`]):
//! `OPTIONS` (with the [`crypto`] challenge), `ANNOUNCE` ([`announce`]:
//! codec and key), `SETUP` (our three UDP ports), `RECORD`, then
//! `SET_PARAMETER`s ([`params`]) and `FLUSH`es until `TEARDOWN`. The audio
//! packets ([`rtp`]) are decrypted, decoded and put in order by the
//! reorder buffer ([`playout`]), which asks for lost packets again.
//!
//! **The sender's timeline is kept** ([`clock`]): its NTP clock mapped onto
//! the server clock and its latest sync packet, so each chunk handed over
//! carries the server-clock time the sender means it to play, and the
//! Sendspin stream places it there ([`crate::audio`]). A `FLUSH` is handed
//! over in line with the audio ([`ReceiverHandler::flush`]). The audio is
//! handed over as soon as it is in order, not when it plays, so the whole
//! of the sender's lead reaches downstream; the drift between the
//! sender's clock and ours is absorbed there, by blastcast's rate match
//! ([`crate::audio`]).
//!
//! **A session** is what shairport-sync's active state was, and what
//! [`MetadataEvent::PlayBegin`] and [`MetadataEvent::PlayEnd`] bracket: it
//! begins at the first `RECORD` and ends [`NativeConfig::end_after`] after
//! the last sender has gone, so a phone that reconnects between tracks
//! doesn't stop the speakers.
//!
//! **The newest sender wins.** A second phone's `ANNOUNCE` drops the first
//! one's connection, rather than turning the second away as shairport
//! does by default. A phone that left a dead connection behind can't lock
//! the receiver.
//!
//! **What it reports.** The receiver is listed in
//! [`NativeConfig::receivers`] while it runs, and each sender's stream in
//! [`NativeConfig::sessions`], for `/v1/ws`. Its timings go to
//! [`NativeConfig::telemetry`] under the receiver's source id: every
//! timing reply and the clock map it leaves, each sync packet, each
//! packet's lead and the buffer behind it, and marks for what the playout
//! did (silence, a jump, running dry), resend
//! requests, flushes, takeovers, and the session's begin and end.
//!
//! Like the rest of AirPlay input, audio only passes through: nothing is
//! written anywhere.

pub mod announce;
pub mod clock;
pub mod connection;
pub mod crypto;
#[cfg(test)]
mod e2e;
pub mod params;
pub mod playout;
pub mod rtp;
pub mod rtsp;

use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use sha2::{Digest, Sha256};
use tas_audio::{PcmChunk, ServerClock};
use tas_mdns::service::{Service, TxtEntry};
use tas_proto::Series;
use tas_telemetry::{Probe, Roster, Telemetry};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle, JoinSet};

use crate::config::ReceiverInfo;
use crate::metadata::MetadataEvent;
use crate::supervisor::ReceiverHandler;

/// What senders browse for.
pub const RAOP_SERVICE_TYPE: &str = "_raop._tcp.local.";

/// Our `Server` header and TXT `vs`: the AirTunes version shairport-sync
/// claims, which senders treat as a plain AirPlay 1 receiver.
pub const AIRTUNES_VERSION: &str = "105.1";

#[derive(Clone)]
pub struct NativeConfig {
    /// Makes each target's device id, which the sender's challenge signs.
    pub server_id: String,
    /// The clock the sender's timeline is mapped onto: the one the
    /// Sendspin server stamps chunks with.
    pub clock: ServerClock,
    /// Where `_raop._tcp` is advertised; nowhere when `None`, as in tests.
    pub mdns: Option<tas_mdns::Mdns>,
    /// The least lead a chunk is handed over with: a lost packet is waited
    /// for [`playout::RESEND_WINDOW`] after it was missed, but no later
    /// than this long before it plays. It is also the `Audio-Latency`
    /// answered to `RECORD`.
    pub latency: Duration,
    /// How long after the last sender leaves the session ends, as
    /// shairport's `active_state_timeout`.
    pub end_after: Duration,
    /// Where a stream's three UDP ports come from, so a firewall can let
    /// the sender's audio in; any free port when `None`.
    pub udp_ports: Option<RangeInclusive<u16>>,
    /// Where an AirPlay 2 session's TCP event and data ports come from,
    /// for the same reason; any free port when `None`.
    pub tcp_ports: Option<RangeInclusive<u16>>,
    /// Where each receiver's timings go, on [`NativeConfig::clock`]; off
    /// by default.
    pub telemetry: Telemetry,
    /// Every receiver running.
    pub receivers: Roster<tas_proto::Receiver>,
    /// Every sender's stream on one.
    pub sessions: Roster<tas_proto::Session>,
    /// What each target's AirPlay 2 key and `pi` come from: the one in
    /// the state file ([`crate::ap2::Identity::load`]), so a phone keeps
    /// recognising it. A new one each run by default, as in tests.
    pub identity: crate::ap2::Identity,
    /// Whether each target also advertises `_airplay._tcp`. Off until
    /// AirPlay 2 can pair and play: a phone that sees it uses it rather
    /// than AirPlay 1. `GET /info` is answered either way.
    pub airplay2: bool,
    /// The process' PTP follower, which AirPlay 2 SETUP follows a phone's
    /// timing with ([`crate::ap2::ptp::Ptp::bind`]). Off by default, as in
    /// tests, so buffered audio is refused.
    pub ptp: crate::ap2::ptp::Ptp,
}

/// The UDP ports streams use by default: three a stream, so ten at once.
/// shairport-sync's start at 6001 too.
pub const UDP_PORTS: RangeInclusive<u16> = 6001..=6030;

/// The TCP ports AirPlay 2 sessions use by default: an event and a data
/// port each, so fifteen at once. The same numbers as [`UDP_PORTS`], so
/// one firewall rule for both protocols lets every stream in.
pub const TCP_PORTS: RangeInclusive<u16> = 6001..=6030;

impl NativeConfig {
    pub fn new(
        server_id: impl Into<String>,
        clock: ServerClock,
        mdns: Option<tas_mdns::Mdns>,
    ) -> Self {
        Self {
            server_id: server_id.into(),
            clock,
            mdns,
            latency: Duration::from_millis(250),
            end_after: Duration::from_secs(10),
            udp_ports: Some(UDP_PORTS),
            tcp_ports: Some(TCP_PORTS),
            telemetry: Telemetry::default(),
            receivers: Roster::default(),
            sessions: Roster::default(),
            identity: crate::ap2::Identity::generate(),
            airplay2: false,
            ptp: crate::ap2::ptp::Ptp::off(),
        }
    }
}

impl std::fmt::Debug for NativeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeConfig")
            .field("server_id", &self.server_id)
            .field("mdns", &self.mdns.as_ref().map(tas_mdns::Mdns::host_name))
            .field("latency", &self.latency)
            .field("end_after", &self.end_after)
            .field("udp_ports", &self.udp_ports)
            .field("tcp_ports", &self.tcp_ports)
            .field("airplay2", &self.airplay2)
            .finish()
    }
}

/// The six bytes a receiver is known by: the hex before the `@` in its
/// advert, and part of what its challenge response signs. The same for
/// the same target on the same server, so a phone remembers it.
pub fn device_id(server_id: &str, target_id: &str) -> [u8; 6] {
    let digest = Sha256::digest(format!("{server_id}/{target_id}").as_bytes());
    let mut id: [u8; 6] = digest[..6].try_into().expect("6 bytes");
    // A locally administered unicast MAC, which it stands in for.
    id[0] = (id[0] & 0xfc) | 0x02;
    id
}

/// The `_raop._tcp` advert: `<device id>@<name>`, with shairport-sync's
/// AirPlay 1 TXT record. `cn=0,1`: PCM or ALAC; `et=0,1`: clear or RSA;
/// `md=0,1,2`: text, art and progress metadata.
pub fn service(device_id: [u8; 6], name: &str, port: u16) -> Service {
    let hex: String = device_id.iter().map(|b| format!("{b:02X}")).collect();
    let name = fit_label(name, hex.len() + 1);
    let txt = [
        ("txtvers", "1"),
        ("ch", "2"),
        ("cn", "0,1"),
        ("et", "0,1"),
        ("ek", "1"),
        ("sv", "false"),
        ("sm", "false"),
        ("da", "true"),
        ("sr", "44100"),
        ("ss", "16"),
        ("pw", "false"),
        ("vn", "3"),
        ("tp", "UDP"),
        ("md", "0,1,2"),
        ("vs", AIRTUNES_VERSION),
        ("am", "TuneAssistant"),
        ("sf", "0x4"),
    ];
    Service {
        service_type: RAOP_SERVICE_TYPE.to_owned(),
        instance: format!("{hex}@{name}"),
        port,
        txt: txt.iter().map(|(k, v)| TxtEntry::new(*k, *v)).collect(),
    }
}

/// As much of `name` as fits a DNS label, at most 63 bytes, after
/// `taken` bytes of it are used.
pub(crate) fn fit_label(name: &str, taken: usize) -> &str {
    let mut name = name;
    while !name.is_empty() && taken + name.len() > 63 {
        let mut end = name.len() - 1;
        while !name.is_char_boundary(end) {
            end -= 1;
        }
        name = &name[..end];
    }
    name
}

/// Listens on `port` on every address, v6 and v4 if the host has both.
pub async fn bind(port: u16) -> io::Result<TcpListener> {
    // Dual-stack on Linux unless net.ipv6.bindv6only is set.
    match TcpListener::bind((Ipv6Addr::UNSPECIFIED, port)).await {
        Ok(l) => Ok(l),
        Err(_) => TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)).await,
    }
}

/// Runs `info`'s receiver on `listener` until `stop`. The handler has been
/// told it `started`; telling it `stopped` is the caller's.
pub async fn serve(
    config: NativeConfig,
    info: ReceiverInfo,
    handler: Arc<dyn ReceiverHandler>,
    listener: TcpListener,
    volume: Option<u8>,
    stop: &mut watch::Receiver<bool>,
) -> io::Result<()> {
    let port = listener.local_addr()?.port();
    let device_id = device_id(&config.server_id, &info.target_id);
    let ap2 = config
        .identity
        .device(device_id, &info.target_id, &info.airplay_name);
    let _advert = match &config.mdns {
        Some(mdns) => Some(
            mdns.advertise(service(device_id, &info.airplay_name, port))
                .map_err(|e| io::Error::other(format!("advertising _raop._tcp: {e}")))?,
        ),
        None => None,
    };
    let _ap2_advert = match &config.mdns {
        Some(mdns) if config.airplay2 => Some(
            mdns.advertise(crate::ap2::info::service(&ap2, port))
                .map_err(|e| io::Error::other(format!("advertising _airplay._tcp: {e}")))?,
        ),
        _ => None,
    };
    let id = crate::route::source_id(&info.target_id);
    config.receivers.set(tas_proto::Receiver {
        id: id.clone(),
        target: info.target_id.clone(),
        name: info.airplay_name.clone(),
        port,
        session: None,
        playing: false,
    });
    let _listed = Listed {
        receivers: config.receivers.clone(),
        sessions: config.sessions.clone(),
        id: id.clone(),
    };
    let receiver = Arc::new(Receiver {
        probe: config.telemetry.probe(&id),
        id,
        config,
        info,
        handler,
        device_id,
        ap2,
        volume,
        state: Mutex::default(),
    });
    let mut connections = JoinSet::new();
    let mut next_id = 0u64;
    loop {
        tokio::select! {
            () = stopped(stop) => break,
            accepted = listener.accept() => {
                let (tcp, peer) = match accepted {
                    Ok(a) => a,
                    Err(e) => {
                        // Out of descriptors, say: don't spin on it.
                        tracing::warn!(error = %e, "accepting an AirPlay connection");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                next_id += 1;
                let id = next_id;
                let span = tracing::info_span!("airplay_sender", conn = id, %peer);
                // Registered under the lock, so the connection can't end
                // (and unregister) before it is registered.
                let mut state = receiver.lock();
                let handle = connections.spawn(tracing::Instrument::instrument(
                    connection::run(receiver.clone(), id, tcp, peer),
                    span,
                ));
                state.connections.insert(id, handle);
            }
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
    connections.shutdown().await;
    if let Some(timer) = receiver.lock().end_timer.take() {
        timer.abort();
    }
    Ok(())
}

/// Unlists a receiver and its sessions however its run ends.
struct Listed {
    receivers: Roster<tas_proto::Receiver>,
    sessions: Roster<tas_proto::Session>,
    id: String,
}

impl Drop for Listed {
    fn drop(&mut self) {
        self.receivers.remove(&self.id);
        self.sessions.retain(|s| s.receiver != self.id);
    }
}

/// One target's receiver, shared by its connections.
pub(crate) struct Receiver {
    /// Its source id: what it is listed and measured under.
    id: String,
    probe: Probe,
    config: NativeConfig,
    info: ReceiverInfo,
    handler: Arc<dyn ReceiverHandler>,
    device_id: [u8; 6],
    /// The target as an AirPlay 2 device.
    ap2: crate::ap2::Device,
    /// The volume to report before a sender has set one, 0-100.
    volume: Option<u8>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    connections: HashMap<u64, AbortHandle>,
    /// The connection that announced last: the one playing.
    current: Option<u64>,
    /// Between PlayBegin and PlayEnd.
    playing: bool,
    /// The connection whose RECORD the session is on.
    owner: Option<u64>,
    end_timer: Option<JoinHandle<()>>,
}

impl Receiver {
    fn lock(&self) -> MutexGuard<'_, State> {
        // Every critical section leaves the state consistent.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn event(&self, event: MetadataEvent) {
        self.handler.event(&self.info, &event);
    }

    pub(crate) fn audio(&self, chunk: &PcmChunk, play_at_us: Option<i64>) {
        self.handler.audio(&self.info, chunk, play_at_us);
    }

    pub(crate) fn flush(&self) {
        self.handler.flush(&self.info);
    }

    fn now_us(&self) -> i64 {
        self.config.clock.now_us()
    }

    /// Marks `kind` on the receiver now.
    fn mark(&self, kind: Series, detail: f64) {
        self.probe.mark(kind, self.now_us(), detail);
    }

    /// The id `conn`'s stream is listed under.
    fn session_id(&self, conn: u64) -> String {
        format!("{}/{conn}", self.id)
    }

    /// `conn` announced `session`: it plays from now, and the connection
    /// that played before is dropped.
    fn take_over(&self, conn: u64, session: tas_proto::Session) {
        let mut state = self.lock();
        if let Some(previous) = state.current.replace(conn)
            && previous != conn
            && let Some(handle) = state.connections.get(&previous)
        {
            tracing::info!(previous, "a new AirPlay sender takes over");
            handle.abort();
            self.mark(Series::Takeover, 0.0);
        }
        drop(state);
        let id = session.id.clone();
        self.config.sessions.set(session);
        self.config
            .receivers
            .modify(&self.id, |r| r.session = Some(id));
    }

    /// `conn` started recording: the session begins, unless it is on.
    fn begin(&self, conn: u64) {
        let mut state = self.lock();
        if let Some(timer) = state.end_timer.take() {
            timer.abort();
        }
        state.owner = Some(conn);
        if !state.playing {
            state.playing = true;
            drop(state);
            tracing::info!(target_id = %self.info.target_id, "AirPlay session begins");
            self.mark(Series::PlayBegin, 0.0);
            self.config.receivers.modify(&self.id, |r| r.playing = true);
            self.event(MetadataEvent::PlayBegin);
        }
    }

    /// `conn`'s task ended (closed or dropped): it leaves the session, and
    /// there is nothing left of it to drop.
    fn end(self: &Arc<Self>, conn: u64) {
        self.lock().connections.remove(&conn);
        self.release(conn);
    }

    /// `conn` stopped streaming (torn down, or ended): if the session was
    /// on it, the session ends unless a sender records again soon. The
    /// connection stays droppable, as a sender can stream on it again.
    fn release(self: &Arc<Self>, conn: u64) {
        self.config.sessions.remove(&self.session_id(conn));
        let mut state = self.lock();
        if state.current == Some(conn) {
            state.current = None;
            self.config.receivers.modify(&self.id, |r| r.session = None);
        }
        if state.owner != Some(conn) {
            return;
        }
        state.owner = None;
        let me = Arc::clone(self);
        let after = self.config.end_after;
        state.end_timer = Some(tokio::spawn(async move {
            tokio::time::sleep(after).await;
            let mut state = me.lock();
            if state.owner.is_none() && state.playing {
                state.playing = false;
                state.end_timer = None;
                drop(state);
                tracing::info!(target_id = %me.info.target_id, "AirPlay session ends");
                me.mark(Series::PlayEnd, 0.0);
                me.config.receivers.modify(&me.id, |r| r.playing = false);
                me.event(MetadataEvent::PlayEnd);
            }
        }));
    }
}

/// An AirPlay 2 stream hands its audio over as an AirPlay 1 stream does.
impl crate::ap2::hold::Sink for Receiver {
    fn audio(&self, chunk: &PcmChunk, play_at_us: Option<i64>) {
        Receiver::audio(self, chunk, play_at_us);
    }

    fn flush(&self) {
        Receiver::flush(self);
    }
}

async fn stopped(stop: &mut watch::Receiver<bool>) {
    let _ = stop.wait_for(|s| *s).await.is_ok();
}

/// A TCP listener for `peer` to connect to: on the first free port of
/// `ports`, or, with none free or no range, any port.
pub(crate) async fn bind_tcp(
    peer: IpAddr,
    ports: Option<&RangeInclusive<u16>>,
) -> io::Result<TcpListener> {
    let any = unspecified(peer);
    if let Some(ports) = ports {
        for port in ports.clone() {
            if let Ok(listener) = TcpListener::bind((any, port)).await {
                return Ok(listener);
            }
        }
        tracing::warn!(
            ?ports,
            "no free AirPlay TCP port in range; using any (a firewall may refuse the sender)"
        );
    }
    TcpListener::bind((any, 0)).await
}

/// Every address of `peer`'s family.
fn unspecified(peer: IpAddr) -> IpAddr {
    match peer {
        IpAddr::V4(_) => Ipv4Addr::UNSPECIFIED.into(),
        IpAddr::V6(_) => Ipv6Addr::UNSPECIFIED.into(),
    }
}

/// A UDP socket for talking to `peer`: on the first free port of `ports`,
/// or, with none free or no range, any port.
pub(crate) async fn bind_udp(
    peer: IpAddr,
    ports: Option<&RangeInclusive<u16>>,
) -> io::Result<tokio::net::UdpSocket> {
    let any = unspecified(peer);
    if let Some(ports) = ports {
        for port in ports.clone() {
            if let Ok(socket) = tokio::net::UdpSocket::bind((any, port)).await {
                return Ok(socket);
            }
        }
        tracing::warn!(
            ?ports,
            "no free AirPlay UDP port in range; using any (a firewall may drop the audio)"
        );
    }
    tokio::net::UdpSocket::bind((any, 0)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn udp_ports_come_from_the_range_while_it_lasts() {
        let local: IpAddr = Ipv4Addr::LOCALHOST.into();
        // A port found free can be taken by a test running beside this one
        // before it is bound again; try a few.
        let mut held = None;
        for _ in 0..20 {
            let free = tokio::net::UdpSocket::bind("0.0.0.0:0").await.unwrap();
            let port = free.local_addr().unwrap().port();
            drop(free);
            let socket = bind_udp(local, Some(&(port..=port))).await.unwrap();
            if socket.local_addr().unwrap().port() == port {
                held = Some((port, socket));
                break;
            }
        }
        let (port, _first) = held.expect("a free port is bound from the range");
        // Taken: any port instead.
        let second = bind_udp(local, Some(&(port..=port))).await.unwrap();
        assert_ne!(second.local_addr().unwrap().port(), port);
        assert!(bind_udp(local, None).await.is_ok());
    }

    #[test]
    fn a_device_id_is_stable_and_per_target() {
        let a = device_id("srv", "kitchen");
        assert_eq!(a, device_id("srv", "kitchen"));
        assert_ne!(a, device_id("srv", "den"));
        assert_ne!(a, device_id("other", "kitchen"));
        assert_eq!(a[0] & 0x03, 0x02, "locally administered unicast");
    }

    #[test]
    fn the_advert_names_the_device_and_fits_a_label() {
        let s = service([0x02, 0xAB, 0, 1, 2, 0xFF], "Kitchen", 7123);
        assert_eq!(s.instance, "02AB000102FF@Kitchen");
        assert_eq!(s.port, 7123);
        let txt = |k: &str| {
            s.txt
                .iter()
                .find(|e| e.key == k)
                .and_then(|e| e.value.as_deref())
        };
        assert_eq!(txt("et"), Some("0,1"));
        assert_eq!(txt("cn"), Some("0,1"));
        assert_eq!(txt("md"), Some("0,1,2"));

        let long = "Ünïcödé ".repeat(10);
        let s = service([0; 6], &long, 1);
        assert!(s.instance.len() <= 63, "{}", s.instance.len());
        assert!(long.starts_with(s.instance.split_once('@').unwrap().1));
    }
}
