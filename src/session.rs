//! One RAOP stream to one speaker: the RTSP setup, then audio, sync and
//! timing over UDP until it is torn down.
//!
//! [`RaopSession::connect`] runs iTunes' sequence: `OPTIONS`, `ANNOUNCE`
//! with the SDP of an unencrypted ALAC stream, `SETUP` with our control and
//! timing ports, `RECORD`, then `SET_PARAMETER` for the volume. From then
//! on the session answers the speaker's timing requests and resend
//! requests by itself.
//!
//! **Where the audio plays** is set by [`RaopSession::anchor`]: the next
//! frame pushed plays at a server-clock time. From the anchor on, the
//! session sends a sync packet each second saying which frame plays at
//! that second, so the speaker plays every later frame on the same
//! timeline, as long as frames are pushed without a gap. A gap or a jump
//! is the caller's to fill or to re-anchor after [`RaopSession::flush`].
//!
//! Audio is pushed as interleaved stereo s16 and sent in packets of
//! [`FRAMES_PER_PACKET`] frames; a short remainder waits for the next push.
//! **Packets are paced**: each is sent [`LATENCY_FRAMES`] before it plays,
//! as iTunes sends them, however early it was pushed. A Sendspin group
//! hands the bridge seconds of audio at once when it starts, and a burst
//! that size overruns a speaker's socket buffer.

use std::collections::VecDeque;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tas_audio::ServerClock;
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, timeout};

use crate::alac::{self, FMTP, FRAMES_PER_PACKET};
use crate::rtp::{self, NtpClock};
use crate::rtsp::{Identity, Response, RtspClient, RtspError, transport_param};

/// The stream's sample rate: every AirPlay 1 receiver takes 44.1 kHz.
pub const SAMPLE_RATE: u32 = 44_100;
/// How far ahead of the playing frame the sync packets say we send: two
/// seconds, as iTunes does. Packets are sent this far ahead of their play
/// time, so speakers that buffer by it have what they need.
pub const LATENCY_FRAMES: u32 = 2 * SAMPLE_RATE;
/// Sent packets kept to answer resend requests: about 8 s.
const RESEND_RING: usize = 1024;
const SYNC_INTERVAL: Duration = Duration::from_secs(1);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// AirPlay's volume range, in dB; -144 is mute.
const VOLUME_MIN_DB: f32 = -30.0;
const VOLUME_MUTE_DB: f32 = -144.0;

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("can't connect: {0}")]
    Connect(std::io::Error),
    #[error("connecting timed out")]
    ConnectTimeout,
    #[error(transparent)]
    Rtsp(#[from] RtspError),
    #[error("the speaker wants a password or pairing ({0})")]
    Auth(u16),
    #[error("the speaker is busy with another sender")]
    Busy,
    #[error("SETUP gave no {0} in its Transport")]
    Transport(&'static str),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// The AirPlay volume, in dB, for a 0-100 level.
pub fn volume_db(level: u8, muted: bool) -> f32 {
    if muted || level == 0 {
        return VOLUME_MUTE_DB;
    }
    VOLUME_MIN_DB - VOLUME_MIN_DB * f32::from(level.min(100)) / 100.0
}

/// Which frame plays when: frame `rtp` at server-clock `at_us`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Anchor {
    pub rtp: u32,
    pub at_us: i64,
}

impl Anchor {
    /// The frame playing at `us`, and the NTP time it starts at exactly.
    fn playing_at(&self, us: i64, ntp: &NtpClock) -> (u32, u64) {
        // The floor of the frames elapsed, then that frame's exact time.
        let frames = frames_at(us - self.at_us);
        let offset = (i128::from(frames) << 32) / i128::from(SAMPLE_RATE);
        let ntp_time = (i128::from(ntp.at_us(self.at_us)) + offset) as u64;
        (self.rtp.wrapping_add(frames as u32), ntp_time)
    }
}

/// Whole frames in `us` microseconds, rounded down.
fn frames_at(us: i64) -> i64 {
    (i128::from(us) * i128::from(SAMPLE_RATE)).div_euclid(1_000_000) as i64
}

/// State the background tasks share with the session.
struct Shared {
    anchor: Option<Anchor>,
    /// The next sync packet is the stream's first.
    first_sync: bool,
    resend: VecDeque<(u16, Vec<u8>)>,
}

pub struct RaopSession {
    rtsp: RtspClient,
    uri: String,
    peer: IpAddr,
    /// To the pacer, which sends each packet when it is due.
    outbox: mpsc::UnboundedSender<Outgoing>,
    /// Bumped by a flush; the pacer drops packets queued before it.
    epoch: Arc<AtomicU64>,
    control: Arc<UdpSocket>,
    control_to: SocketAddr,
    ntp: NtpClock,
    shared: Arc<Mutex<Shared>>,
    tasks: Vec<JoinHandle<()>>,
    ssrc: u32,
    seq: u16,
    /// The RTP time of the first frame in `pending`.
    rtp: u32,
    pending: Vec<i16>,
    /// The next packet carries the marker bit.
    marker: bool,
    /// The speaker's own `Audio-Latency`, in frames, if it said.
    audio_latency: Option<u32>,
}

impl std::fmt::Debug for RaopSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaopSession")
            .field("uri", &self.uri)
            .field("peer", &self.peer)
            .field("seq", &self.seq)
            .field("rtp", &self.rtp)
            .finish_non_exhaustive()
    }
}

impl RaopSession {
    /// Sets up a stream to the speaker at `addr`, at `volume` dB.
    pub async fn connect(
        addr: SocketAddr,
        ntp: NtpClock,
        volume: f32,
    ) -> Result<Self, SessionError> {
        let tcp = timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| SessionError::ConnectTimeout)?
            .map_err(SessionError::Connect)?;
        tcp.set_nodelay(true)?;
        let local = tcp.local_addr()?.ip();
        let peer = addr.ip();
        let bind = SocketAddr::new(local, 0);
        let control = Arc::new(UdpSocket::bind(bind).await?);
        let timing = Arc::new(UdpSocket::bind(bind).await?);
        let audio = UdpSocket::bind(bind).await?;

        let identity = Identity {
            instance: format!("{:016X}", random_u64()),
            active_remote: random_u32(),
        };
        let mut rtsp = RtspClient::new(tcp, identity);
        let sid = random_u32();
        let uri = format!("rtsp://{}/{sid}", host(local));
        let auth = |e: RtspError| match e {
            RtspError::Status {
                status: status @ (401 | 403 | 470),
                ..
            } => SessionError::Auth(status),
            RtspError::Status { status: 453, .. } => SessionError::Busy,
            e => SessionError::Rtsp(e),
        };

        rtsp.request("OPTIONS", "*", &[], None)
            .await
            .map_err(auth)?;
        let sdp = format!(
            "v=0\r\no=iTunes {sid} 0 IN {family} {local}\r\ns=iTunes\r\nc=IN {family} {peer}\r\nt=0 0\r\n\
             m=audio 0 RTP/AVP 96\r\na=rtpmap:96 AppleLossless\r\na=fmtp:96 {FMTP}\r\n",
            family = if local.is_ipv4() { "IP4" } else { "IP6" },
        );
        rtsp.request(
            "ANNOUNCE",
            &uri,
            &[],
            Some(("application/sdp", sdp.as_bytes())),
        )
        .await
        .map_err(auth)?;
        let transport = format!(
            "RTP/AVP/UDP;unicast;interleaved=0-1;mode=record;control_port={};timing_port={}",
            control.local_addr()?.port(),
            timing.local_addr()?.port(),
        );
        let setup = rtsp
            .request("SETUP", &uri, &[("Transport", transport)], None)
            .await
            .map_err(auth)?;
        let (server_port, control_port, timing_port) = ports(&setup)?;
        // The speaker asks our timing port for the time. A stateful
        // firewall here drops that unless we wrote to its timing port
        // first, so we do, with a request it can ignore.
        if let Some(port) = timing_port {
            let _ = timing
                .send_to(&rtp::timing_request(ntp.now()), SocketAddr::new(peer, port))
                .await;
        }
        if let Some(session) = setup.header("Session") {
            // `Session: <id>;timeout=60` names the session by its id alone.
            let id = session.split(';').next().unwrap_or(session).trim();
            rtsp.set_session(id.to_owned());
        }

        let seq = random_u32() as u16;
        let rtp = random_u32();
        let record = rtsp
            .request(
                "RECORD",
                &uri,
                &[
                    ("Range", "npt=0-".into()),
                    ("RTP-Info", format!("seq={seq};rtptime={rtp}")),
                ],
                None,
            )
            .await
            .map_err(auth)?;
        let audio_latency = record.header("Audio-Latency").and_then(|v| v.parse().ok());

        let shared = Arc::new(Mutex::new(Shared {
            anchor: None,
            first_sync: true,
            resend: VecDeque::with_capacity(RESEND_RING),
        }));
        let control_to = SocketAddr::new(peer, control_port);
        let (outbox, queued) = mpsc::unbounded_channel();
        let epoch = Arc::new(AtomicU64::new(0));
        let tasks = vec![
            tokio::spawn(pace(
                audio,
                SocketAddr::new(peer, server_port),
                queued,
                epoch.clone(),
                ntp.server_clock(),
                shared.clone(),
            )),
            tokio::spawn(answer_timing(timing, peer, ntp)),
            tokio::spawn(run_control(
                control.clone(),
                control_to,
                ntp,
                shared.clone(),
            )),
        ];
        let mut session = Self {
            rtsp,
            uri,
            peer,
            outbox,
            epoch,
            control,
            control_to,
            ntp,
            shared,
            tasks,
            ssrc: random_u32(),
            seq,
            rtp,
            pending: Vec::with_capacity(FRAMES_PER_PACKET * 2),
            marker: true,
            audio_latency,
        };
        session.set_volume(volume).await?;
        tracing::debug!(
            uri = session.uri,
            server_port,
            control_port,
            audio_latency,
            "raop session up"
        );
        Ok(session)
    }

    /// The speaker's `Audio-Latency`, in frames, if its RECORD answer had
    /// one.
    pub fn audio_latency(&self) -> Option<u32> {
        self.audio_latency
    }

    /// The RTP time of the next frame pushed.
    pub fn next_rtp(&self) -> u32 {
        self.rtp.wrapping_add((self.pending.len() / 2) as u32)
    }

    /// The anchor the sync packets follow, if any.
    pub fn current_anchor(&self) -> Option<Anchor> {
        self.lock().anchor
    }

    /// When everything pushed so far has played, in server-clock µs, if
    /// the session is anchored.
    pub fn played_out_us(&self) -> Option<i64> {
        let anchor = self.current_anchor()?;
        let frames = i64::from(self.next_rtp().wrapping_sub(anchor.rtp) as i32);
        Some(anchor.at_us + frames * 1_000_000 / i64::from(SAMPLE_RATE))
    }

    /// The next frame pushed plays at server-clock `at_us`. Sends a sync
    /// packet now, so the speaker knows before the first audio arrives.
    pub async fn anchor(&mut self, at_us: i64) -> Result<(), SessionError> {
        let anchor = Anchor {
            rtp: self.next_rtp(),
            at_us,
        };
        self.lock().anchor = Some(anchor);
        send_sync(&self.control, self.control_to, &self.ntp, &self.shared).await?;
        Ok(())
    }

    /// Queues interleaved stereo frames and sends every whole packet.
    pub async fn push(&mut self, samples: &[i16]) -> Result<(), SessionError> {
        let mut rest = samples;
        while !rest.is_empty() {
            let room = FRAMES_PER_PACKET * 2 - self.pending.len();
            let take = room.min(rest.len());
            self.pending.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            if self.pending.len() == FRAMES_PER_PACKET * 2 {
                self.send_pending().await?;
            }
        }
        Ok(())
    }

    /// Sends what is queued as a short packet.
    pub async fn finish(&mut self) -> Result<(), SessionError> {
        if !self.pending.is_empty() {
            self.send_pending().await?;
        }
        Ok(())
    }

    async fn send_pending(&mut self) -> Result<(), SessionError> {
        let frames = self.pending.len() / 2;
        let payload = alac::encode_stereo_s16(&self.pending);
        let packet = rtp::audio_packet(self.marker, self.seq, self.rtp, self.ssrc, &payload);
        // Unanchored, it goes now; anchored, two seconds before it plays.
        let due_us = self.current_anchor().map_or(i64::MIN, |a| {
            let frames = i64::from(self.rtp.wrapping_sub(a.rtp) as i32) - i64::from(LATENCY_FRAMES);
            a.at_us + frames * 1_000_000 / i64::from(SAMPLE_RATE)
        });
        let outgoing = Outgoing {
            epoch: self.epoch.load(Ordering::Acquire),
            due_us,
            seq: self.seq,
            packet,
        };
        if self.outbox.send(outgoing).is_err() {
            return Err(SessionError::Io(std::io::Error::other("the pacer stopped")));
        }
        self.marker = false;
        self.seq = self.seq.wrapping_add(1);
        self.rtp = self.rtp.wrapping_add(frames as u32);
        self.pending.clear();
        Ok(())
    }

    /// Drops what the speaker has buffered and anything queued here, and
    /// stops the sync packets until the next [`RaopSession::anchor`]. The
    /// RTP time carries on from where it was.
    pub async fn flush(&mut self) -> Result<(), SessionError> {
        let rtp = self.next_rtp();
        self.pending.clear();
        self.rtp = rtp;
        self.epoch.fetch_add(1, Ordering::AcqRel);
        {
            let mut shared = self.lock();
            shared.anchor = None;
            shared.first_sync = true;
            shared.resend.clear();
        }
        self.marker = true;
        let info = format!("seq={};rtptime={rtp}", self.seq);
        self.rtsp
            .request("FLUSH", &self.uri, &[("RTP-Info", info)], None)
            .await?;
        Ok(())
    }

    pub async fn set_volume(&mut self, db: f32) -> Result<(), SessionError> {
        let body = format!("volume: {db:.6}\r\n");
        self.rtsp
            .request(
                "SET_PARAMETER",
                &self.uri,
                &[],
                Some(("text/parameters", body.as_bytes())),
            )
            .await?;
        Ok(())
    }

    /// Ends the stream. The speaker is free for another sender after it.
    pub async fn teardown(mut self) {
        if let Err(e) = self.rtsp.request("TEARDOWN", &self.uri, &[], None).await {
            tracing::debug!(error = %e, "teardown failed");
        }
    }

    /// Resolves when the speaker closes the RTSP connection, which it does
    /// when another sender takes it over. Anything it sends unasked is
    /// read and dropped.
    pub async fn closed(&mut self) {
        loop {
            match self.rtsp.read_unsolicited().await {
                Ok(0) | Err(_) => return,
                Ok(n) => tracing::debug!(bytes = n, "speaker sent something unasked"),
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Shared> {
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for RaopSession {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// A packet for the pacer.
struct Outgoing {
    epoch: u64,
    /// Server-clock time to send it at.
    due_us: i64,
    seq: u16,
    packet: Vec<u8>,
}

/// Sends each packet when it is due, in order, and keeps it for resends.
/// Packets from before a flush are dropped.
async fn pace(
    socket: UdpSocket,
    to: SocketAddr,
    mut queued: mpsc::UnboundedReceiver<Outgoing>,
    epoch: Arc<AtomicU64>,
    clock: ServerClock,
    shared: Arc<Mutex<Shared>>,
) {
    while let Some(out) = queued.recv().await {
        // Checked before the wait too, so a flushed queue doesn't hold up
        // what comes after it.
        if out.epoch != epoch.load(Ordering::Acquire) {
            continue;
        }
        if out.due_us != i64::MIN {
            tokio::time::sleep_until(clock.instant_at(out.due_us)).await;
        }
        if out.epoch != epoch.load(Ordering::Acquire) {
            continue;
        }
        if socket.send_to(&out.packet, to).await.is_err() {
            return;
        }
        let mut shared = shared.lock().unwrap_or_else(PoisonError::into_inner);
        if shared.resend.len() == RESEND_RING {
            shared.resend.pop_front();
        }
        shared.resend.push_back((out.seq, out.packet));
    }
}

/// `server_port`, `control_port` and, if given, `timing_port` from a
/// SETUP answer.
fn ports(setup: &Response) -> Result<(u16, u16, Option<u16>), SessionError> {
    let transport = setup
        .header("Transport")
        .ok_or(SessionError::Transport("Transport header"))?;
    let port = |key: &'static str| {
        transport_param(transport, key)
            .and_then(|p| p.parse().ok())
            .ok_or(SessionError::Transport(key))
    };
    let timing = port("timing_port").ok();
    Ok((port("server_port")?, port("control_port")?, timing))
}

/// Answers the speaker's timing requests with our NTP clock.
async fn answer_timing(socket: Arc<UdpSocket>, peer: IpAddr, ntp: NtpClock) {
    let mut buf = [0u8; 128];
    loop {
        let Ok((n, from)) = socket.recv_from(&mut buf).await else {
            return;
        };
        if from.ip() != peer {
            continue;
        }
        if let Some(reply) = rtp::timing_reply(&buf[..n], ntp.now()) {
            let _ = socket.send_to(&reply, from).await;
        }
    }
}

/// Sends a sync packet each second while anchored, and answers resend
/// requests from the ring.
async fn run_control(
    control: Arc<UdpSocket>,
    control_to: SocketAddr,
    ntp: NtpClock,
    shared: Arc<Mutex<Shared>>,
) {
    let mut tick = tokio::time::interval(SYNC_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut buf = [0u8; 64];
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if send_sync(&control, control_to, &ntp, &shared).await.is_err() {
                    return;
                }
            }
            got = control.recv_from(&mut buf) => {
                let Ok((n, from)) = got else { return };
                if from.ip() != control_to.ip() {
                    continue;
                }
                let Some((seq, count)) = rtp::parse_resend_request(&buf[..n]) else {
                    continue;
                };
                let packets: Vec<Vec<u8>> = {
                    let shared = shared.lock().unwrap_or_else(PoisonError::into_inner);
                    (0..count)
                        .filter_map(|i| {
                            let want = seq.wrapping_add(i);
                            shared.resend.iter().find(|(s, _)| *s == want)
                        })
                        .map(|(_, p)| rtp::resend_reply(p))
                        .collect()
                };
                tracing::trace!(seq, count, found = packets.len(), "resend request");
                // Answered on the control port, as libraop does.
                for packet in packets {
                    let _ = control.send_to(&packet, control_to).await;
                }
            }
        }
    }
}

/// One sync packet for now, if anchored.
async fn send_sync(
    control: &UdpSocket,
    to: SocketAddr,
    ntp: &NtpClock,
    shared: &Mutex<Shared>,
) -> std::io::Result<()> {
    let packet = {
        let mut shared = shared.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(anchor) = shared.anchor else {
            return Ok(());
        };
        let (playing, at) = anchor.playing_at(ntp.server_clock().now_us(), ntp);
        let first = std::mem::take(&mut shared.first_sync);
        rtp::sync_packet(first, playing, at, LATENCY_FRAMES)
    };
    control.send_to(&packet, to).await.map(|_| ())
}

fn host(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => format!("[{v6}]"),
    }
}

fn random_u32() -> u32 {
    let mut b = [0u8; 4];
    getrandom::fill(&mut b).expect("the OS has randomness");
    u32::from_ne_bytes(b)
}

fn random_u64() -> u64 {
    let mut b = [0u8; 8];
    getrandom::fill(&mut b).expect("the OS has randomness");
    u64::from_ne_bytes(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volume_maps_to_airplay_db() {
        assert_eq!(volume_db(100, false), 0.0);
        assert_eq!(volume_db(50, false), -15.0);
        assert_eq!(volume_db(1, false), -29.7);
        assert_eq!(volume_db(0, false), -144.0);
        assert_eq!(volume_db(80, true), -144.0);
    }

    #[test]
    fn the_playing_frame_starts_at_its_ntp_time() {
        let ntp = NtpClock::new(ServerClock::new());
        let anchor = Anchor {
            rtp: u32::MAX - 10,
            at_us: 5_000_000,
        };
        // One second on: 44 100 frames, at exactly a second on.
        let (rtp, at) = anchor.playing_at(6_000_000, &ntp);
        assert_eq!(rtp, (u32::MAX - 10).wrapping_add(44_100));
        assert_eq!(at, ntp.at_us(6_000_000));
        // Mid-frame: the frame that started before, at its own start.
        let (rtp, at) = anchor.playing_at(5_000_030, &ntp);
        assert_eq!(rtp, (u32::MAX - 10).wrapping_add(1));
        assert_eq!(ntp.us_at(at), 5_000_023);
        // Before the anchor: frames before it, counted back.
        let (rtp, _) = anchor.playing_at(4_000_000, &ntp);
        assert_eq!(rtp, (u32::MAX - 10).wrapping_sub(44_100));
    }
}
