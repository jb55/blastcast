//! One sender's RTSP connection, and the stream it sets up.
//!
//! Requests are answered in order. Every answer carries the `CSeq` and,
//! when the request had an `Apple-Challenge`, the `Apple-Response`.
//!
//! `SETUP` binds three UDP sockets and starts four tasks, which live as
//! long as the stream:
//!
//! - **audio**: each packet is decrypted, decoded and queued in the
//!   [`Playout`]. A sequence number that skips ahead asks the sender, on
//!   its control port, for the ones skipped.
//! - **control**: the packets sent again come back here and are queued the
//!   same way. Each sync packet is the sender timeline's new anchor.
//! - **timing**: asks the sender's clock every few seconds, as a receiver
//!   does, feeding the timeline's clock map, and answers if it asks ours.
//! - **playout**: wakes when the next frame is due and hands what is due
//!   to the receiver's handler as PCM, stamped with the sender's play
//!   time for it.

use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tas_audio::PcmChunk;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::Instrument;

use super::announce::{Announce, Decoder};
use super::clock::{self, Anchor, Timeline};
use super::crypto::{self, AudioCipher};
use super::playout::{Inserted, Playout};
use super::rtsp::{self, Request, Response};
use super::{AIRTUNES_VERSION, Receiver, bind_udp, params, rtp};
use crate::audio::AIRPLAY_FORMAT;
use crate::metadata::MetadataEvent;

const PUBLIC: &str =
    "ANNOUNCE, SETUP, RECORD, PAUSE, FLUSH, TEARDOWN, OPTIONS, GET_PARAMETER, SET_PARAMETER, POST";
/// How often we ask the sender's clock.
const TIMING_INTERVAL: Duration = Duration::from_secs(3);
/// Asked this many times, [`TIMING_BURST_INTERVAL`] apart, as a stream
/// starts: the first request can wait in the sender's queue while it sets
/// up, which skews that sample, and the map should be good at once.
const TIMING_BURST: u32 = 4;
const TIMING_BURST_INTERVAL: Duration = Duration::from_millis(100);
/// How often the sender's lead is logged.
const LEAD_LOG_INTERVAL: Duration = Duration::from_secs(30);
/// The most packets one resend request asks for. More missing than this
/// is an outage, not a loss, and asking won't save it.
const MAX_RESEND: u16 = 128;

/// Runs one connection until the sender closes it or it is dropped.
pub(crate) async fn run(receiver: Arc<Receiver>, id: u64, tcp: TcpStream, peer: SocketAddr) {
    // Whether it ends by closing, an error or being taken over.
    let _end = EndGuard {
        receiver: receiver.clone(),
        id,
    };
    let _ = tcp.set_nodelay(true);
    let local = match tcp.local_addr() {
        Ok(a) => a.ip().to_canonical(),
        Err(e) => {
            tracing::debug!(error = %e, "AirPlay connection gone at once");
            return;
        }
    };
    let (read, mut write) = tcp.into_split();
    let mut read = BufReader::new(read);
    let mut conn = Connection {
        receiver,
        id,
        peer: peer.ip().to_canonical(),
        local,
        announce: None,
        stream: None,
        volume_db: None,
    };
    tracing::debug!("AirPlay sender connected");
    loop {
        let request = match Request::read(&mut read).await {
            Ok(Some(r)) => r,
            Ok(None) => break,
            Err(e) => {
                tracing::debug!(error = %e, "AirPlay connection ended");
                break;
            }
        };
        tracing::trace!(method = %request.method, uri = %request.uri, "AirPlay request");
        let response = conn.answer(&request).await;
        if let Err(e) = write.write_all(&response.to_bytes()).await {
            tracing::debug!(error = %e, "AirPlay connection ended");
            break;
        }
    }
    tracing::debug!("AirPlay sender disconnected");
}

/// Ends the connection's part in the session however its task ends,
/// aborted included.
struct EndGuard {
    receiver: Arc<Receiver>,
    id: u64,
}

impl Drop for EndGuard {
    fn drop(&mut self) {
        self.receiver.end(self.id);
    }
}

struct Connection {
    receiver: Arc<Receiver>,
    id: u64,
    /// The sender, as a v4 address if it is one.
    peer: IpAddr,
    /// The address it reached us on, which the challenge response signs.
    local: IpAddr,
    announce: Option<Announce>,
    stream: Option<Stream>,
    /// The sender's last volume.
    volume_db: Option<f64>,
}

impl Connection {
    async fn answer(&mut self, req: &Request) -> Response {
        let response = match req.method.as_str() {
            "OPTIONS" => Response::ok().header("Public", PUBLIC),
            "ANNOUNCE" => self.announce(req),
            "SETUP" => self.setup(req).await,
            "RECORD" => self.record(),
            "FLUSH" => self.flush(req),
            "TEARDOWN" => self.teardown(),
            "SET_PARAMETER" => self.set_parameter(req),
            "GET_PARAMETER" => self.get_parameter(req),
            // iOS's keep-alive.
            "POST" if req.uri == "/feedback" => Response::ok(),
            method => {
                tracing::debug!(method, uri = %req.uri, "unsupported AirPlay request");
                Response::new(501)
            }
        };
        let mut response = response
            .header("Server", format!("AirTunes/{AIRTUNES_VERSION}"))
            .header("Audio-Jack-Status", "connected; type=analog");
        if let Some(cseq) = req.header("CSeq") {
            response = response.header("CSeq", cseq);
        }
        if let Some(challenge) = req.header("Apple-Challenge") {
            match crypto::apple_response(challenge, self.local, self.receiver.device_id) {
                Ok(answer) => response = response.header("Apple-Response", answer),
                Err(e) => tracing::warn!(error = %e, "can't answer the AirPlay challenge"),
            }
        }
        response
    }

    fn announce(&mut self, req: &Request) -> Response {
        let sdp = String::from_utf8_lossy(&req.body);
        match Announce::parse(&sdp) {
            Ok(announce) => {
                tracing::info!(
                    codec = ?announce.codec,
                    encrypted = announce.cipher.is_some(),
                    user_agent = req.header("User-Agent").unwrap_or_default(),
                    "AirPlay sender announced a stream"
                );
                self.receiver.take_over(self.id);
                self.stream = None;
                self.announce = Some(announce);
                Response::ok()
            }
            Err(e) => {
                tracing::warn!(error = %e, "refusing an AirPlay stream");
                Response::new(415)
            }
        }
    }

    async fn setup(&mut self, req: &Request) -> Response {
        let Some(announce) = &self.announce else {
            return Response::new(455);
        };
        let transport = req.header("Transport").unwrap_or_default();
        let port = |key| rtsp::param(transport, key).and_then(|p| p.parse::<u16>().ok());
        let ports = ClientPorts {
            control: port("control_port"),
            timing: port("timing_port"),
        };
        self.stream = None;
        let stream = match Stream::start(
            self.receiver.clone(),
            self.peer,
            ports,
            announce.cipher.clone(),
            Decoder::new(&announce.codec),
        )
        .await
        {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "can't open the AirPlay stream's UDP ports");
                return Response::new(500);
            }
        };
        let answer = format!(
            "RTP/AVP/UDP;unicast;mode=record;server_port={};control_port={};timing_port={}",
            stream.ports[0], stream.ports[1], stream.ports[2]
        );
        tracing::debug!(transport = answer, "AirPlay stream set up");
        self.stream = Some(stream);
        Response::ok()
            .header("Transport", answer)
            .header("Session", "1")
    }

    fn record(&mut self) -> Response {
        let Some(stream) = &self.stream else {
            return Response::new(455);
        };
        self.receiver.begin(self.id);
        stream.resume();
        let latency =
            self.receiver.config.latency.as_secs_f64() * f64::from(AIRPLAY_FORMAT.sample_rate);
        Response::ok().header("Audio-Latency", (latency as u32).to_string())
    }

    fn flush(&mut self, req: &Request) -> Response {
        let Some(stream) = &self.stream else {
            return Response::ok();
        };
        let before = req
            .header("RTP-Info")
            .and_then(|i| rtsp::param(i, "rtptime"))
            .and_then(|t| t.parse::<u32>().ok());
        tracing::debug!(rtptime = before, "AirPlay flush");
        stream.flush(before);
        Response::ok()
    }

    fn teardown(&mut self) -> Response {
        self.stream = None;
        self.announce = None;
        self.receiver.end(self.id);
        Response::ok()
    }

    fn set_parameter(&mut self, req: &Request) -> Response {
        let content_type = req.header("Content-Type").unwrap_or_default();
        for event in params::events(content_type, &req.body) {
            if let MetadataEvent::Volume(_) = &event {
                self.volume_db = volume_db(&req.body);
            }
            tracing::debug!(?event, "AirPlay metadata");
            self.receiver.event(event);
        }
        Response::ok()
    }

    fn get_parameter(&self, req: &Request) -> Response {
        if !String::from_utf8_lossy(&req.body).contains("volume") {
            return Response::ok();
        }
        let db = self.volume_db.unwrap_or_else(|| {
            // The level a receiver starts at, on AirPlay's -30..0 dB.
            f64::from(self.receiver.volume.unwrap_or(100).min(100)) / 100.0 * 30.0 - 30.0
        });
        Response::ok().body(
            "text/parameters",
            format!("volume: {db:.6}\r\n").into_bytes(),
        )
    }
}

fn volume_db(body: &[u8]) -> Option<f64> {
    String::from_utf8_lossy(body).lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        (k.trim() == "volume").then(|| v.trim().parse().ok())?
    })
}

/// The sender's ports, from its SETUP.
#[derive(Debug, Clone, Copy)]
struct ClientPorts {
    control: Option<u16>,
    timing: Option<u16>,
}

/// A set-up stream: its sockets' tasks and what they share.
struct Stream {
    /// Our server, control and timing ports.
    ports: [u16; 3],
    shared: Arc<Shared>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for Stream {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

struct Shared {
    receiver: Arc<Receiver>,
    ingest: Mutex<Ingest>,
    /// Wakes the playout when something was queued.
    queued: Notify,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, Ingest> {
        // Every critical section leaves the state consistent.
        self.ingest.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// From packet to queued frames.
struct Ingest {
    cipher: Option<AudioCipher>,
    decoder: Decoder,
    playout: Playout,
    /// The highest sequence number seen since the start or last flush.
    highest: Option<u16>,
    /// Numbers our resend requests.
    resend_seq: u16,
    /// Flushed, and no audio since.
    paused: bool,
    /// Packets that wouldn't decode, for one warning.
    undecodable: u64,
    /// When the sender means each frame to play.
    timeline: Timeline,
}

impl Ingest {
    /// Queues one packet; returns the sequence numbers to ask for again,
    /// as a first number and a count, and whether this was the first audio
    /// after a pause.
    fn packet(
        &mut self,
        now: Instant,
        header: rtp::AudioHeader,
        payload: &[u8],
        resent: bool,
    ) -> (Option<(u16, u16)>, bool) {
        let mut missing = None;
        if !resent {
            match self.highest {
                Some(h) => {
                    let ahead = header.seq.wrapping_sub(h);
                    if (1..0x8000).contains(&ahead) {
                        if ahead > 1 {
                            missing = Some((h.wrapping_add(1), (ahead - 1).min(MAX_RESEND)));
                        }
                        self.highest = Some(header.seq);
                    }
                }
                None => self.highest = Some(header.seq),
            }
        }
        let mut payload = payload.to_vec();
        if let Some(cipher) = &self.cipher {
            cipher.decrypt(&mut payload);
        }
        let frames = match self.decoder.decode(&payload) {
            Ok(f) => f,
            Err(e) => {
                self.undecodable += 1;
                if self.undecodable == 1 {
                    tracing::warn!(error = %e, seq = header.seq, "dropping an AirPlay packet");
                }
                return (missing, false);
            }
        };
        let resumed = std::mem::take(&mut self.paused);
        match self.playout.insert(now, header.timestamp, frames) {
            Inserted::Queued => {}
            Inserted::Overflow => tracing::debug!("AirPlay buffer full; dropping a packet"),
            other => tracing::trace!(seq = header.seq, ?other, "AirPlay packet not queued"),
        }
        (missing, resumed)
    }
}

impl Stream {
    async fn start(
        receiver: Arc<Receiver>,
        peer: IpAddr,
        client: ClientPorts,
        cipher: Option<AudioCipher>,
        decoder: Decoder,
    ) -> std::io::Result<Self> {
        let range = receiver.config.udp_ports.as_ref();
        let audio = bind_udp(peer, range).await?;
        let control = Arc::new(bind_udp(peer, range).await?);
        let timing = bind_udp(peer, range).await?;
        let ports = [
            audio.local_addr()?.port(),
            control.local_addr()?.port(),
            timing.local_addr()?.port(),
        ];
        let latency = receiver.config.latency;
        let clock = receiver.config.clock;
        let shared = Arc::new(Shared {
            receiver,
            ingest: Mutex::new(Ingest {
                cipher,
                decoder,
                playout: Playout::new(latency),
                highest: None,
                resend_seq: 0,
                paused: false,
                undecodable: 0,
                timeline: Timeline::new(clock),
            }),
            queued: Notify::new(),
        });
        let control_to = client.control.map(|p| SocketAddr::new(peer, p));
        let timing_to = client.timing.map(|p| SocketAddr::new(peer, p));
        // Each task logs under the connection's span.
        let tasks = vec![
            tokio::spawn(
                receive_audio(shared.clone(), audio, peer, control.clone(), control_to)
                    .in_current_span(),
            ),
            tokio::spawn(receive_control(shared.clone(), control, peer).in_current_span()),
            tokio::spawn(keep_time(shared.clone(), timing, peer, timing_to).in_current_span()),
            tokio::spawn(play_out(shared.clone()).in_current_span()),
        ];
        Ok(Self {
            ports,
            shared,
            tasks,
        })
    }

    fn flush(&self, before: Option<u32>) {
        let was_paused = {
            let mut ingest = self.shared.lock();
            ingest.playout.flush(before);
            ingest.highest = None;
            std::mem::replace(&mut ingest.paused, true)
        };
        self.shared.queued.notify_one();
        if !was_paused {
            self.shared.receiver.event(MetadataEvent::Paused);
        }
    }

    /// RECORD after a flush resumes as audio would.
    fn resume(&self) {
        let paused = std::mem::take(&mut self.shared.lock().paused);
        if paused {
            self.shared.receiver.event(MetadataEvent::Resumed);
        }
    }
}

/// Queues a packet and asks for any it skipped.
async fn queue(
    shared: &Shared,
    packet: (rtp::AudioHeader, &[u8]),
    resent: bool,
    control: &UdpSocket,
    control_to: Option<SocketAddr>,
) {
    let (missing, resumed, request_seq) = {
        let mut ingest = shared.lock();
        let (missing, resumed) = ingest.packet(Instant::now(), packet.0, packet.1, resent);
        if missing.is_some() {
            ingest.resend_seq = ingest.resend_seq.wrapping_add(1);
        }
        (missing, resumed, ingest.resend_seq)
    };
    shared.queued.notify_one();
    if resumed {
        shared.receiver.event(MetadataEvent::Resumed);
    }
    if let (Some((first, count)), Some(to)) = (missing, control_to) {
        tracing::debug!(first, count, "asking for lost AirPlay packets");
        let _ = control
            .send_to(&rtp::resend_request(request_seq, first, count), to)
            .await;
    }
}

async fn receive_audio(
    shared: Arc<Shared>,
    socket: UdpSocket,
    peer: IpAddr,
    control: Arc<UdpSocket>,
    control_to: Option<SocketAddr>,
) {
    let mut buf = vec![0u8; 2048];
    loop {
        let (n, from) = match socket.recv_from(&mut buf).await {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(error = %e, "AirPlay audio socket");
                continue;
            }
        };
        if from.ip().to_canonical() != peer {
            continue;
        }
        if let Some(packet) = rtp::parse_audio(&buf[..n]) {
            queue(&shared, packet, false, &control, control_to).await;
        }
    }
}

async fn receive_control(shared: Arc<Shared>, socket: Arc<UdpSocket>, peer: IpAddr) {
    let mut buf = vec![0u8; 2048];
    loop {
        let (n, from) = match socket.recv_from(&mut buf).await {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(error = %e, "AirPlay control socket");
                continue;
            }
        };
        if from.ip().to_canonical() != peer {
            continue;
        }
        let packet = &buf[..n];
        match rtp::payload_type(packet) {
            Some(rtp::PAYLOAD_RESEND_REPLY) => {
                if let Some(audio) = rtp::parse_resend_reply(packet) {
                    queue(&shared, audio, true, &socket, None).await;
                }
            }
            Some(rtp::PAYLOAD_SYNC) => {
                if let Some(anchor) = Anchor::parse(packet) {
                    let mut ingest = shared.lock();
                    if ingest.timeline.anchor.is_none() {
                        tracing::debug!(?anchor, "the AirPlay sender's first sync");
                    }
                    ingest.timeline.anchor = Some(anchor);
                }
            }
            other => tracing::trace!(?other, "unknown AirPlay control packet"),
        }
    }
}

async fn keep_time(
    shared: Arc<Shared>,
    socket: UdpSocket,
    peer: IpAddr,
    timing_to: Option<SocketAddr>,
) {
    let clock = shared.receiver.config.clock;
    let mut buf = [0u8; 128];
    let mut asked = 0u32;
    let mut next = Instant::now();
    loop {
        tokio::select! {
            () = tokio::time::sleep_until(next) => {
                asked += 1;
                next += if asked < TIMING_BURST { TIMING_BURST_INTERVAL } else { TIMING_INTERVAL };
                if let Some(to) = timing_to {
                    let request = shared.lock().timeline.request();
                    let _ = socket.send_to(&request, to).await;
                }
            }
            r = socket.recv_from(&mut buf) => {
                let Ok((n, from)) = r else { continue };
                if from.ip().to_canonical() != peer {
                    continue;
                }
                let packet = &buf[..n];
                let now = clock.now_us();
                if shared.lock().timeline.map.reply(packet, now) {
                    continue;
                }
                // It asks ours: the server clock, as NTP.
                if let Some(reply) = rtp::timing_reply(packet, clock::us_ntp(now)) {
                    let _ = socket.send_to(&reply, from).await;
                }
            }
        }
    }
}

async fn play_out(shared: Arc<Shared>) {
    let clock = shared.receiver.config.clock;
    let mut lead_logged: Option<Instant> = None;
    loop {
        let wake = shared.lock().playout.wake_at();
        match wake {
            Some(at) => {
                tokio::select! {
                    () = tokio::time::sleep_until(at) => {}
                    () = shared.queued.notified() => {}
                }
            }
            None => shared.queued.notified().await,
        }
        let due: Vec<_> = {
            let mut ingest = shared.lock();
            let due = ingest.playout.pop_due(Instant::now());
            due.into_iter()
                .map(|p| (ingest.timeline.play_at_us(p.rtp), p.frames))
                .collect()
        };
        for (play_at_us, frames) in due {
            if let Some(at) = play_at_us
                && lead_logged.is_none_or(|t| t.elapsed() >= LEAD_LOG_INTERVAL)
            {
                lead_logged = Some(Instant::now());
                tracing::debug!(
                    lead_ms = (at - clock.now_us()) / 1000,
                    "the AirPlay sender means the audio played now to play this far ahead"
                );
            }
            let samples = frames.iter().map(|&s| f32::from(s) / 32_768.0).collect();
            shared
                .receiver
                .audio(&PcmChunk::new(AIRPLAY_FORMAT, samples), play_at_us);
        }
    }
}
