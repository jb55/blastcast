//! A fake AirPlay 1 speaker for tests: an RTSP listener and the three UDP
//! ports, recording everything a sender does. It is public so other
//! crates' tests can stream to it too.
//!
//! It answers every RTSP request 200 (or the status [`FakeSpeaker::refuse`]
//! set for a method), gives `SETUP` its ports, decodes each audio packet's
//! ALAC and keeps each sync packet. [`FakeSpeaker::play_time_us`] says when
//! a frame plays by the last sync, which is how a test checks timing.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::alac;
use crate::rtp::{self, AudioHeader, NtpClock, Sync};
use crate::session::SAMPLE_RATE;

/// One RTSP request as the speaker read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub method: String,
    pub uri: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// One audio packet: its header, its decoded samples, and whether it came
/// as a resend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    pub header: AudioHeader,
    pub samples: Vec<i16>,
    pub resent: bool,
}

#[derive(Debug, Default)]
pub struct Log {
    pub requests: Vec<Request>,
    pub packets: Vec<Packet>,
    pub syncs: Vec<Sync>,
    /// Timing replies, read as (origin, receive, transmit) NTP times.
    pub timing_replies: Vec<(u64, u64, u64)>,
    /// The sender's control and timing ports, from its SETUP.
    pub sender_control: Option<SocketAddr>,
    pub sender_timing: Option<SocketAddr>,
}

impl Log {
    pub fn methods(&self) -> Vec<&str> {
        self.requests.iter().map(|r| r.method.as_str()).collect()
    }

    /// Every sample received, in order of arrival, resends left out.
    pub fn samples(&self) -> Vec<i16> {
        self.packets
            .iter()
            .filter(|p| !p.resent)
            .flat_map(|p| p.samples.iter().copied())
            .collect()
    }
}

pub struct FakeSpeaker {
    addr: SocketAddr,
    log: Arc<Mutex<Log>>,
    changed: Arc<Notify>,
    refuse: Arc<Mutex<Vec<(String, u16)>>>,
    /// Closes the current RTSP connection when notified.
    hang_up: Arc<Notify>,
    timing: Arc<UdpSocket>,
    control: Arc<UdpSocket>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for FakeSpeaker {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl FakeSpeaker {
    /// A speaker listening on loopback.
    pub async fn start() -> Self {
        let rtsp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = rtsp.local_addr().unwrap();
        let audio = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let control = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let timing = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let log = Arc::new(Mutex::new(Log::default()));
        let changed = Arc::new(Notify::new());
        let refuse = Arc::new(Mutex::new(Vec::new()));
        let hang_up = Arc::new(Notify::new());
        let ports = Ports {
            audio: audio.local_addr().unwrap().port(),
            control: control.local_addr().unwrap().port(),
            timing: timing.local_addr().unwrap().port(),
        };
        let tasks = vec![
            tokio::spawn(serve_rtsp(
                rtsp,
                ports,
                log.clone(),
                changed.clone(),
                refuse.clone(),
                hang_up.clone(),
            )),
            tokio::spawn(read_audio(audio, false, log.clone(), changed.clone())),
            tokio::spawn(read_control(control.clone(), log.clone(), changed.clone())),
            tokio::spawn(read_timing(timing.clone(), log.clone(), changed.clone())),
        ];
        Self {
            addr,
            log,
            changed,
            refuse,
            hang_up,
            timing,
            control,
            tasks,
        }
    }

    /// The RTSP address to stream to.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn log(&self) -> MutexGuard<'_, Log> {
        self.log.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Answers `method` with `status` from now on.
    pub fn refuse(&self, method: &str, status: u16) {
        self.refuse
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((method.to_owned(), status));
    }

    /// Closes the sender's RTSP connection, as a speaker does when another
    /// sender takes it.
    pub fn hang_up(&self) {
        self.hang_up.notify_waiters();
    }

    /// Waits until `f` holds of the log, or panics after `wait`.
    pub async fn wait_for(&self, what: &str, wait: Duration, mut f: impl FnMut(&Log) -> bool) {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let notified = self.changed.notified();
            if f(&self.log()) {
                return;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                let log = self.log();
                assert!(
                    f(&log),
                    "never: {what}; methods {:?}, {} packets, {} frames, {} syncs",
                    log.methods(),
                    log.packets.len(),
                    log.samples().len() / 2,
                    log.syncs.len()
                );
                return;
            }
        }
    }

    /// Asks the sender's timing port for its time, as a speaker does.
    pub async fn ask_time(&self, now: u64) {
        let to = self.log().sender_timing.expect("SETUP named a timing port");
        self.timing
            .send_to(&rtp::timing_request(now), to)
            .await
            .unwrap();
    }

    /// Asks the sender to send `count` packets from `seq` again.
    pub async fn ask_resend(&self, seq: u16, count: u16) {
        let to = self
            .log()
            .sender_control
            .expect("SETUP named a control port");
        self.control
            .send_to(&rtp::resend_request(seq, count), to)
            .await
            .unwrap();
    }

    /// When frame `rtp` plays, in server-clock microseconds, by the last
    /// sync packet.
    pub fn play_time_us(&self, ntp: &NtpClock, rtp: u32) -> Option<i64> {
        let log = self.log();
        let sync = log.syncs.last()?;
        let frames = i64::from(rtp.wrapping_sub(sync.playing) as i32);
        Some(ntp.us_at(sync.ntp) + frames * 1_000_000 / i64::from(SAMPLE_RATE))
    }
}

#[derive(Clone, Copy)]
struct Ports {
    audio: u16,
    control: u16,
    timing: u16,
}

async fn serve_rtsp(
    listener: TcpListener,
    ports: Ports,
    log: Arc<Mutex<Log>>,
    changed: Arc<Notify>,
    refuse: Arc<Mutex<Vec<(String, u16)>>>,
    hang_up: Arc<Notify>,
) {
    loop {
        let Ok((stream, peer)) = listener.accept().await else {
            return;
        };
        let hung_up = hang_up.notified();
        tokio::select! {
            () = hung_up => {}
            () = serve_one(stream, peer, ports, &log, &changed, &refuse) => {}
        }
        changed.notify_waiters();
    }
}

async fn serve_one(
    stream: TcpStream,
    peer: SocketAddr,
    ports: Ports,
    log: &Mutex<Log>,
    changed: &Notify,
    refuse: &Mutex<Vec<(String, u16)>>,
) {
    let mut stream = BufReader::new(stream);
    loop {
        let Some(request) = read_request(&mut stream).await else {
            return;
        };
        let status = refuse
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .find(|(m, _)| *m == request.method)
            .map_or(200, |(_, s)| *s);
        let mut extra = String::new();
        if status == 200 && request.method == "SETUP" {
            let transport = request.header("Transport").unwrap_or("").to_owned();
            let port = |key| {
                crate::rtsp::transport_param(&transport, key)
                    .and_then(|p| p.parse::<u16>().ok())
                    .map(|p| SocketAddr::new(peer.ip(), p))
            };
            {
                let mut log = log.lock().unwrap_or_else(PoisonError::into_inner);
                log.sender_control = port("control_port");
                log.sender_timing = port("timing_port");
            }
            extra = format!(
                "Session: DEADBEEF;timeout=60\r\nTransport: RTP/AVP/UDP;unicast;mode=record;server_port={};control_port={};timing_port={}\r\n",
                ports.audio, ports.control, ports.timing
            );
        }
        if status == 200 && request.method == "RECORD" {
            extra = "Audio-Latency: 11025\r\n".into();
        }
        let cseq = request.header("CSeq").unwrap_or("0").to_owned();
        let response = format!("RTSP/1.0 {status} Status\r\nCSeq: {cseq}\r\n{extra}\r\n");
        log.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .requests
            .push(request);
        changed.notify_waiters();
        if stream
            .get_mut()
            .write_all(response.as_bytes())
            .await
            .is_err()
        {
            return;
        }
    }
}

async fn read_request(r: &mut BufReader<TcpStream>) -> Option<Request> {
    let mut line = String::new();
    if r.read_line(&mut line).await.ok()? == 0 {
        return None;
    }
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_owned();
    let uri = parts.next()?.to_owned();
    let mut headers = Vec::new();
    loop {
        line.clear();
        r.read_line(&mut line).await.ok()?;
        let h = line.trim_end();
        if h.is_empty() {
            break;
        }
        let (k, v) = h.split_once(':')?;
        headers.push((k.trim().to_owned(), v.trim().to_owned()));
    }
    let length = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    let mut body = vec![0; length];
    r.read_exact(&mut body).await.ok()?;
    Some(Request {
        method,
        uri,
        headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}

fn decode(packet: &[u8], resent: bool) -> Option<Packet> {
    let (header, payload) = rtp::parse_audio(packet)?;
    Some(Packet {
        header,
        samples: alac::decode_stereo_s16(payload)?,
        resent,
    })
}

async fn read_audio(
    socket: Arc<UdpSocket>,
    resent: bool,
    log: Arc<Mutex<Log>>,
    changed: Arc<Notify>,
) {
    let mut buf = vec![0u8; 4096];
    loop {
        let Ok(n) = socket.recv(&mut buf).await else {
            return;
        };
        if let Some(p) = decode(&buf[..n], resent) {
            log.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .packets
                .push(p);
            changed.notify_waiters();
        }
    }
}

async fn read_control(socket: Arc<UdpSocket>, log: Arc<Mutex<Log>>, changed: Arc<Notify>) {
    let mut buf = vec![0u8; 4096];
    loop {
        let Ok(n) = socket.recv(&mut buf).await else {
            return;
        };
        let packet = &buf[..n];
        let mut log = log.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(sync) = rtp::parse_sync(packet) {
            log.syncs.push(sync);
        } else if packet.len() > 4
            && packet[1] & 0x7f == rtp::PAYLOAD_RESEND_REPLY
            && let Some(p) = decode(&packet[4..], true)
        {
            log.packets.push(p);
        }
        drop(log);
        changed.notify_waiters();
    }
}

async fn read_timing(socket: Arc<UdpSocket>, log: Arc<Mutex<Log>>, changed: Arc<Notify>) {
    let mut buf = vec![0u8; 128];
    let ntp = |b: &[u8]| u64::from_be_bytes(b.try_into().unwrap());
    loop {
        let Ok(n) = socket.recv(&mut buf).await else {
            return;
        };
        if n >= 32 && buf[1] & 0x7f == rtp::PAYLOAD_TIMING_REPLY {
            let reply = (ntp(&buf[8..16]), ntp(&buf[16..24]), ntp(&buf[24..32]));
            log.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .timing_replies
                .push(reply);
            changed.notify_waiters();
        }
    }
}
