//! The receiver end to end, on localhost: driven by our own RAOP sender
//! (`tas-airplay-out`, as iTunes drives a speaker), and by a hand-rolled
//! sender that does what an iPhone does and ours doesn't: the challenge,
//! encrypted compressed ALAC, metadata, a lost packet and a second phone.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use rsa::Pkcs1v15Sign;
use tas_airplay_out::{NtpClock, RaopSession};
use tas_audio::{PcmChunk, ServerClock};
use tas_proto::Series;
use tas_telemetry::{Point, Telemetry};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::announce::tests::{alac_packet, sdp};
use super::crypto::tests::{encrypt_payload, public_key};
use super::params::tests::listing;
use super::rtsp::{self, Response};
use super::*;
use crate::audio::{AirplayAudio, KEEP_LEAD};
use crate::metadata::{SenderVolume, TrackInfo};
use tas_sendspin::testing::FakeSpeaker;

#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<MetadataEvent>>,
    samples: Mutex<Vec<f32>>,
    /// Each chunk's frame count and play time, and when it was handed
    /// over.
    stamps: Mutex<Vec<(usize, Option<i64>, Instant)>>,
}

impl ReceiverHandler for Recorder {
    fn event(&self, _: &ReceiverInfo, event: &MetadataEvent) {
        self.events.lock().unwrap().push(event.clone());
    }
    fn audio(&self, _: &ReceiverInfo, chunk: &PcmChunk, play_at_us: Option<i64>) {
        self.samples
            .lock()
            .unwrap()
            .extend_from_slice(&chunk.samples);
        let stamp = (chunk.frames(), play_at_us, Instant::now());
        self.stamps.lock().unwrap().push(stamp);
    }
}

impl Recorder {
    fn events(&self) -> Vec<MetadataEvent> {
        self.events.lock().unwrap().clone()
    }
    fn samples(&self) -> Vec<f32> {
        self.samples.lock().unwrap().clone()
    }
}

struct Rig {
    addr: SocketAddr,
    clock: ServerClock,
    hub: Telemetry,
    receivers: tas_telemetry::Roster<tas_proto::Receiver>,
    sessions: tas_telemetry::Roster<tas_proto::Session>,
    recorder: Arc<Recorder>,
    stop: watch::Sender<bool>,
    task: JoinHandle<io::Result<()>>,
}

const DEVICE: &str = "kitchen";

impl Rig {
    async fn start() -> Self {
        Self::start_with(Vec::new()).await
    }

    /// A receiver whose audio and events go to `handlers` as well as the
    /// recorder.
    async fn start_with(handlers: Vec<Arc<dyn ReceiverHandler>>) -> Self {
        Self::start_configured(handlers, |_| {}).await
    }

    /// As [`start_with`](Self::start_with), with `configure` having the
    /// last word on the config.
    async fn start_configured(
        handlers: Vec<Arc<dyn ReceiverHandler>>,
        configure: impl FnOnce(&mut NativeConfig),
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let recorder = Arc::new(Recorder::default());
        let clock = ServerClock::new();
        let mut config = NativeConfig::new("server", clock, None);
        config.latency = Duration::from_millis(100);
        config.end_after = Duration::from_millis(300);
        // Tests run side by side; any port will do.
        config.udp_ports = None;
        config.tcp_ports = None;
        config.telemetry = Telemetry::new();
        configure(&mut config);
        let (hub, receivers, sessions) = (
            config.telemetry.clone(),
            config.receivers.clone(),
            config.sessions.clone(),
        );
        let info = ReceiverInfo::native(DEVICE, "Kitchen".into(), addr.port(), None);
        let (stop, mut stopped) = watch::channel(false);
        let mut all: Vec<Arc<dyn ReceiverHandler>> = vec![recorder.clone()];
        all.extend(handlers);
        let handler: Arc<dyn ReceiverHandler> = Arc::new(all);
        let task = tokio::spawn(async move {
            serve(config, info, handler, listener, Some(50), &mut stopped).await
        });
        Self {
            addr,
            clock,
            hub,
            receivers,
            sessions,
            recorder,
            stop,
            task,
        }
    }

    async fn until(&self, what: &str, f: impl Fn(&Recorder) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !f(&self.recorder) {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Every point of `series` the receiver has emitted.
    fn points(&self, series: Series) -> Vec<Point> {
        let entity = crate::route::source_id(DEVICE);
        self.hub
            .subscribe(self.clock.now_us())
            .history
            .into_iter()
            .filter(|t| t.series == series && *t.entity == *entity)
            .flat_map(|t| t.points)
            .collect()
    }

    async fn until_point(&self, series: Series) -> Vec<Point> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let points = self.points(series);
            if !points.is_empty() {
                return points;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {series:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn shutdown(self) {
        self.stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .expect("serve stopped")
            .unwrap()
            .unwrap();
        assert!(self.receivers.all().is_empty(), "unlisted once it stops");
        assert!(self.sessions.all().is_empty());
    }
}

fn s16(samples: &[f32]) -> Vec<i16> {
    samples.iter().map(|&s| (s * 32_768.0) as i16).collect()
}

fn tone(frames: usize, phase: usize) -> Vec<i16> {
    (0..frames * 2)
        .map(|i| (((i / 2 + phase) as f32 * 0.03).sin() * 9000.0) as i16)
        .collect()
}

#[tokio::test]
async fn our_own_sender_plays_through_it() {
    let rig = Rig::start().await;
    let ntp = NtpClock::new(ServerClock::new());
    let mut sender = RaopSession::connect(rig.addr, ntp, -15.0).await.unwrap();
    assert_eq!(sender.audio_latency(), Some(4410), "our latency, 100 ms");
    let sent = tone(44_100 / 2, 0);
    sender.push(&sent).await.unwrap();
    sender.finish().await.unwrap();

    rig.until("the audio", |r| r.samples().len() >= sent.len())
        .await;
    assert_eq!(s16(&rig.recorder.samples()), sent, "lossless, in order");
    assert_eq!(
        rig.recorder.events(),
        [
            MetadataEvent::PlayBegin,
            MetadataEvent::Volume(SenderVolume {
                level: 50,
                muted: false
            })
        ]
    );

    sender.teardown().await;
    rig.until("the end", |r| {
        r.events().last() == Some(&MetadataEvent::PlayEnd)
    })
    .await;
    rig.shutdown().await;
}

/// The sender's timeline comes through: it anchors frame r0 at server-clock
/// time t (its NTP clock is the server clock, as tas-airplay-out's is), and
/// the chunk the receiver hands over for r0 is stamped t.
#[tokio::test]
async fn chunks_carry_the_senders_play_time() {
    let rig = Rig::start().await;
    let ntp = NtpClock::new(rig.clock);
    let mut sender = RaopSession::connect(rig.addr, ntp, -15.0).await.unwrap();
    // Two seconds and a bit: the sender sends each packet 2 s early, by
    // when the receiver's opening burst of timing requests is done.
    let t = rig.clock.now_us() + 2_500_000;
    sender.anchor(t).await.unwrap();
    let sent = tone(44_100 / 2, 0);
    sender.push(&sent).await.unwrap();
    sender.finish().await.unwrap();

    rig.until("the audio", |r| r.samples().len() >= sent.len())
        .await;
    let stamps = rig.recorder.stamps.lock().unwrap().clone();
    let mut frames = 0;
    for (n, stamp, handed) in stamps {
        let want = t + frames * 1_000_000 / 44_100;
        let got = stamp.expect("every chunk is stamped");
        // The clock map's error on localhost, and a µs of rounding.
        assert!(
            (got - want).abs() < 500,
            "frame {frames}: stamped {got}, sent for {want}"
        );
        // Handed over as it arrived, with the whole of the two seconds the
        // sender sent it ahead by: not the receiver's latency (100 ms here)
        // less.
        let lead = got - rig.clock.us_at(handed);
        assert!(
            lead > 1_960_000,
            "frame {frames}: handed over {lead} µs ahead"
        );
        frames += n as i64;
    }
    sender.teardown().await;
    rig.shutdown().await;
}

/// The phone's timeline reaches a Sendspin player, less its surplus lead:
/// our sender anchors frame r0 at server-clock time t, 2.5 s ahead, and the
/// player gets r0's audio stamped t less the lead the stream trimmed,
/// which leaves it the send-ahead and [`KEEP_LEAD`] after it arrived, then
/// the rest contiguous. A FLUSH reaches it as `stream/clear` at once, with
/// none of the flushed audio after it, and the next anchor is trimmed
/// afresh.
#[tokio::test]
async fn a_senders_timeline_and_flush_reach_a_sendspin_player() {
    let audio = AirplayAudio::new();
    let rig = Rig::start_with(vec![audio.clone()]).await;
    audio.started(&ReceiverInfo::native(DEVICE, "Kitchen".into(), 7000, None));
    let source = audio.source(DEVICE).unwrap().unwrap();
    let stream = tas_sendspin::AudioStream::start(rig.clock, source).unwrap();
    let mut speaker = FakeSpeaker::new(rig.clock, "kitchen");
    stream.add_player(speaker.conn());

    let ntp = NtpClock::new(rig.clock);
    let mut sender = RaopSession::connect(rig.addr, ntp, -15.0).await.unwrap();
    let t = rig.clock.now_us() + 2_500_000;
    sender.anchor(t).await.unwrap();
    sender.push(&tone(44_100, 0)).await.unwrap();
    sender.finish().await.unwrap();

    let (first, seen) = chunks(&mut speaker, 10).await;
    let (added_us, trimmed_us) = offset_us(&speaker, rig.clock.now_us());
    assert_trimmed(first[0].0 - seen, trimmed_us);
    assert_placed(&first, t - trimmed_us, added_us);
    assert_eq!(speaker.control(), ["start"]);

    // Most of the second is still to play: the flush drops it.
    let flushed_at = rig.clock.now_us();
    assert!(flushed_at < t + 500_000, "flushed too late to tell");
    sender.flush().await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut control = Vec::new();
    while !control.contains(&"clear") {
        assert!(Instant::now() < deadline, "no stream/clear: {control:?}");
        tokio::time::sleep(Duration::from_millis(5)).await;
        control.extend(speaker.control());
    }
    // The chunks sent before the clear are the player's to drop; none of
    // the flushed audio may come after it.
    speaker.audio();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(speaker.audio(), [], "the flushed tail still came");

    let t2 = rig.clock.now_us() + 2_200_000;
    sender.anchor(t2).await.unwrap();
    sender.push(&tone(44_100 / 2, 0)).await.unwrap();
    sender.finish().await.unwrap();
    let (second, seen) = chunks(&mut speaker, 10).await;
    let (added_us, trimmed_us) = offset_us(&speaker, rig.clock.now_us());
    assert_trimmed(second[0].0 - seen, trimmed_us);
    assert_placed(&second, t2 - trimmed_us, added_us);

    sender.teardown().await;
    stream.stop().await;
    rig.shutdown().await;
}

/// The first `n` audio chunks the speaker gets, within 5 s, and when the
/// first was seen on the server clock, within the 5 ms it is polled at.
async fn chunks(speaker: &mut FakeSpeaker, n: usize) -> (Vec<(i64, usize)>, i64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut got = Vec::new();
    let mut seen = None;
    while got.len() < n {
        assert!(Instant::now() < deadline, "{} chunks of {n}", got.len());
        tokio::time::sleep(Duration::from_millis(5)).await;
        got.extend(speaker.audio());
        if !got.is_empty() {
            seen.get_or_insert(speaker.conn().clock().now_us());
        }
    }
    (got, seen.expect("seen"))
}

/// What the stream added to the sender's play times, and what it trimmed
/// from them, in µs.
fn offset_us(speaker: &FakeSpeaker, now: i64) -> (i64, i64) {
    let stats = speaker.conn().stats().snapshot(now, 0);
    let stream = stats.stream.expect("streamed to");
    let us = |ms: f64| (ms * 1000.0).round() as i64;
    (us(stream.added_latency_ms), us(stream.trimmed_lead_ms))
}

/// The first chunk of a timeline went out the players' send-ahead and
/// [`KEEP_LEAD`] before it plays, which, seen a poll or a busy scheduler
/// late, is `lead`, and most of a sender's two seconds were trimmed for
/// it.
fn assert_trimmed(lead: i64, trimmed_us: i64) {
    let want = tas_sendspin::stream::DEFAULT_SEND_AHEAD_US + KEEP_LEAD.as_micros() as i64;
    assert!(
        (want - 100_000..=want).contains(&lead),
        "first chunk seen {lead} µs ahead, sent {want} ahead"
    );
    assert!(trimmed_us > 1_500_000, "trimmed {trimmed_us} µs");
}

/// The first chunk plays at `at` and every one after it where the frames
/// before it end, or later by no more than the `added_us` the stream added
/// for chunks handed over late: with no lead kept past the send-ahead, a
/// busy test machine can make some.
fn assert_placed(chunks: &[(i64, usize)], at: i64, added_us: i64) {
    // The clock map's error on localhost, as above.
    let (first, _) = chunks[0];
    assert!(
        (first - at).abs() < 500,
        "first chunk at {first}, sent for {at}"
    );
    let mut gaps = 0;
    let mut end = first;
    for &(ts, n) in chunks {
        assert!(
            ts >= end - 1,
            "{ts} overlaps the chunk before, ending {end}"
        );
        if ts > end + 1 {
            gaps += ts - end;
        }
        end = ts + n as i64 * 1_000_000 / 48_000;
    }
    assert!(
        gaps <= added_us + chunks.len() as i64,
        "{gaps} µs of gaps, {added_us} added"
    );
}

/// A sender that does what an iPhone does, one request at a time.
struct Phone {
    rtsp: BufReader<TcpStream>,
    cseq: u32,
    key: [u8; 16],
    iv: [u8; 16],
    control: UdpSocket,
    timing: Arc<UdpSocket>,
    audio: UdpSocket,
    server: SocketAddr,
    seq: u16,
    rtp: u32,
}

impl Phone {
    async fn connect(addr: SocketAddr) -> Self {
        let tcp = TcpStream::connect(addr).await.unwrap();
        let udp = || UdpSocket::bind("127.0.0.1:0");
        Self {
            rtsp: BufReader::new(tcp),
            cseq: 0,
            key: *b"an aes key, 16 b",
            iv: *b"and its iv, 16 b",
            control: udp().await.unwrap(),
            timing: Arc::new(udp().await.unwrap()),
            audio: udp().await.unwrap(),
            server: addr,
            seq: 65_530,
            rtp: u32::MAX - 1000,
        }
    }

    async fn request(
        &mut self,
        method: &str,
        headers: &[(&str, String)],
        body: Option<(&str, Vec<u8>)>,
    ) -> Response {
        self.request_to(method, "rtsp://127.0.0.1/42", headers, body)
            .await
    }

    async fn request_to(
        &mut self,
        method: &str,
        uri: &str,
        headers: &[(&str, String)],
        body: Option<(&str, Vec<u8>)>,
    ) -> Response {
        self.cseq += 1;
        let mut text = format!("{method} {uri} RTSP/1.0\r\nCSeq: {}\r\n", self.cseq);
        for (k, v) in headers {
            text.push_str(&format!("{k}: {v}\r\n"));
        }
        let bytes = match &body {
            Some((t, b)) => {
                text.push_str(&format!(
                    "Content-Type: {t}\r\nContent-Length: {}\r\n\r\n",
                    b.len()
                ));
                let mut bytes = text.into_bytes();
                bytes.extend_from_slice(b);
                bytes
            }
            None => (text + "\r\n").into_bytes(),
        };
        self.rtsp.get_mut().write_all(&bytes).await.unwrap();
        read_response(&mut self.rtsp).await
    }

    /// OPTIONS (checking the challenge's answer) through RECORD; returns
    /// the receiver's server, control and timing ports.
    async fn start(&mut self) -> [u16; 3] {
        let challenge = [9u8; 16];
        let b64 = base64::engine::general_purpose::STANDARD_NO_PAD;
        let options = self
            .request(
                "OPTIONS",
                &[("Apple-Challenge", b64.encode(challenge))],
                None,
            )
            .await;
        assert_eq!(options.status, 200);
        let mut signed = challenge.to_vec();
        signed.extend_from_slice(&[127, 0, 0, 1]);
        signed.extend_from_slice(&device_id("server", DEVICE));
        signed.resize(32, 0);
        let signature = crypto::decode_base64(options.get("Apple-Response").unwrap()).unwrap();
        public_key()
            .verify(Pkcs1v15Sign::new_unprefixed(), &signed, &signature)
            .expect("the phone accepts the receiver");

        let sdp = sdp(Some((&self.key, &self.iv)));
        let announce = self
            .request("ANNOUNCE", &[], Some(("application/sdp", sdp.into_bytes())))
            .await;
        assert_eq!(announce.status, 200);
        let transport = format!(
            "RTP/AVP/UDP;unicast;interleaved=0-1;mode=record;control_port={};timing_port={}",
            self.control.local_addr().unwrap().port(),
            self.timing.local_addr().unwrap().port()
        );
        let setup = self
            .request("SETUP", &[("Transport", transport)], None)
            .await;
        assert_eq!(setup.status, 200);
        let t = setup.get("Transport").unwrap();
        let port = |k| rtsp::param(t, k).unwrap().parse().unwrap();
        let ports = [
            port("server_port"),
            port("control_port"),
            port("timing_port"),
        ];
        let info = format!("seq={};rtptime={}", self.seq, self.rtp);
        let record = self.request("RECORD", &[("RTP-Info", info)], None).await;
        assert_eq!(record.status, 200);
        ports
    }

    /// Answers the receiver's timing requests from a clock `offset_us`
    /// ahead of `clock`, as long as the phone is around.
    fn keep_time(&self, clock: ServerClock, offset_us: i64) -> JoinHandle<()> {
        let timing = self.timing.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 64];
            while let Ok((n, from)) = timing.recv_from(&mut buf).await {
                let now = clock::us_ntp(clock.now_us() + offset_us);
                if let Some(reply) = tas_airplay_out::rtp::timing_reply(&buf[..n], now) {
                    let _ = timing.send_to(&reply, from).await;
                }
            }
        })
    }

    /// The next packet of `samples`, encrypted and compressed.
    fn packet(&mut self, samples: &[i16]) -> (u16, Vec<u8>) {
        let mut payload = alac_packet(samples);
        encrypt_payload(&self.key, &self.iv, &mut payload);
        let packet = tas_airplay_out::rtp::audio_packet(false, self.seq, self.rtp, 1, &payload);
        let seq = self.seq;
        self.seq = self.seq.wrapping_add(1);
        self.rtp = self.rtp.wrapping_add((samples.len() / 2) as u32);
        (seq, packet)
    }
}

async fn read_response(r: &mut BufReader<TcpStream>) -> Response {
    Response::read(r).await.unwrap().expect("a response")
}

#[tokio::test]
async fn an_iphone_like_sender_with_a_lost_packet_and_metadata() {
    let rig = Rig::start().await;
    let mut phone = Phone::connect(rig.addr).await;
    let [server_port, ..] = phone.start().await;
    let server = SocketAddr::new(phone.server.ip(), server_port);

    // Ten packets across the sequence and timestamp wraps; the fourth is
    // lost on the way.
    let sent: Vec<Vec<i16>> = (0..10).map(|i| tone(352, i * 352)).collect();
    let mut lost = None;
    for (i, samples) in sent.iter().enumerate() {
        let (seq, packet) = phone.packet(samples);
        if i == 3 {
            lost = Some((seq, packet));
            continue;
        }
        phone.audio.send_to(&packet, server).await.unwrap();
    }
    // The receiver asks for it again, and gets it.
    let mut buf = [0u8; 64];
    let (n, from) = tokio::time::timeout(Duration::from_secs(2), phone.control.recv_from(&mut buf))
        .await
        .expect("a resend request")
        .unwrap();
    let (lost_seq, lost_packet) = lost.unwrap();
    assert_eq!(
        tas_airplay_out::rtp::parse_resend_request(&buf[..n]),
        Some((lost_seq, 1))
    );
    phone
        .control
        .send_to(&tas_airplay_out::rtp::resend_reply(&lost_packet), from)
        .await
        .unwrap();

    let want: Vec<i16> = sent.concat();
    rig.until("the audio", |r| r.samples().len() >= want.len())
        .await;
    assert_eq!(s16(&rig.recorder.samples()), want);

    // Metadata, as an iPhone sends it.
    let params = |body: &str| Some(("text/parameters", body.as_bytes().to_vec()));
    phone
        .request("SET_PARAMETER", &[], params("volume: -7.500000\r\n"))
        .await;
    let track = listing("Teardrop", "Massive Attack", "Mezzanine", 330_000);
    phone
        .request(
            "SET_PARAMETER",
            &[],
            Some(("application/x-dmap-tagged", track)),
        )
        .await;
    let art = vec![0xFF, 0xD8, 0xFF, 0xE0, 0, 1, 2];
    phone
        .request("SET_PARAMETER", &[], Some(("image/jpeg", art.clone())))
        .await;
    phone
        .request("SET_PARAMETER", &[], params("progress: 0/44100/441000\r\n"))
        .await;
    let volume = phone
        .request("GET_PARAMETER", &[], params("volume\r\n"))
        .await;
    assert_eq!(volume.body, b"volume: -7.500000\r\n");
    phone
        .request(
            "FLUSH",
            &[(
                "RTP-Info",
                format!("seq={};rtptime={}", phone.seq, phone.rtp),
            )],
            None,
        )
        .await;
    // Audio after the flush resumes.
    let (_, packet) = phone.packet(&tone(352, 0));
    phone.audio.send_to(&packet, server).await.unwrap();
    rig.until("the resume", |r| {
        r.events().last() == Some(&MetadataEvent::Resumed)
    })
    .await;

    let events = rig.recorder.events();
    assert_eq!(events[0], MetadataEvent::PlayBegin);
    assert_eq!(
        events[1],
        MetadataEvent::Volume(SenderVolume {
            level: 75,
            muted: false
        })
    );
    assert_eq!(
        events[2],
        MetadataEvent::Track(TrackInfo {
            title: Some("Teardrop".into()),
            artist: Some("Massive Attack".into()),
            album: Some("Mezzanine".into()),
            duration: Some(Duration::from_secs(330)),
        })
    );
    assert!(matches!(&events[3], MetadataEvent::Artwork(a) if *a.bytes == art[..]));
    assert!(
        matches!(&events[4], MetadataEvent::Progress(p) if p.elapsed == Duration::from_secs(1))
    );
    assert_eq!(events[5..], [MetadataEvent::Paused, MetadataEvent::Resumed]);

    // Closing the connection ends the session, after a moment.
    drop(phone);
    rig.until("the end", |r| {
        r.events().last() == Some(&MetadataEvent::PlayEnd)
    })
    .await;
    rig.shutdown().await;
}

/// What the receiver measures of a phone whose clock reads [`OFFSET_US`]
/// ahead of ours: the offset itself, each sync's mapped play time, each
/// packet's lead, and marks for a lost packet played as silence, a flush
/// and the session's ends. The receiver and the session are listed while
/// they last.
#[tokio::test]
async fn a_phones_timeline_and_its_losses_are_telemetry() {
    const OFFSET_US: i64 = 3_000_000_000;
    const AHEAD_US: i64 = 2_000_000;
    let rig = Rig::start().await;
    let mut phone = Phone::connect(rig.addr).await;
    let _time = phone.keep_time(rig.clock, OFFSET_US);
    let [server_port, control_port, _] = phone.start().await;
    let receiver_id = crate::route::source_id(DEVICE);
    let sessions = rig.sessions.all();
    assert_eq!(sessions.len(), 1, "{sessions:?}");
    let session = &sessions[0];
    assert_eq!(
        (
            &*session.receiver,
            &*session.sender,
            &*session.codec,
            session.encrypted
        ),
        (&*receiver_id, "127.0.0.1", "alac", true)
    );
    let receiver = rig.receivers.all();
    assert_eq!(receiver[0].session.as_ref(), Some(&session.id));
    assert!(receiver[0].playing);
    assert_eq!(rig.points(Series::PlayBegin).len(), 1);

    // The opening burst of timing requests, each answered.
    for p in rig.until_point(Series::ClockMap).await {
        assert!((p.value as i64 - OFFSET_US).abs() < 1_000, "{p:?}");
    }
    for p in rig.points(Series::ClockOffset) {
        assert!((p.value as i64 - OFFSET_US).abs() < 1_000, "{p:?}");
    }
    assert!(
        rig.points(Series::Rtt)
            .iter()
            .all(|p| (0.0..10_000.0).contains(&p.value))
    );

    // The next frame plays two seconds from now, on the phone's clock.
    let at = rig.clock.now_us() + AHEAD_US;
    let sync = tas_airplay_out::rtp::sync_packet(true, phone.rtp, clock::us_ntp(at + OFFSET_US), 0);
    let control = SocketAddr::new(phone.server.ip(), control_port);
    phone.control.send_to(&sync, control).await.unwrap();
    let synced = rig.until_point(Series::Sync).await;
    assert!(
        (synced[0].value as i64 - at).abs() < 1_000,
        "{synced:?} for {at}"
    );

    // Three packets, the second lost and never sent again.
    let server = SocketAddr::new(phone.server.ip(), server_port);
    for i in 0..3 {
        let (_, packet) = phone.packet(&tone(352, i * 352));
        if i != 1 {
            phone.audio.send_to(&packet, server).await.unwrap();
        }
    }
    let lead = rig.until_point(Series::Lead).await;
    assert!((lead[0].value as i64 - AHEAD_US).abs() < 50_000, "{lead:?}");
    assert!(!rig.until_point(Series::Buffered).await.is_empty());
    let asked = rig.until_point(Series::ResendAsked).await;
    assert_eq!(asked[0].value, 1.0);
    let silence = rig.until_point(Series::Silence).await;
    assert_eq!(silence[0].value.round(), 7982.0, "352 frames");

    phone
        .request(
            "FLUSH",
            &[(
                "RTP-Info",
                format!("seq={};rtptime={}", phone.seq, phone.rtp),
            )],
            None,
        )
        .await;
    assert_eq!(rig.points(Series::Flush).len(), 1);

    drop(phone);
    rig.until_point(Series::PlayEnd).await;
    assert!(rig.sessions.all().is_empty());
    assert!(!rig.receivers.all()[0].playing);
    rig.shutdown().await;
}

#[tokio::test]
async fn a_second_phone_takes_over_without_ending_the_session() {
    let rig = Rig::start().await;
    let mut first = Phone::connect(rig.addr).await;
    first.start().await;
    let mut second = Phone::connect(rig.addr).await;
    second.start().await;

    // The first phone's connection is dropped.
    use tokio::io::AsyncReadExt;
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(2), first.rtsp.read(&mut byte))
        .await
        .expect("the first phone is cut off");
    assert!(matches!(read, Ok(0) | Err(_)));

    // Past the first phone's end: the session goes on with the second.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(rig.recorder.events(), [MetadataEvent::PlayBegin]);

    let teardown = second.request("TEARDOWN", &[], None).await;
    assert_eq!(teardown.status, 200);
    rig.until("the end", |r| {
        r.events().last() == Some(&MetadataEvent::PlayEnd)
    })
    .await;
    assert_eq!(
        rig.recorder.events(),
        [MetadataEvent::PlayBegin, MetadataEvent::PlayEnd]
    );
    rig.shutdown().await;
}

#[tokio::test]
async fn a_connection_reused_after_its_teardown_can_still_be_taken_over() {
    let rig = Rig::start().await;
    let mut first = Phone::connect(rig.addr).await;
    first.start().await;
    let teardown = first.request("TEARDOWN", &[], None).await;
    assert_eq!(teardown.status, 200);
    // The same socket, streaming again.
    first.start().await;
    let mut second = Phone::connect(rig.addr).await;
    second.start().await;

    // The first phone's connection is dropped, so only one plays.
    use tokio::io::AsyncReadExt;
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(2), first.rtsp.read(&mut byte))
        .await
        .expect("the first phone is cut off");
    assert!(matches!(read, Ok(0) | Err(_)));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(rig.recorder.events(), [MetadataEvent::PlayBegin]);
    rig.shutdown().await;
}

#[tokio::test]
async fn a_new_record_soon_after_a_teardown_keeps_the_session() {
    let rig = Rig::start().await;
    let mut phone = Phone::connect(rig.addr).await;
    phone.start().await;
    phone.request("TEARDOWN", &[], None).await;
    // Back between tracks, within `end_after`.
    let mut again = Phone::connect(rig.addr).await;
    again.start().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(rig.recorder.events(), [MetadataEvent::PlayBegin]);
    rig.shutdown().await;
}

#[tokio::test]
async fn requests_out_of_order_and_unknown_ones_are_refused() {
    let rig = Rig::start().await;
    let mut phone = Phone::connect(rig.addr).await;
    assert_eq!(phone.request("SETUP", &[], None).await.status, 455);
    assert_eq!(phone.request("RECORD", &[], None).await.status, 455);
    assert_eq!(phone.request("PLAY", &[], None).await.status, 501);
    let fairplay = "a=rtpmap:96 AppleLossless\r\na=fpaeskey:AAAA\r\n";
    let announce = phone
        .request(
            "ANNOUNCE",
            &[],
            Some(("application/sdp", fairplay.as_bytes().to_vec())),
        )
        .await;
    assert_eq!(announce.status, 415);
    let options = phone.request("OPTIONS", &[], None).await;
    assert_eq!(options.get("CSeq"), Some("5"));
    assert!(options.get("Public").unwrap().contains("ANNOUNCE"));
    assert!(rig.recorder.events().is_empty());
    rig.shutdown().await;
}

/// An AirPlay 2 sender's first request, `GET /info`, is answered on the
/// AirPlay 1 receiver's connection, and AirPlay 1 goes on after it.
#[tokio::test]
async fn info_is_answered_beside_airplay_1() {
    let rig = Rig::start().await;
    let mut phone = Phone::connect(rig.addr).await;
    let info = phone.request_to("GET", "/info", &[], None).await;
    assert_eq!(info.status, 200);
    assert_eq!(info.get("CSeq"), Some("1"));
    assert_eq!(info.get("Content-Type"), Some(crate::ap2::BPLIST));
    let plist = plist::Value::from_reader(std::io::Cursor::new(info.body)).unwrap();
    let dict = plist.as_dictionary().unwrap();
    let hex: Vec<String> = device_id("server", DEVICE)
        .iter()
        .map(|b| format!("{b:02X}"))
        .collect();
    let s = |k: &str| dict.get(k).and_then(plist::Value::as_string);
    assert_eq!(s("deviceID"), Some(hex.join(":").as_str()));
    assert_eq!(s("name"), Some("Kitchen"));
    assert_eq!(
        dict.get("features").unwrap().as_unsigned_integer(),
        Some(crate::ap2::FEATURES)
    );

    let options = phone.request("OPTIONS", &[], None).await;
    assert_eq!((options.status, options.get("CSeq")), (200, Some("2")));
    rig.shutdown().await;
}

/// A phone's AirPlay 2 handshake, up to where SETUP will go: transient
/// pairing, then everything encrypted, `/fp-setup` included, with AirPlay
/// 1's methods still answered on the same connection.
#[tokio::test]
async fn an_airplay_2_sender_pairs_and_talks_encrypted() {
    use crate::ap2::testing::{Controller, PairError};
    use crate::ap2::tlv::error;

    let rig = Rig::start().await;
    let mut phone = Controller::connect(rig.addr).await.unwrap();
    let info = phone.info().await.unwrap();
    let pk: [u8; 32] = info
        .get("pk")
        .and_then(plist::Value::as_data)
        .unwrap()
        .try_into()
        .unwrap();
    phone.pair_setup("3939").await.unwrap();
    assert!(phone.keys().is_some());

    // Encrypted now: the same requests, answered the same way.
    let again = phone.info().await.unwrap();
    assert_eq!(
        again.get("pk").and_then(plist::Value::as_data),
        Some(&pk[..])
    );
    let options = phone.request("OPTIONS", "*", &[], None).await.unwrap();
    // /info, M1, M3, /info again, then this.
    assert_eq!((options.status, options.get("CSeq")), (200, Some("5")));
    assert_eq!(options.get("Server"), Some("AirTunes/366.0"));
    for mode in 0..4 {
        let (first, second) = phone.fp_setup(mode).await.unwrap();
        assert_eq!((first.status, first.body.len()), (200, 142));
        assert_eq!(first.body[13], mode, "mode {mode}'s reply");
        assert_eq!(second.body.len(), 32);
        assert_eq!(&second.body[..12], b"FPLY\x03\x01\x04\x00\x00\x00\x00\x14");
        assert_eq!(
            &second.body[12..],
            &(144..164).map(|i| (i - 12) as u8).collect::<Vec<_>>()[..]
        );
    }
    // Pairing twice on one connection isn't.
    assert!(matches!(
        phone.pair_setup("3939").await,
        Err(PairError::Status(455))
    ));

    // A wrong PIN is refused, and the connection stays in the clear.
    let mut wrong = Controller::connect(rig.addr).await.unwrap();
    assert!(matches!(
        wrong.pair_setup("1234").await,
        Err(PairError::Refused {
            state: 4,
            code: error::AUTHENTICATION
        })
    ));
    assert!(wrong.keys().is_none());
    assert_eq!(
        wrong
            .info()
            .await
            .unwrap()
            .get("pk")
            .and_then(plist::Value::as_data),
        Some(&pk[..])
    );

    // Pair-verify, against the advertised key.
    let mut verified = Controller::connect(rig.addr).await.unwrap();
    verified.pair_verify(&pk).await.unwrap();
    let options = verified.request("OPTIONS", "*", &[], None).await.unwrap();
    assert_eq!(options.status, 200);
    // And a sender that expects some other device's key gives up on us.
    let mut other = Controller::connect(rig.addr).await.unwrap();
    assert!(matches!(
        other.pair_verify(&[9; 32]).await,
        Err(PairError::Unproven(_))
    ));

    drop((phone, wrong, verified, other));
    rig.shutdown().await;
}

/// A receiver that can play AirPlay 2 audio: a PTP follower on loopback,
/// and the decoded audio on a channel.
async fn ap2_rig() -> (
    Rig,
    tokio::sync::mpsc::Receiver<crate::ap2::buffered::Decoded>,
) {
    let any = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0));
    let ptp =
        crate::ap2::ptp::Ptp::bind_to(ServerClock::new(), Telemetry::default(), any, any).await;
    let (audio, decoded) = tokio::sync::mpsc::channel(64);
    let rig = Rig::start_configured(Vec::new(), move |config| {
        config.ptp = ptp;
        config.ap2_audio = Some(audio);
    })
    .await;
    (rig, decoded)
}

async fn paired(rig: &Rig) -> crate::ap2::testing::Controller {
    let mut phone = crate::ap2::testing::Controller::connect(rig.addr)
        .await
        .unwrap();
    phone.pair_setup("3939").await.unwrap();
    phone
}

async fn decoded(
    audio: &mut tokio::sync::mpsc::Receiver<crate::ap2::buffered::Decoded>,
) -> crate::ap2::buffered::Decoded {
    tokio::time::timeout(Duration::from_secs(5), audio.recv())
        .await
        .expect("decoded audio in time")
        .expect("the channel is open")
}

/// The first stream's `key` in a phase 2 reply.
fn stream_port(response: &Response, key: &str) -> u16 {
    let reply = crate::ap2::testing::reply_plist(response).unwrap();
    let stream = reply["streams"].as_array().unwrap()[0]
        .as_dictionary()
        .unwrap();
    stream[key].as_unsigned_integer().unwrap() as u16
}

/// The raw AAC frames of an ADTS stream.
fn adts_frames(mut adts: &[u8]) -> Vec<&[u8]> {
    let mut frames = Vec::new();
    while adts.len() > 7 {
        assert_eq!((adts[0], adts[1] & 0xf6), (0xff, 0xf0), "an ADTS sync word");
        let header = if adts[1] & 1 == 1 { 7 } else { 9 };
        let len = (usize::from(adts[3] & 3) << 11)
            | (usize::from(adts[4]) << 3)
            | (usize::from(adts[5]) >> 5);
        frames.push(&adts[header..len]);
        adts = &adts[len..];
    }
    frames
}

/// How closely `got` follows `want` at the lag and gain that fit best, as
/// a signal-to-noise ratio in dB.
fn best_snr(got: &[f64], want: impl Fn(usize) -> f64) -> f64 {
    // Past the encoder's delay (ffmpeg's is 1024 frames), and short of the
    // source's end.
    let window = 4096..12_000;
    let fit = |lag: usize| {
        let (mut gw, mut ww) = (0.0, 0.0);
        for n in window.clone() {
            gw += got[n] * want(n - lag);
            ww += want(n - lag) * want(n - lag);
        }
        (gw, ww)
    };
    let lag = (0..=2048)
        .max_by(|&a, &b| {
            let ((ga, wa), (gb, wb)) = (fit(a), fit(b));
            (ga / wa.sqrt()).total_cmp(&(gb / wb.sqrt()))
        })
        .unwrap();
    let (gw, ww) = fit(lag);
    let gain = gw / ww;
    let (mut signal, mut noise) = (0.0, 0.0);
    for n in window {
        let fit = gain * want(n - lag);
        signal += fit * fit;
        noise += (got[n] - fit) * (got[n] - fit);
    }
    10.0 * (signal / noise).log10()
}

/// SETUP in both phases, the event channel, and buffered audio in each
/// codec: decrypted, decoded and handed on in order with its RTP time.
#[tokio::test]
async fn an_airplay_2_sender_streams_alac_and_aac() {
    use crate::ap2::buffered::TYPE_BUFFERED;
    use crate::ap2::setup::AUDIO_BUFFER_SIZE;
    use crate::ap2::testing::{DataSender, reply_plist};

    let (rig, mut audio) = ap2_rig().await;
    let mut phone = paired(&rig).await;
    let local: IpAddr = std::net::Ipv4Addr::LOCALHOST.into();

    let timing = phone.setup_timing(&[local]).await.unwrap();
    assert_eq!(timing.status, 200);
    let reply = reply_plist(&timing).unwrap();
    let event_port = reply["eventPort"].as_unsigned_integer().unwrap() as u16;
    assert_ne!(event_port, 0);
    let ours = reply["timingPeerInfo"].as_dictionary().unwrap();
    assert_eq!(
        ours["Addresses"].as_array().unwrap()[0].as_string(),
        Some("127.0.0.1")
    );
    // The event channel: encrypted with the Events keys, both ways.
    let mut events = phone.open_events(rig.addr, event_port).await.unwrap();
    let command = events.request("POST", "/command", &[], None).await.unwrap();
    assert_eq!(command.status, 200);

    // ALAC is lossless: what comes out is what went in.
    let shk = [0x5a; 32];
    let alac = phone
        .setup_stream(TYPE_BUFFERED, 2, &shk, 352)
        .await
        .unwrap();
    assert_eq!(alac.status, 200);
    assert_eq!(stream_port(&alac, "type"), TYPE_BUFFERED as u16);
    assert_ne!(stream_port(&alac, "controlPort"), 0);
    let reply = reply_plist(&alac).unwrap();
    let stream = reply["streams"].as_array().unwrap()[0]
        .as_dictionary()
        .unwrap();
    assert_eq!(
        stream["audioBufferSize"].as_unsigned_integer(),
        Some(AUDIO_BUFFER_SIZE)
    );
    let port = stream_port(&alac, "dataPort");
    let mut data = DataSender::connect(rig.addr, port, &shk, 1000)
        .await
        .unwrap();
    let sent: Vec<Vec<i16>> = (0..20).map(|i| tone(352, i * 352)).collect();
    for samples in &sent {
        data.send(&alac_packet(samples), 352).await.unwrap();
    }
    for (i, samples) in sent.iter().enumerate() {
        let got = decoded(&mut audio).await;
        assert_eq!((got.seq, got.rtp), (i as u32, 1000 + 352 * i as u32));
        assert_eq!(&got.frames, samples, "packet {i}");
    }
    // RECORD is answered, as a phone sends it.
    let record = phone
        .request("RECORD", "rtsp://127.0.0.1/1", &[], None)
        .await
        .unwrap();
    assert_eq!(record.status, 200);

    // The stream torn down, and one of AAC-LC set up on the same session.
    let mut only_streams = plist::Dictionary::new();
    only_streams.insert("streams".into(), plist::Value::Array(Vec::new()));
    let teardown = phone
        .plist_request(
            "TEARDOWN",
            "rtsp://127.0.0.1/1",
            &plist::Value::Dictionary(only_streams),
        )
        .await
        .unwrap();
    assert_eq!(teardown.status, 200);
    drop(data);
    let aac = phone
        .setup_stream(TYPE_BUFFERED, 4, &shk, 1024)
        .await
        .unwrap();
    assert_eq!(aac.status, 200);
    let port = stream_port(&aac, "dataPort");
    let mut data = DataSender::connect(rig.addr, port, &shk, 50_000)
        .await
        .unwrap();
    // ffmpeg's 0.5 s of a 440 Hz sine on the left and 660 Hz on the
    // right, at 1/8 of full scale.
    let frames = adts_frames(include_bytes!("../../testdata/sine-440-660.aac"));
    assert!(frames.len() > 20);
    for frame in &frames {
        data.send(frame, 1024).await.unwrap();
    }
    let mut pcm = Vec::new();
    for i in 0..frames.len() {
        let got = decoded(&mut audio).await;
        assert_eq!(got.rtp, 50_000 + 1024 * i as u32);
        assert_eq!(got.frames.len(), 2 * 1024);
        pcm.extend(got.frames);
    }
    // AAC is lossy: the source, within its tolerance.
    for (channel, hz) in [(0, 440.0), (1, 660.0)] {
        let got: Vec<f64> = pcm
            .iter()
            .skip(channel)
            .step_by(2)
            .map(|&s| f64::from(s) / 32_768.0)
            .collect();
        let want = |n: usize| 0.125 * (2.0 * std::f64::consts::PI * hz * n as f64 / 44_100.0).sin();
        let snr = best_snr(&got, want);
        assert!(snr > 25.0, "{hz} Hz decoded at {snr:.1} dB");
    }

    drop((data, events, phone));
    rig.shutdown().await;
}

/// What a test's log says, by every task on its thread.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Logs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Logs {
    /// Captures the log of the current thread (a `#[tokio::test]`'s
    /// runtime) while the guard lives.
    fn capture() -> (Self, tracing::subscriber::DefaultGuard) {
        let logs = Self::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .finish();
        (logs, tracing::subscriber::set_default(subscriber))
    }

    fn contains(&self, what: &str) -> bool {
        String::from_utf8_lossy(&self.0.lock().unwrap()).contains(what)
    }
}

/// What SETUP can't play is refused with a reason in the log, and the
/// connection carries on.
#[tokio::test]
async fn airplay_2_setup_refuses_what_it_cant_play() {
    use crate::ap2::buffered::{TYPE_BUFFERED, TYPE_REALTIME};
    use crate::ap2::testing::{Controller, DataSender};

    let (logs, _guard) = Logs::capture();
    let (rig, mut audio) = ap2_rig().await;
    let local: IpAddr = std::net::Ipv4Addr::LOCALHOST.into();
    let shk = [0x5a; 32];

    // Not before pairing, and no stream before its timing.
    let mut stranger = Controller::connect(rig.addr).await.unwrap();
    assert_eq!(stranger.setup_timing(&[local]).await.unwrap().status, 455);
    let mut phone = paired(&rig).await;
    let early = phone
        .setup_stream(TYPE_BUFFERED, 2, &shk, 352)
        .await
        .unwrap();
    assert_eq!(early.status, 455);
    assert_eq!(phone.setup_timing(&[local]).await.unwrap().status, 200);

    // Realtime audio, naming it.
    let realtime = phone
        .setup_stream(TYPE_REALTIME, 2, &shk, 352)
        .await
        .unwrap();
    assert_eq!(realtime.status, 501);
    assert!(logs.contains("realtime audio stream (type 96)"));
    // A key that isn't one, and AAC-ELD, which symphonia doesn't decode.
    let short = phone
        .setup_stream(TYPE_BUFFERED, 2, &[1; 16], 352)
        .await
        .unwrap();
    assert_eq!(short.status, 400);
    let eld = phone
        .setup_stream(TYPE_BUFFERED, 8, &shk, 480)
        .await
        .unwrap();
    assert_eq!(eld.status, 415);
    assert!(logs.contains("AAC-ELD"));

    // A sender sealing with some other key: nothing decodes, and the data
    // channel is closed on it.
    let ok = phone
        .setup_stream(TYPE_BUFFERED, 2, &shk, 352)
        .await
        .unwrap();
    assert_eq!(ok.status, 200);
    let port = stream_port(&ok, "dataPort");
    let mut data = DataSender::connect(rig.addr, port, &[0x77; 32], 0)
        .await
        .unwrap();
    for i in 0..8 {
        data.send(&alac_packet(&tone(352, i * 352)), 352)
            .await
            .unwrap();
    }
    let _ = tokio::time::timeout(Duration::from_secs(5), data.closed())
        .await
        .expect("the data channel closed");
    assert!(audio.try_recv().is_err());
    assert!(logs.contains("wrong shk"));
    let options = phone.request("OPTIONS", "*", &[], None).await.unwrap();
    assert_eq!(options.status, 200);
    drop((stranger, phone));
    rig.shutdown().await;

    // Without PTP, the timing phase is answered and its audio refused.
    let rig = Rig::start().await;
    let mut phone = paired(&rig).await;
    assert_eq!(phone.setup_timing(&[local]).await.unwrap().status, 200);
    let refused = phone
        .setup_stream(TYPE_BUFFERED, 2, &shk, 352)
        .await
        .unwrap();
    assert_eq!(refused.status, 503);
    assert!(logs.contains("no PTP timing"));
    drop(phone);
    rig.shutdown().await;
}
