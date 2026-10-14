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
use crate::metadata::{SenderVolume, TrackInfo};

#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<MetadataEvent>>,
    samples: Mutex<Vec<f32>>,
    /// Each chunk's frame count and play time.
    stamps: Mutex<Vec<(usize, Option<i64>)>>,
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
        let stamp = (chunk.frames(), play_at_us);
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
    recorder: Arc<Recorder>,
    stop: watch::Sender<bool>,
    task: JoinHandle<io::Result<()>>,
}

const DEVICE: &str = "kitchen";

impl Rig {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let recorder = Arc::new(Recorder::default());
        let clock = ServerClock::new();
        let mut config = NativeConfig::new("server", clock, None);
        config.latency = Duration::from_millis(100);
        config.end_after = Duration::from_millis(300);
        // Tests run side by side; any port will do.
        config.udp_ports = None;
        let info = ReceiverInfo::native(DEVICE, "Kitchen".into(), addr.port(), None);
        let (stop, mut stopped) = watch::channel(false);
        let handler = recorder.clone();
        let task = tokio::spawn(async move {
            serve(config, info, handler, listener, Some(50), &mut stopped).await
        });
        Self {
            addr,
            clock,
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

    async fn shutdown(self) {
        self.stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .expect("serve stopped")
            .unwrap()
            .unwrap();
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
    for (n, stamp) in stamps {
        let want = t + frames * 1_000_000 / 44_100;
        let got = stamp.expect("every chunk is stamped");
        // The clock map's error on localhost, and a µs of rounding.
        assert!(
            (got - want).abs() < 500,
            "frame {frames}: stamped {got}, sent for {want}"
        );
        frames += n as i64;
    }
    // Handed over the receiver's latency after arriving, well ahead of
    // when the sender means it to play: that lead is the budget.
    let lead = t - rig.clock.now_us();
    assert!(lead > 1_000_000, "{lead} µs");
    sender.teardown().await;
    rig.shutdown().await;
}

/// A sender that does what an iPhone does, one request at a time.
struct Phone {
    rtsp: BufReader<TcpStream>,
    cseq: u32,
    key: [u8; 16],
    iv: [u8; 16],
    control: UdpSocket,
    timing: UdpSocket,
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
            timing: udp().await.unwrap(),
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
        self.cseq += 1;
        let mut text = format!(
            "{method} rtsp://127.0.0.1/42 RTSP/1.0\r\nCSeq: {}\r\n",
            self.cseq
        );
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
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};
    let mut line = String::new();
    r.read_line(&mut line).await.unwrap();
    let status: u16 = line.split_whitespace().nth(1).unwrap().parse().unwrap();
    let mut response = Response::new(status);
    loop {
        line.clear();
        r.read_line(&mut line).await.unwrap();
        let text = line.trim_end();
        if text.is_empty() {
            break;
        }
        let (k, v) = text.split_once(':').unwrap();
        response = response.header(k.trim(), v.trim());
    }
    if let Some(len) = response.get("Content-Length") {
        let mut body = vec![0; len.parse().unwrap()];
        r.read_exact(&mut body).await.unwrap();
        response.body = body;
    }
    response
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
