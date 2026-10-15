//! A sender for tests, standing in for a phone's AirPlay 2 controller: it
//! connects, reads `/info`, pairs (transient pair-setup or pair-verify),
//! runs `/fp-setup`, sends any request to any URI, encrypted once paired,
//! runs SETUP's two phases, opens the event channel, streams buffered
//! audio on the data channel, and anchors and flushes it. Built with the `testing` feature for other
//! crates' tests, such as an interop harness.

use std::io;
use std::net::{IpAddr, SocketAddr};

use chacha20poly1305::aead::Aead;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use x25519_dalek::{PublicKey, StaticSecret};

use super::buffered::{AudioKey, Packet};
use super::channel::{Keys, Role, SecureRead, SecureWrite, encrypt_reader};
use super::pairing::{FLAG_TRANSIENT, nonce, verify_cipher};
use super::tlv::{Tlv, ty};
use super::{BPLIST, TLV8, fairplay, srp};
use crate::raop::rtsp::{Response, RtspError};

/// Why pairing didn't finish.
#[derive(Debug, thiserror::Error)]
pub enum PairError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("the receiver answered {0}")]
    Status(u16),
    #[error("the receiver refused at M{state}: error {code}")]
    Refused { state: u8, code: u8 },
    #[error("the receiver's {0} doesn't check out")]
    Unproven(&'static str),
    #[error("a malformed reply: {0}")]
    Malformed(&'static str),
}

/// One connection to a receiver, as a phone holds it.
pub struct Controller {
    read: BufReader<SecureRead<OwnedReadHalf>>,
    write: SecureWrite<OwnedWriteHalf>,
    cseq: u32,
    keys: Option<Keys>,
}

impl Controller {
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let tcp = TcpStream::connect(addr).await?;
        tcp.set_nodelay(true)?;
        let (read, write) = tcp.into_split();
        Ok(Self {
            read: BufReader::new(SecureRead::new(read)),
            write: SecureWrite::new(write),
            cseq: 0,
            keys: None,
        })
    }

    /// What pairing agreed on, once it has.
    pub fn keys(&self) -> Option<&Keys> {
        self.keys.as_ref()
    }

    /// Sends `method uri` with `headers` and `body` (its content type and
    /// bytes), and reads the answer.
    pub async fn request(
        &mut self,
        method: &str,
        uri: &str,
        headers: &[(&str, &str)],
        body: Option<(&str, &[u8])>,
    ) -> io::Result<Response> {
        self.cseq += 1;
        let mut text = format!(
            "{method} {uri} RTSP/1.0\r\nCSeq: {}\r\nUser-Agent: AirPlay/770.8.1\r\n",
            self.cseq
        );
        for (k, v) in headers {
            text.push_str(&format!("{k}: {v}\r\n"));
        }
        if let Some((content_type, bytes)) = body {
            text.push_str(&format!(
                "Content-Type: {content_type}\r\nContent-Length: {}\r\n",
                bytes.len()
            ));
        }
        text.push_str("\r\n");
        let mut message = text.into_bytes();
        if let Some((_, bytes)) = body {
            message.extend_from_slice(bytes);
        }
        self.write.send(&message).await?;
        match Response::read(&mut self.read).await {
            Ok(Some(r)) => Ok(r),
            Ok(None) => Err(io::ErrorKind::UnexpectedEof.into()),
            Err(RtspError::Io(e)) => Err(e),
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        }
    }

    /// `GET /info`'s plist.
    pub async fn info(&mut self) -> io::Result<plist::Dictionary> {
        let r = self.request("GET", "/info", &[], None).await?;
        plist::Value::from_reader(io::Cursor::new(r.body))
            .ok()
            .and_then(plist::Value::into_dictionary)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "/info isn't a plist"))
    }

    async fn pair_step(&mut self, uri: &str, tlv: Tlv) -> Result<Tlv, PairError> {
        let r = self
            .request("POST", uri, &[], Some((TLV8, &tlv.encode())))
            .await?;
        if r.status != 200 {
            return Err(PairError::Status(r.status));
        }
        let reply = Tlv::decode(&r.body).ok_or(PairError::Malformed("not TLV8"))?;
        if let Some(code) = reply.get_u8(ty::ERROR) {
            let state = reply.get_u8(ty::STATE).unwrap_or(0);
            return Err(PairError::Refused { state, code });
        }
        Ok(reply)
    }

    /// Transient pair-setup with `pin` (3939 is the one that works), then
    /// encrypts the connection.
    pub async fn pair_setup(&mut self, pin: &str) -> Result<(), PairError> {
        let m1 = Tlv::new()
            .put_u8(ty::STATE, 1)
            .put_u8(ty::METHOD, 0)
            .put_u8(ty::FLAGS, FLAG_TRANSIENT);
        let m2 = self.pair_step("/pair-setup", m1).await?;
        let (Some(salt), Some(b)) = (m2.get(ty::SALT), m2.get(ty::PUBLIC_KEY)) else {
            return Err(PairError::Malformed("M2 lacks the salt or B"));
        };
        let client = srp::Client::new(pin);
        let (proof, key) = client
            .prove(salt, b)
            .ok_or(PairError::Malformed("B is out of range"))?;
        let m3 = Tlv::new()
            .put_u8(ty::STATE, 3)
            .put(ty::PUBLIC_KEY, client.public_key())
            .put(ty::PROOF, proof.to_vec());
        let m4 = self.pair_step("/pair-setup", m3).await?;
        let server_proof = m4.get(ty::PROOF).unwrap_or_default();
        if !client.check(&proof, &key, server_proof) {
            return Err(PairError::Unproven("SRP proof"));
        }
        self.encrypt(Keys::new(&key));
        Ok(())
    }

    /// Pair-verify against the receiver's `pk`, then encrypts the
    /// connection.
    pub async fn pair_verify(&mut self, pk: &[u8; 32]) -> Result<(), PairError> {
        let mut seed = [0u8; 32];
        getrandom::fill(&mut seed).expect("the system RNG works");
        let ours = StaticSecret::from(seed);
        let our_pk = PublicKey::from(&ours).to_bytes();
        let m1 = Tlv::new()
            .put_u8(ty::STATE, 1)
            .put(ty::PUBLIC_KEY, our_pk.to_vec());
        let m2 = self.pair_step("/pair-verify", m1).await?;
        let theirs: [u8; 32] = m2
            .get(ty::PUBLIC_KEY)
            .and_then(|k| k.try_into().ok())
            .ok_or(PairError::Malformed("M2 has no key"))?;
        let shared = ours.diffie_hellman(&PublicKey::from(theirs)).to_bytes();
        let cipher = verify_cipher(&shared);
        let inner = m2
            .get(ty::ENCRYPTED_DATA)
            .and_then(|sealed| cipher.decrypt(&nonce(b"PV-Msg02"), sealed).ok())
            .and_then(|plain| Tlv::decode(&plain))
            .ok_or(PairError::Unproven("M2 seal"))?;
        let id = inner.get(ty::IDENTIFIER).unwrap_or_default();
        let signature = inner
            .get(ty::SIGNATURE)
            .and_then(|s| Signature::from_slice(s).ok())
            .ok_or(PairError::Malformed("M2 has no signature"))?;
        VerifyingKey::from_bytes(pk)
            .and_then(|k| k.verify(&[&theirs[..], id, &our_pk].concat(), &signature))
            .map_err(|_| PairError::Unproven("signature"))?;

        // We are a controller it has never seen: any key will do.
        let me = SigningKey::from_bytes(&seed);
        let my_id = b"tas-test-controller";
        let signed = me.sign(&[&our_pk[..], my_id, &theirs].concat());
        let inner = Tlv::new()
            .put(ty::IDENTIFIER, my_id.to_vec())
            .put(ty::SIGNATURE, signed.to_bytes().to_vec());
        let sealed = cipher
            .encrypt(&nonce(b"PV-Msg03"), &inner.encode()[..])
            .expect("a short message seals");
        let m3 = Tlv::new()
            .put_u8(ty::STATE, 3)
            .put(ty::ENCRYPTED_DATA, sealed);
        self.pair_step("/pair-verify", m3).await?;
        self.encrypt(Keys::new(&shared));
        Ok(())
    }

    fn encrypt(&mut self, keys: Keys) {
        let (sealer, opener) = keys.control(Role::Controller);
        encrypt_reader(&mut self.read, opener);
        self.write.encrypt(sealer);
        self.keys = Some(keys);
    }

    /// Both `/fp-setup` steps, as a phone runs them with `mode`; the two
    /// replies.
    pub async fn fp_setup(&mut self, mode: u8) -> io::Result<(Response, Response)> {
        let mut step1 = b"FPLY\x03\x01\x01\x00\x00\x00\x00\x04\x02\x00\x00\x00".to_vec();
        step1[14] = mode;
        let first = self
            .request(
                "POST",
                "/fp-setup",
                &[],
                Some((fairplay::CONTENT_TYPE, &step1)),
            )
            .await?;
        let mut step2 = vec![0u8; 164];
        step2[..12].copy_from_slice(b"FPLY\x03\x01\x03\x00\x00\x00\x00\x98");
        for (i, b) in step2[12..].iter_mut().enumerate() {
            *b = i as u8;
        }
        let second = self
            .request(
                "POST",
                "/fp-setup",
                &[],
                Some((fairplay::CONTENT_TYPE, &step2)),
            )
            .await?;
        Ok((first, second))
    }

    /// Sends `method uri` with a binary plist body, as SETUP is sent.
    pub async fn plist_request(
        &mut self,
        method: &str,
        uri: &str,
        body: &plist::Value,
    ) -> io::Result<Response> {
        let mut bytes = Vec::new();
        body.to_writer_binary(&mut bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        self.request(method, uri, &[], Some((BPLIST, &bytes))).await
    }
}

/// What the test phone calls itself in SETUP.
pub const PHONE_NAME: &str = "Test iPhone";

impl Controller {
    /// SETUP's first phase, as a phone sends it: PTP timing, with `peers`
    /// as the `timingPeerInfo` to follow.
    pub async fn setup_timing(&mut self, peers: &[IpAddr]) -> io::Result<Response> {
        let addresses: Vec<plist::Value> = peers
            .iter()
            .map(|p| plist::Value::String(p.to_string()))
            .collect();
        let mut info = plist::Dictionary::new();
        info.insert("Addresses".into(), plist::Value::Array(addresses));
        info.insert(
            "ID".into(),
            plist::Value::String(peers.first().map(ToString::to_string).unwrap_or_default()),
        );
        let mut body = plist::Dictionary::new();
        body.insert("timingProtocol".into(), plist::Value::String("PTP".into()));
        body.insert("timingPeerInfo".into(), plist::Value::Dictionary(info));
        body.insert(
            "sessionUUID".into(),
            plist::Value::String("5A3D4E3B-0000-4000-8000-746173746573".into()),
        );
        body.insert("isMultiSelectAirPlay".into(), plist::Value::Boolean(true));
        body.insert("name".into(), plist::Value::String(PHONE_NAME.into()));
        self.plist_request(
            "SETUP",
            "rtsp://127.0.0.1/1",
            &plist::Value::Dictionary(body),
        )
        .await
    }

    /// SETUP's second phase: one stream of `kind` (103 is buffered), codec
    /// `ct`, key `shk` and `spf` frames a packet.
    pub async fn setup_stream(
        &mut self,
        kind: u64,
        ct: u64,
        shk: &[u8],
        spf: u64,
    ) -> io::Result<Response> {
        let mut stream = plist::Dictionary::new();
        stream.insert("type".into(), plist::Value::from(kind));
        stream.insert("ct".into(), plist::Value::from(ct));
        stream.insert("shk".into(), plist::Value::Data(shk.to_vec()));
        stream.insert("spf".into(), plist::Value::from(spf));
        stream.insert("sr".into(), plist::Value::from(44_100u64));
        stream.insert("controlPort".into(), plist::Value::from(0u64));
        stream.insert("latencyMin".into(), plist::Value::from(11_025u64));
        let mut body = plist::Dictionary::new();
        body.insert(
            "streams".into(),
            plist::Value::Array(vec![plist::Value::Dictionary(stream)]),
        );
        self.plist_request(
            "SETUP",
            "rtsp://127.0.0.1/1",
            &plist::Value::Dictionary(body),
        )
        .await
    }

    /// SETRATEANCHORTIME: `rate` (1 plays, 0 pauses), and when `anchor`
    /// is given, frame `rtp` playing at `ns` on PTP timeline `timeline`:
    /// `(timeline, rtp, ns)`.
    pub async fn set_rate_anchor(
        &mut self,
        rate: u64,
        anchor: Option<(u64, u32, i128)>,
    ) -> io::Result<Response> {
        let mut body = plist::Dictionary::new();
        body.insert("rate".into(), plist::Value::from(rate));
        if let Some((timeline, rtp, ns)) = anchor {
            let (secs, frac) = super::hold::network_time(ns);
            body.insert("networkTimeSecs".into(), plist::Value::from(secs));
            body.insert("networkTimeFrac".into(), plist::Value::from(frac));
            body.insert("networkTimeTimelineID".into(), plist::Value::from(timeline));
            body.insert("networkTimeFlags".into(), plist::Value::from(0u64));
            body.insert("rtpTime".into(), plist::Value::from(u64::from(rtp)));
        }
        self.plist_request(
            "SETRATEANCHORTIME",
            "rtsp://127.0.0.1/1",
            &plist::Value::Dictionary(body),
        )
        .await
    }

    /// FLUSHBUFFERED: RTP times `from` (everything before, when `None`) up
    /// to `until`, with the sequence numbers a phone sends beside them,
    /// `(seq, rtp)` each.
    pub async fn flush_buffered(
        &mut self,
        from: Option<(u32, u32)>,
        until: (u32, u32),
    ) -> io::Result<Response> {
        let mut body = plist::Dictionary::new();
        if let Some((seq, ts)) = from {
            body.insert("flushFromSeq".into(), plist::Value::from(u64::from(seq)));
            body.insert("flushFromTS".into(), plist::Value::from(u64::from(ts)));
        }
        body.insert(
            "flushUntilSeq".into(),
            plist::Value::from(u64::from(until.0)),
        );
        body.insert(
            "flushUntilTS".into(),
            plist::Value::from(u64::from(until.1)),
        );
        self.plist_request(
            "FLUSHBUFFERED",
            "rtsp://127.0.0.1/1",
            &plist::Value::Dictionary(body),
        )
        .await
    }

    /// Opens the event channel SETUP's first phase answered with, on
    /// `receiver`'s address.
    pub async fn open_events(&self, receiver: SocketAddr, port: u16) -> io::Result<Self> {
        let keys = self
            .keys
            .clone()
            .ok_or_else(|| io::Error::other("not paired"))?;
        let mut events = Self::connect(SocketAddr::new(receiver.ip(), port)).await?;
        let (sealer, opener) = keys.events(Role::Controller);
        encrypt_reader(&mut events.read, opener);
        events.write.encrypt(sealer);
        events.keys = Some(keys);
        Ok(events)
    }
}

/// A plist reply's dictionary.
pub fn reply_plist(response: &Response) -> io::Result<plist::Dictionary> {
    plist::Value::from_reader(io::Cursor::new(&response.body))
        .ok()
        .and_then(plist::Value::into_dictionary)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "not a plist"))
}

/// A phone's end of a buffered stream's data channel.
pub struct DataSender {
    tcp: TcpStream,
    key: AudioKey,
    seq: u32,
    /// The RTP time of the next packet.
    pub rtp: u32,
}

impl DataSender {
    /// Connects to `port` on `receiver`, to send audio sealed with `shk`
    /// from RTP time `rtp`.
    pub async fn connect(
        receiver: SocketAddr,
        port: u16,
        shk: &[u8],
        rtp: u32,
    ) -> io::Result<Self> {
        let key = AudioKey::new(shk).ok_or_else(|| io::Error::other("shk isn't 32 bytes"))?;
        let tcp = TcpStream::connect(SocketAddr::new(receiver.ip(), port)).await?;
        tcp.set_nodelay(true)?;
        Ok(Self {
            tcp,
            key,
            seq: 0,
            rtp,
        })
    }

    /// Sends one packet of `frames` frames: one ALAC packet or AAC frame.
    pub async fn send(&mut self, payload: &[u8], frames: u32) -> io::Result<()> {
        let packet = Packet {
            seq: self.seq,
            rtp: self.rtp,
            // shairplay's AudioSsrc for 44.1 kHz.
            ssrc: 0x0000_face,
            payload: payload.to_vec(),
        };
        let wire = self.key.seal(&packet, u64::from(self.seq).to_le_bytes());
        self.seq = (self.seq + 1) & 0xff_ffff;
        self.rtp = self.rtp.wrapping_add(frames);
        self.tcp.write_all(&wire).await
    }

    /// Waits for the receiver to close the channel.
    pub async fn closed(mut self) -> io::Result<()> {
        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; 64];
        while self.tcp.read(&mut buf).await? > 0 {}
        Ok(())
    }
}

/// A PTP grandmaster for tests, standing in for the phone's clock: it
/// writes the Announce, Sync and Follow_Up messages [`super::ptp`]
/// follows, and sends them to its ports.
#[derive(Debug, Clone)]
pub struct Grandmaster {
    /// Its clock id: the timeline id a SETRATEANCHORTIME names.
    pub clock: u64,
    pub announce: super::ptp::Announce,
    sequence: u16,
}

impl Grandmaster {
    /// Clock `clock`, announcing itself as an iPhone does: priorities 248,
    /// so a HomePod's 128 beats it.
    pub fn new(clock: u64) -> Self {
        Self {
            clock,
            announce: super::ptp::Announce {
                priority1: 248,
                class: 248,
                accuracy: 0xfe,
                variance: 0xffff,
                priority2: 248,
                grandmaster: clock,
                steps_removed: 0,
            },
            sequence: 0,
        }
    }

    fn message(&self, two_step: bool, body: super::ptp::Body) -> Vec<u8> {
        super::ptp::Message {
            source: super::ptp::PortIdentity {
                clock: self.clock,
                port: 1,
            },
            sequence: self.sequence,
            domain: 0,
            two_step,
            correction_ns: 0,
            body,
        }
        .to_bytes()
    }

    /// Its Announce, for the general port.
    pub fn announce(&self) -> Vec<u8> {
        self.message(false, super::ptp::Body::Announce(self.announce))
    }

    /// A two-step Sync that leaves at `sent_ns` on its clock: the Sync,
    /// for the event port, and its Follow_Up, for the general port.
    pub fn sync(&mut self, sent_ns: i128) -> (Vec<u8>, Vec<u8>) {
        self.sequence = self.sequence.wrapping_add(1);
        let at = super::ptp::Timestamp {
            secs: (sent_ns / 1_000_000_000) as u64,
            nanos: (sent_ns % 1_000_000_000) as u32,
        };
        // A two-step Sync's own timestamp is rough; here, a µs early.
        let rough = super::ptp::Timestamp {
            nanos: at.nanos.saturating_sub(1_000),
            ..at
        };
        (
            self.message(true, super::ptp::Body::Sync(rough)),
            self.message(false, super::ptp::Body::FollowUp(at)),
        )
    }

    /// Sends a Sync leaving at `sent_ns` and its Follow_Up from `socket`
    /// to a follower's `event` and `general` ports, and an Announce first
    /// when `announce`.
    pub async fn send(
        &mut self,
        socket: &tokio::net::UdpSocket,
        event: SocketAddr,
        general: SocketAddr,
        sent_ns: i128,
        announce: bool,
    ) -> io::Result<()> {
        if announce {
            socket.send_to(&self.announce(), general).await?;
        }
        let (sync, follow_up) = self.sync(sent_ns);
        socket.send_to(&sync, event).await?;
        socket.send_to(&follow_up, general).await?;
        Ok(())
    }
}
