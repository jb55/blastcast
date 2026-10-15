//! A sender for tests, standing in for a phone's AirPlay 2 controller: it
//! connects, reads `/info`, pairs (transient pair-setup or pair-verify),
//! runs `/fp-setup`, and sends any request to any URI, encrypted once
//! paired. Built with the `testing` feature for other crates' tests, such
//! as an interop harness.

use std::io;
use std::net::SocketAddr;

use chacha20poly1305::aead::Aead;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use tokio::io::BufReader;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use x25519_dalek::{PublicKey, StaticSecret};

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
