//! Noise `KKpsk2` over Curve25519 and SHA-256, with ChaChaPoly or AES-GCM:
//! the two suites the Sendspin spec defines (`connection.md`, Encryption).
//!
//! Only this one pattern is written out, from the Noise spec (revision 34):
//!
//! ```text
//! KKpsk2:
//!   -> s
//!   <- s
//!   ...
//!   -> e, es, ss
//!   <- e, ee, se, psk
//! ```
//!
//! The server is the initiator, the client the responder. A [`Handshake`]
//! is `Clone`, which is what the server's Sentinel fallback needs: it reads
//! message 2 under the PSK its message 1 named and, if that fails, reads
//! the same bytes again from a copy taken before, under the Sentinel.

use aes_gcm::Aes256Gcm;
use chacha20poly1305::ChaCha20Poly1305;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey, StaticSecret};

/// The largest Noise message, handshake or transport.
pub const MAX_MESSAGE: usize = 65535;
/// Every AEAD tag, in both suites.
pub const TAG_LEN: usize = 16;
const DH_LEN: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Suite {
    ChaChaPoly,
    AesGcm,
}

impl Suite {
    /// As `client/init` names it.
    pub fn name(self) -> &'static str {
        match self {
            Suite::ChaChaPoly => "25519_ChaChaPoly_SHA256",
            Suite::AesGcm => "25519_AESGCM_SHA256",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        [Suite::ChaChaPoly, Suite::AesGcm]
            .into_iter()
            .find(|s| s.name() == name)
    }

    fn protocol_name(self) -> String {
        format!("Noise_KKpsk2_{}", self.name())
    }

    /// The 12-byte nonce for counter `n`: four zero bytes, then `n`, little
    /// endian for ChaChaPoly and big endian for AES-GCM.
    fn nonce(self, n: u64) -> [u8; 12] {
        let mut nonce = [0u8; 12];
        match self {
            Suite::ChaChaPoly => nonce[4..].copy_from_slice(&n.to_le_bytes()),
            Suite::AesGcm => nonce[4..].copy_from_slice(&n.to_be_bytes()),
        }
        nonce
    }

    /// One AEAD seal with a 32-byte key.
    pub fn seal(self, key: &[u8; 32], nonce: &[u8; 12], ad: &[u8], plain: &[u8]) -> Vec<u8> {
        let payload = Payload {
            msg: plain,
            aad: ad,
        };
        let sealed = match self {
            Suite::ChaChaPoly => ChaCha20Poly1305::new(key.into()).encrypt(nonce.into(), payload),
            Suite::AesGcm => Aes256Gcm::new(key.into()).encrypt(nonce.into(), payload),
        };
        sealed.expect("sealing to a Vec never fails")
    }

    /// One AEAD open; `None` when the tag doesn't verify.
    pub fn open(
        self,
        key: &[u8; 32],
        nonce: &[u8; 12],
        ad: &[u8],
        sealed: &[u8],
    ) -> Option<Vec<u8>> {
        let payload = Payload {
            msg: sealed,
            aad: ad,
        };
        match self {
            Suite::ChaChaPoly => ChaCha20Poly1305::new(key.into()).decrypt(nonce.into(), payload),
            Suite::AesGcm => Aes256Gcm::new(key.into()).decrypt(nonce.into(), payload),
        }
        .ok()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NoiseError {
    #[error("handshake message has the wrong length")]
    Length,
    #[error("AEAD authentication failed")]
    Decrypt,
    #[error("a Diffie-Hellman result was all zeros")]
    LowOrder,
    #[error("message longer than Noise's 65535 bytes")]
    TooLong,
    #[error("nonce exhausted")]
    Exhausted,
    #[error("handshake message out of order")]
    Order,
}

/// One direction's transport cipher: a key and the counter that numbers
/// each message, which is what makes a replayed or reordered frame fail.
#[derive(Clone)]
pub struct CipherState {
    suite: Suite,
    key: [u8; 32],
    n: u64,
}

impl std::fmt::Debug for CipherState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CipherState")
            .field("suite", &self.suite)
            .field("n", &self.n)
            .finish_non_exhaustive()
    }
}

impl CipherState {
    fn new(suite: Suite, key: [u8; 32]) -> Self {
        Self { suite, key, n: 0 }
    }

    pub fn encrypt(&mut self, ad: &[u8], plain: &[u8]) -> Result<Vec<u8>, NoiseError> {
        if plain.len() + TAG_LEN > MAX_MESSAGE {
            return Err(NoiseError::TooLong);
        }
        let nonce = self.next_nonce()?;
        Ok(self.suite.seal(&self.key, &nonce, ad, plain))
    }

    pub fn decrypt(&mut self, ad: &[u8], sealed: &[u8]) -> Result<Vec<u8>, NoiseError> {
        if sealed.len() > MAX_MESSAGE {
            return Err(NoiseError::TooLong);
        }
        let nonce = self.suite.nonce(self.n);
        if self.n == u64::MAX {
            return Err(NoiseError::Exhausted);
        }
        let plain = self
            .suite
            .open(&self.key, &nonce, ad, sealed)
            .ok_or(NoiseError::Decrypt)?;
        self.n += 1;
        Ok(plain)
    }

    fn next_nonce(&mut self) -> Result<[u8; 12], NoiseError> {
        // 2^64 - 1 is reserved by the spec.
        if self.n == u64::MAX {
            return Err(NoiseError::Exhausted);
        }
        let nonce = self.suite.nonce(self.n);
        self.n += 1;
        Ok(nonce)
    }
}

/// The two transport ciphers a finished handshake splits into, and its
/// hash `h`, which binds pairing to this session and is the prologue of a
/// re-handshake.
#[derive(Debug, Clone)]
pub struct Transport {
    pub send: CipherState,
    pub recv: CipherState,
    pub handshake_hash: [u8; 32],
}

#[derive(Clone)]
struct Symmetric {
    suite: Suite,
    ck: [u8; 32],
    h: [u8; 32],
    k: Option<[u8; 32]>,
    n: u64,
}

impl Symmetric {
    fn new(suite: Suite) -> Self {
        let name = suite.protocol_name();
        // A name that fits in HASHLEN is h itself, zero padded (the
        // AES-GCM one is exactly 32 bytes); a longer one is hashed.
        let h: [u8; 32] = if name.len() <= 32 {
            let mut h = [0u8; 32];
            h[..name.len()].copy_from_slice(name.as_bytes());
            h
        } else {
            Sha256::digest(name.as_bytes()).into()
        };
        Self {
            suite,
            ck: h,
            h,
            k: None,
            n: 0,
        }
    }

    fn mix_hash(&mut self, data: &[u8]) {
        let mut hasher = Sha256::new();
        hasher.update(self.h);
        hasher.update(data);
        self.h = hasher.finalize().into();
    }

    fn mix_key(&mut self, ikm: &[u8]) {
        let [ck, k, _] = hkdf(&self.ck, ikm, 2);
        self.ck = ck;
        self.k = Some(k);
        self.n = 0;
    }

    fn mix_key_and_hash(&mut self, ikm: &[u8]) {
        let [ck, h, k] = hkdf(&self.ck, ikm, 3);
        self.ck = ck;
        self.mix_hash(&h);
        self.k = Some(k);
        self.n = 0;
    }

    fn encrypt_and_hash(&mut self, plain: &[u8]) -> Vec<u8> {
        let out = match self.k {
            Some(k) => {
                let nonce = self.suite.nonce(self.n);
                self.n += 1;
                self.suite.seal(&k, &nonce, &self.h, plain)
            }
            None => plain.to_vec(),
        };
        self.mix_hash(&out);
        out
    }

    fn decrypt_and_hash(&mut self, data: &[u8]) -> Result<Vec<u8>, NoiseError> {
        let out = match self.k {
            Some(k) => {
                let nonce = self.suite.nonce(self.n);
                let plain = self
                    .suite
                    .open(&k, &nonce, &self.h, data)
                    .ok_or(NoiseError::Decrypt)?;
                self.n += 1;
                plain
            }
            None => data.to_vec(),
        };
        self.mix_hash(data);
        Ok(out)
    }

    fn split(&self, initiator: bool) -> Transport {
        let [k1, k2, _] = hkdf(&self.ck, &[], 2);
        let (send, recv) = if initiator { (k1, k2) } else { (k2, k1) };
        Transport {
            send: CipherState::new(self.suite, send),
            recv: CipherState::new(self.suite, recv),
            handshake_hash: self.h,
        }
    }
}

/// Noise's HKDF with HMAC-SHA256: two or three 32-byte outputs (the third
/// is zeros when only two are asked for).
fn hkdf(ck: &[u8; 32], ikm: &[u8], outputs: usize) -> [[u8; 32]; 3] {
    let temp = hmac(ck, &[ikm]);
    let o1 = hmac(&temp, &[&[1]]);
    let o2 = hmac(&temp, &[&o1, &[2]]);
    let o3 = if outputs == 3 {
        hmac(&temp, &[&o2, &[3]])
    } else {
        [0; 32]
    };
    [o1, o2, o3]
}

fn hmac(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("HMAC takes any key length");
    for part in parts {
        mac.update(part);
    }
    mac.finalize().into_bytes().into()
}

fn dh(secret: &StaticSecret, public: &[u8; 32]) -> Result<[u8; 32], NoiseError> {
    let shared = secret.diffie_hellman(&PublicKey::from(*public));
    if !shared.was_contributory() {
        return Err(NoiseError::LowOrder);
    }
    Ok(shared.to_bytes())
}

/// A `KKpsk2` handshake in progress, on either side.
#[derive(Clone)]
pub struct Handshake {
    sym: Symmetric,
    initiator: bool,
    s: StaticSecret,
    rs: [u8; 32],
    e: StaticSecret,
    re: Option<[u8; 32]>,
    psk: [u8; 32],
    step: u8,
}

impl std::fmt::Debug for Handshake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handshake")
            .field("initiator", &self.initiator)
            .field("step", &self.step)
            .finish_non_exhaustive()
    }
}

impl Handshake {
    /// The server's side. `ephemeral` is a fresh key for every handshake.
    pub fn initiator(
        suite: Suite,
        prologue: &[u8],
        local: StaticSecret,
        remote: [u8; 32],
        ephemeral: StaticSecret,
        psk: [u8; 32],
    ) -> Self {
        Self::new(suite, prologue, true, local, remote, ephemeral, psk)
    }

    /// The client's side. Its PSK is chosen from message 1's payload, so
    /// it is set with [`Handshake::set_psk`] before message 2 is written.
    pub fn responder(
        suite: Suite,
        prologue: &[u8],
        local: StaticSecret,
        remote: [u8; 32],
        ephemeral: StaticSecret,
    ) -> Self {
        Self::new(suite, prologue, false, local, remote, ephemeral, [0; 32])
    }

    fn new(
        suite: Suite,
        prologue: &[u8],
        initiator: bool,
        s: StaticSecret,
        rs: [u8; 32],
        e: StaticSecret,
        psk: [u8; 32],
    ) -> Self {
        let mut sym = Symmetric::new(suite);
        sym.mix_hash(prologue);
        let local_pub = PublicKey::from(&s).to_bytes();
        // Pre-messages: the initiator's static, then the responder's.
        let (first, second) = if initiator {
            (local_pub, rs)
        } else {
            (rs, local_pub)
        };
        sym.mix_hash(&first);
        sym.mix_hash(&second);
        Self {
            sym,
            initiator,
            s,
            rs,
            e,
            re: None,
            psk,
            step: 0,
        }
    }

    pub fn set_psk(&mut self, psk: [u8; 32]) {
        self.psk = psk;
    }

    /// Writes the next message: message 1 from the initiator, message 2
    /// from the responder. Message 2 finishes the handshake.
    pub fn write(&mut self, payload: &[u8]) -> Result<Vec<u8>, NoiseError> {
        let mut out = Vec::with_capacity(DH_LEN + payload.len() + TAG_LEN);
        let e_pub = PublicKey::from(&self.e).to_bytes();
        match (self.initiator, self.step) {
            // -> e, es, ss
            (true, 0) => {
                out.extend_from_slice(&e_pub);
                self.sym.mix_hash(&e_pub);
                self.sym.mix_key(&e_pub);
                self.sym.mix_key(&dh(&self.e, &self.rs)?);
                self.sym.mix_key(&dh(&self.s, &self.rs)?);
            }
            // <- e, ee, se, psk
            (false, 1) => {
                let re = self.re.ok_or(NoiseError::Order)?;
                out.extend_from_slice(&e_pub);
                self.sym.mix_hash(&e_pub);
                self.sym.mix_key(&e_pub);
                self.sym.mix_key(&dh(&self.e, &re)?);
                self.sym.mix_key(&dh(&self.e, &self.rs)?);
                self.sym.mix_key_and_hash(&self.psk);
            }
            _ => return Err(NoiseError::Order),
        }
        out.extend_from_slice(&self.sym.encrypt_and_hash(payload));
        if out.len() > MAX_MESSAGE {
            return Err(NoiseError::TooLong);
        }
        self.step += 1;
        Ok(out)
    }

    /// Reads the next message and returns its payload.
    pub fn read(&mut self, message: &[u8]) -> Result<Vec<u8>, NoiseError> {
        if message.len() < DH_LEN + TAG_LEN || message.len() > MAX_MESSAGE {
            return Err(NoiseError::Length);
        }
        let (re, rest) = message.split_at(DH_LEN);
        let re: [u8; 32] = re.try_into().expect("32 bytes");
        match (self.initiator, self.step) {
            // <- e, ee, se, psk
            (true, 1) => {
                self.sym.mix_hash(&re);
                self.sym.mix_key(&re);
                self.sym.mix_key(&dh(&self.e, &re)?);
                self.sym.mix_key(&dh(&self.s, &re)?);
                self.sym.mix_key_and_hash(&self.psk);
            }
            // -> e, es, ss
            (false, 0) => {
                self.sym.mix_hash(&re);
                self.sym.mix_key(&re);
                self.sym.mix_key(&dh(&self.s, &re)?);
                self.sym.mix_key(&dh(&self.s, &self.rs)?);
            }
            _ => return Err(NoiseError::Order),
        }
        self.re = Some(re);
        let payload = self.sym.decrypt_and_hash(rest)?;
        self.step += 1;
        Ok(payload)
    }

    /// The transport ciphers, once both messages are through.
    pub fn finish(self) -> Result<Transport, NoiseError> {
        if self.step != 2 {
            return Err(NoiseError::Order);
        }
        Ok(self.sym.split(self.initiator))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn key(start: u8) -> StaticSecret {
        let bytes: [u8; 32] = std::array::from_fn(|i| start + i as u8);
        StaticSecret::from(bytes)
    }

    /// The same handshake through the Python `noiseprotocol` library
    /// (aiosendspin's), with the ephemerals fixed: server static 1..=32,
    /// client static 33..=64, server ephemeral 65..=96, client ephemeral
    /// 97..=128, the Sentinel PSK.
    #[test]
    fn matches_the_python_noise_library() {
        let vectors = [
            (
                Suite::ChaChaPoly,
                "64b101b1d0be5a8704bd078f9895001fc03e8e9f9522f188dd128d9846d48466d3935f1b7ca29eba9acd42d05cc2a0f6b817fb1df9c8220c87610215d86bd328db5c953677552331b9d489ad561e0acc00ee",
                "244fe3b963e899dd295baffce248d3530f3a9a7479ba063002680ebfe7adad491455e1d734a7297c290ba03fa933c2cb6e4a",
                "0d667297ee241720bdabb46509447e1a8afbe0fc79bfb99e2b6ae070f5c7f62e",
                "095f84ff9ed731ea1725c896d99fc7616616bd88c630044ca92da8e549c076f39f8f17dd6aabfeb1",
                "49d2850ef91032542a1b0bdbaa6bc164d7f1179b8b1900",
                "2563f561f01b1ad723b18031302380de7195361cba19a95871d25370f5877dc754640d471c18f940",
            ),
            (
                Suite::AesGcm,
                "64b101b1d0be5a8704bd078f9895001fc03e8e9f9522f188dd128d9846d484666e34496dc391082b306f45e9ed4830cff2bb2abb80fb177448dbca31e3a5dfb0aa4cc119bd1361c7afe8dfeb8df6e4caf0ad",
                "244fe3b963e899dd295baffce248d3530f3a9a7479ba063002680ebfe7adad492cd5386bed349e22247662178612c373c7f4",
                "dcd2a930bf1179a0ac4fc42842e58c636ce4ad422c2b7a4035b69cedd1214ec3",
                "c90bf17f8d6042149d4a138da99f08f5a389de5054a033bb8ab5900db7101dae665d562a35d23f2c",
                "6c3143e0e166916fe11d8fe36dc6adb21bbb2436ed0036",
                "f4621872605240652a4fb8cc4e4544553905905f4857a8d4a075a0c1f794a19be6cda32d24879687",
            ),
        ];
        let prologue = br#"{"type":"client/init"}{"type":"server/init"}"#;
        let psk: [u8; 32] = Sha256::digest(b"sendspin-sentinel-psk-v1").into();
        for (suite, m1, m2, h, t1, t2, t3) in vectors {
            let client_pub = PublicKey::from(&key(33)).to_bytes();
            let server_pub = PublicKey::from(&key(1)).to_bytes();
            let mut server =
                Handshake::initiator(suite, prologue, key(1), client_pub, key(65), psk);
            let mut client = Handshake::responder(suite, prologue, key(33), server_pub, key(97));

            let msg1 = server
                .write(br#"{"psk_id":"x","psk_category":"sn"}"#)
                .unwrap();
            assert_eq!(msg1, hex(m1), "{suite:?} message 1");
            assert_eq!(
                client.read(&msg1).unwrap(),
                br#"{"psk_id":"x","psk_category":"sn"}"#
            );
            client.set_psk(psk);
            let msg2 = client.write(b"{}").unwrap();
            assert_eq!(msg2, hex(m2), "{suite:?} message 2");
            assert_eq!(server.read(&msg2).unwrap(), b"{}");

            let mut s = server.finish().unwrap();
            let mut c = client.finish().unwrap();
            assert_eq!(s.handshake_hash.to_vec(), hex(h));
            assert_eq!(c.handshake_hash, s.handshake_hash);
            let first = s
                .send
                .encrypt(&[], b"\x00{\"type\":\"server/hello\"}")
                .unwrap();
            assert_eq!(first, hex(t1));
            assert_eq!(s.send.encrypt(&[], b"\x00second").unwrap(), hex(t2));
            let reply = c
                .send
                .encrypt(&[], b"\x00{\"type\":\"client/hello\"}")
                .unwrap();
            assert_eq!(reply, hex(t3));
            assert_eq!(
                c.recv.decrypt(&[], &first).unwrap(),
                b"\x00{\"type\":\"server/hello\"}"
            );
            assert_eq!(
                s.recv.decrypt(&[], &reply).unwrap(),
                b"\x00{\"type\":\"client/hello\"}"
            );
        }
    }

    #[test]
    fn a_wrong_psk_fails_message_2_and_a_copy_can_retry() {
        let suite = Suite::ChaChaPoly;
        let client_pub = PublicKey::from(&key(33)).to_bytes();
        let server_pub = PublicKey::from(&key(1)).to_bytes();
        let mut server = Handshake::initiator(suite, b"p", key(1), client_pub, key(65), [7; 32]);
        let mut client = Handshake::responder(suite, b"p", key(33), server_pub, key(97));
        client.read(&server.write(b"{}").unwrap()).unwrap();
        client.set_psk([9; 32]);
        let msg2 = client.write(b"{}").unwrap();

        let mut retry = server.clone();
        assert_eq!(server.read(&msg2), Err(NoiseError::Decrypt));
        retry.set_psk([9; 32]);
        assert_eq!(retry.read(&msg2).unwrap(), b"{}");
        assert!(retry.finish().is_ok());
    }

    #[test]
    fn transport_rejects_replays_and_tampering() {
        let suite = Suite::AesGcm;
        let client_pub = PublicKey::from(&key(33)).to_bytes();
        let server_pub = PublicKey::from(&key(1)).to_bytes();
        let mut server = Handshake::initiator(suite, b"", key(1), client_pub, key(65), [1; 32]);
        let mut client = Handshake::responder(suite, b"", key(33), server_pub, key(97));
        client.read(&server.write(b"{}").unwrap()).unwrap();
        client.set_psk([1; 32]);
        server.read(&client.write(b"{}").unwrap()).unwrap();
        let (mut s, mut c) = (server.finish().unwrap(), client.finish().unwrap());

        let a = s.send.encrypt(&[], b"a").unwrap();
        let b = s.send.encrypt(&[], b"b").unwrap();
        assert_eq!(
            c.recv.decrypt(&[], &b),
            Err(NoiseError::Decrypt),
            "out of order"
        );
        assert_eq!(c.recv.decrypt(&[], &a).unwrap(), b"a");
        assert_eq!(
            c.recv.decrypt(&[], &a),
            Err(NoiseError::Decrypt),
            "replayed"
        );
        let mut bad = b.clone();
        bad[0] ^= 1;
        assert_eq!(
            c.recv.decrypt(&[], &bad),
            Err(NoiseError::Decrypt),
            "tampered"
        );
        assert_eq!(c.recv.decrypt(&[], &b).unwrap(), b"b");
    }
}
