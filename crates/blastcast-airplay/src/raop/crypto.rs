//! AirPlay 1's two uses of the AirPort Express key, and the audio cipher.
//!
//! Every AirPlay 1 receiver holds the same RSA key, extracted from an
//! AirPort Express long ago and shipped in shairport-sync (`rtsp.c`). A
//! sender uses its public half twice:
//!
//! - **`Apple-Challenge`**: the sender sends 16 random bytes and checks the
//!   `Apple-Response`, a PKCS#1 v1.5 signature (no digest prefix) over the
//!   challenge, the address the sender reached us on and the receiver's
//!   device id, zero-padded to 32 bytes.
//! - **`a=rsaaeskey`** in the ANNOUNCE: the stream's AES key, encrypted
//!   with RSA-OAEP (SHA-1).
//!
//! The audio is AES-128-CBC with `a=aesiv`, the IV starting afresh for each
//! packet. Only the whole 16-byte blocks are encrypted; the tail is sent
//! as is.

use std::net::IpAddr;
use std::sync::OnceLock;

use aes::cipher::{BlockDecryptMut, KeyIvInit, block_padding::NoPadding};
use base64::Engine;
use base64::engine::{GeneralPurpose, GeneralPurposeConfig, general_purpose};
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::{Oaep, Pkcs1v15Sign, RsaPrivateKey};

const AIRPORT_KEY: &str = include_str!("airport.pem");

/// Senders write base64 with or without its `=` padding.
const BASE64_ANY_PADDING: GeneralPurpose = GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    GeneralPurposeConfig::new()
        .with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent),
);

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("bad base64: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("a challenge is 16 bytes, not {0}")]
    Challenge(usize),
    #[error("an AES key or IV is 16 bytes, not {0}")]
    KeyLength(usize),
    #[error(transparent)]
    Rsa(#[from] rsa::Error),
}

fn key() -> &'static RsaPrivateKey {
    static KEY: OnceLock<RsaPrivateKey> = OnceLock::new();
    KEY.get_or_init(|| RsaPrivateKey::from_pkcs1_pem(AIRPORT_KEY).expect("the AirPort key parses"))
}

/// Base64 as senders write it, padded or not.
pub fn decode_base64(text: &str) -> Result<Vec<u8>, base64::DecodeError> {
    BASE64_ANY_PADDING.decode(text.trim())
}

/// The `Apple-Response` to `challenge` (its base64), from a receiver with
/// device id `device_id`, reached at `local_ip`. Unpadded, as shairport
/// sends it.
pub fn apple_response(
    challenge: &str,
    local_ip: IpAddr,
    device_id: [u8; 6],
) -> Result<String, CryptoError> {
    let challenge = decode_base64(challenge)?;
    if challenge.len() != 16 {
        return Err(CryptoError::Challenge(challenge.len()));
    }
    let mut message = challenge;
    match local_ip.to_canonical() {
        IpAddr::V4(ip) => message.extend_from_slice(&ip.octets()),
        IpAddr::V6(ip) => message.extend_from_slice(&ip.octets()),
    }
    message.extend_from_slice(&device_id);
    if message.len() < 32 {
        message.resize(32, 0);
    }
    let signature = key().sign(Pkcs1v15Sign::new_unprefixed(), &message)?;
    Ok(general_purpose::STANDARD_NO_PAD.encode(signature))
}

/// The stream's AES key, from the ANNOUNCE's `a=rsaaeskey` (its base64).
pub fn decrypt_aes_key(rsaaeskey: &str) -> Result<[u8; 16], CryptoError> {
    let encrypted = decode_base64(rsaaeskey)?;
    let key = key().decrypt(Oaep::new::<sha1::Sha1>(), &encrypted)?;
    let len = key.len();
    key.try_into().map_err(|_| CryptoError::KeyLength(len))
}

/// An `a=aesiv` (its base64).
pub fn decode_iv(aesiv: &str) -> Result<[u8; 16], CryptoError> {
    let iv = decode_base64(aesiv)?;
    let len = iv.len();
    iv.try_into().map_err(|_| CryptoError::KeyLength(len))
}

/// Decrypts each audio packet's payload.
#[derive(Clone)]
pub struct AudioCipher {
    key: [u8; 16],
    iv: [u8; 16],
}

impl std::fmt::Debug for AudioCipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AudioCipher(..)")
    }
}

impl AudioCipher {
    pub fn new(key: [u8; 16], iv: [u8; 16]) -> Self {
        Self { key, iv }
    }

    /// Decrypts `payload` in place: its whole blocks, leaving the tail.
    pub fn decrypt(&self, payload: &mut [u8]) {
        let whole = payload.len() - payload.len() % 16;
        let cipher = cbc::Decryptor::<aes::Aes128>::new(&self.key.into(), &self.iv.into());
        cipher
            .decrypt_padded_mut::<NoPadding>(&mut payload[..whole])
            .expect("whole blocks need no padding");
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use aes::cipher::BlockEncryptMut;
    use rsa::RsaPublicKey;

    use super::*;

    pub(crate) fn public_key() -> RsaPublicKey {
        key().to_public_key()
    }

    /// What a sender puts in `a=rsaaeskey`.
    pub(crate) fn encrypt_aes_key(aes_key: &[u8; 16]) -> String {
        let encrypted = public_key()
            .encrypt(&mut Rng, Oaep::new::<sha1::Sha1>(), aes_key)
            .unwrap();
        general_purpose::STANDARD.encode(encrypted)
    }

    /// What a sender does to each payload.
    pub(crate) fn encrypt_payload(key: &[u8; 16], iv: &[u8; 16], payload: &mut [u8]) {
        let whole = payload.len() - payload.len() % 16;
        let cipher = cbc::Encryptor::<aes::Aes128>::new(key.into(), iv.into());
        cipher
            .encrypt_padded_mut::<NoPadding>(&mut payload[..whole], whole)
            .unwrap();
    }

    /// OAEP's seed. Determinism is fine for a test.
    struct Rng;

    impl rsa::rand_core::RngCore for Rng {
        fn next_u32(&mut self) -> u32 {
            7
        }
        fn next_u64(&mut self) -> u64 {
            7
        }
        fn fill_bytes(&mut self, dest: &mut [u8]) {
            dest.fill(7);
        }
        fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rsa::rand_core::Error> {
            dest.fill(7);
            Ok(())
        }
    }

    impl rsa::rand_core::CryptoRng for Rng {}

    #[test]
    fn the_response_verifies_with_the_airport_public_key() {
        let challenge = [0x11u8; 16];
        let ip: IpAddr = "192.168.1.20".parse().unwrap();
        let device_id = [0xAA, 0xBB, 0xCC, 0x01, 0x02, 0x03];
        // Unpadded, as iOS sends it.
        let sent = general_purpose::STANDARD_NO_PAD.encode(challenge);
        let response = apple_response(&sent, ip, device_id).unwrap();
        assert!(!response.ends_with('='));

        let mut expected = challenge.to_vec();
        expected.extend_from_slice(&[192, 168, 1, 20]);
        expected.extend_from_slice(&device_id);
        expected.resize(32, 0);
        let signature = decode_base64(&response).unwrap();
        public_key()
            .verify(Pkcs1v15Sign::new_unprefixed(), &expected, &signature)
            .expect("a sender accepts it");

        // A v4 client on a dual-stack socket signs its v4 address.
        let mapped: IpAddr = "::ffff:192.168.1.20".parse().unwrap();
        assert_eq!(apple_response(&sent, mapped, device_id).unwrap(), response);
    }

    #[test]
    fn a_v6_response_is_not_padded() {
        let ip: std::net::Ipv6Addr = "fe80::1".parse().unwrap();
        let sent = general_purpose::STANDARD.encode([3u8; 16]);
        let response = apple_response(&sent, ip.into(), [1; 6]).unwrap();
        let mut expected = vec![3u8; 16];
        expected.extend_from_slice(&ip.octets());
        expected.extend_from_slice(&[1; 6]);
        assert_eq!(expected.len(), 38);
        public_key()
            .verify(
                Pkcs1v15Sign::new_unprefixed(),
                &expected,
                &decode_base64(&response).unwrap(),
            )
            .unwrap();
    }

    #[test]
    fn a_short_challenge_is_refused() {
        let sent = general_purpose::STANDARD.encode([1u8; 8]);
        assert!(matches!(
            apple_response(&sent, "10.0.0.1".parse().unwrap(), [0; 6]),
            Err(CryptoError::Challenge(8))
        ));
    }

    #[test]
    fn the_aes_key_and_audio_decrypt() {
        let aes_key = *b"0123456789abcdef";
        let iv = *b"fedcba9876543210";
        assert_eq!(
            decrypt_aes_key(&encrypt_aes_key(&aes_key)).unwrap(),
            aes_key
        );

        let clear: Vec<u8> = (0..=100).collect();
        let mut sent = clear.clone();
        encrypt_payload(&aes_key, &iv, &mut sent);
        assert_ne!(sent[..96], clear[..96]);
        assert_eq!(sent[96..], clear[96..], "the tail goes in the clear");
        AudioCipher::new(aes_key, iv).decrypt(&mut sent);
        assert_eq!(sent, clear);
    }
}
