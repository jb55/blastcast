//! The values the spec derives from keys and codes: identities, `psk_id`,
//! the Sentinel PSK, pairing tokens, the dynamic pairing code, `commit_B`
//! and the wrapping of `wrapped_psk` and `wrapped_nonce_B`.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey, StaticSecret};

use super::noise::Suite;

/// `SHA-256("sendspin-sentinel-psk-v1")`, public, used when no other PSK
/// applies.
pub fn sentinel_psk() -> [u8; 32] {
    Sha256::digest(b"sendspin-sentinel-psk-v1").into()
}

/// `base64url(SHA-256("sendspin-psk-id-v1" || psk))`.
pub fn psk_id(psk: &[u8; 32]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"sendspin-psk-id-v1");
    hasher.update(psk);
    b64url(&hasher.finalize())
}

pub fn b64url(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Decodes base64url without padding into exactly `N` bytes.
pub fn b64url_array<const N: usize>(text: &str) -> Option<[u8; N]> {
    URL_SAFE_NO_PAD.decode(text).ok()?.try_into().ok()
}

/// 32 bytes from the system CSPRNG.
pub fn random32() -> [u8; 32] {
    let mut out = [0u8; 32];
    getrandom::fill(&mut out).expect("the system RNG works");
    out
}

/// A static or ephemeral Curve25519 key pair; its public half, base64url,
/// is a `server_id` or `client_id`.
#[derive(Clone)]
pub struct Identity {
    secret: StaticSecret,
}

impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Identity")
            .field("id", &self.id())
            .finish_non_exhaustive()
    }
}

impl Identity {
    pub fn generate() -> Self {
        Self::from_bytes(random32())
    }

    pub fn from_bytes(private: [u8; 32]) -> Self {
        Self {
            secret: StaticSecret::from(private),
        }
    }

    /// The base64url private key, as it is persisted.
    pub fn from_b64url(text: &str) -> Option<Self> {
        b64url_array(text).map(Self::from_bytes)
    }

    pub fn to_b64url(&self) -> String {
        b64url(self.secret.as_bytes())
    }

    pub fn public(&self) -> [u8; 32] {
        PublicKey::from(&self.secret).to_bytes()
    }

    /// The 43-character id: `server_id` or `client_id`.
    pub fn id(&self) -> String {
        b64url(&self.public())
    }

    pub fn secret(&self) -> StaticSecret {
        self.secret.clone()
    }
}

/// A decoded pairing token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PairingToken {
    /// Version 0: the client's public key and its pairing PSK.
    PairingPsk {
        client_key: [u8; 32],
        pairing_psk: [u8; 32],
    },
    /// Version 1: a dynamic pairing code in the `qr_code` format.
    Code([u8; 24]),
}

impl PairingToken {
    /// Reads operator input leniently: whitespace and case don't matter
    /// and the `SP:` prefix may be left off.
    pub fn decode(input: &str) -> Option<Self> {
        let text = input.trim().to_ascii_uppercase();
        let text = text.strip_prefix("SP:").unwrap_or(&text);
        let mut chars = text.chars();
        let version = chars.next()?;
        let payload = base32_decode(&chars.as_str().replace('9', "2"))?;
        match version {
            '0' if payload.len() >= 64 => Some(Self::PairingPsk {
                client_key: payload[..32].try_into().expect("32 bytes"),
                pairing_psk: payload[32..64].try_into().expect("32 bytes"),
            }),
            '1' if payload.len() >= 24 => {
                Some(Self::Code(payload[..24].try_into().expect("24 bytes")))
            }
            _ => None,
        }
    }

    pub fn encode(&self) -> String {
        let (version, payload) = match self {
            Self::PairingPsk {
                client_key,
                pairing_psk,
            } => (
                '0',
                [client_key.as_slice(), pairing_psk.as_slice()].concat(),
            ),
            Self::Code(code) => ('1', code.to_vec()),
        };
        format!("SP:{version}{}", base32_encode(&payload).replace('2', "9"))
    }
}

const BASE32: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/// RFC 4648 base32, without padding.
fn base32_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(5) * 8);
    for chunk in data.chunks(5) {
        let mut buf = [0u8; 5];
        buf[..chunk.len()].copy_from_slice(chunk);
        let bits = buf.iter().fold(0u64, |acc, &b| (acc << 8) | u64::from(b));
        let chars = (chunk.len() * 8).div_ceil(5);
        for i in 0..chars {
            let index = (bits >> (35 - i * 5)) & 0x1f;
            out.push(BASE32[index as usize] as char);
        }
    }
    out
}

/// RFC 4648 base32 with the padding gone; `None` on a character outside
/// the alphabet or a length no encoding produces.
fn base32_decode(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len() * 5 / 8);
    let (mut acc, mut bits) = (0u32, 0u32);
    for c in text.bytes() {
        let value = BASE32.iter().position(|&b| b == c)? as u32;
        acc = (acc << 5) | value;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    // Leftover bits are padding: fewer than five, and zero.
    (bits < 5 && acc == 0).then_some(out)
}

/// The dynamic pairing code's digest of the handshake and both nonces.
fn code_digest(h: &[u8; 32], nonce_a: &[u8; 32], nonce_b: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"sendspin-pairing-code-derive-v1");
    hasher.update(h);
    hasher.update(nonce_a);
    hasher.update(nonce_b);
    hasher.finalize().into()
}

/// The six-digit code in the `digits` format: the digest, big endian,
/// mod 10^6.
pub fn digits_code(h: &[u8; 32], nonce_a: &[u8; 32], nonce_b: &[u8; 32]) -> String {
    let digest = code_digest(h, nonce_a, nonce_b);
    let rem = digest
        .iter()
        .fold(0u64, |acc, &b| (acc * 256 + u64::from(b)) % 1_000_000);
    format!("{rem:06}")
}

/// The 24-byte code in the `qr_code` format.
pub fn qr_code(h: &[u8; 32], nonce_a: &[u8; 32], nonce_b: &[u8; 32]) -> [u8; 24] {
    code_digest(h, nonce_a, nonce_b)[..24]
        .try_into()
        .expect("24 bytes")
}

/// `commit_B = SHA-256("sendspin-pair-commit-v1" || nonce_B)`.
pub fn commit(nonce_b: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"sendspin-pair-commit-v1");
    hasher.update(nonce_b);
    hasher.finalize().into()
}

/// The CPace `sid`: the label, `h`, and how many pairing activations
/// there have been since the handshake.
pub fn pake_sid(h: &[u8; 32], counter: u32) -> Vec<u8> {
    [
        b"sendspin-pair-pake-v1".as_slice(),
        h,
        &counter.to_be_bytes(),
    ]
    .concat()
}

/// What a wrapped value is: the label of its key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wrapped {
    Psk,
    NonceB,
}

impl Wrapped {
    fn label(self) -> &'static [u8] {
        match self {
            Wrapped::Psk => b"sendspin-pair-psk-wrap-v1",
            Wrapped::NonceB => b"sendspin-pair-nonce-wrap-v1",
        }
    }
}

fn wrap_key(kind: Wrapped, sid: &[u8], isk: &[u8; 64]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(kind.label());
    hasher.update(sid);
    hasher.update(isk);
    hasher.finalize().into()
}

/// Seals a 32-byte value under the CPace output: 48 bytes.
pub fn wrap(suite: Suite, kind: Wrapped, sid: &[u8], isk: &[u8; 64], value: &[u8; 32]) -> Vec<u8> {
    suite.seal(&wrap_key(kind, sid, isk), &[0; 12], &[], value)
}

/// Opens a 48-byte wrapping; `None` when it doesn't authenticate.
pub fn unwrap(
    suite: Suite,
    kind: Wrapped,
    sid: &[u8],
    isk: &[u8; 64],
    sealed: &[u8],
) -> Option<[u8; 32]> {
    suite
        .open(&wrap_key(kind, sid, isk), &[0; 12], &[], sealed)?
        .try_into()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sentinel_values_are_the_published_constants() {
        let psk = sentinel_psk();
        assert_eq!(
            b64url(&psk),
            b64url(&[
                0x1b, 0x5e, 0x24, 0xdb, 0xc1, 0xae, 0xd9, 0x5f, 0xc2, 0xa5, 0xa3, 0x38, 0xa9, 0x0c,
                0x05, 0xdf, 0x44, 0xbd, 0x10, 0xf5, 0xec, 0x1f, 0x4c, 0xd6, 0x6c, 0xbf, 0x86, 0x27,
                0x27, 0x67, 0xb9, 0xd3
            ])
        );
        assert_eq!(psk_id(&psk), "GFsV9tLaSQm9HcFWpKsgYQOr7wFTvNUtkmFwuVz3zoo");
    }

    #[test]
    fn pairing_psk_token_reference_vector() {
        let token = PairingToken::PairingPsk {
            client_key: std::array::from_fn(|i| i as u8),
            pairing_psk: std::array::from_fn(|i| 0xe0 + i as u8),
        };
        let text = "SP:0AAAQEAYEAUDAOCAJBIFQYDIOB4IBCEQTCQKRMFYYDENBWHA5DYP6BYPC4PSOLZXH5DU6V97M5XXO74HR6LZ7J5PW674PT6X37T6757Y";
        assert_eq!(token.encode(), text);
        assert_eq!(PairingToken::decode(text), Some(token.clone()));
        // Lenient: no prefix, lower case, whitespace.
        let sloppy = format!("  {}\n", text[3..].to_ascii_lowercase());
        assert_eq!(PairingToken::decode(&sloppy), Some(token));
    }

    #[test]
    fn qr_code_token_reference_vector() {
        let token = PairingToken::Code(std::array::from_fn(|i| 0xe0 + i as u8));
        let text = "SP:14DQ6FY7E4XTOP9HJ5LV6Z3PO57YPD4XT6T97N5Y";
        assert_eq!(token.encode(), text);
        assert_eq!(PairingToken::decode(text), Some(token));
    }

    #[test]
    fn bad_tokens_are_refused() {
        assert_eq!(PairingToken::decode(""), None);
        assert_eq!(PairingToken::decode("SP:2AAAA"), None, "unknown version");
        assert_eq!(PairingToken::decode("SP:0AAAA"), None, "short payload");
        assert_eq!(PairingToken::decode("SP:1A!AA"), None, "not base32");
    }

    #[test]
    fn digits_code_is_six_digits_of_the_digest() {
        let code = digits_code(&[1; 32], &[2; 32], &[3; 32]);
        assert_eq!(code.len(), 6);
        assert!(code.bytes().all(|b| b.is_ascii_digit()));
        // Cross-checked against Python: int.from_bytes(digest, "big") % 10**6.
        let digest = code_digest(&[1; 32], &[2; 32], &[3; 32]);
        let mut rem = 0u128;
        for b in digest {
            rem = (rem * 256 + u128::from(b)) % 1_000_000;
        }
        assert_eq!(code, format!("{rem:06}"));
        assert_eq!(qr_code(&[1; 32], &[2; 32], &[3; 32]), digest[..24]);
    }

    #[test]
    fn wrapping_round_trips_and_binds_its_label() {
        let (sid, isk) = (pake_sid(&[5; 32], 1), [6u8; 64]);
        for suite in [Suite::ChaChaPoly, Suite::AesGcm] {
            let sealed = wrap(suite, Wrapped::Psk, &sid, &isk, &[7; 32]);
            assert_eq!(sealed.len(), 48);
            assert_eq!(
                unwrap(suite, Wrapped::Psk, &sid, &isk, &sealed),
                Some([7; 32])
            );
            assert_eq!(unwrap(suite, Wrapped::NonceB, &sid, &isk, &sealed), None);
        }
    }

    #[test]
    fn identities_round_trip() {
        let id = Identity::generate();
        let back = Identity::from_b64url(&id.to_b64url()).unwrap();
        assert_eq!(back.id(), id.id());
        assert_eq!(id.id().len(), 43);
    }
}
