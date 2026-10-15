//! SRP-6a as AirPlay 2's `/pair-setup` runs it: RFC 5054's 3072-bit group
//! with `g = 5`, SHA-512, the user `Pair-Setup` and, for transient
//! pairing, the password `3939`.
//!
//! Where it differs from the RFC, it follows Apple (and shairport-sync's
//! csrp fork, which matches Apple):
//!
//! - `k = H(PAD(N) | PAD(g))` and `u = H(PAD(A) | PAD(B))`, padded to the
//!   modulus' 384 bytes;
//! - the session key is `K = H(S)`, 64 bytes;
//! - `M1 = H(H(N) xor H(g) | H(I) | s | A | B | K)` and
//!   `HAMK = H(A | M1 | K)`, with `N`, `g`, `A`, `B` and `S` as their
//!   shortest big-endian bytes.
//!
//! Ported from `openairplay2` 0.5's srp.rs (MIT, Copyright (c) 2026 Stefan
//! Arentz), on crypto-bigint's constant-time Montgomery arithmetic rather
//! than num-bigint.

use std::sync::LazyLock;

use crypto_bigint::modular::runtime_mod::{DynResidue, DynResidueParams};
use crypto_bigint::{Encoding, U256, U512, U3072};
use sha2::{Digest, Sha512};

/// The SRP user name.
pub const USERNAME: &str = "Pair-Setup";
/// Transient pairing's fixed password.
pub const TRANSIENT_PIN: &str = "3939";

/// The modulus' length in bytes.
const N_LEN: usize = 384;

/// RFC 5054's 3072-bit prime.
const N: U3072 = U3072::from_be_hex(
    "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74\
     020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F1437\
     4FE1356D6D51C245E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7ED\
     EE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3DC2007CB8A163BF05\
     98DA48361C55D39A69163FA8FD24CF5F83655D23DCA3AD961C62F356208552BB\
     9ED529077096966D670C354E4ABC9804F1746C08CA18217C32905E462E36CE3B\
     E39E772C180E86039B2783A2EC07A28FB5C55DF06F4C52C9DE2BCBF695581718\
     3995497CEA956AE515D2261898FA051015728E5A8AAAC42DAD33170D04507A33\
     A85521ABDF1CBA64ECFB850458DBEF0A8AEA71575D060C7DB3970F85A6E1E4C7\
     ABF5AE8CDB0933D71E8C94E04A25619DCEE3D2261AD2EE6BF12FFA06D98A0864\
     D87602733EC86A64521F2B18177B200CBBE117577A615D6C770988C0BAD946E2\
     08E24FA074E5AB3143DB5BFCE0FD108E4B82D120A93AD2CAFFFFFFFFFFFFFFFF",
);

/// `N`'s Montgomery constants, worked out once at run time: as a
/// constant they take the compiler minutes.
static PARAMS: LazyLock<DynResidueParams<{ U3072::LIMBS }>> =
    LazyLock::new(|| DynResidueParams::new(&N));

type Mod = DynResidue<{ U3072::LIMBS }>;

fn modn(n: &U3072) -> Mod {
    Mod::new(n, *PARAMS)
}

fn g() -> Mod {
    modn(&U3072::from_u8(5))
}

fn h(parts: &[&[u8]]) -> [u8; 64] {
    let mut hash = Sha512::new();
    for p in parts {
        hash.update(p);
    }
    hash.finalize().into()
}

/// `n` as 384 big-endian bytes.
fn pad(n: &U3072) -> [u8; N_LEN] {
    n.to_be_bytes()
}

/// `n` as its shortest big-endian bytes.
fn short(n: &U3072) -> Vec<u8> {
    let bytes = pad(n);
    let zeros = bytes.iter().take_while(|&&b| b == 0).count();
    bytes[zeros..].to_vec()
}

/// A number off the wire, if it is below the modulus.
fn read(bytes: &[u8]) -> Option<U3072> {
    if bytes.len() > N_LEN {
        return None;
    }
    let mut padded = [0u8; N_LEN];
    padded[N_LEN - bytes.len()..].copy_from_slice(bytes);
    let n = U3072::from_be_slice(&padded);
    (n < N).then_some(n)
}

fn hash_u512(parts: &[&[u8]]) -> U512 {
    U512::from_be_slice(&h(parts))
}

/// `k = H(PAD(N) | PAD(g))`.
fn k() -> Mod {
    modn(&hash_u512(&[&pad(&N), &pad(&g().retrieve())]).resize())
}

/// `u = H(PAD(A) | PAD(B))`.
fn u(a: &U3072, b: &U3072) -> U512 {
    hash_u512(&[&pad(a), &pad(b)])
}

/// `x = H(s | H(I ":" p))`.
fn x(salt: &[u8], password: &str) -> U512 {
    let inner = h(&[USERNAME.as_bytes(), b":", password.as_bytes()]);
    hash_u512(&[salt, &inner])
}

/// The client's proof.
fn m1(salt: &[u8], a: &U3072, b: &U3072, key: &[u8; 64]) -> [u8; 64] {
    let hn = h(&[&short(&N)]);
    let hg = h(&[&short(&g().retrieve())]);
    let mut xor = [0u8; 64];
    for (o, (n, g)) in xor.iter_mut().zip(hn.iter().zip(&hg)) {
        *o = n ^ g;
    }
    let hi = h(&[USERNAME.as_bytes()]);
    h(&[&xor, &hi, salt, &short(a), &short(b), key])
}

/// The server's proof.
fn hamk(a: &U3072, m1: &[u8], key: &[u8; 64]) -> [u8; 64] {
    h(&[&short(a), m1, key])
}

fn random<const N: usize>() -> [u8; N] {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes).expect("the system RNG works");
    bytes
}

fn eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The accessory's side: one run, from M1's salt and `B` to M3's proof.
pub struct Server {
    salt: [u8; 16],
    v: Mod,
    b: U256,
    big_b: U3072,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("srp::Server").finish_non_exhaustive()
    }
}

impl Server {
    pub fn new(password: &str) -> Self {
        Self::with(password, random(), random())
    }

    /// With a fixed salt and private key, for test vectors.
    fn with(password: &str, salt: [u8; 16], b: [u8; 32]) -> Self {
        let v = g().pow(&x(&salt, password));
        let b = U256::from_be_slice(&b);
        // B = k v + g^b
        let big_b = (k() * v + g().pow(&b)).retrieve();
        Self { salt, v, b, big_b }
    }

    pub fn salt(&self) -> &[u8; 16] {
        &self.salt
    }

    /// `B`, as the sender reads it.
    pub fn public_key(&self) -> Vec<u8> {
        short(&self.big_b)
    }

    /// Checks the client's `A` and proof; on a match, the session key and
    /// our proof.
    pub fn verify(&self, a: &[u8], proof: &[u8]) -> Option<([u8; 64], [u8; 64])> {
        let a = read(a)?;
        // A mod N = 0 would make S known to anyone.
        if a == U3072::ZERO {
            return None;
        }
        // S = (A v^u)^b
        let s = (modn(&a) * self.v.pow(&u(&a, &self.big_b))).pow(&self.b);
        let key = h(&[&short(&s.retrieve())]);
        let expected = m1(&self.salt, &a, &self.big_b, &key);
        eq(&expected, proof).then(|| (key, hamk(&a, proof, &key)))
    }
}

/// The sender's side, for the test sender.
#[cfg(any(test, feature = "testing"))]
pub struct Client {
    a: U256,
    big_a: U3072,
    password: String,
}

#[cfg(any(test, feature = "testing"))]
impl Client {
    pub fn new(password: &str) -> Self {
        Self::with(password, random())
    }

    fn with(password: &str, a: [u8; 32]) -> Self {
        let a = U256::from_be_slice(&a);
        Self {
            a,
            big_a: g().pow(&a).retrieve(),
            password: password.to_owned(),
        }
    }

    /// `A`.
    pub fn public_key(&self) -> Vec<u8> {
        short(&self.big_a)
    }

    /// From the server's salt and `B`: our proof and the session key.
    pub fn prove(&self, salt: &[u8], b: &[u8]) -> Option<([u8; 64], [u8; 64])> {
        let big_b = read(b)?;
        let x = x(salt, &self.password);
        let u = u(&self.big_a, &big_b);
        // S = (B - k g^x)^(a + u x)
        let base = modn(&big_b) - k() * g().pow(&x);
        let exp = self.a.resize::<{ U3072::LIMBS }>().wrapping_add(
            &u.resize::<{ U3072::LIMBS }>()
                .wrapping_mul(&x.resize::<{ U3072::LIMBS }>()),
        );
        let s = base.pow_bounded_exp(&exp, 1040);
        let key = h(&[&short(&s.retrieve())]);
        Some((m1(salt, &self.big_a, &big_b, &key), key))
    }

    /// Whether the server's proof matches ours.
    pub fn check(&self, proof: &[u8; 64], key: &[u8; 64], server_proof: &[u8]) -> bool {
        eq(&hamk(&self.big_a, proof, key), server_proof)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(b: &[u8]) -> String {
        b.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Worked out by hand in Python with plain integers, from the formulas
    /// in this module's header: salt 01..10, b 20..3f, a 40..5f.
    #[test]
    fn matches_an_independent_vector() {
        let salt: [u8; 16] = std::array::from_fn(|i| i as u8 + 1);
        let server = Server::with(TRANSIENT_PIN, salt, std::array::from_fn(|i| i as u8 + 0x20));
        let client = Client::with(TRANSIENT_PIN, std::array::from_fn(|i| i as u8 + 0x40));
        assert_eq!(
            hex(&h(&[&server.public_key()])),
            "4372bf6962dc514a179936ed6d080fd189dcef04add9273b669b448d798e1fbd\
             afc5fc74d64a29e2a5d7609a5a9d9b7a10a2f3591bce21b3af5bba43dc75bbf0"
        );
        assert_eq!(
            hex(&h(&[&client.public_key()])),
            "3f1a22dba580df1fa7e822a71b29040d53c475940e1d938f744968d8a2271d07\
             42d810ac851035fd3428dd926c667180928cf59d0a0d65c913031c8f600b35b0"
        );
        let (proof, key) = client.prove(&salt, &server.public_key()).unwrap();
        assert_eq!(
            hex(&key),
            "17eb7fe5469c3944c297261de16e94bea662623dd34737add90fe5c7450ad0e4\
             e1c5e06e990cfe77fadcd98fb22ec5d04bd36536215f206c99f4fc3a4b5cd541"
        );
        assert_eq!(
            hex(&proof),
            "1eac5025374629ba12cf539eefbaa5a6cf2ae39ec6f57fd08e5de72e02c96031\
             8f866a64be1eb299ffa3a1e46fb396ee26b6ef84b611396192dabf96c7d59fd0"
        );
        let (server_key, server_proof) = server.verify(&client.public_key(), &proof).unwrap();
        assert_eq!(server_key, key);
        assert_eq!(
            hex(&server_proof),
            "a64010a826c0fb7337944e09553919633f48c4c945bc0bab56cca8e87e462a4d\
             e4dce42abac802d9921e966eb9392ed2bf94fdf26a82e8b5fd9aef455625ea09"
        );
        assert!(client.check(&proof, &key, &server_proof));
    }

    #[test]
    fn a_wrong_password_is_refused() {
        let server = Server::new(TRANSIENT_PIN);
        let client = Client::new("0000");
        let (proof, _) = client.prove(server.salt(), &server.public_key()).unwrap();
        assert!(server.verify(&client.public_key(), &proof).is_none());
    }

    #[test]
    fn a_zero_or_oversized_key_is_refused() {
        let server = Server::new(TRANSIENT_PIN);
        assert!(server.verify(&[0], &[0; 64]).is_none());
        assert!(server.verify(&short(&N), &[0; 64]).is_none());
        assert!(server.verify(&[1; N_LEN + 1], &[0; 64]).is_none());
    }
}
