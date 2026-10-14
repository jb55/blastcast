//! CPACE-X25519-SHA512 (draft-irtf-cfrg-cpace-21) in initiator-responder
//! mode with explicit mutual confirmation, the PAKE behind Sendspin's
//! code-based pairing (`pairing.md`, PAKE). The server is `A`, the client
//! `B`.
//!
//! Ported from the Python `cpace` package that aiosendspin uses, and
//! checked against it with fixed scalars.

use crypto_bigint::modular::constant_mod::{Residue, ResidueParams};
use crypto_bigint::{Encoding, U256, impl_modulus};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha512};

const DSI: &[u8] = b"CPace255";
const DSI_ISK: &[u8] = b"CPace255_ISK";
const MAC_LABEL: &[u8] = b"CPaceMac";
const SHA512_BLOCK: usize = 128;

impl_modulus!(
    P25519,
    U256,
    "7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffed"
);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CpaceError {
    #[error("a CPace share is not 32 bytes")]
    Length,
    #[error("a CPace share encodes a low-order point")]
    LowOrder,
}

/// Which side of the run this is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// `A`, the server.
    Initiator,
    /// `B`, the client.
    Responder,
}

/// One side of a CPace run, from its share to the session key.
pub struct Cpace {
    role: Role,
    sid: Vec<u8>,
    ad: Vec<u8>,
    scalar: [u8; 32],
    share: [u8; 32],
}

impl std::fmt::Debug for Cpace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cpace")
            .field("role", &self.role)
            .finish_non_exhaustive()
    }
}

/// What both sides hold after [`Cpace::derive`]: the intermediate session
/// key and the two confirmation tags.
pub struct Derived {
    pub isk: [u8; 64],
    /// This side's tag, to send.
    pub own_tag: [u8; 64],
    /// The tag the peer must send.
    peer_tag: [u8; 64],
}

impl Derived {
    /// Whether `tag` is the peer's confirmation tag, in constant time.
    pub fn verify(&self, tag: &[u8]) -> bool {
        super::ct_eq(&self.peer_tag, tag)
    }
}

impl Cpace {
    /// Starts a run with `scalar` drawn from a CSPRNG. `prs` is the pairing
    /// code, `ad` this side's associated data. The channel identifier is
    /// empty, as Sendspin sets it.
    pub fn start(
        role: Role,
        prs: &[u8],
        sid: &[u8],
        ad: &[u8],
        scalar: [u8; 32],
    ) -> Result<Self, CpaceError> {
        let generator = generator(prs, b"", sid);
        let share = scalar_mult(&scalar, &generator).ok_or(CpaceError::LowOrder)?;
        Ok(Self {
            role,
            sid: sid.to_vec(),
            ad: ad.to_vec(),
            scalar,
            share,
        })
    }

    /// This side's public share, `Ya` or `Yb`.
    pub fn share(&self) -> [u8; 32] {
        self.share
    }

    /// Takes the peer's share and associated data.
    pub fn derive(self, peer_share: &[u8], peer_ad: &[u8]) -> Result<Derived, CpaceError> {
        let peer: [u8; 32] = peer_share.try_into().map_err(|_| CpaceError::Length)?;
        let k = scalar_mult(&self.scalar, &peer).ok_or(CpaceError::LowOrder)?;
        let mine = (self.share.as_slice(), self.ad.as_slice());
        let theirs = (peer.as_slice(), peer_ad);
        let (a, b) = match self.role {
            Role::Initiator => (mine, theirs),
            Role::Responder => (theirs, mine),
        };
        let mut transcript = lv_cat(&[a.0, a.1]);
        transcript.extend_from_slice(&lv_cat(&[b.0, b.1]));

        let mut hasher = Sha512::new();
        hasher.update(lv_cat(&[DSI_ISK, &self.sid, &k]));
        hasher.update(&transcript);
        let isk: [u8; 64] = hasher.finalize().into();

        let mut hasher = Sha512::new();
        hasher.update(MAC_LABEL);
        hasher.update(&self.sid);
        hasher.update(isk);
        let mac_key: [u8; 64] = hasher.finalize().into();

        let tag = |(share, ad): (&[u8], &[u8])| -> [u8; 64] {
            let mut mac = <Hmac<Sha512> as Mac>::new_from_slice(&mac_key).expect("any key");
            mac.update(&lv_cat(&[share, ad]));
            mac.finalize().into_bytes().into()
        };
        let (ta, tb) = (tag(a), tag(b));
        // A reflected share would make the peer's tag our own.
        let reflected = a == b;
        let (own_tag, mut peer_tag) = match self.role {
            Role::Initiator => (ta, tb),
            Role::Responder => (tb, ta),
        };
        if reflected {
            // Nothing the peer sends can match.
            peer_tag = [0; 64];
            peer_tag[0] = !own_tag[0];
        }
        Ok(Derived {
            isk,
            own_tag,
            peer_tag,
        })
    }
}

/// `prepend_len`: a LEB128 length, then the bytes.
fn prepend_len(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 2);
    let mut len = data.len();
    loop {
        let byte = (len & 0x7f) as u8;
        len >>= 7;
        if len == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
    out.extend_from_slice(data);
    out
}

fn lv_cat(parts: &[&[u8]]) -> Vec<u8> {
    parts.iter().flat_map(|p| prepend_len(p)).collect()
}

fn generator(prs: &[u8], ci: &[u8], sid: &[u8]) -> [u8; 32] {
    let zpad = SHA512_BLOCK.saturating_sub(1 + prepend_len(prs).len() + prepend_len(DSI).len());
    let string = lv_cat(&[DSI, prs, &vec![0; zpad], ci, sid]);
    let hash = Sha512::digest(&string);
    let mut u: [u8; 32] = hash[..32].try_into().expect("32 bytes");
    u[31] &= 0x7f;
    elligator2(U256::from_le_bytes(u))
}

/// Elligator 2 onto Curve25519's Montgomery u-coordinate, with `Z = 2`,
/// as the draft (and RFC 9380) define it.
fn elligator2(r: U256) -> [u8; 32] {
    // r < 2^255, but may still be >= p; reduce it.
    let r = fe(&r.wrapping_rem(&P25519::MODULUS));
    let one = fe(&U256::ONE);
    let a = fe(&U256::from_u32(486_662));
    let two = fe(&U256::from_u32(2));
    let p_minus_2 = P25519::MODULUS.wrapping_sub(&U256::from_u32(2));
    let half_p = P25519::MODULUS.wrapping_sub(&U256::ONE).shr_vartime(1);

    // inv0 by Fermat: maps 0 to 0.
    let denom = (one + two * r * r).pow(&p_minus_2);
    let v = -(a * denom);
    let eps = (v * v * v + a * v * v + v).pow(&half_p);
    let inv2 = two.pow(&p_minus_2);
    let x = eps * v - (one - eps) * a * inv2;
    x.retrieve().to_le_bytes()
}

/// A field element mod 2^255 - 19.
type Fe = Residue<P25519, { U256::LIMBS }>;

fn fe(x: &U256) -> Fe {
    Fe::new(x)
}

/// X25519, or `None` when the result is the identity.
fn scalar_mult(scalar: &[u8; 32], point: &[u8; 32]) -> Option<[u8; 32]> {
    let out = x25519_dalek::x25519(*scalar, *point);
    (out != [0; 32]).then_some(out)
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

    /// One run through the Python `cpace` package, scalars fixed: `A`'s
    /// 10..=41, `B`'s 50..=81, PRS `12345678`, a Sendspin sid with `h`
    /// 0..=31 and counter 1.
    #[test]
    fn matches_the_python_cpace_package() {
        let sid = hex(
            "73656e647370696e2d706169722d70616b652d7631000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f00000001",
        );
        assert_eq!(
            generator(b"12345678", b"", &sid).to_vec(),
            hex("79c9392c93ec0f7ae71b5b9c82a7d987a6c77b7769e77f4563bcbb069e31d64f")
        );
        let a_scalar: [u8; 32] = std::array::from_fn(|i| 10 + i as u8);
        let b_scalar: [u8; 32] = std::array::from_fn(|i| 50 + i as u8);
        let a = Cpace::start(Role::Initiator, b"12345678", &sid, b"server", a_scalar).unwrap();
        let b = Cpace::start(Role::Responder, b"12345678", &sid, b"client", b_scalar).unwrap();
        let (ya, yb) = (a.share(), b.share());
        assert_eq!(
            ya.to_vec(),
            hex("1b76e9b7324352678230ab966d081d751c7d1eba2d9d49960188c24d7fc9432c")
        );
        assert_eq!(
            yb.to_vec(),
            hex("b0f2d0060f3479a82829b1a670d2138866466fda199f69492a25fbb0f23de865")
        );
        let a = a.derive(&yb, b"client").unwrap();
        let b = b.derive(&ya, b"server").unwrap();
        assert_eq!(
            a.isk.to_vec(),
            hex(
                "6c68631f854b1dd28bd5543b258bcf59fbb28f1b7d518a126c92c22b7876a261bc689430d80b6f05d5db72e11d0c0058aeed61d2a96ab969005292cbf3c77011"
            )
        );
        assert_eq!(a.isk, b.isk);
        assert_eq!(
            a.own_tag.to_vec(),
            hex(
                "e0e51baec8461ecae23d8ade4d55ac68e2e4e237cebd8e1c4153d34b46964c27788871e6c96944b12f56a567742380d1b6cc969d5314e6b8c01c11639db32ff0"
            )
        );
        assert_eq!(
            b.own_tag.to_vec(),
            hex(
                "1d62770e300e0eba1c579a2f053b51bfe1cc7312596f6e1ab7ead7324a0ff374fd280743e8692b1f3b87ec96810c4a339e9265f77f914b90094d0beffe086569"
            )
        );
        assert!(a.verify(&b.own_tag));
        assert!(b.verify(&a.own_tag));
        assert!(!a.verify(&a.own_tag));
    }

    #[test]
    fn a_wrong_code_fails_confirmation() {
        let sid = b"sid";
        let a = Cpace::start(Role::Initiator, b"11111111", sid, b"server", [3; 32]).unwrap();
        let b = Cpace::start(Role::Responder, b"22222222", sid, b"client", [4; 32]).unwrap();
        let (ya, yb) = (a.share(), b.share());
        let a = a.derive(&yb, b"client").unwrap();
        let b = b.derive(&ya, b"server").unwrap();
        assert!(!b.verify(&a.own_tag));
        assert!(!a.verify(&b.own_tag));
    }

    #[test]
    fn low_order_and_short_shares_are_refused() {
        let a = Cpace::start(Role::Initiator, b"1", b"sid", b"server", [3; 32]).unwrap();
        assert_eq!(
            a.derive(&[0; 32], b"client").err(),
            Some(CpaceError::LowOrder)
        );
        let a = Cpace::start(Role::Initiator, b"1", b"sid", b"server", [3; 32]).unwrap();
        assert_eq!(
            a.derive(&[9; 31], b"client").err(),
            Some(CpaceError::Length)
        );
    }

    #[test]
    fn prepend_len_is_leb128() {
        assert_eq!(prepend_len(b"ab"), [2, b'a', b'b']);
        let long = vec![0u8; 200];
        assert_eq!(&prepend_len(&long)[..2], &[0xc8, 0x01]);
    }
}
