//! The current spec's secure connection: the cleartext `client/init` and
//! `server/init`, the Noise `KKpsk2` handshake, the encrypted channel, and
//! pairing (`connection.md`, Encryption; `pairing.md`).

pub mod channel;
pub mod client;
pub mod cpace;
pub mod keys;
pub mod link;
pub mod noise;
pub mod session;
pub mod trust;

/// Whether two byte strings are equal, taking the same time wherever they
/// differ.
pub(crate) fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
