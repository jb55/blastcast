//! The accessory's side of `/pair-setup` and `/pair-verify`, which end in
//! the [`Keys`] the connection is encrypted with.
//!
//! - **Transient pair-setup** is SRP with the fixed PIN 3939: M1 asks with
//!   the transient flag, M2 is our salt and `B`, M3 the sender's `A` and
//!   proof, M4 our proof. SRP's `K` is the shared secret. What a phone
//!   uses with a receiver that advertises transient pairing (bit 47) and
//!   not HomeKit pairing (46). Persistent pairing (no transient flag, then
//!   M5 and M6) is refused.
//! - **Pair-verify** is X25519 between two fresh keys. M2 carries ours and,
//!   sealed under the agreed key, the device id signed with the target's
//!   Ed25519 key ([`Device::key`]), so the sender can check who we are
//!   against `pk`. M3 carries the sender's signature, which we can't check:
//!   we keep no paired controllers, since persistent pairing is out of the
//!   epic. So it proves only that the sender ran the exchange, which is
//!   all transient pairing proves too, with its published PIN. X25519's
//!   shared secret is the channel's.
//!
//! In both, the reply that finishes is sent in the clear and everything
//! after it encrypted. A failure is a TLV `ERROR` in a 200, as HomeKit
//! answers.
//!
//! Pair-setup is ported from `openairplay2` 0.5's pairing.rs (MIT,
//! Copyright (c) 2026 Stefan Arentz); pair-verify from shairplay 0.10's
//! crypto/pairing_homekit.rs `PairVerifyServer`, read for the message
//! layout only (LGPL-3.0, so nothing copied).

use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Nonce};
use ed25519_dalek::Signer;
use x25519_dalek::{PublicKey, StaticSecret};

use super::Device;
use super::channel::{Keys, hkdf_sha512};
use super::srp;
use super::tlv::{Tlv, error, ty};

/// M1's flag for transient pairing.
pub const FLAG_TRANSIENT: u8 = 0x10;
pub const VERIFY_SALT: &[u8] = b"Pair-Verify-Encrypt-Salt";
pub const VERIFY_INFO: &[u8] = b"Pair-Verify-Encrypt-Info";

/// What one pairing request comes to.
#[derive(Debug)]
pub enum Step {
    /// Reply, and stay in the clear.
    Reply(Vec<u8>),
    /// Reply in the clear, then encrypt both ways with these.
    Paired(Vec<u8>, Keys),
}

/// One connection's pairing so far.
#[derive(Debug, Default)]
pub struct Pairing {
    setup: Option<srp::Server>,
    verify: Option<Verify>,
}

/// Between M2 and M3: X25519's shared secret.
struct Verify {
    shared: [u8; 32],
}

impl std::fmt::Debug for Verify {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Verify").finish_non_exhaustive()
    }
}

fn refuse(state: u8, code: u8, why: &str) -> Step {
    tracing::warn!(state, why, "refusing AirPlay 2 pairing");
    Step::Reply(
        Tlv::new()
            .put_u8(ty::STATE, state)
            .put_u8(ty::ERROR, code)
            .encode(),
    )
}

/// `label` as a pairing nonce: four zero bytes, then the eight-byte label.
pub fn nonce(label: &[u8; 8]) -> Nonce {
    let mut nonce = [0u8; 12];
    nonce[4..].copy_from_slice(label);
    nonce.into()
}

impl Pairing {
    /// One `/pair-setup` body.
    pub fn setup(&mut self, body: &[u8]) -> Step {
        let Some(tlv) = Tlv::decode(body) else {
            return refuse(2, error::AUTHENTICATION, "pair-setup isn't TLV8");
        };
        match tlv.get_u8(ty::STATE) {
            Some(1) => {
                if tlv.get_u8(ty::METHOD).unwrap_or(0) != 0 {
                    return refuse(2, error::UNAVAILABLE, "pair-setup with MFi");
                }
                let flags = tlv.get(ty::FLAGS).and_then(|f| f.last().copied());
                if flags.is_none_or(|f| f & FLAG_TRANSIENT == 0) {
                    return refuse(2, error::UNAVAILABLE, "persistent pairing");
                }
                let srp = srp::Server::new(srp::TRANSIENT_PIN);
                let reply = Tlv::new()
                    .put_u8(ty::STATE, 2)
                    .put(ty::SALT, srp.salt().to_vec())
                    .put(ty::PUBLIC_KEY, srp.public_key());
                self.setup = Some(srp);
                Step::Reply(reply.encode())
            }
            Some(3) => {
                let Some(srp) = self.setup.take() else {
                    return refuse(4, error::AUTHENTICATION, "pair-setup M3 before M1");
                };
                let (Some(a), Some(proof)) = (tlv.get(ty::PUBLIC_KEY), tlv.get(ty::PROOF)) else {
                    return refuse(4, error::AUTHENTICATION, "pair-setup M3 is missing a field");
                };
                let Some((key, our_proof)) = srp.verify(a, proof) else {
                    return refuse(4, error::AUTHENTICATION, "a wrong pair-setup proof");
                };
                tracing::debug!("AirPlay 2 transient pairing done");
                let reply = Tlv::new()
                    .put_u8(ty::STATE, 4)
                    .put(ty::PROOF, our_proof.to_vec());
                Step::Paired(reply.encode(), Keys::new(&key))
            }
            state => refuse(
                2,
                error::UNAVAILABLE,
                &format!("pair-setup state {state:?}"),
            ),
        }
    }

    /// One `/pair-verify` body, to `device`.
    pub fn verify(&mut self, device: &Device, body: &[u8]) -> Step {
        let Some(tlv) = Tlv::decode(body) else {
            return refuse(2, error::AUTHENTICATION, "pair-verify isn't TLV8");
        };
        match tlv.get_u8(ty::STATE) {
            Some(1) => {
                let Some(theirs) = tlv
                    .get(ty::PUBLIC_KEY)
                    .and_then(|k| <[u8; 32]>::try_from(k).ok())
                else {
                    return refuse(2, error::AUTHENTICATION, "pair-verify M1 has no key");
                };
                let mut seed = [0u8; 32];
                getrandom::fill(&mut seed).expect("the system RNG works");
                let secret = StaticSecret::from(seed);
                let ours = PublicKey::from(&secret).to_bytes();
                let shared = secret.diffie_hellman(&PublicKey::from(theirs)).to_bytes();
                let id = device.device_id_text();
                let signed = [&ours[..], id.as_bytes(), &theirs].concat();
                let inner = Tlv::new()
                    .put(ty::IDENTIFIER, id.into_bytes())
                    .put(ty::SIGNATURE, device.key.sign(&signed).to_bytes().to_vec());
                let sealed = verify_cipher(&shared)
                    .encrypt(&nonce(b"PV-Msg02"), &inner.encode()[..])
                    .expect("a short message seals");
                self.verify = Some(Verify { shared });
                let reply = Tlv::new()
                    .put_u8(ty::STATE, 2)
                    .put(ty::PUBLIC_KEY, ours.to_vec())
                    .put(ty::ENCRYPTED_DATA, sealed);
                Step::Reply(reply.encode())
            }
            Some(3) => {
                let Some(verify) = self.verify.take() else {
                    return refuse(4, error::AUTHENTICATION, "pair-verify M3 before M1");
                };
                let opened = tlv.get(ty::ENCRYPTED_DATA).and_then(|sealed| {
                    verify_cipher(&verify.shared)
                        .decrypt(&nonce(b"PV-Msg03"), sealed)
                        .ok()
                });
                let Some(inner) = opened.as_deref().and_then(Tlv::decode) else {
                    return refuse(4, error::AUTHENTICATION, "pair-verify M3 doesn't open");
                };
                tracing::debug!(
                    controller = %String::from_utf8_lossy(inner.get(ty::IDENTIFIER).unwrap_or_default()),
                    "AirPlay 2 pair-verify done; the controller's signature isn't checked"
                );
                let reply = Tlv::new().put_u8(ty::STATE, 4);
                Step::Paired(reply.encode(), Keys::new(&verify.shared))
            }
            state => refuse(
                2,
                error::UNAVAILABLE,
                &format!("pair-verify state {state:?}"),
            ),
        }
    }
}

/// The key M2 and M3's inner messages are sealed with.
pub fn verify_cipher(shared: &[u8; 32]) -> ChaCha20Poly1305 {
    ChaCha20Poly1305::new(&hkdf_sha512(shared, VERIFY_SALT, VERIFY_INFO).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ap2::Identity;

    fn reply(step: Step) -> Tlv {
        match step {
            Step::Reply(r) => Tlv::decode(&r).unwrap(),
            Step::Paired(..) => panic!("paired early"),
        }
    }

    fn m1(flags: Option<u8>) -> Vec<u8> {
        let tlv = Tlv::new().put_u8(ty::STATE, 1).put_u8(ty::METHOD, 0);
        match flags {
            Some(f) => tlv.put_u8(ty::FLAGS, f),
            None => tlv,
        }
        .encode()
    }

    /// M1 to M3 as a sender with `pin`.
    fn setup(pairing: &mut Pairing, pin: &str) -> (Step, srp::Client) {
        let m2 = reply(pairing.setup(&m1(Some(FLAG_TRANSIENT))));
        assert_eq!(m2.get_u8(ty::STATE), Some(2));
        let client = srp::Client::new(pin);
        let (proof, _) = client
            .prove(m2.get(ty::SALT).unwrap(), m2.get(ty::PUBLIC_KEY).unwrap())
            .unwrap();
        let m3 = Tlv::new()
            .put_u8(ty::STATE, 3)
            .put(ty::PUBLIC_KEY, client.public_key())
            .put(ty::PROOF, proof.to_vec());
        (pairing.setup(&m3.encode()), client)
    }

    #[test]
    fn transient_pairing_ends_in_the_srp_key() {
        let mut pairing = Pairing::default();
        let (step, _) = setup(&mut pairing, "3939");
        let Step::Paired(m4, _) = step else {
            panic!("not paired")
        };
        let m4 = Tlv::decode(&m4).unwrap();
        assert_eq!(m4.get_u8(ty::STATE), Some(4));
        assert_eq!(m4.get(ty::PROOF).map(<[u8]>::len), Some(64));
    }

    #[test]
    fn a_wrong_proof_is_refused() {
        let mut pairing = Pairing::default();
        let m4 = reply(setup(&mut pairing, "1234").0);
        assert_eq!(m4.get_u8(ty::STATE), Some(4));
        assert_eq!(m4.get_u8(ty::ERROR), Some(error::AUTHENTICATION));
        // And the run is over: M3 again isn't taken.
        let again = Tlv::new().put_u8(ty::STATE, 3).encode();
        assert_eq!(
            reply(pairing.setup(&again)).get_u8(ty::ERROR),
            Some(error::AUTHENTICATION)
        );
    }

    #[test]
    fn persistent_pairing_is_refused() {
        let mut pairing = Pairing::default();
        for body in [m1(None), m1(Some(0)), b"\x06".to_vec()] {
            let m2 = reply(pairing.setup(&body));
            assert!(m2.get_u8(ty::ERROR).is_some(), "{body:?}");
        }
    }

    #[test]
    fn verify_signs_as_the_device_and_opens_m3() {
        use ed25519_dalek::{Signature, Verifier, VerifyingKey};

        let device = Identity::generate().device([2; 6], "kitchen", "Kitchen");
        let mut pairing = Pairing::default();
        let phone = StaticSecret::from([7u8; 32]);
        let phone_pk = PublicKey::from(&phone).to_bytes();
        let m1 = Tlv::new()
            .put_u8(ty::STATE, 1)
            .put(ty::PUBLIC_KEY, phone_pk.to_vec());
        let m2 = reply(pairing.verify(&device, &m1.encode()));
        let ours: [u8; 32] = m2.get(ty::PUBLIC_KEY).unwrap().try_into().unwrap();
        let shared = phone.diffie_hellman(&PublicKey::from(ours)).to_bytes();
        let inner = verify_cipher(&shared)
            .decrypt(&nonce(b"PV-Msg02"), m2.get(ty::ENCRYPTED_DATA).unwrap())
            .unwrap();
        let inner = Tlv::decode(&inner).unwrap();
        let id = inner.get(ty::IDENTIFIER).unwrap();
        assert_eq!(id, device.device_id_text().as_bytes());
        let signature = Signature::from_slice(inner.get(ty::SIGNATURE).unwrap()).unwrap();
        VerifyingKey::from_bytes(&device.public_key())
            .unwrap()
            .verify(&[&ours[..], id, &phone_pk].concat(), &signature)
            .expect("signed with pk");

        // M3 sealed under some other key doesn't open.
        let wrong = Tlv::new().put_u8(ty::STATE, 3).put(
            ty::ENCRYPTED_DATA,
            verify_cipher(&[0; 32])
                .encrypt(&nonce(b"PV-Msg03"), &b"\x01\x01x"[..])
                .unwrap(),
        );
        let refused = reply(pairing.verify(&device, &wrong.encode()));
        assert_eq!(refused.get_u8(ty::ERROR), Some(error::AUTHENTICATION));
    }
}
