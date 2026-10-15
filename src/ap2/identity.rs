//! Who each AirPlay 2 receiver is: an Ed25519 key (`pk`) and a `pi` UUID
//! per target, which a phone remembers it by.
//!
//! Both are derived from one 32-byte secret per server, kept in the state
//! file's `airplay.secret`, and the target's id. So there is one thing to
//! keep, a target added later gets its identity without a save, and the
//! same target on the same server is the same device after a restart, as
//! its [`device_id`](crate::raop::device_id) already is.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::SigningKey;
use sha2::{Digest, Sha512};
use tas_core::state::StateStore;

/// The server's AirPlay 2 secret.
#[derive(Clone)]
pub struct Identity {
    secret: [u8; 32],
}

impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Identity").finish_non_exhaustive()
    }
}

impl Identity {
    pub fn generate() -> Self {
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret).expect("the system RNG works");
        Self { secret }
    }

    /// The base64url secret, as it is persisted.
    pub fn from_b64url(text: &str) -> Option<Self> {
        let secret = URL_SAFE_NO_PAD.decode(text).ok()?.try_into().ok()?;
        Some(Self { secret })
    }

    pub fn to_b64url(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.secret)
    }

    /// The one in `store`, or a new one saved there.
    pub fn load(store: &StateStore) -> Self {
        store.refresh();
        let saved = store.read(|s| s.airplay.secret.clone());
        if let Some(identity) = saved.as_deref().and_then(Self::from_b64url) {
            return identity;
        }
        if saved.is_some() {
            tracing::warn!("the AirPlay identity in the state file is unusable; making a new one");
        }
        let identity = Self::generate();
        let text = identity.to_b64url();
        match store.update(|s| s.airplay.secret = Some(text)) {
            Ok(()) => tracing::info!("made an AirPlay identity"),
            Err(e) => tracing::error!(
                error = %e,
                "can't save the AirPlay identity; phones will see new devices after a restart"
            ),
        }
        identity
    }

    /// The device `target_id` is, advertised as `name` with `device_id`.
    pub fn device(&self, device_id: [u8; 6], target_id: &str, name: &str) -> Device {
        let seed: [u8; 32] = self.derive(b"tas-airplay2-pk", target_id)[..32]
            .try_into()
            .expect("32 bytes");
        let mut uuid: [u8; 16] = self.derive(b"tas-airplay2-pi", target_id)[..16]
            .try_into()
            .expect("16 bytes");
        // Shaped like the random (v4) UUID other receivers use.
        uuid[6] = (uuid[6] & 0x0f) | 0x40;
        uuid[8] = (uuid[8] & 0x3f) | 0x80;
        Device {
            device_id,
            name: name.to_owned(),
            key: SigningKey::from_bytes(&seed),
            pi: format_uuid(&uuid),
        }
    }

    fn derive(&self, label: &[u8], target_id: &str) -> [u8; 64] {
        let mut hash = Sha512::new();
        hash.update(label);
        hash.update(self.secret);
        hash.update(target_id.as_bytes());
        hash.finalize().into()
    }
}

/// One target as an AirPlay 2 device.
#[derive(Clone)]
pub struct Device {
    /// The same six bytes as its AirPlay 1 advert.
    pub device_id: [u8; 6],
    /// The name the phone shows.
    pub name: String,
    /// Signs pair-verify; its public half is `pk`.
    pub key: SigningKey,
    /// `pi`, and `gid` while it is in no group.
    pub pi: String,
}

impl std::fmt::Debug for Device {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Device")
            .field("device_id", &self.device_id_text())
            .field("name", &self.name)
            .field("pk", &self.public_key_hex())
            .field("pi", &self.pi)
            .finish()
    }
}

impl Device {
    /// `deviceid`: `AA:BB:CC:DD:EE:FF`.
    pub fn device_id_text(&self) -> String {
        let hex: Vec<String> = self.device_id.iter().map(|b| format!("{b:02X}")).collect();
        hex.join(":")
    }

    pub fn public_key(&self) -> [u8; 32] {
        self.key.verifying_key().to_bytes()
    }

    /// `pk` in the TXT record: lowercase hex.
    pub fn public_key_hex(&self) -> String {
        self.public_key()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
}

fn format_uuid(b: &[u8; 16]) -> String {
    let hex: String = b.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_device_is_stable_and_per_target() {
        let id = Identity::generate();
        let a = id.device([2; 6], "kitchen", "Kitchen");
        let again = id.device([2; 6], "kitchen", "Kitchen");
        assert_eq!(a.public_key(), again.public_key());
        assert_eq!(a.pi, again.pi);
        let den = id.device([2; 6], "den", "Den");
        assert_ne!(a.public_key(), den.public_key());
        assert_ne!(a.pi, den.pi);
        let other = Identity::generate().device([2; 6], "kitchen", "Kitchen");
        assert_ne!(a.public_key(), other.public_key());
        assert_ne!(a.pi, other.pi);
    }

    #[test]
    fn a_device_reads_as_its_txt_fields() {
        let d = Identity::generate().device([0x02, 0xAB, 0, 1, 2, 0xFF], "k", "K");
        assert_eq!(d.device_id_text(), "02:AB:00:01:02:FF");
        assert_eq!(d.public_key_hex().len(), 64);
        let groups: Vec<usize> = d.pi.split('-').map(str::len).collect();
        assert_eq!(groups, [8, 4, 4, 4, 12]);
        assert_eq!(&d.pi[14..15], "4", "a v4 UUID: {}", d.pi);
    }

    #[test]
    fn the_identity_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let first = {
            let store = StateStore::open(dir.path()).unwrap();
            let id = Identity::load(&store);
            assert!(store.read(|s| s.airplay.secret.is_some()));
            id.device([2; 6], "kitchen", "Kitchen")
        };
        let store = StateStore::open(dir.path()).unwrap();
        let again = Identity::load(&store).device([2; 6], "kitchen", "Kitchen");
        assert_eq!(first.public_key(), again.public_key());
        assert_eq!(first.pi, again.pi);
    }

    #[test]
    fn an_unusable_secret_is_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let store = StateStore::open(dir.path()).unwrap();
        store
            .update(|s| s.airplay.secret = Some("not base64url!".into()))
            .unwrap();
        let id = Identity::load(&store);
        assert_eq!(
            store.read(|s| s.airplay.secret.clone()),
            Some(id.to_b64url())
        );
    }
}
