//! What the relay keeps between runs, in one JSON file in its directory
//! (`relay.json`): its server id, its AirPlay 2 secret, and its Sendspin
//! identity, pairings and approvals. A phone and a paired speaker know the
//! relay again after a restart.
//!
//! Pairing requests are the one thing not kept: nothing outside the
//! process makes them, so they live as long as it does.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use blastcast_airplay::ap2::IdentityStore;
use blastcast_sendspin::secure::keys::{Identity, b64url, b64url_array};
use blastcast_sendspin::secure::trust::{MemoryTrust, PairingRequest, Trust, TrustError};
use serde::{Deserialize, Serialize};

/// The file in the relay's directory.
pub const FILE: &str = "relay.json";

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct Saved {
    server_id: Option<String>,
    /// The AirPlay 2 secret, as [`blastcast_airplay::ap2::Identity`] writes it.
    airplay_secret: Option<String>,
    /// The Sendspin static key, base64url.
    sendspin_identity: Option<String>,
    /// Each paired client's long-term PSK, base64url.
    #[serde(default)]
    pairings: BTreeMap<String, String>,
    #[serde(default)]
    approved: BTreeSet<String>,
}

/// Share it as `Arc<Store>`: it is the AirPlay identity's store and the
/// Sendspin server's [`Trust`].
pub struct Store {
    path: PathBuf,
    saved: Mutex<Saved>,
    /// Only the pairing requests are kept here.
    requests: MemoryTrust,
}

impl fmt::Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Store")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl Store {
    /// The store in `dir`, made if it isn't there yet.
    pub fn open(dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let path = dir.join(FILE);
        let saved = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{}: {e}", path.display()),
                )
            })?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Saved::default(),
            Err(e) => return Err(e),
        };
        Ok(Self {
            path,
            saved: Mutex::new(saved),
            requests: MemoryTrust::new(),
        })
    }

    /// The relay's server id, made and kept the first time it is asked
    /// for. Its AirPlay device id and mDNS host name come from it.
    pub fn server_id(&self) -> io::Result<String> {
        if let Some(id) = self.lock().server_id.clone() {
            return Ok(id);
        }
        let mut bytes = [0u8; 8];
        getrandom::fill(&mut bytes).map_err(io::Error::other)?;
        let id: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        self.update(|s| s.server_id.get_or_insert(id).clone())
    }

    fn lock(&self) -> MutexGuard<'_, Saved> {
        // Every critical section leaves the value consistent.
        self.saved.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Changes what is saved and writes it out, or leaves it as it was if
    /// the write fails.
    fn update<R>(&self, f: impl FnOnce(&mut Saved) -> R) -> io::Result<R> {
        let mut saved = self.lock();
        let mut next = saved.clone();
        let out = f(&mut next);
        self.write(&next)?;
        *saved = next;
        Ok(out)
    }

    /// Writes beside the file and renames over it, so a crash leaves the
    /// old one or the new one. Only the user may read it: it holds keys.
    fn write(&self, saved: &Saved) -> io::Result<()> {
        let tmp = self.path.with_extension("json.tmp");
        let mut options = fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let mut file = options.open(&tmp)?;
        file.write_all(&serde_json::to_vec_pretty(saved).map_err(io::Error::other)?)?;
        file.sync_all()?;
        fs::rename(&tmp, &self.path)
    }

    fn save(&self, f: impl FnOnce(&mut Saved)) -> Result<(), TrustError> {
        self.update(f).map_err(|e| TrustError::Save(e.to_string()))
    }
}

impl IdentityStore for Store {
    fn load(&self) -> Option<String> {
        self.lock().airplay_secret.clone()
    }

    fn save(&self, secret: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let secret = secret.to_owned();
        Ok(self.update(|s| s.airplay_secret = Some(secret))?)
    }
}

impl Trust for Store {
    fn identity(&self) -> Identity {
        let saved = self.lock().sendspin_identity.clone();
        if let Some(identity) = saved.as_deref().and_then(Identity::from_b64url) {
            return identity;
        }
        if saved.is_some() {
            tracing::warn!("the saved Sendspin identity is unusable; making a new one");
        }
        let identity = Identity::generate();
        let text = identity.to_b64url();
        if let Err(e) = self.update(|s| s.sendspin_identity = Some(text)) {
            tracing::error!(error = %e, "can't save the Sendspin identity");
        }
        identity
    }

    fn long_term_psk(&self, client_id: &str) -> Option<[u8; 32]> {
        b64url_array(self.lock().pairings.get(client_id)?)
    }

    fn pair(&self, client_id: &str, psk: [u8; 32]) -> Result<(), TrustError> {
        self.save(|s| {
            s.pairings.insert(client_id.to_owned(), b64url(&psk));
            s.approved.remove(client_id);
        })?;
        self.requests.end_pairing_request(client_id)
    }

    fn unpair(&self, client_id: &str) -> Result<bool, TrustError> {
        let mut had = false;
        self.save(|s| had = s.pairings.remove(client_id).is_some())?;
        Ok(had)
    }

    fn is_approved(&self, client_id: &str) -> bool {
        self.lock().approved.contains(client_id)
    }

    fn approve(&self, client_id: &str) -> Result<(), TrustError> {
        self.save(|s| {
            s.approved.insert(client_id.to_owned());
        })
    }

    fn revoke(&self, client_id: &str) -> Result<bool, TrustError> {
        let mut had = false;
        self.save(|s| had = s.approved.remove(client_id))?;
        Ok(had)
    }

    fn pairing_request(&self, client_id: &str) -> Option<PairingRequest> {
        self.requests.pairing_request(client_id)
    }

    fn request_pairing(&self, client_id: &str, request: PairingRequest) -> Result<(), TrustError> {
        self.requests.request_pairing(client_id, request)
    }

    fn end_pairing_request(&self, client_id: &str) -> Result<(), TrustError> {
        self.requests.end_pairing_request(client_id)
    }
}

#[cfg(test)]
mod tests {
    use blastcast_sendspin::testing::check_trust;

    use super::*;

    #[test]
    fn what_it_keeps_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        check_trust(&store);
        let server_id = store.server_id().unwrap();
        let sendspin = store.identity().id();
        let airplay = blastcast_airplay::ap2::Identity::load(&store).to_b64url();
        let client = Identity::generate().id();
        store.pair(&client, [3; 32]).unwrap();

        let again = Store::open(dir.path()).unwrap();
        assert_eq!(again.server_id().unwrap(), server_id);
        assert_eq!(again.identity().id(), sendspin);
        assert_eq!(
            blastcast_airplay::ap2::Identity::load(&again).to_b64url(),
            airplay
        );
        assert_eq!(again.long_term_psk(&client), Some([3; 32]));
    }

    #[test]
    fn a_file_it_cant_read_is_an_error_not_a_new_identity() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(FILE), "not json").unwrap();
        assert_eq!(
            Store::open(dir.path()).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
