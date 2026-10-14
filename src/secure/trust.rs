//! What the server remembers about encrypted clients: its own identity
//! key, the long-term PSK of each paired client, which clients the
//! operator let play unpaired, and the pairings the operator asked for.
//!
//! [`Trust`] is implemented over the state file, so `tas sendspin pair`
//! can ask for a pairing while the server runs, and by [`MemoryTrust`] for
//! tests and servers with nowhere to keep it.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use tas_core::state::{SendspinPairing, SendspinPairingRequest, StateStore};

use super::keys::{Identity, PairingToken, b64url, b64url_array};

/// A pairing method, as `server/activate` names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PairMethod {
    PairingPsk,
    StaticCode,
    DynamicCode,
}

impl PairMethod {
    pub fn name(self) -> &'static str {
        match self {
            PairMethod::PairingPsk => "pairing_psk",
            PairMethod::StaticCode => "static_pairing_code",
            PairMethod::DynamicCode => "dynamic_pairing_code",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        [Self::PairingPsk, Self::StaticCode, Self::DynamicCode]
            .into_iter()
            .find(|m| m.name() == name)
    }
}

/// A pairing the operator asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PairingRequest {
    /// From a version-0 pairing token: the client's pairing PSK.
    PairingPsk { pairing_psk: [u8; 32] },
    /// The client's fixed eight-digit code.
    StaticCode { code: String },
    /// The six digits the client shows once the attempt starts; `None`
    /// until the operator enters them.
    DynamicCode { code: Option<String> },
}

impl PairingRequest {
    pub fn method(&self) -> PairMethod {
        match self {
            Self::PairingPsk { .. } => PairMethod::PairingPsk,
            Self::StaticCode { .. } => PairMethod::StaticCode,
            Self::DynamicCode { .. } => PairMethod::DynamicCode,
        }
    }

    /// A request from a pairing token, for the client the token names.
    pub fn from_token(token: &str) -> Result<(String, Self), TrustError> {
        match PairingToken::decode(token) {
            Some(PairingToken::PairingPsk {
                client_key,
                pairing_psk,
            }) => Ok((b64url(&client_key), Self::PairingPsk { pairing_psk })),
            Some(PairingToken::Code(_)) => Err(TrustError::Invalid(
                "a version 1 token is a dynamic code, entered during an attempt".into(),
            )),
            None => Err(TrustError::Invalid("not a pairing token".into())),
        }
    }

    /// A static code request; separators are dropped, as the spec asks.
    pub fn static_code(code: &str) -> Result<Self, TrustError> {
        let code = digits(code);
        if code.len() != 8 {
            return Err(TrustError::Invalid(
                "a static pairing code is eight digits".into(),
            ));
        }
        Ok(Self::StaticCode { code })
    }
}

/// The digits of operator input, with spaces and hyphens gone.
pub fn digits(input: &str) -> String {
    input
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '-')
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TrustError {
    #[error("{0}")]
    Invalid(String),
    #[error("saving: {0}")]
    Save(String),
}

pub trait Trust: Send + Sync + fmt::Debug {
    /// The server's static key, made and kept the first time it is asked
    /// for. Its public half is the `server_id`.
    fn identity(&self) -> Identity;

    /// The long-term PSK of a paired client.
    fn long_term_psk(&self, client_id: &str) -> Option<[u8; 32]>;

    /// Keeps a new pairing, replacing any old one. Pairing ends unpaired
    /// approval and the request that asked for it.
    fn pair(&self, client_id: &str, psk: [u8; 32]) -> Result<(), TrustError>;

    /// Forgets a pairing; whether there was one.
    fn unpair(&self, client_id: &str) -> Result<bool, TrustError>;

    fn is_approved(&self, client_id: &str) -> bool;

    fn approve(&self, client_id: &str) -> Result<(), TrustError>;

    /// Withdraws approval; whether there was any.
    fn revoke(&self, client_id: &str) -> Result<bool, TrustError>;

    fn pairing_request(&self, client_id: &str) -> Option<PairingRequest>;

    /// Asks for a pairing, replacing any earlier request for the client.
    fn request_pairing(&self, client_id: &str, request: PairingRequest) -> Result<(), TrustError>;

    /// Ends a request, whether or not it paired.
    fn end_pairing_request(&self, client_id: &str) -> Result<(), TrustError>;
}

/// Enters the code a client shows during a dynamic pairing attempt. False
/// when no dynamic pairing was asked for.
pub fn enter_code(trust: &dyn Trust, client_id: &str, code: &str) -> Result<bool, TrustError> {
    match trust.pairing_request(client_id) {
        Some(PairingRequest::DynamicCode { .. }) => {
            let code = digits(code);
            if code.len() != 6 || !code.bytes().all(|b| b.is_ascii_digit()) {
                return Err(TrustError::Invalid(
                    "a dynamic pairing code is six digits".into(),
                ));
            }
            trust.request_pairing(client_id, PairingRequest::DynamicCode { code: Some(code) })?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// Trust that lasts as long as the process.
#[derive(Default)]
pub struct MemoryTrust {
    inner: Mutex<Memory>,
}

#[derive(Default)]
struct Memory {
    identity: Option<Identity>,
    pairings: BTreeMap<String, [u8; 32]>,
    approved: BTreeSet<String>,
    requests: BTreeMap<String, PairingRequest>,
}

impl fmt::Debug for MemoryTrust {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let m = self.lock();
        f.debug_struct("MemoryTrust")
            .field("paired", &m.pairings.keys().collect::<Vec<_>>())
            .field("approved", &m.approved)
            .finish_non_exhaustive()
    }
}

impl MemoryTrust {
    pub fn new() -> Self {
        Self::default()
    }

    /// With a given identity, for tests that need to know the server's.
    pub fn with_identity(identity: Identity) -> Self {
        let trust = Self::default();
        trust.lock().identity = Some(identity);
        trust
    }

    fn lock(&self) -> MutexGuard<'_, Memory> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Trust for MemoryTrust {
    fn identity(&self) -> Identity {
        self.lock()
            .identity
            .get_or_insert_with(Identity::generate)
            .clone()
    }

    fn long_term_psk(&self, client_id: &str) -> Option<[u8; 32]> {
        self.lock().pairings.get(client_id).copied()
    }

    fn pair(&self, client_id: &str, psk: [u8; 32]) -> Result<(), TrustError> {
        let mut m = self.lock();
        m.pairings.insert(client_id.to_owned(), psk);
        m.approved.remove(client_id);
        m.requests.remove(client_id);
        Ok(())
    }

    fn unpair(&self, client_id: &str) -> Result<bool, TrustError> {
        Ok(self.lock().pairings.remove(client_id).is_some())
    }

    fn is_approved(&self, client_id: &str) -> bool {
        self.lock().approved.contains(client_id)
    }

    fn approve(&self, client_id: &str) -> Result<(), TrustError> {
        self.lock().approved.insert(client_id.to_owned());
        Ok(())
    }

    fn revoke(&self, client_id: &str) -> Result<bool, TrustError> {
        Ok(self.lock().approved.remove(client_id))
    }

    fn pairing_request(&self, client_id: &str) -> Option<PairingRequest> {
        self.lock().requests.get(client_id).cloned()
    }

    fn request_pairing(&self, client_id: &str, request: PairingRequest) -> Result<(), TrustError> {
        self.lock().requests.insert(client_id.to_owned(), request);
        Ok(())
    }

    fn end_pairing_request(&self, client_id: &str) -> Result<(), TrustError> {
        self.lock().requests.remove(client_id);
        Ok(())
    }
}

/// Trust kept in the state file. Every read picks up a change another
/// process (`tas sendspin pair`) made to it.
impl Trust for StateStore {
    fn identity(&self) -> Identity {
        self.refresh();
        let saved = self.read(|s| s.sendspin.identity.clone());
        if let Some(identity) = saved.as_deref().and_then(Identity::from_b64url) {
            return identity;
        }
        if saved.is_some() {
            tracing::warn!("the sendspin identity in the state file is unusable; making a new one");
        }
        let identity = Identity::generate();
        let text = identity.to_b64url();
        match self.update(|s| s.sendspin.identity = Some(text)) {
            Ok(()) => tracing::info!(server_id = identity.id(), "made a sendspin identity"),
            Err(e) => tracing::error!(error = %e, "can't save the sendspin identity"),
        }
        identity
    }

    fn long_term_psk(&self, client_id: &str) -> Option<[u8; 32]> {
        self.refresh();
        self.read(|s| {
            let pairing = s.sendspin.pairings.get(client_id)?;
            b64url_array(&pairing.long_term_psk)
        })
    }

    fn pair(&self, client_id: &str, psk: [u8; 32]) -> Result<(), TrustError> {
        let paired_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        self.update(|s| {
            s.sendspin.pairings.insert(
                client_id.to_owned(),
                SendspinPairing {
                    long_term_psk: b64url(&psk),
                    paired_at,
                },
            );
            s.sendspin.approved.retain(|c| c != client_id);
            s.sendspin.pairing_requests.remove(client_id);
        })
        .map_err(|e| TrustError::Save(e.to_string()))
    }

    fn unpair(&self, client_id: &str) -> Result<bool, TrustError> {
        self.update(|s| s.sendspin.pairings.remove(client_id).is_some())
            .map_err(|e| TrustError::Save(e.to_string()))
    }

    fn is_approved(&self, client_id: &str) -> bool {
        self.refresh();
        self.read(|s| s.sendspin.approved.iter().any(|c| c == client_id))
    }

    fn approve(&self, client_id: &str) -> Result<(), TrustError> {
        self.update(|s| {
            if !s.sendspin.approved.iter().any(|c| c == client_id) {
                s.sendspin.approved.push(client_id.to_owned());
            }
        })
        .map_err(|e| TrustError::Save(e.to_string()))
    }

    fn revoke(&self, client_id: &str) -> Result<bool, TrustError> {
        self.update(|s| {
            let before = s.sendspin.approved.len();
            s.sendspin.approved.retain(|c| c != client_id);
            s.sendspin.approved.len() != before
        })
        .map_err(|e| TrustError::Save(e.to_string()))
    }

    fn pairing_request(&self, client_id: &str) -> Option<PairingRequest> {
        self.refresh();
        let saved = self.read(|s| s.sendspin.pairing_requests.get(client_id).cloned())?;
        let request = match PairMethod::from_name(&saved.method)? {
            PairMethod::PairingPsk => PairingRequest::PairingPsk {
                pairing_psk: PairingRequest::from_token(saved.secret.as_deref()?)
                    .ok()
                    .and_then(|(id, r)| (id == client_id).then_some(r))?
                    .pairing_psk()?,
            },
            PairMethod::StaticCode => PairingRequest::StaticCode {
                code: saved.secret?,
            },
            PairMethod::DynamicCode => PairingRequest::DynamicCode { code: saved.code },
        };
        Some(request)
    }

    fn request_pairing(&self, client_id: &str, request: PairingRequest) -> Result<(), TrustError> {
        let saved = match &request {
            PairingRequest::PairingPsk { pairing_psk } => {
                let client_key = b64url_array(client_id).ok_or_else(|| {
                    TrustError::Invalid(format!("{client_id} is not an encrypted client's id"))
                })?;
                let token = PairingToken::PairingPsk {
                    client_key,
                    pairing_psk: *pairing_psk,
                };
                SendspinPairingRequest {
                    method: request.method().name().into(),
                    secret: Some(token.encode()),
                    code: None,
                }
            }
            PairingRequest::StaticCode { code } => SendspinPairingRequest {
                method: request.method().name().into(),
                secret: Some(code.clone()),
                code: None,
            },
            PairingRequest::DynamicCode { code } => SendspinPairingRequest {
                method: request.method().name().into(),
                secret: None,
                code: code.clone(),
            },
        };
        self.update(|s| {
            s.sendspin
                .pairing_requests
                .insert(client_id.to_owned(), saved);
        })
        .map_err(|e| TrustError::Save(e.to_string()))
    }

    fn end_pairing_request(&self, client_id: &str) -> Result<(), TrustError> {
        self.update(|s| {
            s.sendspin.pairing_requests.remove(client_id);
        })
        .map_err(|e| TrustError::Save(e.to_string()))
    }
}

impl PairingRequest {
    fn pairing_psk(&self) -> Option<[u8; 32]> {
        match self {
            Self::PairingPsk { pairing_psk } => Some(*pairing_psk),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(trust: &dyn Trust) {
        let id = trust.identity();
        assert_eq!(trust.identity().id(), id.id(), "the identity is kept");

        let client = Identity::generate().id();
        assert!(trust.long_term_psk(&client).is_none());
        trust.approve(&client).unwrap();
        assert!(trust.is_approved(&client));

        let token = PairingToken::PairingPsk {
            client_key: b64url_array(&client).unwrap(),
            pairing_psk: [4; 32],
        }
        .encode();
        let (named, request) = PairingRequest::from_token(&token).unwrap();
        assert_eq!(named, client);
        trust.request_pairing(&client, request.clone()).unwrap();
        assert_eq!(trust.pairing_request(&client), Some(request));

        trust.pair(&client, [9; 32]).unwrap();
        assert_eq!(trust.long_term_psk(&client), Some([9; 32]));
        assert!(!trust.is_approved(&client), "pairing ends approval");
        assert_eq!(trust.pairing_request(&client), None, "and the request");

        trust
            .request_pairing(&client, PairingRequest::DynamicCode { code: None })
            .unwrap();
        assert!(enter_code(trust, &client, "123-456").unwrap());
        assert_eq!(
            trust.pairing_request(&client),
            Some(PairingRequest::DynamicCode {
                code: Some("123456".into())
            })
        );
        assert!(enter_code(trust, &client, "12345").is_err());
        trust
            .request_pairing(&client, PairingRequest::static_code("1234 5678").unwrap())
            .unwrap();
        assert!(!enter_code(trust, &client, "123456").unwrap());
        assert_eq!(
            trust.pairing_request(&client),
            Some(PairingRequest::StaticCode {
                code: "12345678".into()
            })
        );
        trust.end_pairing_request(&client).unwrap();
        assert_eq!(trust.pairing_request(&client), None);

        assert!(trust.unpair(&client).unwrap());
        assert!(!trust.unpair(&client).unwrap());
        assert!(trust.long_term_psk(&client).is_none());
    }

    #[test]
    fn memory_trust() {
        check(&MemoryTrust::new());
    }

    #[test]
    fn state_file_trust_survives_a_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let store = StateStore::open(tmp.path()).unwrap();
        check(&store);
        let client = Identity::generate().id();
        store.pair(&client, [3; 32]).unwrap();
        let id = store.identity().id();

        let reopened = StateStore::open(tmp.path()).unwrap();
        assert_eq!(reopened.identity().id(), id);
        assert_eq!(reopened.long_term_psk(&client), Some([3; 32]));
    }

    #[test]
    fn another_process_can_ask_for_a_pairing() {
        let tmp = tempfile::tempdir().unwrap();
        let server = StateStore::open(tmp.path()).unwrap();
        let client = Identity::generate().id();
        assert_eq!(server.pairing_request(&client), None);
        let cli = StateStore::open(tmp.path()).unwrap();
        cli.request_pairing(&client, PairingRequest::DynamicCode { code: None })
            .unwrap();
        assert_eq!(
            server.pairing_request(&client),
            Some(PairingRequest::DynamicCode { code: None })
        );
    }

    #[test]
    fn static_codes_are_eight_digits() {
        assert!(PairingRequest::static_code("1234-5678").is_ok());
        assert!(PairingRequest::static_code("1234567").is_err());
    }
}
