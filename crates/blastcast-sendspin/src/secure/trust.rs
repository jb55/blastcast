//! What the server remembers about encrypted clients: its own identity
//! key, the long-term PSK of each paired client, which clients the
//! operator let play unpaired, and the pairings the operator asked for.
//!
//! An app keeps it wherever it keeps its state by implementing [`Trust`];
//! [`MemoryTrust`] is for tests and servers with nowhere to keep it.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Mutex, MutexGuard, PoisonError};

use super::keys::{Identity, PairingToken, b64url};

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

impl PairingRequest {
    /// A version-0 token's pairing PSK.
    pub fn pairing_psk(&self) -> Option<[u8; 32]> {
        match self {
            Self::PairingPsk { pairing_psk } => Some(*pairing_psk),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_trust() {
        crate::testing::check_trust(&MemoryTrust::new());
    }

    #[test]
    fn static_codes_are_eight_digits() {
        assert!(PairingRequest::static_code("1234-5678").is_ok());
        assert!(PairingRequest::static_code("1234567").is_err());
    }
}
