//! The `{"type", "payload"}` envelope every text frame carries.
//!
//! Encoding is serde's adjacently tagged enum. Decoding goes through
//! [`Envelope`] first, so a type this build doesn't know is its own error
//! (the connection logs it and carries on) instead of a parse failure, and a
//! missing or `null` payload reads as `{}`.

use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

/// Why a text frame didn't decode.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("not a message envelope: {0}")]
    Envelope(#[source] serde_json::Error),
    #[error("unknown message type {0:?}")]
    UnknownType(String),
    #[error("bad {kind} payload: {source}")]
    Payload {
        kind: String,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Deserialize)]
pub(crate) struct Envelope {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub payload: Value,
}

impl Envelope {
    pub fn parse(text: &str) -> Result<Self, DecodeError> {
        serde_json::from_str(text).map_err(DecodeError::Envelope)
    }

    /// Decode the payload as `T`, reading a missing or `null` payload as an
    /// empty object.
    pub fn payload<T: DeserializeOwned>(self) -> Result<T, DecodeError> {
        let payload = match self.payload {
            Value::Null => Value::Object(Map::new()),
            v => v,
        };
        serde_json::from_value(payload).map_err(|source| DecodeError::Payload {
            kind: self.kind,
            source,
        })
    }
}

/// Serde helpers for a field that has three states on the wire: omitted
/// (`None`, "unchanged"), `null` (`Some(None)`, "clear it") and a value
/// (`Some(Some(v))`). Use with `#[serde(default, skip_serializing_if =
/// "Option::is_none", with = "tristate")]`.
pub(crate) mod tristate {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<T: Serialize, S: Serializer>(
        value: &Option<Option<T>>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(inner) => inner.serialize(s),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, T: Deserialize<'de>, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Option<Option<T>>, D::Error> {
        Option::<T>::deserialize(d).map(Some)
    }
}
