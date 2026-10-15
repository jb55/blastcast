//! What we advertise: a service type, an instance name, a port and TXT
//! entries, plus builders for the two adverts the server publishes.

use serde_json::Value;

/// Clients find the Sendspin server through this (client-initiated
/// connections).
pub const SENDSPIN_SERVER_TYPE: &str = "_sendspin-server._tcp.local.";
/// Sendspin players advertise this when they want the server to dial them
/// (server-initiated connections).
pub const SENDSPIN_CLIENT_TYPE: &str = "_sendspin._tcp.local.";
/// Music Assistant's server advert; Tune Assistant and other MA clients
/// browse for it.
pub const MASS_TYPE: &str = "_mass._tcp.local.";
/// The websocket path the Sendspin spec recommends.
pub const SENDSPIN_PATH: &str = "/sendspin";

/// One TXT string: `key=value`, or a bare `key` when the value is None.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxtEntry {
    pub key: String,
    pub value: Option<String>,
}

impl TxtEntry {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: Some(value.into()),
        }
    }

    pub fn flag(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: None,
        }
    }
}

/// A service instance to advertise. The host name and addresses come from
/// the [`crate::Mdns`] it is advertised on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Service {
    /// Fully qualified type, e.g. [`MASS_TYPE`].
    pub service_type: String,
    /// The instance label, e.g. the server id for `_mass._tcp`.
    pub instance: String,
    pub port: u16,
    pub txt: Vec<TxtEntry>,
}

impl Service {
    /// The DNS-SD full name, `<instance>.<type>`, which is how the advert is
    /// addressed on the wire and in browse results.
    pub fn fullname(&self) -> String {
        format!("{}.{}", self.instance, self.service_type)
    }
}

/// `_sendspin-server._tcp` for client-initiated connections: `path` is
/// required by the spec, `name` should match the name in `server/hello`.
pub fn sendspin_server(instance: &str, name: &str, port: u16) -> Service {
    Service {
        service_type: SENDSPIN_SERVER_TYPE.to_owned(),
        instance: instance.to_owned(),
        port,
        txt: vec![
            TxtEntry::new("path", SENDSPIN_PATH),
            TxtEntry::new("name", name),
        ],
    }
}

/// `_mass._tcp` as Music Assistant publishes it: the instance is the server
/// id and the TXT record is the `server_info` object, one entry per field.
/// `server_info` is what the API returns for the `server_info` command, so
/// the two never drift.
pub fn mass(server_id: &str, port: u16, server_info: &Value) -> Service {
    Service {
        service_type: MASS_TYPE.to_owned(),
        instance: server_id.to_owned(),
        port,
        txt: txt_from_json(server_info),
    }
}

/// TXT entries for a JSON object, with each value written as Python's
/// `str()` writes it, since MA clients parse what python-zeroconf publishes:
/// `True`/`False` for booleans, a bare key for null, numbers and strings as
/// is. Anything that isn't an object gives no entries.
pub fn txt_from_json(value: &Value) -> Vec<TxtEntry> {
    let Some(map) = value.as_object() else {
        return Vec::new();
    };
    map.iter()
        .map(|(key, value)| match value {
            Value::Null => TxtEntry::flag(key),
            Value::Bool(true) => TxtEntry::new(key, "True"),
            Value::Bool(false) => TxtEntry::new(key, "False"),
            Value::String(s) => TxtEntry::new(key, s),
            Value::Number(n) => TxtEntry::new(key, n.to_string()),
            other => TxtEntry::new(key, other.to_string()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sendspin_server_carries_path_and_name() {
        let s = sendspin_server("tas-1a2b3c4d", "Tune Assistant", 8927);
        assert_eq!(s.fullname(), "tas-1a2b3c4d._sendspin-server._tcp.local.");
        assert_eq!(s.port, 8927);
        assert_eq!(
            s.txt,
            vec![
                TxtEntry::new("path", "/sendspin"),
                TxtEntry::new("name", "Tune Assistant"),
            ]
        );
    }

    #[test]
    fn mass_txt_matches_python_zeroconf() {
        let info = json!({
            "server_id": "abcdef",
            "server_version": "0.1.0",
            "schema_version": 28,
            "onboard_done": true,
            "homeassistant_addon": false,
            "external_url": null,
            "status": "running",
        });
        let s = mass("abcdef", 8095, &info);
        assert_eq!(s.fullname(), "abcdef._mass._tcp.local.");
        let get = |k: &str| s.txt.iter().find(|e| e.key == k).unwrap().value.clone();
        assert_eq!(get("server_id").as_deref(), Some("abcdef"));
        assert_eq!(get("schema_version").as_deref(), Some("28"));
        assert_eq!(get("onboard_done").as_deref(), Some("True"));
        assert_eq!(get("homeassistant_addon").as_deref(), Some("False"));
        assert_eq!(get("external_url"), None);
        assert_eq!(get("status").as_deref(), Some("running"));
        assert_eq!(s.txt.len(), 7);
    }

    #[test]
    fn non_object_gives_no_txt() {
        assert!(txt_from_json(&json!([1, 2])).is_empty());
        assert!(txt_from_json(&Value::Null).is_empty());
    }
}
