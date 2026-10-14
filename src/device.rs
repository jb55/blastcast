//! An AirPlay speaker, read from its `_raop._tcp` advert.
//!
//! The instance name is `<device id>@<name>`, the device id the speaker's
//! MAC in hex; the player id is `ap` and the id in lower case, as Music
//! Assistant names its AirPlay players. The TXT record says what the
//! speaker takes: `et` its encryption types (0 is none), `cn` its codecs
//! (1 is ALAC), `pw` whether it wants a password, `am` its model.
//!
//! This sender speaks unencrypted ALAC at 44.1 kHz, 16 bits, stereo, with
//! no password or pairing. A speaker that can't take that is skipped, with
//! the reason logged: an Apple TV or HomePod that insists on pairing, or
//! one behind a password.

use std::net::{IpAddr, SocketAddr};

use tas_mdns::browse::Discovered;

/// The mDNS service type AirPlay 1 receivers advertise.
pub const RAOP_SERVICE_TYPE: &str = "_raop._tcp.local.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaopDevice {
    /// `ap` and the device id, lower case: the player id.
    pub player_id: String,
    pub name: String,
    pub addr: SocketAddr,
    /// TXT `am`, e.g. `AirPort10,115` or `ShairportSync`.
    pub model: Option<String>,
    pub manufacturer: Option<String>,
    /// The device id in upper-case hex, if the instance carries a MAC.
    pub mac: Option<String>,
    /// `<instance>.<type>`: what a later removal names.
    pub fullname: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Unsupported {
    #[error("no usable address")]
    NoAddress,
    #[error("the instance name {0:?} isn't <device id>@<name>")]
    BadInstance(String),
    #[error("it wants a password")]
    Password,
    #[error("it takes no unencrypted stream (et={0})")]
    Encrypted(String),
    #[error("it doesn't decode ALAC (cn={0})")]
    NoAlac(String),
    #[error("it doesn't take 44.1 kHz 16-bit stereo ({0})")]
    Format(String),
}

impl RaopDevice {
    pub fn from_discovered(found: &Discovered) -> Result<Self, Unsupported> {
        let (raw_id, name) = found
            .instance
            .split_once('@')
            .filter(|(id, name)| !id.is_empty() && !name.is_empty())
            .ok_or_else(|| Unsupported::BadInstance(found.instance.clone()))?;
        let ip = found.best_addr().ok_or(Unsupported::NoAddress)?;
        if found.port == 0 {
            return Err(Unsupported::NoAddress);
        }
        if found
            .txt_value("pw")
            .is_some_and(|v| v.eq_ignore_ascii_case("true") || v == "1")
        {
            return Err(Unsupported::Password);
        }
        if let Some(et) = found.txt_value("et")
            && !list_has(et, "0")
        {
            return Err(Unsupported::Encrypted(et.to_owned()));
        }
        if let Some(cn) = found.txt_value("cn")
            && !list_has(cn, "1")
        {
            return Err(Unsupported::NoAlac(cn.to_owned()));
        }
        for (key, want) in [("sr", "44100"), ("ss", "16"), ("ch", "2")] {
            if let Some(v) = found.txt_value(key)
                && !list_has(v, want)
            {
                return Err(Unsupported::Format(format!("{key}={v}")));
            }
        }
        let is_mac = raw_id.len() == 12 && raw_id.chars().all(|c| c.is_ascii_hexdigit());
        Ok(Self {
            player_id: format!("ap{}", raw_id.to_ascii_lowercase()),
            name: name.to_owned(),
            addr: SocketAddr::new(ip, found.port),
            model: found.txt_value("am").map(str::to_owned),
            manufacturer: found.txt_value("manufacturer").map(str::to_owned),
            mac: is_mac.then(|| raw_id.to_ascii_uppercase()),
            fullname: found.fullname.clone(),
        })
    }

    /// The MAC as `AA:BB:CC:DD:EE:FF`, for the player's device info.
    pub fn mac_address(&self) -> Option<String> {
        let mac = self.mac.as_ref()?;
        let pairs: Vec<&str> = (0..6).map(|i| &mac[i * 2..i * 2 + 2]).collect();
        Some(pairs.join(":"))
    }

    pub fn ip(&self) -> IpAddr {
        self.addr.ip()
    }
}

fn list_has(list: &str, want: &str) -> bool {
    list.split(',').any(|v| v.trim() == want)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::Ipv4Addr;

    use super::*;

    fn found(instance: &str, txt: &[(&str, &str)]) -> Discovered {
        Discovered {
            fullname: format!("{instance}.{RAOP_SERVICE_TYPE}"),
            instance: instance.into(),
            host: "kitchen.local.".into(),
            addresses: vec![IpAddr::V4(Ipv4Addr::new(192, 168, 1, 30))],
            port: 7000,
            txt: txt
                .iter()
                .map(|(k, v)| (k.to_string(), Some(v.to_string())))
                .collect::<BTreeMap<_, _>>(),
        }
    }

    #[test]
    fn reads_a_shairport_advert() {
        let d = RaopDevice::from_discovered(&found(
            "A1B2C3D4E5F6@Kitchen",
            &[
                ("cn", "0,1"),
                ("et", "0,1"),
                ("sr", "44100"),
                ("ss", "16"),
                ("ch", "2"),
                ("am", "ShairportSync"),
            ],
        ))
        .unwrap();
        assert_eq!(d.player_id, "apa1b2c3d4e5f6");
        assert_eq!(d.name, "Kitchen");
        assert_eq!(d.addr, "192.168.1.30:7000".parse().unwrap());
        assert_eq!(d.model.as_deref(), Some("ShairportSync"));
        assert_eq!(d.mac_address().as_deref(), Some("A1:B2:C3:D4:E5:F6"));
    }

    #[test]
    fn a_name_may_hold_an_at_sign() {
        let d = RaopDevice::from_discovered(&found("0011@Den @ home", &[])).unwrap();
        assert_eq!(
            (d.player_id.as_str(), d.name.as_str()),
            ("ap0011", "Den @ home")
        );
        assert_eq!(d.mac_address(), None);
    }

    #[test]
    fn skips_what_it_cant_stream_to() {
        let err = |instance, txt: &[(&str, &str)]| {
            RaopDevice::from_discovered(&found(instance, txt)).unwrap_err()
        };
        assert_eq!(
            err("Kitchen", &[]),
            Unsupported::BadInstance("Kitchen".into())
        );
        assert_eq!(err("AB@K", &[("pw", "true")]), Unsupported::Password);
        assert_eq!(
            err("AB@K", &[("et", "3,5")]),
            Unsupported::Encrypted("3,5".into())
        );
        assert_eq!(
            err("AB@K", &[("cn", "0,2")]),
            Unsupported::NoAlac("0,2".into())
        );
        assert_eq!(
            err("AB@K", &[("sr", "48000")]),
            Unsupported::Format("sr=48000".into())
        );
        let mut no_addr = found("AB@K", &[]);
        no_addr.addresses.clear();
        assert_eq!(
            RaopDevice::from_discovered(&no_addr),
            Err(Unsupported::NoAddress)
        );
    }
}
