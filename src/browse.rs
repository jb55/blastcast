//! Browse results: services found on the network, and the Sendspin players
//! among them that want the server to dial them.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};

use mdns_sd::{Receiver, ResolvedService, ServiceEvent};

use crate::Mdns;
use crate::service::SENDSPIN_PATH;

/// A resolved service instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Discovered {
    /// `<instance>.<type>`; the key a later [`BrowseEvent::Removed`] uses.
    pub fullname: String,
    /// The instance label, `fullname` without the type.
    pub instance: String,
    /// The target host name, e.g. `kitchen.local.`.
    pub host: String,
    /// Best first: see [`Discovered::best_addr`].
    pub addresses: Vec<IpAddr>,
    pub port: u16,
    /// TXT entries; a bare key maps to None.
    pub txt: BTreeMap<String, Option<String>>,
}

impl Discovered {
    pub(crate) fn from_resolved(service: &ResolvedService) -> Self {
        let suffix = format!(".{}", service.ty_domain);
        let instance = service
            .fullname
            .strip_suffix(&suffix)
            .unwrap_or(&service.fullname)
            .to_owned();
        let mut addresses: Vec<IpAddr> =
            service.addresses.iter().map(|ip| ip.to_ip_addr()).collect();
        addresses.sort_by_key(|ip| (addr_rank(ip), *ip));
        let txt = service
            .txt_properties
            .iter()
            .map(|p| (p.key().to_owned(), p.val().map(|_| p.val_str().to_owned())))
            .collect();
        Self {
            fullname: service.fullname.clone(),
            instance,
            host: service.host.clone(),
            addresses,
            port: service.port,
            txt,
        }
    }

    /// The TXT value for `key`, if it has one.
    pub fn txt_value(&self, key: &str) -> Option<&str> {
        self.txt.get(key)?.as_deref()
    }

    /// The address to dial. IPv4 first, as sendspin-go does, since a
    /// link-local IPv6 address needs a scope id we don't carry; loopback last.
    pub fn best_addr(&self) -> Option<IpAddr> {
        self.addresses
            .first()
            .copied()
            .filter(|ip| addr_rank(ip) < UNUSABLE)
    }

    /// Read a `_sendspin._tcp` advert as a player to dial. A missing `path`
    /// falls back to `/sendspin` and a missing `name` to the instance label,
    /// as sendspin-go does; no usable address or port gives None.
    pub fn sendspin_client(&self) -> Option<SendspinClient> {
        let addr = self.best_addr()?;
        if self.port == 0 {
            return None;
        }
        let path = match self.txt_value("path") {
            Some(p) if p.starts_with('/') => p.to_owned(),
            Some(p) if !p.is_empty() => format!("/{p}"),
            _ => SENDSPIN_PATH.to_owned(),
        };
        let name = self
            .txt_value("name")
            .filter(|n| !n.is_empty())
            .unwrap_or(&self.instance)
            .to_owned();
        Some(SendspinClient {
            fullname: self.fullname.clone(),
            name,
            addr: SocketAddr::new(addr, self.port),
            path,
        })
    }
}

const UNUSABLE: u8 = 4;

fn addr_rank(ip: &IpAddr) -> u8 {
    match ip {
        IpAddr::V4(v4) if v4.is_loopback() => 2,
        IpAddr::V4(v4) if v4.is_link_local() => 1,
        IpAddr::V4(_) => 0,
        IpAddr::V6(v6) if v6.is_loopback() => 3,
        IpAddr::V6(v6) if v6.is_unicast_link_local() => UNUSABLE,
        IpAddr::V6(_) => 1,
    }
}

/// A Sendspin player that asked to be dialed: connect to
/// `ws://{addr}{path}` and run the handshake with the roles reversed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendspinClient {
    /// The advert's full name, to match a later removal.
    pub fullname: String,
    /// A discovery-time hint; `client/hello` has the authoritative name.
    pub name: String,
    pub addr: SocketAddr,
    pub path: String,
}

impl SendspinClient {
    pub fn url(&self) -> String {
        format!("ws://{}{}", self.addr, self.path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrowseEvent {
    /// A service was resolved, or its record changed (new address, TXT or
    /// port). The same `fullname` can arrive more than once.
    Resolved(Discovered),
    /// A service said goodbye or its records expired.
    Removed { fullname: String },
}

/// A running browse for one service type. Dropping it stops the browse.
pub struct Browser {
    pub(crate) mdns: Mdns,
    pub(crate) service_type: String,
    pub(crate) events: Receiver<ServiceEvent>,
}

impl Browser {
    /// The next resolved or removed service. None once the daemon has shut
    /// down or the browse was stopped.
    pub async fn next(&mut self) -> Option<BrowseEvent> {
        loop {
            match self.events.recv_async().await.ok()? {
                ServiceEvent::ServiceResolved(service) => {
                    return Some(BrowseEvent::Resolved(Discovered::from_resolved(&service)));
                }
                ServiceEvent::ServiceRemoved(_, fullname) => {
                    return Some(BrowseEvent::Removed { fullname });
                }
                ServiceEvent::SearchStopped(_) => return None,
                _ => {}
            }
        }
    }
}

impl Drop for Browser {
    fn drop(&mut self) {
        // Fails only when the daemon is already gone, which stops the browse
        // anyway.
        let _ = self.mdns.daemon().stop_browse(&self.service_type);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn discovered(addresses: Vec<IpAddr>, txt: &[(&str, Option<&str>)]) -> Discovered {
        Discovered {
            fullname: "kitchen._sendspin._tcp.local.".into(),
            instance: "kitchen".into(),
            host: "kitchen.local.".into(),
            addresses,
            port: 8928,
            txt: txt
                .iter()
                .map(|(k, v)| (k.to_string(), v.map(str::to_owned)))
                .collect(),
        }
    }

    #[test]
    fn ipv4_ranks_before_ipv6_and_loopback() {
        let mut addrs = [
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6("fe80::1".parse::<Ipv6Addr>().unwrap()),
            IpAddr::V6("2001:db8::1".parse::<Ipv6Addr>().unwrap()),
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20)),
        ];
        addrs.sort_by_key(|ip| (addr_rank(ip), *ip));
        assert_eq!(addrs[0], IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20)));
        assert_eq!(addrs[1], "2001:db8::1".parse::<IpAddr>().unwrap());
        assert_eq!(addrs[3], "fe80::1".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn sendspin_client_reads_the_advert() {
        let d = discovered(
            vec![IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20))],
            &[("path", Some("/sendspin")), ("name", Some("Kitchen"))],
        );
        let c = d.sendspin_client().unwrap();
        assert_eq!(c.name, "Kitchen");
        assert_eq!(c.url(), "ws://192.168.1.20:8928/sendspin");
    }

    #[test]
    fn sendspin_client_falls_back_like_sendspin_go() {
        let d = discovered(vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5))], &[]);
        let c = d.sendspin_client().unwrap();
        assert_eq!(c.name, "kitchen");
        assert_eq!(c.path, "/sendspin");

        let d = discovered(
            vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5))],
            &[("path", Some("ws")), ("name", Some(""))],
        );
        let c = d.sendspin_client().unwrap();
        assert_eq!(c.path, "/ws");
        assert_eq!(c.name, "kitchen");
    }

    #[test]
    fn link_local_ipv6_alone_is_not_dialable() {
        let d = discovered(vec!["fe80::1".parse().unwrap()], &[]);
        assert_eq!(d.sendspin_client(), None);
        let d = discovered(vec![], &[]);
        assert_eq!(d.sendspin_client(), None);
    }

    #[test]
    fn ipv6_url_is_bracketed() {
        let d = discovered(vec!["2001:db8::1".parse().unwrap()], &[]);
        assert_eq!(
            d.sendspin_client().unwrap().url(),
            "ws://[2001:db8::1]:8928/sendspin"
        );
    }
}
