//! mDNS for the whole server: one shared responder that advertises our
//! services and browses for others, wrapped around `mdns-sd`.
//!
//! - **Advertise** `_sendspin-server._tcp` so Sendspin players can dial us,
//!   and `_mass._tcp` so Tune Assistant and other MA clients find the API
//!   ([`service::sendspin_server`], [`service::mass`]).
//! - **Browse** `_sendspin._tcp` for players that want the server to dial
//!   them ([`Mdns::browse`], [`Discovered::sendspin_client`]).
//!
//! Every component shares one [`Mdns`]; it is a cheap clone, and the
//! responder runs until the last clone, [`Advert`] and [`Browser`] is gone.
//! It can be held to one network interface.

pub mod browse;
pub mod service;

use std::io;
use std::net::{IpAddr, Ipv4Addr, UdpSocket};
use std::sync::Arc;

use mdns_sd::{IfKind, IfPredicate, ServiceDaemon, ServiceInfo};

pub use browse::{BrowseEvent, Browser, Discovered, SendspinClient};
pub use service::{Service, TxtEntry};

#[derive(Debug, thiserror::Error)]
pub enum MdnsError {
    #[error("mdns: {0}")]
    Daemon(#[from] mdns_sd::Error),
    #[error("listing network interfaces: {0}")]
    Interfaces(io::Error),
    #[error("no network interface named or addressed {wanted:?}; have: {}", available.join(", "))]
    NoSuchInterface {
        wanted: String,
        available: Vec<String>,
    },
}

pub type Result<T, E = MdnsError> = std::result::Result<T, E>;

/// Which interface to run mDNS on: an interface name (`eth0`) or one of its
/// addresses (`192.168.1.10`). The default, None, is every interface.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MdnsConfig {
    pub interface: Option<String>,
}

/// The shared mDNS responder and browser.
#[derive(Clone)]
pub struct Mdns {
    inner: Arc<Inner>,
}

struct Inner {
    daemon: ServiceDaemon,
    host_name: String,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Adverts were unregistered before this (they hold the Arc), so their
        // goodbye packets are already queued ahead of the shutdown.
        if let Err(err) = self.daemon.shutdown() {
            tracing::debug!(%err, "mdns daemon already stopped");
        }
    }
}

impl Mdns {
    /// Start the responder. `host_label` names this machine in our records
    /// as `<host_label>.local.`; mdns-sd renames it on a conflict.
    pub fn new(host_label: &str, config: &MdnsConfig) -> Result<Self> {
        let interface = config
            .interface
            .as_deref()
            .map(resolve_interface)
            .transpose()?;
        let daemon = ServiceDaemon::new()?;
        if let Some(kind) = interface {
            daemon.disable_interface(IfKind::All)?;
            daemon.enable_interface(kind)?;
        }
        Ok(Self {
            inner: Arc::new(Inner {
                daemon,
                host_name: format!("{}.local.", host_label.trim_end_matches('.')),
            }),
        })
    }

    /// The host name our adverts point at, e.g. `tas-1a2b3c4d.local.`.
    pub fn host_name(&self) -> &str {
        &self.inner.host_name
    }

    /// Publish `service` on every enabled interface, with that interface's
    /// addresses, following address changes. It stays up until the returned
    /// [`Advert`] is dropped.
    pub fn advertise(&self, service: Service) -> Result<Advert> {
        self.register(&service)?;
        tracing::info!(
            name = %service.fullname(),
            port = service.port,
            "advertising over mdns"
        );
        Ok(Advert {
            mdns: self.clone(),
            service,
        })
    }

    fn register(&self, service: &Service) -> Result<()> {
        let txt: Vec<mdns_sd::TxtProperty> = service
            .txt
            .iter()
            .map(|e| match &e.value {
                Some(v) => (e.key.as_str(), v.as_str()).into(),
                None => e.key.as_str().into(),
            })
            .collect();
        let info = ServiceInfo::new(
            &service.service_type,
            &service.instance,
            &self.inner.host_name,
            "",
            service.port,
            txt,
        )?
        .enable_addr_auto();
        self.inner.daemon.register(info)?;
        Ok(())
    }

    /// Browse for `service_type` (e.g. [`service::SENDSPIN_CLIENT_TYPE`]).
    /// The browse runs until the returned [`Browser`] is dropped.
    pub fn browse(&self, service_type: &str) -> Result<Browser> {
        let events = self.inner.daemon.browse(service_type)?;
        Ok(Browser {
            mdns: self.clone(),
            service_type: service_type.to_owned(),
            events,
        })
    }

    pub(crate) fn daemon(&self) -> &ServiceDaemon {
        &self.inner.daemon
    }
}

/// A published service. Dropping it sends a goodbye and stops advertising.
pub struct Advert {
    mdns: Mdns,
    service: Service,
}

impl Advert {
    pub fn service(&self) -> &Service {
        &self.service
    }

    /// Replace the record, e.g. when `server_info` changes. A new instance
    /// name or type takes the old advert down first.
    pub fn update(&mut self, service: Service) -> Result<()> {
        if service == self.service {
            return Ok(());
        }
        if service.fullname() != self.service.fullname() {
            self.unregister();
        }
        self.mdns.register(&service)?;
        self.service = service;
        Ok(())
    }

    fn unregister(&self) {
        let fullname = self.service.fullname();
        if let Err(err) = self.mdns.daemon().unregister(&fullname) {
            tracing::debug!(%err, name = %fullname, "mdns unregister");
        }
    }
}

impl Drop for Advert {
    fn drop(&mut self) {
        self.unregister();
    }
}

/// The host label for a server: `tas-` and the first eight characters of
/// its id that are valid in a DNS label, so two servers on one network get
/// different host names and one server keeps its name across restarts.
pub fn host_label(server_id: &str) -> String {
    let id: String = server_id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(8)
        .collect::<String>()
        .to_ascii_lowercase();
    if id.is_empty() {
        "tas".to_owned()
    } else {
        format!("tas-{id}")
    }
}

/// The name of the interface `wanted` names, by name or by one of its
/// addresses, for tools that only take a name. `None` if there is none.
pub fn interface_name(wanted: &str) -> Option<String> {
    let interfaces = if_addrs::get_if_addrs().ok()?;
    let found = match wanted.parse::<IpAddr>() {
        Ok(ip) => interfaces.into_iter().find(|i| i.ip() == ip),
        Err(_) => interfaces.into_iter().find(|i| i.name == wanted),
    };
    found.map(|i| i.name)
}

/// The address a device on the LAN can reach us on, for URLs we hand out
/// (MA's publish IP). On `interface` (a name or one of its addresses) when
/// one is given; otherwise the one the default route leaves from, else any
/// IPv4 that isn't loopback or link-local. Loopback when there is none.
pub fn publish_ip(interface: Option<&str>) -> IpAddr {
    let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
    if let Some(wanted) = interface {
        if let Ok(ip) = wanted.parse::<IpAddr>() {
            return ip;
        }
        let ips: Vec<IpAddr> = if_addrs::get_if_addrs()
            .unwrap_or_default()
            .into_iter()
            .filter(|i| i.name == wanted)
            .map(|i| i.ip())
            .collect();
        return ips
            .iter()
            .find(|ip| ip.is_ipv4())
            .or(ips.first())
            .copied()
            .unwrap_or(loopback);
    }
    default_route_ip()
        .or_else(|| {
            if_addrs::get_if_addrs()
                .ok()?
                .into_iter()
                .map(|i| i.ip())
                .find(|ip| matches!(ip, IpAddr::V4(v4) if !v4.is_loopback() && !v4.is_link_local()))
        })
        .unwrap_or(loopback)
}

/// The source address of the default IPv4 route. Connecting a UDP socket
/// only picks the route; nothing is sent.
fn default_route_ip() -> Option<IpAddr> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
    // TEST-NET-2, which nobody answers; any routed address would do.
    socket.connect((Ipv4Addr::new(198, 51, 100, 1), 9)).ok()?;
    let ip = socket.local_addr().ok()?.ip();
    (!ip.is_unspecified() && !ip.is_loopback()).then_some(ip)
}

/// Map the configured interface to what mdns-sd matches on, checking it
/// exists so a typo fails at startup instead of advertising nowhere.
fn resolve_interface(wanted: &str) -> Result<IfKind> {
    let interfaces = if_addrs::get_if_addrs().map_err(MdnsError::Interfaces)?;
    let found = match wanted.parse::<IpAddr>() {
        Ok(ip) => interfaces.iter().any(|i| i.ip() == ip),
        Err(_) => interfaces.iter().any(|i| i.name == wanted),
    };
    if !found {
        let mut available: Vec<String> = interfaces
            .iter()
            .map(|i| format!("{} ({})", i.name, i.ip()))
            .collect();
        available.dedup();
        return Err(MdnsError::NoSuchInterface {
            wanted: wanted.to_owned(),
            available,
        });
    }
    Ok(match wanted.parse::<IpAddr>() {
        Ok(ip) => IfKind::Predicate(IfPredicate::new(only(ip))),
        Err(_) => IfKind::Name(wanted.to_owned()),
    })
}

/// Matches the one address `ip`, not the interface it is on. mdns-sd's
/// `IfKind::Addr` takes every address of `ip`'s interface: on a NIC with a
/// LAN and a WAN address (monad's eno1 has 192.168.1.68 and 65.7.8.70) it
/// would query from either, and the speakers drop queries from off their
/// subnet, and it would publish both, and a phone handed the WAN one can't
/// connect to a receiver.
fn only(ip: IpAddr) -> impl Fn(&if_addrs::Interface) -> bool + Send + Sync + 'static {
    move |i| i.ip() == ip
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interface_name_from_a_name_or_an_address() {
        assert_eq!(interface_name("lo").as_deref(), Some("lo"));
        assert_eq!(interface_name("127.0.0.1").as_deref(), Some("lo"));
        assert_eq!(interface_name("no-such-if0"), None);
    }

    #[test]
    fn publish_ip_on_an_interface() {
        let lo = IpAddr::V4(Ipv4Addr::LOCALHOST);
        assert_eq!(publish_ip(Some("lo")), lo);
        assert_eq!(
            publish_ip(Some("10.1.2.3")),
            "10.1.2.3".parse::<IpAddr>().unwrap()
        );
        // Without an interface it is whatever this machine has, but never
        // unspecified.
        assert!(!publish_ip(None).is_unspecified());
    }

    #[test]
    fn host_label_is_a_dns_label() {
        assert_eq!(host_label("1A2b3c4d5e6f"), "tas-1a2b3c4d");
        assert_eq!(host_label("my server!"), "tas-myserver");
        assert_eq!(host_label("..."), "tas");
    }

    #[test]
    fn unknown_interface_is_an_error() {
        let err = resolve_interface("no-such-if0").unwrap_err();
        assert!(matches!(err, MdnsError::NoSuchInterface { .. }), "{err}");
        assert!(err.to_string().contains("no-such-if0"));
    }

    #[test]
    fn an_address_pins_that_address_not_its_interface() {
        assert!(matches!(
            resolve_interface("127.0.0.1"),
            Ok(IfKind::Predicate(_))
        ));
        let v4: IpAddr = "127.0.0.1".parse().unwrap();
        let pinned = only(v4);
        let lo = |ip: &str| if_addrs::Interface {
            name: "lo".into(),
            addr: match ip.parse().unwrap() {
                IpAddr::V4(ip) => if_addrs::IfAddr::V4(if_addrs::Ifv4Addr {
                    ip,
                    netmask: Ipv4Addr::new(255, 0, 0, 0),
                    prefixlen: 8,
                    broadcast: None,
                }),
                IpAddr::V6(ip) => if_addrs::IfAddr::V6(if_addrs::Ifv6Addr {
                    ip,
                    netmask: std::net::Ipv6Addr::UNSPECIFIED,
                    prefixlen: 128,
                    broadcast: None,
                }),
            },
            index: Some(1),
            oper_status: if_addrs::IfOperStatus::Up,
            is_p2p: false,
        };
        assert!(pinned(&lo("127.0.0.1")));
        // Another address on the same interface, as 65.7.8.70 is beside
        // 192.168.1.68 on monad's eno1.
        assert!(!pinned(&lo("127.0.0.2")));
        assert!(!pinned(&lo("::1")));
    }
}
