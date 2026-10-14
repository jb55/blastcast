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
use std::net::IpAddr;
use std::sync::Arc;

use mdns_sd::{IfKind, ServiceDaemon, ServiceInfo};

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
        Ok(ip) => IfKind::Addr(ip),
        Err(_) => IfKind::Name(wanted.to_owned()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn loopback_resolves_by_address() {
        assert!(matches!(
            resolve_interface("127.0.0.1"),
            Ok(IfKind::Addr(ip)) if ip.is_loopback()
        ));
    }
}
