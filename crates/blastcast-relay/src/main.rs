//! `blastcast-relay`: an AirPlay receiver whose audio every Sendspin
//! player on the network plays, in sync. See the crate's docs.

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use blastcast::ServerClock;
use blastcast_airplay::ap2::{Identity, ptp::Ptp};
use blastcast_airplay::{NativeConfig, Receiver, ReceiverConfig};
use blastcast_mdns::{Mdns, MdnsConfig};
use blastcast_relay::{Relay, Store, receiver_info};
use blastcast_sendspin::{Dialer, DialerConfig, ServerConfig, dialer};
use blastcast_telemetry::Telemetry;
use clap::Parser;
use tokio::net::TcpListener;

/// How often players that wait to be dialed (`_sendspin._tcp`) are looked
/// for afresh.
const REBROWSE: Duration = Duration::from_secs(60);

/// Play a phone's AirPlay to every Sendspin player, in sync.
#[derive(Debug, Parser)]
#[command(version)]
struct Args {
    /// The name the phone shows, and the Sendspin server's.
    #[arg(long, env = "BLASTCAST_RELAY_NAME", default_value = "blastcast")]
    name: String,

    /// The AirPlay receiver's RTSP port.
    #[arg(long, env = "BLASTCAST_RELAY_AIRPLAY_PORT", default_value_t = 7000)]
    airplay_port: u16,

    /// The Sendspin server's port.
    #[arg(long, env = "BLASTCAST_RELAY_SENDSPIN_PORT", default_value_t = 8927)]
    sendspin_port: u16,

    /// Also advertise AirPlay 2, with pairing and PTP timing. PTP binds
    /// ports 319 and 320, which takes CAP_NET_BIND_SERVICE; without it,
    /// AirPlay 2 runs without timing.
    #[arg(long, env = "BLASTCAST_RELAY_AIRPLAY2")]
    airplay2: bool,

    /// Where the relay keeps its identities (`relay.json`). By default
    /// `$XDG_STATE_HOME/blastcast-relay`, or `~/.local/state/blastcast-relay`.
    #[arg(long, env = "BLASTCAST_RELAY_STATE_DIR")]
    state_dir: Option<PathBuf>,

    /// The interface (or one of its addresses) to run mDNS on; every one
    /// by default.
    #[arg(long, env = "BLASTCAST_RELAY_MDNS_INTERFACE")]
    mdns_interface: Option<String>,

    /// The log filter, as RUST_LOG.
    #[arg(long, env = "BLASTCAST_RELAY_LOG", default_value = "info")]
    log_level: String,
}

impl Args {
    fn state_dir(&self) -> Result<PathBuf> {
        if let Some(dir) = &self.state_dir {
            return Ok(dir.clone());
        }
        let base = std::env::var_os("XDG_STATE_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/state")))
            .context("no --state-dir, and neither XDG_STATE_HOME nor HOME is set")?;
        Ok(base.join("blastcast-relay"))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(&args.log_level))
        .init();

    let dir = args.state_dir()?;
    let store = Arc::new(Store::open(&dir).with_context(|| format!("opening {}", dir.display()))?);
    let server_id = store.server_id().context("saving the server id")?;
    let host = blastcast_mdns::host_label(&server_id);
    let mdns = Mdns::new(
        &host,
        &MdnsConfig {
            interface: args.mdns_interface.clone(),
        },
    )
    .context("starting mDNS")?;
    tracing::info!(state_dir = %dir.display(), server_id, mdns_host = mdns.host_name(), "blastcast relay starting");

    // Every time on the wire, the receiver's and Sendspin's, is read from
    // this one clock.
    let clock = ServerClock::new();
    let telemetry = Telemetry::default();

    let mut sendspin = ServerConfig::new(&server_id, &args.name);
    sendspin.clock = clock;
    sendspin.trust = store.clone();
    let relay = Relay::new(sendspin);
    tokio::spawn(relay.clone().run());

    let mut native = NativeConfig::new(&server_id, clock, Some(mdns.clone()));
    native.identity = Identity::load(&*store);
    native.airplay2 = args.airplay2;
    if args.airplay2 {
        native.ptp = Ptp::bind(clock, telemetry).await;
    }
    let receiver = Receiver::spawn(
        ReceiverConfig::new(native),
        receiver_info(&args.name, args.airplay_port),
        relay.handler(),
        None,
    );

    let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, args.sendspin_port))
        .await
        .with_context(|| format!("binding the Sendspin port {}", args.sendspin_port))?;
    let port = listener.local_addr()?.port();
    // Advertised once it listens; dropping it sends the goodbye.
    let _advert = mdns
        .advertise(blastcast_mdns::service::sendspin_server(
            &host, &args.name, port,
        ))
        .context("advertising Sendspin")?;
    tracing::info!(
        port,
        path = blastcast_sendspin::server::PATH,
        "Sendspin listening"
    );

    // Players that advertise `_sendspin._tcp` wait to be dialed.
    let dialer = Dialer::new(relay.server().clone(), DialerConfig::default());
    tokio::spawn(dialer::discover(dialer, mdns.clone(), REBROWSE));

    tokio::select! {
        r = relay.server().clone().serve(listener) => r.context("the Sendspin server stopped")?,
        r = tokio::signal::ctrl_c() => r.context("waiting for ctrl-c")?,
    }
    tracing::info!("shutting down");
    receiver.stop().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn cli_is_well_formed() {
        Args::command().debug_assert();
    }

    #[test]
    fn the_default_ports_are_airplays_and_sendspins() {
        let args = Args::parse_from(["blastcast-relay"]);
        assert_eq!((args.airplay_port, args.sendspin_port), (7000, 8927));
        assert!(!args.airplay2);
    }
}
