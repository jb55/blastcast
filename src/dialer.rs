//! Server-initiated connections: dialing Sendspin players that advertise
//! `_sendspin._tcp`, and players configured by address.
//!
//! The [`Dialer`] runs one task per target, keyed by the advert's full name
//! (or the configured URL), so an advert that resolves again only updates the
//! address the next dial uses. Each task dials `ws://host:port/path` and runs
//! the same handshake as the listener with [`ConnectionReason::Discovery`]:
//! the lowest priority a client admits, so the dialer never takes a speaker
//! away from a server that is playing on it. What the task does when a
//! connection ends is [`next_step`]'s call.
//!
//! [`discover`] feeds a dialer from an mDNS browse.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt, future};
use tas_mdns::service::{SENDSPIN_CLIENT_TYPE, SENDSPIN_PATH};
use tas_mdns::{BrowseEvent, Mdns, SendspinClient};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use crate::connection::Frame;
use crate::protocol::{ConnectionReason, GoodbyeReason};
use crate::server::{Disconnect, MAX_MESSAGE, SendspinServer};

/// The port the spec recommends for a player's own websocket.
pub const DEFAULT_CLIENT_PORT: u16 = 8928;

#[derive(Debug, Clone)]
pub struct DialerConfig {
    /// The first retry after a failure waits this long; each further
    /// failure doubles it, up to `max_backoff`.
    pub base_backoff: Duration,
    pub max_backoff: Duration,
    /// A connection that lasts this long resets the backoff; one that ends
    /// sooner counts as a failure, so a player that drops every connection
    /// isn't redialed in a tight loop.
    pub stable_after: Duration,
    /// How long to wait before trying again after a player turned us away
    /// for another server.
    pub rejected_retry: Duration,
    /// How long the TCP connect and websocket upgrade may take.
    pub connect_timeout: Duration,
}

impl Default for DialerConfig {
    fn default() -> Self {
        // sendspin-go's backoff, 1 s doubling to 30 s.
        Self {
            base_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            stable_after: Duration::from_secs(10),
            rejected_retry: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(10),
        }
    }
}

/// What a target's task does after a dial or a connection ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Dial again now: the player restarted or dropped without a goodbye.
    Redial,
    /// Dial again after the backoff: it wasn't reachable, refused the
    /// hello, shut down, or dropped too soon after connecting.
    Backoff,
    /// Dial again after [`DialerConfig::rejected_retry`]: it chose another
    /// server, or another connection of ours took its place. Dialing it
    /// again is harmless, since a discovery connection never displaces
    /// a busier one.
    Rejected,
    /// Stop until the player announces itself again. The user asked it to
    /// leave us.
    Park,
}

/// The step after a dial: `None` when the dial itself failed. `stable`
/// says whether the connection lasted [`DialerConfig::stable_after`].
pub fn next_step(outcome: Option<&Disconnect>, stable: bool) -> Step {
    let Some(disconnect) = outcome else {
        return Step::Backoff;
    };
    match disconnect {
        Disconnect::Refused(_) => Step::Backoff,
        Disconnect::Ended { replaced: true, .. } => Step::Rejected,
        Disconnect::Ended { goodbye, .. } => match goodbye {
            None | Some(GoodbyeReason::Restart) if stable => Step::Redial,
            None | Some(GoodbyeReason::Restart) => Step::Backoff,
            Some(GoodbyeReason::Shutdown) => Step::Backoff,
            Some(GoodbyeReason::UserRequest) => Step::Park,
            // `another_server`, and `concurrent_attempt` from clients newer
            // than our enum.
            Some(GoodbyeReason::AnotherServer | GoodbyeReason::Unknown) => Step::Rejected,
        },
    }
}

/// Reads a configured player address as a URL to dial: `ws://...` as
/// given, otherwise `host[:port][/path]` with the port defaulting to
/// [`DEFAULT_CLIENT_PORT`] and the path to `/sendspin`. An IPv6 address
/// with a port is written `[addr]:port`.
pub fn manual_url(target: &str) -> Result<String, String> {
    let target = target.trim();
    if target.starts_with("ws://") {
        return Ok(target.to_owned());
    }
    if target.contains("://") {
        return Err(format!("{target:?}: only ws:// is supported"));
    }
    let (authority, path) = match target.find('/') {
        Some(i) => (&target[..i], &target[i..]),
        None => (target, SENDSPIN_PATH),
    };
    if authority.is_empty() {
        return Err(format!("{target:?}: no host"));
    }
    let authority = if authority.parse::<std::net::Ipv6Addr>().is_ok() {
        format!("[{authority}]:{DEFAULT_CLIENT_PORT}")
    } else if authority.starts_with('[') {
        if authority.ends_with(']') {
            format!("{authority}:{DEFAULT_CLIENT_PORT}")
        } else {
            authority.to_owned()
        }
    } else if let Some((host, port)) = authority.rsplit_once(':') {
        port.parse::<u16>()
            .map_err(|_| format!("{target:?}: bad port {port:?}"))?;
        if host.is_empty() {
            return Err(format!("{target:?}: no host"));
        }
        authority.to_owned()
    } else {
        format!("{authority}:{DEFAULT_CLIENT_PORT}")
    };
    Ok(format!("ws://{authority}{path}"))
}

/// One target: its task, and the URL it dials, which is None once the
/// advert is gone.
struct Slot {
    url: watch::Sender<Option<String>>,
    task: JoinHandle<()>,
}

/// Dials Sendspin players and keeps them connected. Share it as
/// `Arc<Dialer>`; dropping it stops every target's task.
pub struct Dialer {
    server: Arc<SendspinServer>,
    config: DialerConfig,
    slots: Mutex<HashMap<String, Slot>>,
}

impl std::fmt::Debug for Dialer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dialer")
            .field("config", &self.config)
            .field("targets", &self.lock().keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl Dialer {
    pub fn new(server: Arc<SendspinServer>, config: DialerConfig) -> Arc<Self> {
        Arc::new(Self {
            server,
            config,
            slots: Mutex::new(HashMap::new()),
        })
    }

    /// A player's advert resolved, or resolved again. Starts dialing it,
    /// unless it is already being dialed (then the next dial uses the new
    /// address) or it is parked and hasn't been removed since.
    pub fn resolved(&self, client: &SendspinClient) {
        self.upsert(&client.fullname, client.url());
    }

    /// A player's advert is gone. Its task stops retrying; a live
    /// connection is left to end on its own.
    pub fn removed(&self, fullname: &str) {
        if let Some(slot) = self.lock().get(fullname) {
            slot.url.send_replace(None);
        }
    }

    /// Dial a configured player for as long as the server runs. `url` is
    /// what [`manual_url`] gives.
    pub fn add_manual(&self, url: &str) {
        self.upsert(url, url.to_owned());
    }

    /// Whether a target's task is still running: dialing, connected or
    /// waiting to retry, rather than parked or stopped after a removal.
    pub fn is_active(&self, key: &str) -> bool {
        self.lock().get(key).is_some_and(|s| !s.task.is_finished())
    }

    fn upsert(&self, key: &str, url: String) {
        let mut slots = self.lock();
        if let Some(slot) = slots.get(key) {
            // A parked task only comes back once the advert has gone away
            // and returned; a re-resolve alone just records the address.
            let removed = slot.url.borrow().is_none();
            if !slot.task.is_finished() || !removed {
                slot.url.send_replace(Some(url));
                return;
            }
        }
        tracing::debug!(key, url, "dialing sendspin player");
        let (tx, rx) = watch::channel(Some(url));
        let task = tokio::spawn(run_target(
            self.server.clone(),
            self.config.clone(),
            key.to_owned(),
            rx,
        ));
        if let Some(old) = slots.insert(key.to_owned(), Slot { url: tx, task }) {
            old.task.abort();
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Slot>> {
        self.slots.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for Dialer {
    fn drop(&mut self) {
        for slot in self.lock().values() {
            slot.task.abort();
        }
    }
}

/// One target's life: dial, serve the connection, decide, repeat, until
/// the target is removed or parked.
async fn run_target(
    server: Arc<SendspinServer>,
    config: DialerConfig,
    key: String,
    mut url: watch::Receiver<Option<String>>,
) {
    let mut failures: u32 = 0;
    // Learned from the first hello; lets a redial wait out a connection
    // the client made some other way.
    let mut client_id: Option<String> = None;
    loop {
        if let Some(id) = &client_id
            && let Some(conn) = server.connection(id)
        {
            tracing::debug!(key, client_id = id, "already connected; waiting");
            let mut closed = conn.closed();
            tokio::select! {
                _ = closed.wait_for(|c| *c) => {}
                _ = url.wait_for(Option::is_none) => {}
            }
            // Give a client that reconnects on its own the first chance.
            if !wait(&mut url, config.base_backoff).await {
                return;
            }
            continue;
        }
        let Some(target) = url.borrow_and_update().clone() else {
            return;
        };

        let started = Instant::now();
        let outcome = match dial(&target, config.connect_timeout).await {
            Ok((stream, sink)) => {
                tracing::info!(key, url = target, "dialed sendspin player");
                Some(
                    server
                        .run_connection(stream, sink, ConnectionReason::Discovery)
                        .await,
                )
            }
            Err(e) => {
                tracing::debug!(key, url = target, error = %e, "dial failed");
                None
            }
        };
        if let Some(Disconnect::Ended { client_id: id, .. }) = &outcome {
            client_id = Some(id.clone());
        }
        let stable = started.elapsed() >= config.stable_after;
        let step = next_step(outcome.as_ref(), stable);
        let delay = match step {
            Step::Redial => {
                failures = 0;
                Duration::ZERO
            }
            Step::Backoff => {
                if stable {
                    failures = 0;
                }
                failures = failures.saturating_add(1);
                backoff(&config, failures)
            }
            Step::Rejected => {
                failures = 0;
                config.rejected_retry
            }
            Step::Park => {
                tracing::info!(key, "parked until the player announces itself again");
                return;
            }
        };
        if !delay.is_zero() {
            tracing::debug!(key, ?step, ?delay, "redialing later");
        }
        if !wait(&mut url, delay).await {
            return;
        }
    }
}

/// The wait before retry number `failures` (1 for the first).
fn backoff(config: &DialerConfig, failures: u32) -> Duration {
    let doublings = failures.saturating_sub(1).min(16);
    config
        .base_backoff
        .saturating_mul(1 << doublings)
        .min(config.max_backoff)
}

/// Sleeps for `delay`. False if the target was removed meanwhile.
async fn wait(url: &mut watch::Receiver<Option<String>>, delay: Duration) -> bool {
    let removed = tokio::select! {
        _ = sleep(delay) => false,
        _ = url.wait_for(Option::is_none) => true,
    };
    !removed && url.borrow().is_some()
}

type DialedStream = futures_util::stream::BoxStream<'static, Result<Frame, String>>;
type DialedSink = std::pin::Pin<Box<dyn futures_util::Sink<Frame, Error = String> + Send>>;

/// Opens a websocket to a player, adapted to [`Frame`]s.
async fn dial(url: &str, connect_timeout: Duration) -> Result<(DialedStream, DialedSink), String> {
    let config = WebSocketConfig::default().max_message_size(Some(MAX_MESSAGE));
    let connect = tokio_tungstenite::connect_async_with_config(url, Some(config), false);
    let (socket, _) = timeout(connect_timeout, connect)
        .await
        .map_err(|_| format!("no answer within {connect_timeout:?}"))?
        .map_err(|e| e.to_string())?;
    let (sink, stream) = socket.split();
    let stream = stream
        .filter_map(|m| {
            future::ready(match m {
                Ok(m) => frame_from_tungstenite(m).map(Ok),
                Err(e) => Some(Err(e.to_string())),
            })
        })
        .boxed();
    let sink = sink
        .sink_map_err(|e| e.to_string())
        .with(|f: Frame| future::ready(Ok::<_, String>(frame_to_tungstenite(f))));
    Ok((stream, Box::pin(sink)))
}

fn frame_from_tungstenite(msg: Message) -> Option<Frame> {
    Some(match msg {
        Message::Text(text) => Frame::Text(text.as_str().to_owned()),
        Message::Binary(data) => Frame::Binary(data),
        Message::Ping(data) => Frame::Ping(data),
        Message::Pong(data) => Frame::Pong(data),
        Message::Close(_) => Frame::Close,
        // Only seen when writing raw frames, which we don't.
        Message::Frame(_) => return None,
    })
}

fn frame_to_tungstenite(frame: Frame) -> Message {
    match frame {
        Frame::Text(text) => Message::text(text),
        Frame::Binary(data) => Message::Binary(data),
        Frame::Ping(data) => Message::Ping(data),
        Frame::Pong(data) => Message::Pong(data),
        Frame::Close => Message::Close(None),
    }
}

/// Browses `_sendspin._tcp` and feeds what it finds to `dialer`, for as
/// long as it runs.
///
/// The browse restarts every `rebrowse`. mDNS on a busy LAN can miss a
/// whole query round: with three players advertising, one browse found
/// all of them in 300 ms and the next found none for 25 s. A restart
/// queries again, and replays what the cache already holds, which the
/// dialer ignores for targets it has.
pub async fn discover(dialer: Arc<Dialer>, mdns: Mdns, rebrowse: Duration) {
    loop {
        match mdns.browse(SENDSPIN_CLIENT_TYPE) {
            Ok(mut browser) => {
                let deadline = sleep(rebrowse);
                tokio::pin!(deadline);
                loop {
                    let event = tokio::select! {
                        _ = &mut deadline => break,
                        event = browser.next() => event,
                    };
                    match event {
                        Some(BrowseEvent::Resolved(found)) => match found.sendspin_client() {
                            Some(client) => dialer.resolved(&client),
                            None => tracing::debug!(
                                name = found.fullname,
                                "sendspin player has no usable address"
                            ),
                        },
                        Some(BrowseEvent::Removed { fullname }) => dialer.removed(&fullname),
                        None => {
                            tracing::warn!("sendspin browse stopped; restarting");
                            break;
                        }
                    }
                }
                // Dropped before the next browse: stopping a browse stops
                // every browse of its type.
                drop(browser);
            }
            Err(e) => tracing::warn!(error = %e, "can't browse for sendspin players"),
        }
        // Paces a browse that keeps failing or stopping at once.
        sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::HandshakeError;

    fn ended(goodbye: Option<GoodbyeReason>, replaced: bool) -> Disconnect {
        Disconnect::Ended {
            client_id: "c".into(),
            goodbye,
            replaced,
        }
    }

    #[test]
    fn steps_after_each_outcome() {
        use GoodbyeReason::*;
        assert_eq!(next_step(None, false), Step::Backoff);
        let refused = Disconnect::Refused(HandshakeError::NotHello);
        assert_eq!(next_step(Some(&refused), false), Step::Backoff);
        assert_eq!(next_step(Some(&ended(None, false)), true), Step::Redial);
        assert_eq!(next_step(Some(&ended(None, false)), false), Step::Backoff);
        assert_eq!(
            next_step(Some(&ended(Some(Restart), false)), true),
            Step::Redial
        );
        assert_eq!(
            next_step(Some(&ended(Some(Shutdown), false)), true),
            Step::Backoff
        );
        assert_eq!(
            next_step(Some(&ended(Some(AnotherServer), false)), true),
            Step::Rejected
        );
        assert_eq!(
            next_step(Some(&ended(Some(Unknown), false)), true),
            Step::Rejected
        );
        assert_eq!(next_step(Some(&ended(None, true)), true), Step::Rejected);
        assert_eq!(
            next_step(Some(&ended(Some(UserRequest), false)), true),
            Step::Park
        );
    }

    #[test]
    fn backoff_doubles_to_the_cap() {
        let c = DialerConfig::default();
        let secs: Vec<u64> = (1..=7).map(|n| backoff(&c, n).as_secs()).collect();
        assert_eq!(secs, [1, 2, 4, 8, 16, 30, 30]);
        assert_eq!(backoff(&c, u32::MAX), c.max_backoff);
    }

    #[test]
    fn manual_urls() {
        let ok = |s| manual_url(s).unwrap();
        assert_eq!(ok("192.168.1.20"), "ws://192.168.1.20:8928/sendspin");
        assert_eq!(ok(" 192.168.1.20:9000 "), "ws://192.168.1.20:9000/sendspin");
        assert_eq!(ok("kitchen.local/ws"), "ws://kitchen.local:8928/ws");
        assert_eq!(ok("kitchen:1/x/y"), "ws://kitchen:1/x/y");
        assert_eq!(ok("fe80::1"), "ws://[fe80::1]:8928/sendspin");
        assert_eq!(ok("[2001:db8::1]"), "ws://[2001:db8::1]:8928/sendspin");
        assert_eq!(ok("[2001:db8::1]:7/p"), "ws://[2001:db8::1]:7/p");
        assert_eq!(ok("ws://h:1/p"), "ws://h:1/p");
        assert!(manual_url("wss://h/p").is_err());
        assert!(manual_url("h:port").is_err());
        assert!(manual_url(":8928").is_err());
        assert!(manual_url("/sendspin").is_err());
    }
}
