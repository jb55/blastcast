//! The server's side of an encrypted connection's setup, from
//! `client/init` to the `server/activate` that lets it play
//! (`messaging.md`, Communication; `pairing.md`).
//!
//! Setup runs as one sequential task that owns the socket, so the
//! re-handshake after a pairing needs no coordination between a reader
//! and a writer: nothing else is reading or writing yet. Only when the
//! client may play does the link split into the encrypted stream and sink
//! the ordinary read and write loops run on.
//!
//! What the server does after the hello depends on the PSK that matched
//! and on the operator:
//!
//! - a pairing the operator asked for ([`Trust::pairing_request`]) is
//!   run first, on whatever PSK the session has, and a successful one
//!   re-handshakes to the new long-term PSK and starts again from the
//!   hello;
//! - a long-term PSK plays;
//! - the Sentinel plays when the client offers unpaired access and the
//!   operator approved it, or `auto_approve_unpaired` approves it now, as
//!   Music Assistant does for guest access;
//! - anything else is held at empty activities, answering `client/time`,
//!   until the operator asks for a pairing or the client leaves.

use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{Sink, Stream};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::time::{Instant, MissedTickBehavior};

use super::cpace::{Cpace, Role as PakeRole};
use super::keys::{
    Identity, Wrapped, b64url, b64url_array, commit, digits_code, pake_sid, psk_id, random32,
    sentinel_psk, unwrap,
};
use super::link::{Link, LinkError, Message};
use super::noise::{Handshake, Suite};
use super::trust::{PairMethod, PairingRequest};
use crate::connection::Frame;
use crate::protocol::{ClientHello, ConnectionReason, GoodbyeReason, ServerTime};
use crate::server::{HandshakeError, SendspinServer};

/// How long a client has to send `client/pair-init`, which waits for a
/// gesture on the device when the attempt is gesture-gated: the spec's
/// recommended pairing window.
const PAIR_INIT_WAIT: Duration = Duration::from_secs(5 * 60);
/// How long the operator has to enter a dynamic code: a little over the
/// client's own two-minute attempt timeout, so its `pair/abort` comes
/// first.
const CODE_WAIT: Duration = Duration::from_secs(150);
/// How often a held connection looks for a pairing request.
const REQUEST_POLL: Duration = Duration::from_millis(500);

/// Which PSK a handshake used, as the spec's `psk_category` names them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PskKind {
    LongTerm,
    Pairing,
    Sentinel,
}

impl PskKind {
    fn code(self) -> &'static str {
        match self {
            PskKind::LongTerm => "lt",
            PskKind::Pairing => "pr",
            PskKind::Sentinel => "sn",
        }
    }
}

/// How an encrypted client got in, for `/debug`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Security {
    pub suite: &'static str,
    pub psk: PskKind,
}

/// How setup ended.
// One per connection, moved once: not worth boxing the link.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Outcome<St, Si> {
    /// The client may play: the hello, the roles activated, and the link
    /// to split.
    Ready {
        link: Link<St, Si>,
        hello: ClientHello,
        active_roles: Vec<String>,
        security: Security,
    },
    /// The connection ended after `client/init` was accepted.
    Ended {
        client_id: String,
        goodbye: Option<GoodbyeReason>,
        error: Option<String>,
    },
    Refused(HandshakeError),
}

#[derive(Deserialize)]
struct ClientInit {
    client_id: String,
    version: u32,
    suite: String,
}

#[derive(Deserialize)]
struct NoiseMessage {
    data: String,
}

#[derive(Deserialize)]
struct Goodbye {
    #[serde(default)]
    reason: Option<GoodbyeReason>,
}

#[derive(Deserialize)]
struct PairingIndex {
    pairing_index: u32,
    #[serde(default, rename = "commit_B")]
    commit_b: Option<String>,
}

#[derive(Deserialize)]
struct PairAuth {
    pake_msg_2: String,
}

#[derive(Deserialize)]
struct PairConfirm {
    client_kc: String,
    #[serde(default, rename = "wrapped_nonce_B")]
    wrapped_nonce_b: Option<String>,
}

#[derive(Deserialize)]
struct PairFinalize {
    #[serde(default)]
    long_term_psk: Option<String>,
    #[serde(default)]
    wrapped_psk: Option<String>,
}

#[derive(Deserialize)]
struct PairAbort {
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Deserialize)]
struct TimeRequest {
    client_transmitted: i64,
}

/// One connection's setup state.
struct Session {
    client_id: String,
    client_key: [u8; 32],
    suite: Suite,
    identity: Identity,
    psk: PskKind,
    /// Message 2 only verified under the Sentinel though message 1 named
    /// the client's long-term PSK: it lost its pairing. It can't play
    /// until it is paired again.
    mismatch: bool,
    /// Pairing activations since the last handshake: the CPace `sid`
    /// counter and the `pairing_index` the client must echo.
    activations: u32,
    name: String,
}

/// Why a pairing attempt didn't pair.
enum PairFail {
    /// It ended within the protocol: an abort, a wrong code, a timeout.
    /// The connection goes on.
    Declined(String),
    /// The connection is over.
    Link(LinkError),
    /// The client left.
    Goodbye(Option<GoodbyeReason>),
}

impl From<LinkError> for PairFail {
    fn from(e: LinkError) -> Self {
        PairFail::Link(e)
    }
}

/// What the decision after a hello came to.
enum Next {
    Play {
        activities: Vec<&'static str>,
        roles: Vec<String>,
    },
    /// Paired and re-handshaken: the hellos again.
    Rehello,
    Ended {
        goodbye: Option<GoodbyeReason>,
        error: Option<String>,
    },
}

impl SendspinServer {
    /// Runs setup on a connection whose first message, `init`, is
    /// `client/init`.
    pub(crate) async fn secure_setup<St, Si, E>(
        self: &Arc<Self>,
        mut link: Link<St, Si>,
        init: String,
        reason: ConnectionReason,
    ) -> Outcome<St, Si>
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin,
        Si::Error: Display,
    {
        let config = self.config();
        let parsed = Message::parse(init.clone())
            .and_then(|m| m.payload::<ClientInit>())
            .map_err(|e| HandshakeError::Init(e.to_string()));
        let checked = parsed.and_then(|init| {
            if init.version != 1 {
                return Err(HandshakeError::Init(format!("version {}", init.version)));
            }
            let suite = Suite::from_name(&init.suite)
                .ok_or_else(|| HandshakeError::Init(format!("unknown suite {}", init.suite)))?;
            let key = b64url_array::<32>(&init.client_id)
                .filter(|_| init.client_id.len() == 43)
                .ok_or_else(|| HandshakeError::Init("client_id is not a public key".into()))?;
            Ok((init.client_id, key, suite))
        });
        let (client_id, client_key, suite) = match checked {
            Ok(v) => v,
            Err(e) => {
                link.close().await;
                return Outcome::Refused(e);
            }
        };
        let trust = config.trust.clone();
        let identity = trust.identity();
        let mut s = Session {
            name: client_id.clone(),
            client_id,
            client_key,
            suite,
            identity,
            psk: PskKind::Sentinel,
            mismatch: false,
            activations: 0,
        };
        if let Err(e) = self.handshake(&mut link, &mut s, &init).await {
            tracing::warn!(client_id = s.client_id, error = %e, "noise handshake failed");
            link.close().await;
            return Outcome::Refused(HandshakeError::Noise(e.to_string()));
        }
        tracing::debug!(client_id = s.client_id, psk = ?s.psk, mismatch = s.mismatch, "noise handshake done");

        let ended = |goodbye, error: Option<String>| Outcome::Ended {
            client_id: String::new(),
            goodbye,
            error,
        };
        loop {
            let hello = match self.hellos(&mut link, &s).await {
                Ok(Ok(hello)) => hello,
                Ok(Err(goodbye)) => {
                    let mut out = ended(goodbye, None);
                    set_client(&mut out, &s.client_id);
                    link.close().await;
                    return out;
                }
                Err(e) => {
                    link.close().await;
                    let mut out = ended(None, Some(e.to_string()));
                    set_client(&mut out, &s.client_id);
                    return out;
                }
            };
            s.name.clone_from(&hello.name);
            match self.decide(&mut link, &mut s, &hello, reason).await {
                Next::Play { activities, roles } => {
                    let sent = link
                        .send(
                            "server/activate",
                            json!({ "activities": activities, "active_roles": roles }),
                        )
                        .await;
                    self.unhold(&s.client_id);
                    if let Err(e) = sent {
                        let mut out = ended(None, Some(e.to_string()));
                        set_client(&mut out, &s.client_id);
                        return out;
                    }
                    return Outcome::Ready {
                        link,
                        hello,
                        active_roles: roles,
                        security: Security {
                            suite: s.suite.name(),
                            psk: s.psk,
                        },
                    };
                }
                Next::Rehello => continue,
                Next::Ended { goodbye, error } => {
                    self.unhold(&s.client_id);
                    if let Some(e) = &error {
                        tracing::info!(client_id = s.client_id, error = e, "encrypted setup ended");
                    }
                    link.close().await;
                    let mut out = ended(goodbye, error);
                    set_client(&mut out, &s.client_id);
                    return out;
                }
            }
        }
    }

    /// `server/init`, then the two handshake messages, in cleartext.
    async fn handshake<St, Si, E>(
        &self,
        link: &mut Link<St, Si>,
        s: &mut Session,
        init: &str,
    ) -> Result<(), LinkError>
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin,
        Si::Error: Display,
    {
        let config = self.config();
        let server_init = link
            .send(
                "server/init",
                json!({ "server_id": s.identity.id(), "version": 1 }),
            )
            .await?;
        let prologue = [init.as_bytes(), server_init.as_bytes()].concat();

        let trust = &config.trust;
        let (psk, kind) = match trust.long_term_psk(&s.client_id) {
            Some(psk) => (psk, PskKind::LongTerm),
            None => match trust.pairing_request(&s.client_id) {
                Some(PairingRequest::PairingPsk { pairing_psk }) => (pairing_psk, PskKind::Pairing),
                _ => (sentinel_psk(), PskKind::Sentinel),
            },
        };
        let mut hs = Handshake::initiator(
            s.suite,
            &prologue,
            s.identity.secret(),
            s.client_key,
            Identity::generate().secret(),
            psk,
        );
        let msg1 = hs
            .write(handshake_payload(&psk, kind).as_bytes())
            .map_err(LinkError::protocol)?;
        link.send("noise/handshake", json!({ "data": b64url(&msg1) }))
            .await?;
        let reply = link.recv(config.hello_timeout).await?;
        let msg2 = noise_data(&reply)?;

        let mut fallback = hs.clone();
        let payload = match hs.read(&msg2) {
            Ok(payload) => {
                s.psk = kind;
                link.secure(hs.finish().map_err(LinkError::protocol)?);
                payload
            }
            Err(_) if kind != PskKind::Sentinel => {
                fallback.set_psk(sentinel_psk());
                let payload = fallback.read(&msg2).map_err(LinkError::protocol)?;
                link.secure(fallback.finish().map_err(LinkError::protocol)?);
                s.psk = PskKind::Sentinel;
                if kind == PskKind::LongTerm {
                    s.mismatch = true;
                    tracing::warn!(
                        client_id = s.client_id,
                        "the client no longer has our pairing; it needs pairing again"
                    );
                } else {
                    tracing::warn!(
                        client_id = s.client_id,
                        "the client doesn't know the pairing PSK in its token; dropping the request"
                    );
                    self.end_request(&s.client_id, "the client didn't recognize the token");
                }
                payload
            }
            Err(e) => return Err(LinkError::protocol(e)),
        };
        check_empty_object(&payload)
    }

    /// Rekeys the encrypted link to `psk` without closing it.
    async fn rehandshake<St, Si, E>(
        &self,
        link: &mut Link<St, Si>,
        s: &mut Session,
        psk: [u8; 32],
        kind: PskKind,
    ) -> Result<(), LinkError>
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin,
        Si::Error: Display,
    {
        let mut hs = Handshake::initiator(
            s.suite,
            &link.handshake_hash(),
            s.identity.secret(),
            s.client_key,
            Identity::generate().secret(),
            psk,
        );
        let msg1 = hs
            .write(handshake_payload(&psk, kind).as_bytes())
            .map_err(LinkError::protocol)?;
        link.send("noise/handshake", json!({ "data": b64url(&msg1) }))
            .await?;
        let reply = link.recv(self.config().hello_timeout).await?;
        let payload = hs.read(&noise_data(&reply)?).map_err(LinkError::protocol)?;
        check_empty_object(&payload)?;
        link.secure(hs.finish().map_err(LinkError::protocol)?);
        s.psk = kind;
        s.mismatch = false;
        s.activations = 0;
        tracing::info!(client_id = s.client_id, psk = ?kind, "re-handshake done");
        Ok(())
    }

    /// `server/hello`, then the client's hello or goodbye.
    async fn hellos<St, Si, E>(
        &self,
        link: &mut Link<St, Si>,
        s: &Session,
    ) -> Result<Result<ClientHello, Option<GoodbyeReason>>, LinkError>
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin,
        Si::Error: Display,
    {
        let config = self.config();
        link.send("server/hello", json!({ "name": config.name }))
            .await?;
        let msg = link.recv(config.hello_timeout).await?;
        match msg.kind.as_str() {
            "client/hello" => {
                let mut hello: ClientHello = msg.payload()?;
                hello.client_id.clone_from(&s.client_id);
                crate::server::check_hello(hello)
                    .map(Ok)
                    .map_err(LinkError::protocol)
            }
            "client/goodbye" => {
                let goodbye: Goodbye = msg.payload()?;
                tracing::info!(client_id = s.client_id, reason = ?goodbye.reason, "goodbye instead of hello");
                Ok(Err(goodbye.reason))
            }
            other => Err(LinkError::protocol(format!(
                "{other} instead of client/hello"
            ))),
        }
    }

    async fn decide<St, Si, E>(
        &self,
        link: &mut Link<St, Si>,
        s: &mut Session,
        hello: &ClientHello,
        reason: ConnectionReason,
    ) -> Next
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin,
        Si::Error: Display,
    {
        let trust = self.config().trust.clone();
        let mut held = false;
        loop {
            if let Some(request) = trust.pairing_request(&s.client_id) {
                self.hold(&s.client_id, &s.name, "pairing");
                match self.pair(link, s, hello, request).await {
                    Ok(()) => {
                        let psk = trust.long_term_psk(&s.client_id);
                        let Some(psk) = psk else {
                            return Next::Ended {
                                goodbye: None,
                                error: Some("the new pairing wasn't kept".into()),
                            };
                        };
                        return match self.rehandshake(link, s, psk, PskKind::LongTerm).await {
                            Ok(()) => Next::Rehello,
                            Err(e) => Next::Ended {
                                goodbye: None,
                                error: Some(format!("re-handshake after pairing: {e}")),
                            },
                        };
                    }
                    Err(PairFail::Declined(why)) => {
                        tracing::warn!(client_id = s.client_id, why, "pairing didn't complete");
                        self.end_request(&s.client_id, &format!("failed: {why}"));
                        if s.psk == PskKind::Pairing {
                            // The pairing PSK allows only pairing activities; the
                            // client reconnects on another PSK.
                            return Next::Ended {
                                goodbye: None,
                                error: Some(format!("pairing on the pairing PSK failed: {why}")),
                            };
                        }
                        held = false;
                    }
                    Err(PairFail::Goodbye(goodbye)) => {
                        self.end_request(&s.client_id, "the client left");
                        return Next::Ended {
                            goodbye,
                            error: None,
                        };
                    }
                    Err(PairFail::Link(e)) => {
                        self.end_request(&s.client_id, &format!("failed: {e}"));
                        return Next::Ended {
                            goodbye: None,
                            error: Some(format!("pairing: {e}")),
                        };
                    }
                }
            }

            if let Some(roles) = self.may_play(s, hello) {
                let activities = match reason {
                    // Dialed: we hold it but aren't playing, so another
                    // server's playback keeps it, as with `discovery`.
                    ConnectionReason::Discovery => vec![],
                    _ => vec!["playback"],
                };
                return Next::Play { activities, roles };
            }

            if !held {
                let why = if s.mismatch {
                    "lost its pairing; pair it again"
                } else if hello.unpaired_access.is_some_and(|u| u.enabled) {
                    "waiting for approval or pairing"
                } else {
                    "needs pairing"
                };
                tracing::info!(
                    client_id = s.client_id,
                    name = s.name,
                    why,
                    "holding an encrypted client; `tas sendspin pair` pairs it"
                );
                self.hold(&s.client_id, &s.name, why);
                let sent = link
                    .send(
                        "server/activate",
                        json!({ "activities": [], "active_roles": [] }),
                    )
                    .await;
                if let Err(e) = sent {
                    return Next::Ended {
                        goodbye: None,
                        error: Some(e.to_string()),
                    };
                }
                held = true;
            }
            let unpaired = hello.unpaired_access.is_some_and(|u| u.enabled);
            match self.wait_for_request(link, s, unpaired).await {
                Ok(None) => continue,
                Ok(Some(goodbye)) => {
                    return Next::Ended {
                        goodbye,
                        error: None,
                    };
                }
                Err(e) => {
                    return Next::Ended {
                        goodbye: None,
                        error: Some(e.to_string()),
                    };
                }
            }
        }
    }

    /// The roles a client may play with now, if it may.
    fn may_play(&self, s: &Session, hello: &ClientHello) -> Option<Vec<String>> {
        let config = self.config();
        let roles = self.activate_roles(&hello.supported_roles);
        match s.psk {
            PskKind::LongTerm => Some(roles),
            PskKind::Pairing => None,
            PskKind::Sentinel if s.mismatch => None,
            PskKind::Sentinel => {
                if !hello.unpaired_access.is_some_and(|u| u.enabled) {
                    return None;
                }
                if config.trust.is_approved(&s.client_id) {
                    return Some(roles);
                }
                if !config.auto_approve_unpaired {
                    return None;
                }
                tracing::info!(
                    client_id = s.client_id,
                    name = s.name,
                    "approving unpaired access"
                );
                if let Err(e) = config.trust.approve(&s.client_id) {
                    tracing::warn!(client_id = s.client_id, error = %e, "can't keep the approval");
                }
                Some(roles)
            }
        }
    }

    /// Idles at empty activities until the operator asks for a pairing or
    /// approves a client that offers `unpaired` access (`None`), or the
    /// client leaves (`Some`, its goodbye). Answers `client/time`
    /// meanwhile, and pings.
    async fn wait_for_request<St, Si, E>(
        &self,
        link: &mut Link<St, Si>,
        s: &Session,
        unpaired: bool,
    ) -> Result<Option<Option<GoodbyeReason>>, LinkError>
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin,
        Si::Error: Display,
    {
        let config = self.config();
        let approvable = unpaired && s.psk == PskKind::Sentinel && !s.mismatch;
        let mut poll = tokio::time::interval(REQUEST_POLL);
        poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut ping =
            tokio::time::interval_at(Instant::now() + config.ping_interval, config.ping_interval);
        loop {
            tokio::select! {
                msg = link.recv_any() => {
                    if let Some(goodbye) = self.idle_message(link, &msg?).await? {
                        return Ok(Some(goodbye));
                    }
                }
                _ = poll.tick() => {
                    if config.trust.pairing_request(&s.client_id).is_some()
                        || (approvable && config.trust.is_approved(&s.client_id))
                    {
                        return Ok(None);
                    }
                }
                _ = ping.tick() => {
                    link.send_frame(Frame::Ping(Default::default())).await?;
                }
            }
        }
    }

    /// A message that arrived while nothing was expected: time is answered,
    /// a goodbye ends things (`Some`), anything else (a pairing message
    /// left over from an attempt that ended) is dropped.
    async fn idle_message<St, Si, E>(
        &self,
        link: &mut Link<St, Si>,
        msg: &Message,
    ) -> Result<Option<Option<GoodbyeReason>>, LinkError>
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin,
        Si::Error: Display,
    {
        let clock = self.config().clock;
        match msg.kind.as_str() {
            "client/time" => {
                let received = clock.now_us();
                let t: TimeRequest = msg.payload()?;
                let reply = ServerTime {
                    client_transmitted: t.client_transmitted,
                    server_received: received,
                    server_transmitted: clock.now_us(),
                };
                link.send("server/time", reply).await?;
                Ok(None)
            }
            "client/goodbye" => Ok(Some(msg.payload::<Goodbye>()?.reason)),
            kind => {
                tracing::debug!(kind, "dropping a message during setup");
                Ok(None)
            }
        }
    }

    async fn pair<St, Si, E>(
        &self,
        link: &mut Link<St, Si>,
        s: &mut Session,
        hello: &ClientHello,
        request: PairingRequest,
    ) -> Result<(), PairFail>
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin,
        Si::Error: Display,
    {
        let method = request.method();
        let offered = hello.supported_pair_methods.as_ref();
        let offers = match method {
            PairMethod::PairingPsk => offered.is_some_and(|m| m.pairing_psk.is_some()),
            PairMethod::StaticCode => offered.is_some_and(|m| m.static_pairing_code.is_some()),
            PairMethod::DynamicCode => offered
                .and_then(|m| m.dynamic_pairing_code.as_ref())
                .is_some_and(|d| {
                    // Speaking the code needs a digit audio pack, which
                    // this server can't supply; it has to be shown.
                    d.out_channels.iter().any(|c| c == "display")
                        && d.formats.iter().any(|f| f == "digits")
                }),
        };
        if !offers {
            return Err(PairFail::Declined(format!(
                "the client doesn't offer {}{}",
                method.name(),
                if method == PairMethod::DynamicCode {
                    " on a display, in digits"
                } else {
                    ""
                }
            )));
        }
        tracing::info!(client_id = s.client_id, method = method.name(), "pairing");
        match request {
            PairingRequest::PairingPsk { pairing_psk } => {
                if s.psk != PskKind::Pairing {
                    // One try: a client that doesn't know the PSK fails
                    // the re-handshake and the connection with it.
                    self.end_request(&s.client_id, "re-handshaking to the pairing PSK");
                    self.rehandshake(link, s, pairing_psk, PskKind::Pairing)
                        .await?;
                }
                self.activate_pairing(link, s, method).await?;
                let finalize: PairFinalize = match self.pairing_reply(link, PAIR_INIT_WAIT).await? {
                    Reply::Message(m) if m.kind == "client/pair-finalize" => m.payload()?,
                    other => return Err(other.unexpected("client/pair-finalize")),
                };
                let psk = finalize
                    .long_term_psk
                    .as_deref()
                    .and_then(b64url_array::<32>)
                    .ok_or_else(|| LinkError::protocol("pair-finalize without a long_term_psk"))?;
                self.finalize(link, s, psk).await
            }
            PairingRequest::StaticCode { code } => {
                self.activate_pairing(link, s, method).await?;
                self.pair_init(link, s, false).await?;
                self.pake(link, s, code.as_bytes(), None).await
            }
            PairingRequest::DynamicCode { .. } => {
                self.activate_pairing(link, s, method).await?;
                let commit_b = self.pair_init(link, s, true).await?;
                let nonce_a = random32();
                link.send("server/pair-init", json!({ "nonce_A": b64url(&nonce_a) }))
                    .await?;
                self.hold(
                    &s.client_id,
                    &s.name,
                    "pairing: enter the code the device shows",
                );
                tracing::info!(
                    client_id = s.client_id,
                    name = s.name,
                    "enter the code the device shows with `tas sendspin code`"
                );
                let code = self.wait_for_code(link, s).await?;
                let dynamic = Dynamic {
                    commit_b: commit_b.expect("asked for"),
                    nonce_a,
                    code: code.clone(),
                };
                self.pake(link, s, code.as_bytes(), Some(dynamic)).await
            }
        }
    }

    async fn activate_pairing<St, Si, E>(
        &self,
        link: &mut Link<St, Si>,
        s: &mut Session,
        method: PairMethod,
    ) -> Result<(), LinkError>
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin,
        Si::Error: Display,
    {
        let mut pairing = json!({ "method": method.name() });
        if method == PairMethod::DynamicCode {
            pairing["format"] = json!("digits");
        }
        s.activations += 1;
        link.send(
            "server/activate",
            json!({ "activities": ["pairing"], "active_roles": [], "pairing": pairing }),
        )
        .await?;
        Ok(())
    }

    /// Waits out `client/pair-pending` for `client/pair-init`, returning
    /// its `commit_B` when `dynamic`.
    async fn pair_init<St, Si, E>(
        &self,
        link: &mut Link<St, Si>,
        s: &Session,
        dynamic: bool,
    ) -> Result<Option<[u8; 32]>, PairFail>
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin,
        Si::Error: Display,
    {
        let deadline = Instant::now() + PAIR_INIT_WAIT;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let msg = match self.pairing_reply(link, left).await? {
                Reply::Message(m) => m,
                other => return Err(other.unexpected("client/pair-init")),
            };
            let pending = match msg.kind.as_str() {
                "client/pair-pending" => true,
                "client/pair-init" => false,
                _ => return Err(Reply::Message(msg).unexpected("client/pair-init")),
            };
            let index: PairingIndex = msg.payload()?;
            if index.pairing_index < s.activations {
                continue; // from a superseded attempt
            }
            if index.pairing_index > s.activations {
                return Err(LinkError::protocol("pairing_index from the future").into());
            }
            if pending {
                tracing::info!(
                    client_id = s.client_id,
                    name = s.name,
                    "press the pairing button on the device"
                );
                self.hold(
                    &s.client_id,
                    &s.name,
                    "pairing: waiting for a gesture on the device",
                );
                continue;
            }
            if !dynamic {
                return Ok(None);
            }
            let commit_b = index
                .commit_b
                .as_deref()
                .and_then(b64url_array::<32>)
                .ok_or_else(|| LinkError::protocol("client/pair-init without commit_B"))?;
            return Ok(Some(commit_b));
        }
    }

    /// Waits for the operator to enter the dynamic code, or for the
    /// client to give up.
    async fn wait_for_code<St, Si, E>(
        &self,
        link: &mut Link<St, Si>,
        s: &Session,
    ) -> Result<String, PairFail>
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin,
        Si::Error: Display,
    {
        let trust = self.config().trust.clone();
        let deadline = tokio::time::sleep(CODE_WAIT);
        tokio::pin!(deadline);
        let mut poll = tokio::time::interval(REQUEST_POLL);
        loop {
            tokio::select! {
                _ = &mut deadline => {
                    return Err(PairFail::Declined("no code entered in time".into()));
                }
                _ = poll.tick() => match trust.pairing_request(&s.client_id) {
                    Some(PairingRequest::DynamicCode { code: Some(code) }) => return Ok(code),
                    Some(PairingRequest::DynamicCode { code: None }) => {}
                    _ => return Err(PairFail::Declined("the request was withdrawn".into())),
                },
                msg = link.recv_any() => {
                    let msg = msg?;
                    match msg.kind.as_str() {
                        "pair/abort" | "client/goodbye" => {
                            return Err(Reply::Message(msg).unexpected("the code"));
                        }
                        _ => {
                            if let Some(goodbye) = self.idle_message(link, &msg).await? {
                                return Err(PairFail::Goodbye(goodbye));
                            }
                        }
                    }
                }
            }
        }
    }

    /// The CPace round and the finalize, for both code methods.
    async fn pake<St, Si, E>(
        &self,
        link: &mut Link<St, Si>,
        s: &mut Session,
        code: &[u8],
        dynamic: Option<Dynamic>,
    ) -> Result<(), PairFail>
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin,
        Si::Error: Display,
    {
        let wait = self.config().hello_timeout;
        let h = link.handshake_hash();
        let sid = pake_sid(&h, s.activations);
        let cpace = Cpace::start(PakeRole::Initiator, code, &sid, b"server", random32())
            .map_err(LinkError::protocol)?;
        link.send(
            "server/pair-auth",
            json!({ "pake_msg_1": b64url(&cpace.share()) }),
        )
        .await?;
        let auth: PairAuth = match self.pairing_reply(link, wait).await? {
            Reply::Message(m) if m.kind == "client/pair-auth" => m.payload()?,
            other => return Err(other.unexpected("client/pair-auth")),
        };
        let yb = b64url_array::<32>(&auth.pake_msg_2)
            .ok_or_else(|| LinkError::protocol("pake_msg_2 is not 32 bytes"))?;
        let derived = cpace.derive(&yb, b"client").map_err(LinkError::protocol)?;
        link.send(
            "server/pair-confirm",
            json!({ "server_kc": b64url(&derived.own_tag) }),
        )
        .await?;
        let confirm: PairConfirm = match self.pairing_reply(link, wait).await? {
            Reply::Message(m) if m.kind == "client/pair-confirm" => m.payload()?,
            other => return Err(other.unexpected("client/pair-confirm")),
        };
        let kc_ok = b64url_array::<64>(&confirm.client_kc).is_some_and(|kc| derived.verify(&kc));
        if !kc_ok {
            link.send("pair/abort", json!({ "reason": "pairing_code_mismatch" }))
                .await?;
            return Err(PairFail::Declined("wrong pairing code".into()));
        }
        if let Some(d) = dynamic {
            let nonce_b = confirm
                .wrapped_nonce_b
                .as_deref()
                .and_then(base64_bytes)
                .and_then(|w| unwrap(s.suite, Wrapped::NonceB, &sid, &derived.isk, &w))
                .ok_or_else(|| LinkError::protocol("wrapped_nonce_B doesn't open"))?;
            if !super::ct_eq(&commit(&nonce_b), &d.commit_b) {
                return Err(LinkError::protocol("nonce_B doesn't match commit_B").into());
            }
            if digits_code(&h, &d.nonce_a, &nonce_b) != d.code {
                return Err(LinkError::protocol("the code isn't bound to this handshake").into());
            }
        }
        let finalize: PairFinalize = match self.pairing_reply(link, wait).await? {
            Reply::Message(m) if m.kind == "client/pair-finalize" => m.payload()?,
            other => return Err(other.unexpected("client/pair-finalize")),
        };
        let psk = finalize
            .wrapped_psk
            .as_deref()
            .and_then(base64_bytes)
            .and_then(|w| unwrap(s.suite, Wrapped::Psk, &sid, &derived.isk, &w))
            .ok_or_else(|| LinkError::protocol("wrapped_psk doesn't open"))?;
        self.finalize(link, s, psk).await
    }

    /// Keeps the pairing, then acknowledges it.
    async fn finalize<St, Si, E>(
        &self,
        link: &mut Link<St, Si>,
        s: &Session,
        psk: [u8; 32],
    ) -> Result<(), PairFail>
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin,
        Si::Error: Display,
    {
        if let Err(e) = self.config().trust.pair(&s.client_id, psk) {
            return Err(PairFail::Declined(format!("can't keep the pairing: {e}")));
        }
        tracing::info!(client_id = s.client_id, name = s.name, "paired");
        self.record_pairing(&s.client_id, "paired");
        link.send("server/pair-finalize", json!({})).await?;
        Ok(())
    }

    /// The next pairing message, a `pair/abort`, or a goodbye. Time is
    /// answered on the way.
    async fn pairing_reply<St, Si, E>(
        &self,
        link: &mut Link<St, Si>,
        wait: Duration,
    ) -> Result<Reply, LinkError>
    where
        St: Stream<Item = Result<Frame, E>> + Unpin,
        E: Display,
        Si: Sink<Frame> + Unpin,
        Si::Error: Display,
    {
        let deadline = Instant::now() + wait;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let msg = match link.recv(left).await {
                Err(LinkError::Timeout(_)) => return Ok(Reply::Timeout),
                other => other?,
            };
            match msg.kind.as_str() {
                "client/time" => {
                    self.idle_message(link, &msg).await?;
                }
                "client/goodbye" => return Ok(Reply::Goodbye(msg.payload::<Goodbye>()?.reason)),
                "pair/abort" => {
                    let abort: PairAbort = msg.payload()?;
                    return Ok(Reply::Abort(abort.reason.unwrap_or_default()));
                }
                _ => return Ok(Reply::Message(msg)),
            }
        }
    }
}

/// What a dynamic attempt checks the code against.
struct Dynamic {
    commit_b: [u8; 32],
    nonce_a: [u8; 32],
    code: String,
}

enum Reply {
    Message(Message),
    Abort(String),
    Goodbye(Option<GoodbyeReason>),
    Timeout,
}

impl Reply {
    fn unexpected(self, wanted: &str) -> PairFail {
        match self {
            Reply::Abort(reason) => PairFail::Declined(format!("the client aborted: {reason}")),
            Reply::Goodbye(goodbye) => PairFail::Goodbye(goodbye),
            Reply::Timeout => PairFail::Declined(format!("no {wanted} in time")),
            Reply::Message(m) if m.kind == "pair/abort" => {
                let reason = m.payload::<PairAbort>().ok().and_then(|a| a.reason);
                PairFail::Declined(format!(
                    "the client aborted: {}",
                    reason.unwrap_or_default()
                ))
            }
            Reply::Message(m) if m.kind == "client/goodbye" => {
                PairFail::Goodbye(m.payload::<Goodbye>().ok().and_then(|g| g.reason))
            }
            // Out of sequence for the method: a protocol error.
            Reply::Message(m) => PairFail::Link(LinkError::protocol(format!(
                "{} instead of {wanted}",
                m.kind
            ))),
        }
    }
}

fn set_client<St, Si>(out: &mut Outcome<St, Si>, id: &str) {
    if let Outcome::Ended { client_id, .. } = out {
        id.clone_into(client_id);
    }
}

/// Message 1's payload: which PSK, and as what.
fn handshake_payload(psk: &[u8; 32], kind: PskKind) -> String {
    json!({ "psk_id": psk_id(psk), "psk_category": kind.code() }).to_string()
}

fn noise_data(msg: &Message) -> Result<Vec<u8>, LinkError> {
    if msg.kind != "noise/handshake" {
        return Err(LinkError::protocol(format!(
            "{} instead of noise/handshake",
            msg.kind
        )));
    }
    let data: NoiseMessage = msg.payload()?;
    base64_bytes(&data.data)
        .ok_or_else(|| LinkError::protocol("noise/handshake data isn't base64url"))
}

fn base64_bytes(text: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(text)
        .ok()
}

/// Message 2's payload must be the two bytes `{}`, or at least an empty
/// object.
fn check_empty_object(payload: &[u8]) -> Result<(), LinkError> {
    match serde_json::from_slice::<serde_json::Value>(payload) {
        Ok(serde_json::Value::Object(_)) => Ok(()),
        _ => Err(LinkError::protocol("handshake payload isn't a JSON object")),
    }
}
