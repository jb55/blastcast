//! The client's half of the encrypted protocol: what a speaker does, from
//! `client/init` to the playback `server/activate`, pairing on the way if
//! the server asks. The server never runs it; tests drive the server with
//! it, and it is checked against the server's half and the spec, not
//! against itself alone.

use std::fmt::Display;

use futures_util::{Sink, Stream};
use serde::Deserialize;
use serde_json::{Value, json};

use super::cpace::{Cpace, Role as PakeRole};
use super::keys::{
    Identity, Wrapped, b64url, b64url_array, commit, digits_code, pake_sid, psk_id, random32,
    sentinel_psk, wrap,
};
use super::link::{Link, LinkError, Message};
use super::noise::{Handshake, Suite};
use std::time::Duration;

use crate::connection::Frame;

/// A long-term pairing the client holds, bound to a server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub server_id: String,
    pub psk: [u8; 32],
}

/// Who the client is and what it will pair with.
#[derive(Debug, Clone)]
pub struct ClientSpec {
    pub identity: Identity,
    pub suite: Suite,
    /// The `client/hello` payload.
    pub hello: Value,
    pub records: Vec<Record>,
    pub pairing_psk: Option<[u8; 32]>,
    pub static_code: Option<String>,
    /// Gate every code attempt behind a gesture: send
    /// `client/pair-pending` and wait this long before `client/pair-init`.
    pub gesture: Option<Duration>,
}

/// How far the client got.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Activated {
    pub activities: Vec<String>,
    pub active_roles: Vec<String>,
    /// The PSK category the session's last handshake matched.
    pub psk_category: String,
}

/// A connected client, past `client/init`.
pub struct Client<St, Si> {
    pub link: Link<St, Si>,
    pub spec: ClientSpec,
    server_id: String,
    server_key: [u8; 32],
    psk_category: String,
    /// The code a dynamic attempt shows, for the test to type in.
    pub shown_codes: tokio::sync::mpsc::UnboundedSender<String>,
    /// Every pairing activation since the last handshake.
    activations: u32,
    /// The record a finalize delivered, kept when `server/pair-finalize`
    /// arrives.
    pending: Option<[u8; 32]>,
    /// Pairing messages and aborts, in order, for tests to check.
    pub log: Vec<String>,
}

#[derive(Deserialize)]
struct ServerInit {
    server_id: String,
    version: u32,
}

#[derive(Deserialize)]
struct Noise {
    data: String,
}

#[derive(Deserialize)]
struct Msg1 {
    psk_id: String,
    psk_category: String,
}

#[derive(Deserialize)]
struct Activate {
    #[serde(default)]
    activities: Vec<String>,
    #[serde(default)]
    active_roles: Vec<String>,
    #[serde(default)]
    pairing: Option<PairingParams>,
}

#[derive(Deserialize)]
struct PairingParams {
    method: String,
}

fn base64(text: &str) -> Result<Vec<u8>, LinkError> {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(text)
        .map_err(|_| LinkError::protocol("not base64url"))
}

impl<St, Si, E> Client<St, Si>
where
    St: Stream<Item = Result<Frame, E>> + Unpin,
    E: Display,
    Si: Sink<Frame> + Unpin,
    Si::Error: Display,
{
    /// Sends `client/init` and does the handshake.
    pub async fn connect(
        stream: St,
        sink: Si,
        spec: ClientSpec,
        shown_codes: tokio::sync::mpsc::UnboundedSender<String>,
    ) -> Result<Self, LinkError> {
        let mut link = Link::new(stream, sink);
        let init = link
            .send(
                "client/init",
                json!({
                    "client_id": spec.identity.id(),
                    "version": 1,
                    "suite": spec.suite.name(),
                }),
            )
            .await?;
        let wait = Duration::from_secs(10);
        let server_init_msg = link.recv(wait).await?;
        if server_init_msg.kind != "server/init" {
            return Err(LinkError::protocol("expected server/init"));
        }
        let server_init: ServerInit = server_init_msg.payload()?;
        if server_init.version != 1 {
            return Err(LinkError::protocol("server/init version"));
        }
        let server_key = b64url_array::<32>(&server_init.server_id)
            .ok_or_else(|| LinkError::protocol("server_id isn't a key"))?;
        let prologue = [init.as_bytes(), server_init_msg.raw.as_bytes()].concat();
        let mut client = Self {
            link,
            spec,
            server_id: server_init.server_id,
            server_key,
            psk_category: String::new(),
            shown_codes,
            activations: 0,
            pending: None,
            log: Vec::new(),
        };
        let msg1 = client.link.recv(wait).await?;
        client.respond(&prologue, &msg1, true).await?;
        Ok(client)
    }

    /// The `server_id` the server gave.
    pub fn server_id(&self) -> &str {
        &self.server_id
    }

    /// Answers noise message 1 with message 2 and switches keys.
    async fn respond(
        &mut self,
        prologue: &[u8],
        msg1: &Message,
        initial: bool,
    ) -> Result<(), LinkError> {
        if msg1.kind != "noise/handshake" {
            return Err(LinkError::protocol(format!(
                "{} instead of noise/handshake",
                msg1.kind
            )));
        }
        let data = base64(&msg1.payload::<Noise>()?.data)?;
        let mut hs = Handshake::responder(
            self.spec.suite,
            prologue,
            self.spec.identity.secret(),
            self.server_key,
            Identity::generate().secret(),
        );
        let payload = hs.read(&data).map_err(LinkError::protocol)?;
        let m1: Msg1 = serde_json::from_slice(&payload).map_err(LinkError::protocol)?;
        let found = self.lookup(&m1);
        let (psk, category) = match found {
            Some(psk) => (psk, m1.psk_category.clone()),
            // A miss: the Sentinel, in the initial handshake only.
            None if initial => (sentinel_psk(), "sn".to_owned()),
            None => return Err(LinkError::protocol("unknown psk_id in a re-handshake")),
        };
        hs.set_psk(psk);
        let msg2 = hs.write(b"{}").map_err(LinkError::protocol)?;
        self.link
            .send("noise/handshake", json!({ "data": b64url(&msg2) }))
            .await?;
        self.link.secure(hs.finish().map_err(LinkError::protocol)?);
        self.psk_category = category;
        self.activations = 0;
        Ok(())
    }

    fn lookup(&self, m1: &Msg1) -> Option<[u8; 32]> {
        let candidates: Vec<[u8; 32]> = match m1.psk_category.as_str() {
            "lt" => self
                .spec
                .records
                .iter()
                // The stored-pubkey model: the record must be this server's.
                .filter(|r| r.server_id == self.server_id)
                .map(|r| r.psk)
                .collect(),
            "pr" => self.spec.pairing_psk.into_iter().collect(),
            "sn" => vec![sentinel_psk()],
            _ => vec![],
        };
        candidates.into_iter().find(|p| psk_id(p) == m1.psk_id)
    }

    /// Answers hellos, pairs when asked, follows re-handshakes, until a
    /// `server/activate` that isn't pairing.
    pub async fn run_until_activated(&mut self) -> Result<Activated, LinkError> {
        loop {
            let msg = self.link.recv(Duration::from_secs(60 * 5)).await?;
            match msg.kind.as_str() {
                "server/hello" => {
                    let hello = self.spec.hello.clone();
                    self.link.send("client/hello", hello).await?;
                }
                "noise/handshake" => {
                    let prologue = self.link.handshake_hash();
                    self.respond(&prologue, &msg, false).await?;
                    self.log.push(format!("rehandshake {}", self.psk_category));
                }
                "server/activate" => {
                    let activate: Activate = msg.payload()?;
                    if activate.activities.iter().any(|a| a == "pairing") {
                        self.activations += 1;
                        let method = activate.pairing.map(|p| p.method).unwrap_or_default();
                        self.log.push(format!("pairing {method}"));
                        self.pair(&method).await?;
                        continue;
                    }
                    return Ok(Activated {
                        activities: activate.activities,
                        active_roles: activate.active_roles,
                        psk_category: self.psk_category.clone(),
                    });
                }
                "server/pair-finalize" => {
                    if let Some(psk) = self.pending.take() {
                        let server_id = self.server_id.clone();
                        self.spec.records.retain(|r| r.server_id != server_id);
                        self.spec.records.push(Record { server_id, psk });
                        self.log.push("paired".into());
                    }
                }
                "pair/abort" => self.log.push(format!("abort {}", msg.payload)),
                other => self.log.push(format!("ignored {other}")),
            }
        }
    }

    async fn pair(&mut self, method: &str) -> Result<(), LinkError> {
        match method {
            "pairing_psk" => {
                if self.psk_category != "pr" {
                    self.link
                        .send("pair/abort", json!({ "reason": "method_not_supported" }))
                        .await?;
                    return Ok(());
                }
                let psk = random32();
                self.pending = Some(psk);
                self.link
                    .send(
                        "client/pair-finalize",
                        json!({ "long_term_psk": b64url(&psk) }),
                    )
                    .await?;
                Ok(())
            }
            "static_pairing_code" => {
                let Some(code) = self.spec.static_code.clone() else {
                    self.link
                        .send("pair/abort", json!({ "reason": "method_not_supported" }))
                        .await?;
                    return Ok(());
                };
                self.gesture().await?;
                self.link
                    .send(
                        "client/pair-init",
                        json!({ "pairing_index": self.activations }),
                    )
                    .await?;
                self.pake(code.as_bytes(), None).await
            }
            "dynamic_pairing_code" => {
                let nonce_b = random32();
                self.link
                    .send(
                        "client/pair-init",
                        json!({
                            "pairing_index": self.activations,
                            "commit_B": b64url(&commit(&nonce_b)),
                        }),
                    )
                    .await?;
                let init = self.expect("server/pair-init").await?;
                let Some(init) = init else { return Ok(()) };
                let nonce_a = init.payload["nonce_A"]
                    .as_str()
                    .and_then(b64url_array::<32>)
                    .ok_or_else(|| LinkError::protocol("nonce_A"))?;
                let code = digits_code(&self.link.handshake_hash(), &nonce_a, &nonce_b);
                let _ = self.shown_codes.send(code.clone());
                self.pake(code.as_bytes(), Some(nonce_b)).await
            }
            _ => {
                self.link
                    .send("pair/abort", json!({ "reason": "method_not_supported" }))
                    .await?;
                Ok(())
            }
        }
    }

    async fn gesture(&mut self) -> Result<(), LinkError> {
        if let Some(wait) = self.spec.gesture {
            self.link
                .send(
                    "client/pair-pending",
                    json!({ "pairing_index": self.activations }),
                )
                .await?;
            tokio::time::sleep(wait).await;
        }
        Ok(())
    }

    /// The next message, which must be `kind`; `None` for a `pair/abort`
    /// or a cancelling `server/activate`, which ends the attempt.
    async fn expect(&mut self, kind: &str) -> Result<Option<Message>, LinkError> {
        let msg = self.link.recv(Duration::from_secs(60 * 5)).await?;
        if msg.kind == kind {
            return Ok(Some(msg));
        }
        self.log.push(format!("got {} for {kind}", msg.kind));
        Ok(None)
    }

    async fn pake(&mut self, code: &[u8], nonce_b: Option<[u8; 32]>) -> Result<(), LinkError> {
        let Some(auth) = self.expect("server/pair-auth").await? else {
            return Ok(());
        };
        let ya = auth.payload["pake_msg_1"]
            .as_str()
            .and_then(b64url_array::<32>)
            .ok_or_else(|| LinkError::protocol("pake_msg_1"))?;
        let h = self.link.handshake_hash();
        let sid = pake_sid(&h, self.activations);
        let cpace = Cpace::start(PakeRole::Responder, code, &sid, b"client", random32())
            .map_err(LinkError::protocol)?;
        self.link
            .send(
                "client/pair-auth",
                json!({ "pake_msg_2": b64url(&cpace.share()) }),
            )
            .await?;
        let derived = cpace.derive(&ya, b"server").map_err(LinkError::protocol)?;
        let Some(confirm) = self.expect("server/pair-confirm").await? else {
            return Ok(());
        };
        let ok = confirm.payload["server_kc"]
            .as_str()
            .and_then(b64url_array::<64>)
            .is_some_and(|kc| derived.verify(&kc));
        if !ok {
            self.log.push("server_kc mismatch".into());
            self.link
                .send("pair/abort", json!({ "reason": "pairing_code_mismatch" }))
                .await?;
            return Ok(());
        }
        let mut payload = json!({ "client_kc": b64url(&derived.own_tag) });
        if let Some(nonce_b) = nonce_b {
            let wrapped = wrap(
                self.spec.suite,
                Wrapped::NonceB,
                &sid,
                &derived.isk,
                &nonce_b,
            );
            payload["wrapped_nonce_B"] = json!(b64url(&wrapped));
        }
        self.link.send("client/pair-confirm", payload).await?;
        let psk = random32();
        let wrapped = wrap(self.spec.suite, Wrapped::Psk, &sid, &derived.isk, &psk);
        self.pending = Some(psk);
        self.link
            .send(
                "client/pair-finalize",
                json!({ "wrapped_psk": b64url(&wrapped) }),
            )
            .await?;
        Ok(())
    }
}
