//! AirPlay 2's SETUP, in its two phases, and the session it leaves on the
//! connection.
//!
//! **Phase 1** (no `streams`) is timing: `timingProtocol` PTP and the
//! `timingPeerInfo` to follow, which go to the process' PTP follower
//! ([`Ptp::follow`]). We answer with our own `timingPeerInfo` and an
//! `eventPort`, and serve the event channel there ([`events`](super::events)).
//! Its `name` is what the sender calls itself.
//!
//! **Phase 2** (`streams`) is the audio: a stream of `type` 103 (buffered)
//! with `ct` (the codec), `shk` (its key) and `spf`. We answer with a TCP
//! `dataPort`, a UDP `controlPort` and `audioBufferSize`, and read the
//! data channel ([`buffered`](super::buffered)). Realtime audio (`type`
//! 96) is refused, and so is buffered audio when the follower couldn't
//! follow phase 1's peers: without PTP there is nothing to play it by.
//! A stream set up is the sender taking the target over, as AirPlay 1's
//! ANNOUNCE is ([`crate::raop`]).
//!
//! The TCP ports come from [`NativeConfig::tcp_ports`] and the UDP one
//! from [`NativeConfig::udp_ports`], so a firewall can let them in.
//!
//! SETPEERS and SETPEERSX change the peers followed; TEARDOWN with
//! `streams` ends the stream and without them the session.
//! SETRATEANCHORTIME and FLUSHBUFFERED go to the stream's [`hold`], which
//! hands its audio to the receiver as it falls due.
//!
//! Ported from `openairplay2` 0.5's session.rs (MIT, Copyright (c) 2026
//! Stefan Arentz), with the stream checks from shairplay 0.10's
//! handlers_ap2.rs.
//!
//! [`NativeConfig::tcp_ports`]: crate::raop::NativeConfig::tcp_ports
//! [`NativeConfig::udp_ports`]: crate::raop::NativeConfig::udp_ports

use std::io;
use std::net::IpAddr;
use std::ops::RangeInclusive;
use std::sync::Arc;

use blastcast::ServerClock;
use blastcast_telemetry::Probe;
use plist::{Dictionary, Value};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::Instrument;

use super::buffered::{self, AudioKey, Codec, Decoded, Decoder, TYPE_BUFFERED, TYPE_REALTIME};
use super::hold::{self, Command, Sink};
use super::ptp::{self, Follow, Ptp};
use super::{BPLIST, Keys, events};
use crate::raop::rtsp::{Request, Response};
use crate::raop::{bind_tcp, bind_udp};

/// What we let the sender push ahead, as openairplay2 and shairport-sync
/// do: the most it may have sent and we not yet played. The [`hold`]
/// keeps this many bytes of it decoded, at most.
pub const AUDIO_BUFFER_SIZE: u64 = 8 * 1024 * 1024;

/// Decoded packets between the data channel and the hold: few, so a full
/// hold stops the reading at once.
const DECODED_QUEUE: usize = 16;

/// What SETUP needs from the connection and the receiver.
pub struct Context<'a> {
    /// The sender.
    pub peer: IpAddr,
    /// The address it reached us on: our `timingPeerInfo`.
    pub local: IpAddr,
    pub tcp_ports: Option<&'a RangeInclusive<u16>>,
    pub udp_ports: Option<&'a RangeInclusive<u16>>,
    pub ptp: &'a Ptp,
    /// Where the audio goes as it falls due: the receiver.
    pub sink: Arc<dyn Sink>,
    /// The receiver's telemetry, timed on `clock`.
    pub probe: Probe,
    pub clock: ServerClock,
}

/// A connection's timing, from phase 1.
#[derive(Debug, Default)]
enum Timing {
    /// No phase 1 yet.
    #[default]
    Unset,
    /// Phase 1 asked for this protocol, not PTP.
    Other(Option<String>),
    Following(Follow),
    /// PTP, but the follower can't: why.
    Refused(String),
}

/// A task that stops when this is dropped.
#[derive(Debug)]
struct Task(JoinHandle<()>);

impl Drop for Task {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A buffered stream: its data and control channels, and the hold its
/// audio waits in.
#[derive(Debug)]
pub struct Stream {
    pub codec: Codec,
    pub data_port: u16,
    pub control_port: u16,
    hold: mpsc::UnboundedSender<Command>,
    _tasks: [Task; 3],
}

/// What SETUP has set up on one connection.
#[derive(Debug, Default)]
pub struct Session {
    timing: Timing,
    /// What the sender calls itself: phase 1's `name`.
    sender_name: Option<String>,
    events: Option<Task>,
    stream: Option<Stream>,
}

impl Session {
    /// The stream phase 2 set up, while it lasts.
    pub fn stream(&self) -> Option<&Stream> {
        self.stream.as_ref()
    }

    /// What the sender called itself in phase 1, if it did.
    pub fn sender_name(&self) -> Option<&str> {
        self.sender_name.as_deref()
    }

    /// Answers a SETUP, either phase, on a connection paired with `keys`.
    pub async fn setup(&mut self, ctx: Context<'_>, keys: &Keys, req: &Request) -> Response {
        let Some(dict) = dictionary(&req.body) else {
            tracing::debug!(
                len = req.body.len(),
                "an AirPlay 2 SETUP that isn't a plist"
            );
            return Response::new(400);
        };
        let reply = match dict.get("streams").and_then(Value::as_array) {
            Some(streams) => self.streams(ctx, streams).await,
            None => self.timing(ctx, keys, &dict).await,
        };
        match reply {
            Ok(dict) => {
                let mut body = Vec::new();
                Value::Dictionary(dict)
                    .to_writer_binary(&mut body)
                    .expect("a plist of ours encodes");
                Response::ok().body(BPLIST, body)
            }
            Err(status) => Response::new(status),
        }
    }

    async fn timing(
        &mut self,
        ctx: Context<'_>,
        keys: &Keys,
        dict: &Dictionary,
    ) -> Result<Dictionary, u16> {
        if let Some(name) = dict.get("name").and_then(Value::as_string) {
            self.sender_name = Some(name.to_owned());
        }
        let protocol = dict.get("timingProtocol").and_then(Value::as_string);
        self.timing = if protocol == Some("PTP") {
            let peers = dict
                .get("timingPeerInfo")
                .map(ptp::peer_addresses)
                .unwrap_or_default();
            match ctx.ptp.follow(peers.iter().copied()) {
                Ok(follow) => {
                    tracing::debug!(?peers, "AirPlay 2 SETUP: following PTP");
                    Timing::Following(follow)
                }
                Err(why) => {
                    tracing::warn!(
                        reason = %why,
                        "AirPlay 2 SETUP asks for PTP timing, which we can't follow; its audio will be refused"
                    );
                    Timing::Refused(why)
                }
            }
        } else {
            tracing::debug!(?protocol, "AirPlay 2 SETUP without PTP timing");
            Timing::Other(protocol.map(str::to_owned))
        };
        let listener = bind_tcp(ctx.peer, ctx.tcp_ports).await.map_err(|e| {
            tracing::warn!(error = %e, "can't open the AirPlay 2 event port");
            500u16
        })?;
        let event_port = port(&listener)?;
        self.events = Some(Task(tokio::spawn(
            events::serve(listener, keys.clone()).in_current_span(),
        )));

        let mut reply = Dictionary::new();
        reply.insert("eventPort".into(), Value::from(u64::from(event_port)));
        reply.insert("timingPort".into(), Value::from(0u64));
        if matches!(self.timing, Timing::Following(_) | Timing::Refused(_)) {
            let us = Value::String(ctx.local.to_string());
            let mut info = Dictionary::new();
            info.insert("Addresses".into(), Value::Array(vec![us.clone()]));
            info.insert("ID".into(), us);
            reply.insert("timingPeerInfo".into(), Value::Dictionary(info));
        }
        Ok(reply)
    }

    async fn streams(&mut self, ctx: Context<'_>, streams: &[Value]) -> Result<Dictionary, u16> {
        let Some(spec) = streams.first().and_then(Value::as_dictionary) else {
            tracing::debug!("an AirPlay 2 SETUP with no stream");
            return Err(400);
        };
        let number = |key| spec.get(key).and_then(Value::as_unsigned_integer);
        match number("type") {
            Some(TYPE_BUFFERED) => {}
            Some(TYPE_REALTIME) => {
                tracing::warn!(
                    "refusing an AirPlay 2 realtime audio stream (type 96): only buffered audio (type 103) is supported"
                );
                return Err(501);
            }
            other => {
                tracing::warn!(stream_type = ?other, "refusing an AirPlay 2 stream we don't know");
                return Err(501);
            }
        }
        match &self.timing {
            Timing::Following(_) => {}
            Timing::Refused(why) => {
                tracing::warn!(reason = %why, "refusing AirPlay 2 buffered audio: no PTP timing");
                return Err(503);
            }
            Timing::Other(protocol) => {
                tracing::warn!(
                    ?protocol,
                    "refusing AirPlay 2 buffered audio: its SETUP didn't ask for PTP timing"
                );
                return Err(503);
            }
            Timing::Unset => {
                tracing::warn!("refusing AirPlay 2 buffered audio: no timing SETUP before it");
                return Err(455);
            }
        }
        let codec =
            Codec::from_setup(number("ct"), number("spf"), number("sr")).map_err(|why| {
                tracing::warn!(codec = %why, "refusing an AirPlay 2 stream we can't decode");
                415u16
            })?;
        let shk = spec.get("shk").and_then(Value::as_data).unwrap_or_default();
        let key = AudioKey::new(shk).ok_or_else(|| {
            tracing::warn!(
                len = shk.len(),
                "refusing an AirPlay 2 stream: its shk isn't 32 bytes"
            );
            400u16
        })?;
        let decoder = Decoder::new(codec).map_err(|e| {
            tracing::warn!(error = %e, "can't decode an AirPlay 2 stream");
            500u16
        })?;

        // The old stream's ports are free again first.
        self.stream = None;
        let stream = Stream::start(&ctx, codec, key, decoder)
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, "can't open the AirPlay 2 stream's ports");
                500u16
            })?;
        tracing::info!(
            codec = codec.name(),
            data_port = stream.data_port,
            "AirPlay 2 buffered stream set up"
        );
        let mut reply = Dictionary::new();
        reply.insert("type".into(), Value::from(TYPE_BUFFERED));
        reply.insert("dataPort".into(), Value::from(u64::from(stream.data_port)));
        reply.insert(
            "controlPort".into(),
            Value::from(u64::from(stream.control_port)),
        );
        reply.insert("audioBufferSize".into(), Value::from(AUDIO_BUFFER_SIZE));
        self.stream = Some(stream);
        let mut out = Dictionary::new();
        out.insert(
            "streams".into(),
            Value::Array(vec![Value::Dictionary(reply)]),
        );
        Ok(out)
    }

    /// SETRATEANCHORTIME or FLUSHBUFFERED (`command` reads which from
    /// `body`), for the stream's hold.
    pub fn command(&self, body: &[u8], command: fn(&[u8]) -> Option<Command>) -> Response {
        let Some(command) = command(body) else {
            tracing::debug!(len = body.len(), "an AirPlay 2 request we can't read");
            return Response::new(400);
        };
        match &self.stream {
            Some(stream) => {
                let _ = stream.hold.send(command);
            }
            None => tracing::debug!(?command, "AirPlay 2 request without a stream"),
        }
        Response::ok()
    }

    /// SETPEERS or SETPEERSX: the peers PTP follows are these now.
    pub fn set_peers(&self, body: &[u8]) -> Response {
        let Ok(value) = Value::from_reader(io::Cursor::new(body)) else {
            return Response::new(400);
        };
        let peers = ptp::peer_addresses(&value);
        match &self.timing {
            Timing::Following(follow) => {
                tracing::debug!(?peers, "AirPlay 2 timing peers");
                follow.set_peers(peers);
            }
            _ => tracing::debug!(?peers, "AirPlay 2 timing peers, while not following PTP"),
        }
        Response::ok()
    }

    /// TEARDOWN: of the stream when `body` names `streams`, else of
    /// everything SETUP set up. Returns whether it was everything.
    pub fn teardown(&mut self, body: &[u8]) -> bool {
        let streams_only = is_streams(body);
        self.stream = None;
        if !streams_only {
            *self = Self::default();
        }
        !streams_only
    }
}

/// Whether a SETUP with this body is its second phase, the streams.
pub fn is_streams(body: &[u8]) -> bool {
    dictionary(body).is_some_and(|d| d.contains_key("streams"))
}

fn dictionary(body: &[u8]) -> Option<Dictionary> {
    Value::from_reader(io::Cursor::new(body))
        .ok()?
        .into_dictionary()
}

fn port(listener: &TcpListener) -> Result<u16, u16> {
    listener.local_addr().map(|a| a.port()).map_err(|e| {
        tracing::warn!(error = %e, "an AirPlay 2 port without an address");
        500
    })
}

impl Stream {
    async fn start(
        ctx: &Context<'_>,
        codec: Codec,
        key: AudioKey,
        decoder: Decoder,
    ) -> io::Result<Self> {
        let data = bind_tcp(ctx.peer, ctx.tcp_ports).await?;
        let control = bind_udp(ctx.peer, ctx.udp_ports).await?;
        let data_port = data.local_addr()?.port();
        let control_port = control.local_addr()?.port();
        let (decoded, held) = mpsc::channel(DECODED_QUEUE);
        let (commands, received) = mpsc::unbounded_channel();
        let out = hold::Out {
            sink: ctx.sink.clone(),
            ptp: ctx.ptp.clone(),
            clock: ctx.clock,
            probe: ctx.probe.clone(),
        };
        let hold = tokio::spawn(hold::run(held, received, out).in_current_span());
        let data = tokio::spawn(read_data(data, ctx.peer, key, decoder, decoded).in_current_span());
        let control = tokio::spawn(
            async move {
                // Nothing a buffered stream needs comes here; read it so it
                // doesn't queue.
                let mut buf = [0u8; 2048];
                while let Ok((n, from)) = control.recv_from(&mut buf).await {
                    tracing::trace!(n, %from, "AirPlay 2 control packet");
                }
            }
            .in_current_span(),
        );
        Ok(Self {
            codec,
            data_port,
            control_port,
            hold: commands,
            _tasks: [Task(data), Task(control), Task(hold)],
        })
    }
}

/// Reads the sender's data channel, and again if it reconnects.
async fn read_data(
    listener: TcpListener,
    peer: IpAddr,
    key: AudioKey,
    mut decoder: Decoder,
    out: mpsc::Sender<Decoded>,
) {
    loop {
        let (tcp, from) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                tracing::debug!(error = %e, "accepting the AirPlay 2 data channel");
                return;
            }
        };
        if from.ip().to_canonical() != peer {
            tracing::debug!(%from, "an AirPlay 2 data channel from someone else; closing it");
            continue;
        }
        tracing::debug!("AirPlay 2 data channel open");
        match buffered::receive(tcp, &key, &mut decoder, Some(out.clone())).await {
            Ok(tally) => tracing::debug!(?tally, "AirPlay 2 data channel closed"),
            Err(e) => tracing::warn!(error = %e, "AirPlay 2 data channel ended"),
        }
    }
}
