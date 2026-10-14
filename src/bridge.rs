//! An AirPlay speaker as a Sendspin player: a Sendspin client run inside
//! the server, over in-memory frames, that plays what it is sent on a RAOP
//! session. It is Music Assistant's approach too
//! (`providers/airplay/sendspin_bridge.py`).
//!
//! Because the speaker is a Sendspin player, everything the Sendspin
//! server does for its players it does for AirPlay speakers as well: they
//! are listed, grouped (with Sendspin speakers or with each other), given
//! the group's volume and fed the group's one timeline. A chunk's
//! timestamp is server-clock time, and the RAOP session's NTP clock is the
//! server clock, so a chunk that plays at `t` on a Sendspin speaker is
//! anchored to play at `t` on the AirPlay one: a mixed group plays in
//! sync with no translation beyond the sync packets.
//!
//! - **The hello** asks for PCM at 44.1 kHz, 16 bits, stereo, what RAOP
//!   carries, and takes volume, mute and the output delay.
//! - **The buffer** it reports is [`MIN_BUFFER_MS`]: RAOP sends two
//!   seconds ahead, so the group's send-ahead grows to cover it.
//! - **The RAOP session** opens on the first chunk and closes
//!   [`LINGER`] after the stream ends, so a seek or the next track
//!   doesn't reconnect. Chunks that arrive while it connects are held.
//! - **The output delay** is subtracted from each chunk's timestamp, as
//!   Sendspin players do: it is how long the speaker itself takes.
//! - **Another sender** taking the speaker closes our session. The bridge
//!   reports itself unavailable for a moment, which moves it out of its
//!   group, stopped, the way a Sendspin speaker leaves when its output is
//!   taken.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc;
use futures_util::StreamExt;
use tas_sendspin::protocol::{
    BinaryFrame, ClientHello, ClientMessage, ClientState, Codec, ConnectionReason, DeviceInfo,
    HeaderLayout, PlayerCommand, PlayerCommandKind, PlayerStateReport, PlayerSupport,
    PlayerSyncState, ServerMessage, StreamFormat, binary, roles,
};
use tas_sendspin::{Disconnect, Frame, SendspinServer};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};

use crate::device::RaopDevice;
use crate::rtp::NtpClock;
use crate::session::{RaopSession, SAMPLE_RATE, SessionError, volume_db};

/// The audio the bridge asks to be kept queued. RAOP sends
/// [`LATENCY_FRAMES`](crate::session::LATENCY_FRAMES) ahead.
pub const MIN_BUFFER_MS: u32 = 2_000;
/// The startup lead it asks for on a source that isn't live: the buffer
/// and a RAOP connect.
pub const REQUIRED_LEAD_MS: u32 = 2_500;
/// How long an idle session is kept open after its stream ends.
pub const LINGER: Duration = Duration::from_secs(4);
/// How long after a failed connect the bridge waits before trying again
/// within the same stream. Chunks in between are dropped.
const RETRY_AFTER: Duration = Duration::from_secs(5);
/// Chunks held while the session connects: about 10 s of 20 ms chunks.
const MAX_HELD: usize = 512;
/// A chunk this close to its play time, or past it, when the session comes
/// up is dropped rather than sent too late to play.
const MIN_LEAD_US: i64 = 50_000;
/// RTP times within this many frames of where the last chunk ended are
/// taken as contiguous: the timeline's rounding, not a gap.
const CONTIGUOUS_FRAMES: i64 = 2;
/// A gap up to this long is filled with silence; a longer one, or a jump
/// back, re-anchors the session.
const MAX_FILL_FRAMES: i64 = 2 * SAMPLE_RATE as i64;
/// Bytes of PCM the bridge says it can hold: 8 s.
const BUFFER_CAPACITY: u64 = 8 * SAMPLE_RATE as u64 * 4;

/// The format the bridge takes: what RAOP carries.
pub const FORMAT: StreamFormat = StreamFormat {
    codec: Codec::Pcm,
    channels: 2,
    sample_rate: SAMPLE_RATE,
    bit_depth: 16,
};

/// The volume and mute a bridge starts with, and keeps between its runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Volume {
    pub level: u8,
    pub muted: bool,
}

impl Default for Volume {
    fn default() -> Self {
        Self {
            level: 50,
            muted: false,
        }
    }
}

/// The hello the bridge sends for `device`.
pub fn hello(device: &RaopDevice) -> ClientHello {
    ClientHello {
        client_id: device.player_id.clone(),
        name: device.name.clone(),
        version: tas_sendspin::protocol::PROTOCOL_VERSION,
        supported_roles: vec![roles::PLAYER_V1.into()],
        device_info: Some(DeviceInfo {
            product_name: Some(device.model.clone().unwrap_or_else(|| "AirPlay".into())),
            manufacturer: device.manufacturer.clone(),
            software_version: None,
            mac_address: device.mac_address(),
        }),
        player_v1_support: Some(PlayerSupport {
            supported_formats: vec![FORMAT],
            buffer_capacity: BUFFER_CAPACITY,
            supported_commands: vec![
                PlayerCommandKind::Volume,
                PlayerCommandKind::Mute,
                PlayerCommandKind::SetOutputDelay,
            ],
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Runs the bridge for `device` until `stop` turns true or the server
/// drops the connection, and returns how the connection ended. `volume`
/// is where it starts and is kept up to date.
pub async fn run(
    server: Arc<SendspinServer>,
    device: RaopDevice,
    ntp: NtpClock,
    volume: Arc<std::sync::Mutex<Volume>>,
    mut stop: watch::Receiver<bool>,
) -> Disconnect {
    let (to_server, from_bridge) = mpsc::unbounded::<Result<Frame, Infallible>>();
    let (to_bridge, from_server) = mpsc::unbounded::<Frame>();
    let hello = hello(&device);
    let _ = to_server.unbounded_send(Ok(Frame::Text(
        ClientMessage::Hello(Box::new(hello)).to_json(),
    )));
    let connection = tokio::spawn({
        let server = server.clone();
        async move {
            server
                .run_local_connection(from_bridge, to_bridge, ConnectionReason::Playback)
                .await
        }
    });
    let start = *volume
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut bridge = Bridge {
        device,
        ntp,
        to_server,
        volume,
        level: start,
        output_delay_ms: 0,
        streaming: false,
        output: Output::Idle,
        retry_at: None,
    };
    bridge.report(PlayerSyncState::Synchronized);
    bridge.run(from_server, &mut stop).await;
    bridge.shut_down().await;
    // Dropping the sender ends the server's read of us.
    drop(bridge);
    connection.await.unwrap_or_else(|e| {
        tracing::warn!(error = %e, "the bridge's connection task failed");
        Disconnect::Ended {
            client_id: String::new(),
            goodbye: None,
            replaced: false,
        }
    })
}

/// Where the RAOP side is.
enum Output {
    Idle,
    /// Connecting, with the chunks that came meanwhile: each one's play
    /// time and samples.
    Connecting {
        task: JoinHandle<Result<RaopSession, SessionError>>,
        held: VecDeque<(i64, Vec<i16>)>,
    },
    Playing(RaopSession),
    /// The stream ended; torn down at the instant unless another starts.
    Lingering(RaopSession, Instant),
}

struct Bridge {
    device: RaopDevice,
    ntp: NtpClock,
    to_server: mpsc::UnboundedSender<Result<Frame, Infallible>>,
    volume: Arc<std::sync::Mutex<Volume>>,
    level: Volume,
    output_delay_ms: u32,
    /// Between a player stream/start and its stream/end.
    streaming: bool,
    output: Output,
    /// No connect before this, after one failed.
    retry_at: Option<Instant>,
}

/// What woke the bridge.
enum Wake {
    Frame(Option<Frame>),
    Connected(Box<Result<RaopSession, SessionError>>),
    Closed,
    Linger,
    Stop,
}

impl Bridge {
    async fn run(
        &mut self,
        mut from_server: mpsc::UnboundedReceiver<Frame>,
        stop: &mut watch::Receiver<bool>,
    ) {
        loop {
            let wake = tokio::select! {
                _ = stop.wait_for(|s| *s) => Wake::Stop,
                frame = from_server.next() => Wake::Frame(frame),
                wake = output_event(&mut self.output) => wake,
            };
            match wake {
                Wake::Stop => {
                    self.send(ClientMessage::Goodbye(
                        tas_sendspin::protocol::ClientGoodbye {
                            reason: Some(tas_sendspin::protocol::GoodbyeReason::Shutdown),
                        },
                    ));
                    return;
                }
                Wake::Frame(None | Some(Frame::Close)) => return,
                Wake::Frame(Some(frame)) => self.on_frame(frame).await,
                Wake::Connected(r) => self.on_connected(*r).await,
                Wake::Closed => self.on_closed(),
                Wake::Linger => {
                    if let Output::Lingering(session, _) =
                        std::mem::replace(&mut self.output, Output::Idle)
                    {
                        tracing::debug!("idle; closing the airplay session");
                        session.teardown().await;
                    }
                }
            }
        }
    }

    async fn on_frame(&mut self, frame: Frame) {
        match frame {
            Frame::Text(text) => match ServerMessage::decode(&text) {
                Ok(msg) => self.on_message(msg).await,
                Err(e) => tracing::debug!(error = %e, "ignoring a server message"),
            },
            Frame::Binary(data) => {
                let Ok(frame) = BinaryFrame::decode(&data, HeaderLayout::Legacy) else {
                    return;
                };
                if frame.kind == binary::AUDIO_CHUNK {
                    self.on_chunk(frame.timestamp_us, frame.payload).await;
                }
            }
            Frame::Ping(payload) => {
                let _ = self.to_server.unbounded_send(Ok(Frame::Pong(payload)));
            }
            Frame::Pong(_) | Frame::Close => {}
        }
    }

    async fn on_message(&mut self, msg: ServerMessage) {
        match msg {
            ServerMessage::StreamStart(start) => {
                let Some(player) = start.player else { return };
                if player.format() != FORMAT {
                    tracing::warn!(format = ?player.format(), "sent a format RAOP can't carry");
                    self.streaming = false;
                    return;
                }
                self.streaming = true;
                self.retry_at = None;
                self.restart().await;
            }
            ServerMessage::StreamClear(clear) => {
                if clear
                    .roles
                    .as_ref()
                    .is_none_or(|r| r.iter().any(|r| r == "player"))
                {
                    self.restart().await;
                }
            }
            ServerMessage::StreamEnd(end) => {
                if end
                    .roles
                    .as_ref()
                    .is_none_or(|r| r.iter().any(|r| r == "player"))
                {
                    self.streaming = false;
                    self.end().await;
                }
            }
            ServerMessage::Command(command) => {
                if let Some(player) = command.player {
                    self.on_command(player).await;
                }
            }
            _ => {}
        }
    }

    /// A new stream, or a seek: what the speaker holds is dropped, and the
    /// next chunk anchors the session again.
    async fn restart(&mut self) {
        match std::mem::replace(&mut self.output, Output::Idle) {
            Output::Playing(mut session) | Output::Lingering(mut session, _) => {
                match session.flush().await {
                    Ok(()) => self.output = Output::Playing(session),
                    Err(e) => self.failed(&e),
                }
            }
            Output::Connecting { task, mut held } => {
                held.clear();
                self.output = Output::Connecting { task, held };
            }
            Output::Idle => {}
        }
    }

    /// The stream ended: what is queued goes out, and the session lingers.
    async fn end(&mut self) {
        match std::mem::replace(&mut self.output, Output::Idle) {
            Output::Playing(mut session) => {
                if let Err(e) = session.finish().await {
                    self.failed(&e);
                    return;
                }
                self.output = Output::Lingering(session, Instant::now() + LINGER);
            }
            // What it holds still plays once it is up.
            other => self.output = other,
        }
    }

    async fn on_command(&mut self, command: PlayerCommand) {
        match command.command {
            PlayerCommandKind::Volume => {
                if let Some(v) = command.volume {
                    self.level.level = v.min(100) as u8;
                }
            }
            PlayerCommandKind::Mute => {
                if let Some(m) = command.mute {
                    self.level.muted = m;
                }
            }
            PlayerCommandKind::SetOutputDelay | PlayerCommandKind::SetStaticDelay => {
                if let Some(ms) = command.output_delay_ms.or(command.static_delay_ms) {
                    self.output_delay_ms = ms.min(tas_sendspin::protocol::MAX_OUTPUT_DELAY_MS);
                }
                self.report(PlayerSyncState::Synchronized);
                return;
            }
            PlayerCommandKind::Unknown => return,
        }
        *self
            .volume
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = self.level;
        let db = volume_db(self.level.level, self.level.muted);
        if let Output::Playing(session) | Output::Lingering(session, _) = &mut self.output
            && let Err(e) = session.set_volume(db).await
        {
            tracing::warn!(error = %e, "can't set the airplay volume");
        }
        self.report(PlayerSyncState::Synchronized);
    }

    async fn on_chunk(&mut self, timestamp_us: i64, payload: &[u8]) {
        if !self.streaming {
            return;
        }
        let samples: Vec<i16> = payload
            .as_chunks::<2>()
            .0
            .iter()
            .map(|b| i16::from_le_bytes(*b))
            .collect();
        let at_us = timestamp_us - i64::from(self.output_delay_ms) * 1000;
        match &mut self.output {
            Output::Playing(session) => {
                if let Err(e) = feed(session, at_us, &samples).await {
                    self.failed(&e);
                }
            }
            Output::Connecting { held, .. } => {
                if held.len() == MAX_HELD {
                    held.pop_front();
                }
                held.push_back((at_us, samples));
            }
            Output::Lingering(..) => {
                // A chunk after stream/end isn't sent, but be safe.
            }
            Output::Idle => {
                if self.retry_at.is_some_and(|at| Instant::now() < at) {
                    return;
                }
                let (addr, ntp) = (self.device.addr, self.ntp);
                let db = volume_db(self.level.level, self.level.muted);
                tracing::info!(addr = %addr, "connecting to the airplay speaker");
                let task = tokio::spawn(RaopSession::connect(addr, ntp, db));
                let mut held = VecDeque::new();
                held.push_back((at_us, samples));
                self.output = Output::Connecting { task, held };
            }
        }
    }

    async fn on_connected(&mut self, result: Result<RaopSession, SessionError>) {
        let Output::Connecting { held, .. } = std::mem::replace(&mut self.output, Output::Idle)
        else {
            return;
        };
        let mut session = match result {
            Ok(session) => session,
            Err(e) => {
                self.failed(&e);
                return;
            }
        };
        tracing::info!(
            audio_latency = session.audio_latency(),
            "airplay session up"
        );
        // Clears an error a failed connect reported.
        self.report(PlayerSyncState::Synchronized);
        let now = self.ntp.server_clock().now_us();
        for (at_us, samples) in held {
            let end_us = at_us + samples.len() as i64 / 2 * 1_000_000 / i64::from(SAMPLE_RATE);
            if end_us < now + MIN_LEAD_US {
                continue;
            }
            if let Err(e) = feed(&mut session, at_us, &samples).await {
                self.failed(&e);
                return;
            }
        }
        if self.streaming {
            self.output = Output::Playing(session);
        } else if let Err(e) = session.finish().await {
            self.failed(&e);
        } else {
            // The stream ended while it connected.
            self.output = Output::Lingering(session, Instant::now() + LINGER);
        }
    }

    /// The speaker closed our session: another sender has it.
    fn on_closed(&mut self) {
        tracing::info!("the airplay speaker closed the session; another sender has it");
        self.output = Output::Idle;
        if self.streaming {
            self.retry_at = Some(Instant::now() + RETRY_AFTER);
            // Out of its group, stopped; then free to be picked again.
            self.report(PlayerSyncState::ExternalSource);
            self.report(PlayerSyncState::Synchronized);
        }
    }

    fn failed(&mut self, e: &SessionError) {
        tracing::warn!(error = %e, addr = %self.device.addr, "airplay output failed");
        self.output = Output::Idle;
        self.retry_at = Some(Instant::now() + RETRY_AFTER);
        self.report(PlayerSyncState::Error);
    }

    async fn shut_down(&mut self) {
        match std::mem::replace(&mut self.output, Output::Idle) {
            Output::Playing(session) | Output::Lingering(session, _) => session.teardown().await,
            Output::Connecting { task, .. } => task.abort(),
            Output::Idle => {}
        }
    }

    fn report(&self, state: PlayerSyncState) {
        self.send(ClientMessage::State(ClientState {
            player: Some(PlayerStateReport {
                state: Some(state),
                volume: Some(self.level.level.into()),
                muted: Some(self.level.muted),
                output_delay_ms: Some(self.output_delay_ms),
                min_buffer_ms: Some(MIN_BUFFER_MS),
                required_lead_time_ms: Some(REQUIRED_LEAD_MS),
                ..Default::default()
            }),
            ..Default::default()
        }));
    }

    fn send(&self, msg: ClientMessage) {
        let _ = self
            .to_server
            .unbounded_send(Ok(Frame::Text(msg.to_json())));
    }
}

/// Pushes one chunk that plays from `at_us`: anchoring the session if it
/// isn't, filling a small gap with silence, trimming a small overlap, and
/// re-anchoring after anything bigger.
async fn feed(session: &mut RaopSession, at_us: i64, samples: &[i16]) -> Result<(), SessionError> {
    let Some(anchor) = session.current_anchor() else {
        session.anchor(at_us).await?;
        return session.push(samples).await;
    };
    let want = anchor
        .rtp
        .wrapping_add(frames_in(at_us - anchor.at_us) as u32);
    let off = i64::from(want.wrapping_sub(session.next_rtp()) as i32);
    let frames = (samples.len() / 2) as i64;
    if off.abs() <= CONTIGUOUS_FRAMES {
        session.push(samples).await
    } else if off > 0 && off <= MAX_FILL_FRAMES {
        tracing::debug!(frames = off, "filling a gap in the stream with silence");
        session.push(&vec![0; off as usize * 2]).await?;
        session.push(samples).await
    } else if off < 0 && -off < frames {
        session.push(&samples[(-off) as usize * 2..]).await
    } else {
        tracing::debug!(frames = off, "the stream jumped; re-anchoring");
        session.flush().await?;
        session.anchor(at_us).await?;
        session.push(samples).await
    }
}

/// Frames in `us`, to the nearest one.
fn frames_in(us: i64) -> i64 {
    let n = i128::from(us) * i128::from(SAMPLE_RATE);
    ((n + 500_000).div_euclid(1_000_000)) as i64
}

/// What the RAOP side has to say: a connect finished, the speaker closed
/// the session, or a lingering one is due to close. Cancel safe.
async fn output_event(output: &mut Output) -> Wake {
    match output {
        Output::Connecting { task, .. } => Wake::Connected(Box::new(match task.await {
            Ok(r) => r,
            Err(e) => Err(SessionError::Io(std::io::Error::other(e.to_string()))),
        })),
        Output::Playing(session) => {
            session.closed().await;
            Wake::Closed
        }
        Output::Lingering(session, at) => {
            let at = *at;
            tokio::select! {
                () = session.closed() => Wake::Closed,
                () = sleep_until(at) => Wake::Linger,
            }
        }
        Output::Idle => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_to_the_nearest() {
        assert_eq!(frames_in(1_000_000), 44_100);
        assert_eq!(frames_in(20_000), 882);
        assert_eq!(frames_in(11), 0);
        assert_eq!(frames_in(12), 1);
        assert_eq!(frames_in(-1_000_000), -44_100);
    }

    #[test]
    fn hello_asks_for_raop_pcm() {
        let device = RaopDevice {
            player_id: "apa1b2c3d4e5f6".into(),
            name: "Kitchen".into(),
            addr: "192.168.1.30:7000".parse().unwrap(),
            model: Some("AirPort10,115".into()),
            manufacturer: None,
            mac: Some("A1B2C3D4E5F6".into()),
            fullname: "A1B2C3D4E5F6@Kitchen._raop._tcp.local.".into(),
        };
        let hello = hello(&device);
        assert_eq!(hello.client_id, "apa1b2c3d4e5f6");
        let support = hello.player().unwrap();
        assert_eq!(support.formats(), [FORMAT]);
        assert!(support.supports(PlayerCommandKind::Volume));
        assert_eq!(
            hello.device_info.unwrap().mac_address.as_deref(),
            Some("A1:B2:C3:D4:E5:F6")
        );
    }
}
