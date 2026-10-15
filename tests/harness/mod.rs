//! A fake Sendspin player for interop tests, close enough to a speaker to
//! measure sync: it keeps its own clock far from the server's, runs the
//! time filter on `server/time` the whole time it is connected, decodes
//! PCM, FLAC and Opus with decoders that share no code with the server,
//! and records when it plays each sample (see [`playout`]).
//!
//! Shared by the integration tests that need it as `mod harness;`; each
//! uses a different part, hence the `dead_code` allowance.

#![allow(dead_code)]

pub mod noise;
pub mod playout;
pub mod time_filter;

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use base64::Engine;
use blastcast::ServerClock;
use futures_util::{SinkExt, StreamExt};
use tas_sendspin::protocol::binary::AUDIO_CHUNK;
use tas_sendspin::protocol::{
    BinaryFrame, ClientHello, ClientMessage, ClientState, ClientTime, Codec, HeaderLayout,
    PlaybackState, PlayerSupport, RequestFormatPlayer, ServerMessage, ServerTime, StreamFormat,
    StreamRequestFormat, StreamStartPlayer,
};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;

use playout::Playout;
use time_filter::{ClientClock, TimeFilter};

/// sendspin-go's clock sync: a burst of `client/time`s, of which only the
/// fastest round trip reaches the filter. It bursts once on connecting,
/// before it says it is available, and then every 10 s; here every second,
/// so the filter moves a few times in a test.
const BURST_SIZE: usize = 8;
const BURST_INTERVAL: Duration = Duration::from_secs(1);
const REPLY_TIMEOUT: Duration = Duration::from_millis(500);

/// A 48 kHz stereo format.
pub const fn format(codec: Codec, bit_depth: u32) -> StreamFormat {
    StreamFormat {
        codec,
        channels: 2,
        sample_rate: 48_000,
        bit_depth,
    }
}

/// Who a fake player is.
pub struct Spec {
    pub client_id: &'static str,
    /// In order of preference; the server sends the first it can.
    pub formats: Vec<StreamFormat>,
    /// Where the player's own clock starts, µs: a different one per player
    /// makes each filter find its own offset.
    pub clock_base_us: i64,
}

pub struct FakePlayer {
    pub client_id: &'static str,
    out: mpsc::UnboundedSender<Message>,
    state: Arc<Mutex<State>>,
}

struct State {
    clock: ClientClock,
    /// Server minus client, exactly: what the filter estimates.
    true_offset: i64,
    filter: TimeFilter,
    /// Bursts fed to the filter.
    syncs: u32,
    decoder: Option<Decoder>,
    playout: Playout,
    log: Vec<String>,
}

impl State {
    /// Now on the true server clock.
    fn now_true_us(&self) -> f64 {
        (self.clock.now_us() + self.true_offset) as f64
    }

    /// Anything but `server/time`, which the sync task takes.
    fn on_message(&mut self, msg: ServerMessage) {
        match msg {
            ServerMessage::Hello(_) => self.log.push("hello".into()),
            ServerMessage::GroupUpdate(u) => {
                let state = match u.playback_state {
                    Some(PlaybackState::Playing) => "playing",
                    Some(_) => "stopped",
                    None => "-",
                };
                self.log.push(format!("group {state}"));
            }
            ServerMessage::StreamStart(s) => {
                let Some(player) = s.player else { return };
                self.log
                    .push(format!("start {:?}/{}", player.codec, player.bit_depth));
                match Decoder::new(&player) {
                    Ok(d) => self.decoder = Some(d),
                    Err(e) => self.log.push(format!("error: {e}")),
                }
            }
            ServerMessage::StreamClear(_) => {
                let now = self.now_true_us();
                self.playout.cut(now);
                self.log.push("clear".into());
            }
            ServerMessage::StreamEnd(_) => {
                let now = self.now_true_us();
                self.playout.cut(now);
                self.decoder = None;
                self.log.push("end".into());
            }
            ServerMessage::Time(_) | ServerMessage::State(_) | ServerMessage::Command(_) => {}
        }
    }

    fn on_binary(&mut self, frame: &[u8]) {
        let frame = match BinaryFrame::decode(frame, HeaderLayout::Legacy) {
            Ok(f) if f.kind == AUDIO_CHUNK => f,
            Ok(_) => return,
            Err(e) => return self.log.push(format!("error: {e}")),
        };
        let Some(decoder) = &mut self.decoder else {
            return self.log.push("error: audio outside a stream".into());
        };
        match decoder.decode(frame.payload) {
            Ok(samples) => {
                // Scheduled through the filter, as a speaker does; the
                // record is on the true clock.
                let local = self.filter.compute_client_time(frame.timestamp_us);
                let start = (local + self.true_offset) as f64;
                self.playout.insert(start, samples);
            }
            Err(e) => self.log.push(format!("error: {e}")),
        }
    }
}

impl FakePlayer {
    /// Connects, says hello, syncs its clock, then reports itself
    /// available. Keeps syncing while connected.
    pub async fn connect(url: &str, server_clock: ServerClock, spec: Spec) -> Self {
        let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let (mut sink, mut stream) = ws.split();
        let clock = ClientClock {
            epoch: Instant::now(),
            base_us: spec.clock_base_us,
        };
        let state = Arc::new(Mutex::new(State {
            true_offset: server_clock.us_at(clock.epoch) - clock.base_us,
            clock,
            filter: TimeFilter::new(),
            syncs: 0,
            decoder: None,
            playout: Playout::default(),
            log: Vec::new(),
        }));

        let (out, mut rx) = mpsc::unbounded_channel::<Message>();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if sink.send(msg).await.is_err() {
                    break;
                }
            }
        });
        // T4 is read the moment a reply arrives, before anything else
        // waits on the state.
        let (times, mut replies) = mpsc::unbounded_channel::<(ServerTime, i64)>();
        let reader = state.clone();
        tokio::spawn(async move {
            while let Some(Ok(msg)) = stream.next().await {
                let arrived = clock.now_us();
                match msg {
                    Message::Text(text) => match ServerMessage::decode(&text) {
                        Ok(ServerMessage::Time(t)) => {
                            let _ = times.send((t, arrived));
                        }
                        Ok(msg) => lock(&reader).on_message(msg),
                        Err(e) => lock(&reader)
                            .log
                            .push(format!("error: undecodable {text}: {e}")),
                    },
                    Message::Binary(frame) => lock(&reader).on_binary(&frame),
                    _ => {}
                }
            }
        });

        let player = Self {
            client_id: spec.client_id,
            out,
            state,
        };
        player.send(ClientMessage::Hello(Box::new(ClientHello {
            client_id: spec.client_id.into(),
            name: spec.client_id.into(),
            version: 1,
            supported_roles: vec!["player@v1".into()],
            player_v1_support: Some(PlayerSupport {
                supported_formats: spec.formats,
                buffer_capacity: 1 << 20,
                ..Default::default()
            }),
            ..Default::default()
        })));

        let (out, syncing) = (player.out.clone(), player.state.clone());
        tokio::spawn(async move {
            loop {
                let Some(best) = burst(&out, &clock, &mut replies).await else {
                    break;
                };
                if let Some((t1, t2, t3, t4)) = best {
                    let mut state = lock(&syncing);
                    state.filter.update(
                        ((t2 - t1) + (t3 - t4)) / 2,
                        ((t4 - t1) - (t3 - t2)) / 2,
                        t4,
                    );
                    state.syncs += 1;
                }
                tokio::time::sleep(BURST_INTERVAL).await;
            }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while lock(&player.state).syncs == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("clock sync timed out");
        player.send(ClientMessage::State(ClientState {
            available: Some(true),
            ..Default::default()
        }));
        player
    }

    pub fn send(&self, msg: ClientMessage) {
        self.out.send(Message::text(msg.to_json())).unwrap();
    }

    /// Asks for another format, as `stream/request-format`.
    pub fn request_format(&self, player: RequestFormatPlayer) {
        self.send(ClientMessage::RequestFormat(StreamRequestFormat {
            player: Some(player),
            artwork: None,
        }));
    }

    /// What it has played so far, on the true clock.
    pub fn playout(&self) -> Playout {
        lock(&self.state).playout.clone()
    }

    /// The control messages it got, in order, and anything it couldn't
    /// handle as `error: …`.
    pub fn log(&self) -> Vec<String> {
        lock(&self.state).log.clone()
    }

    /// How far its filter's offset is from the truth, µs.
    pub fn offset_error_us(&self) -> f64 {
        let state = lock(&self.state);
        (state.filter.offset - state.true_offset as f64).abs()
    }
}

/// One burst: T1-T4 of the fastest round trip, if any came back, or
/// `None` once the connection is gone.
async fn burst(
    out: &mpsc::UnboundedSender<Message>,
    clock: &ClientClock,
    replies: &mut mpsc::UnboundedReceiver<(ServerTime, i64)>,
) -> Option<Option<(i64, i64, i64, i64)>> {
    let mut best: Option<(i64, i64, i64, i64)> = None;
    let rtt = |(t1, t2, t3, t4): (i64, i64, i64, i64)| (t4 - t1) - (t3 - t2);
    for _ in 0..BURST_SIZE {
        let t1 = clock.now_us();
        let msg = ClientMessage::Time(ClientTime {
            client_transmitted: t1,
        });
        out.send(Message::text(msg.to_json())).ok()?;
        let (reply, t4) = match tokio::time::timeout(REPLY_TIMEOUT, replies.recv()).await {
            Ok(Some(reply)) => reply,
            Ok(None) => return None,
            Err(_) => continue,
        };
        if reply.client_transmitted != t1 {
            continue;
        }
        let sample = (t1, reply.server_received, reply.server_transmitted, t4);
        if best.is_none_or(|b| rtt(sample) < rtt(b)) {
            best = Some(sample);
        }
    }
    Some(best)
}

fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    state.lock().unwrap_or_else(|e| e.into_inner())
}

/// A player's decoder for the format of its current stream. Only the left
/// channel is kept: the test signals are the same on both.
enum Decoder {
    Pcm { bit_depth: u32 },
    Flac { header: Vec<u8> },
    Opus(opus::Decoder),
}

impl Decoder {
    fn new(player: &StreamStartPlayer) -> Result<Self, String> {
        if player.channels != 2 {
            return Err(format!("{} channels", player.channels));
        }
        Ok(match player.codec {
            Codec::Pcm => Self::Pcm {
                bit_depth: player.bit_depth,
            },
            Codec::Flac => {
                let header = player
                    .codec_header
                    .as_deref()
                    .ok_or("FLAC with no header")?;
                Self::Flac {
                    header: base64::engine::general_purpose::STANDARD
                        .decode(header)
                        .map_err(|e| e.to_string())?,
                }
            }
            Codec::Opus => Self::Opus(
                opus::Decoder::new(player.sample_rate, opus::Channels::Stereo)
                    .map_err(|e| e.to_string())?,
            ),
            other => return Err(format!("can't decode {other:?}")),
        })
    }

    fn decode(&mut self, payload: &[u8]) -> Result<Vec<f32>, String> {
        match self {
            Self::Pcm { bit_depth: 16 } => Ok(payload
                .as_chunks::<4>()
                .0
                .iter()
                .map(|f| f32::from(i16::from_le_bytes([f[0], f[1]])) / 32_768.0)
                .collect()),
            Self::Pcm { bit_depth: 24 } => Ok(payload
                .as_chunks::<6>()
                .0
                .iter()
                .map(|f| (i32::from_le_bytes([0, f[0], f[1], f[2]]) >> 8) as f32 / 8_388_608.0)
                .collect()),
            Self::Pcm { bit_depth } => Err(format!("PCM at {bit_depth} bits")),
            Self::Flac { header } => {
                // Each chunk is a whole frame: behind the header it is a
                // stream of its own.
                let bytes = [header.as_slice(), payload].concat();
                let mut reader = claxon::FlacReader::new(&bytes[..]).map_err(|e| e.to_string())?;
                let info = reader.streaminfo();
                let scale = (1u32 << (info.bits_per_sample - 1)) as f32;
                let samples: Vec<i32> = reader
                    .samples()
                    .collect::<Result<_, _>>()
                    .map_err(|e| e.to_string())?;
                Ok(samples
                    .chunks_exact(info.channels as usize)
                    .map(|f| f[0] as f32 / scale)
                    .collect())
            }
            Self::Opus(decoder) => {
                let mut out = vec![0.0; 5_760 * 2];
                let frames = decoder
                    .decode_float(payload, &mut out, false)
                    .map_err(|e| e.to_string())?;
                Ok(out[..frames * 2].iter().step_by(2).copied().collect())
            }
        }
    }
}
