//! For other crates' tests (the `testing` feature).
//!
//! [`FakeSpeaker`] is a Sendspin speaker that lives in memory: a
//! [`Connection`] an [`AudioStream`] can play to, with what was sent to it
//! read back off its queues. There is no socket and no time sync, so each
//! chunk's timestamp is the server-clock time the stream sent it for.
//!
//! [`RecordingHost`] is a [`Host`] that keeps what it heard, for tests of
//! the server.
//!
//! [`check_trust`] is the test an app's own [`Trust`] passes.
//!
//! [`AudioStream`]: crate::AudioStream

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use blastcast::ServerClock;

use crate::connection::{Connection, Queues};
use crate::host::Host;
use crate::player::SendspinPlayer;
use crate::protocol::binary::AUDIO_CHUNK;
use crate::protocol::{
    BinaryFrame, ClientHello, ClientState, Codec, ConnectionReason, HeaderLayout, PlayerSupport,
    ServerMessage, StreamFormat,
};
use crate::secure::keys::{Identity, PairingToken, b64url_array};
use crate::secure::trust::{PairingRequest, Trust, enter_code};
use crate::status::PlayerStatus;

/// One s16 stereo frame.
const FRAME_BYTES: usize = 4;

pub struct FakeSpeaker {
    conn: Arc<Connection>,
    queues: Queues,
}

impl FakeSpeaker {
    /// A player named `client_id` that takes 48 kHz 16-bit PCM, available.
    pub fn new(clock: ServerClock, client_id: &str) -> Self {
        let hello = ClientHello {
            client_id: client_id.into(),
            name: client_id.into(),
            player_v1_support: Some(PlayerSupport {
                supported_formats: vec![StreamFormat {
                    codec: Codec::Pcm,
                    channels: 2,
                    sample_rate: 48_000,
                    bit_depth: 16,
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        let (conn, queues) = Connection::new(
            hello,
            vec!["player@v1".into()],
            ConnectionReason::Playback,
            clock,
        );
        conn.apply_state(&ClientState {
            available: Some(true),
            ..Default::default()
        });
        Self {
            conn: Arc::new(conn),
            queues,
        }
    }

    /// What a stream plays to: [`AudioStream::add_player`].
    ///
    /// [`AudioStream::add_player`]: crate::AudioStream::add_player
    pub fn conn(&self) -> Arc<Connection> {
        self.conn.clone()
    }

    /// The stream messages sent since the last call: `start`, `clear` or
    /// `end`. Panics on any other.
    pub fn control(&mut self) -> Vec<&'static str> {
        let mut out = Vec::new();
        while let Ok(msg) = self.queues.control.try_recv() {
            out.push(match msg {
                ServerMessage::StreamStart(_) => "start",
                ServerMessage::StreamClear(_) => "clear",
                ServerMessage::StreamEnd(_) => "end",
                other => panic!("unexpected {other:?}"),
            });
        }
        out
    }

    /// The audio chunks sent since the last call: each one's timestamp and
    /// frame count.
    pub fn audio(&mut self) -> Vec<(i64, usize)> {
        let mut out = Vec::new();
        while let Ok((_, frame)) = self.queues.audio.try_recv() {
            let f = BinaryFrame::decode(&frame, HeaderLayout::Legacy).unwrap();
            assert_eq!(f.kind, AUDIO_CHUNK);
            out.push((f.timestamp_us, f.payload.len() / FRAME_BYTES));
        }
        out
    }
}

/// A [`Host`] that keeps each connected player's name and latest status,
/// and hands out the output delays it is given.
#[derive(Debug, Default)]
pub struct RecordingHost {
    players: Mutex<HashMap<String, Recorded>>,
    delays: Mutex<HashMap<String, u32>>,
}

/// A connected player, as the host last heard of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recorded {
    pub name: String,
    pub status: PlayerStatus,
}

impl RecordingHost {
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    /// `client_id`'s player, while it is connected.
    pub fn get(&self, client_id: &str) -> Option<Recorded> {
        lock(&self.players).get(client_id).cloned()
    }

    pub fn contains(&self, client_id: &str) -> bool {
        lock(&self.players).contains_key(client_id)
    }

    /// Whether `client_id` is connected and said it is available.
    pub fn is_available(&self, client_id: &str) -> bool {
        self.get(client_id).is_some_and(|p| p.status.available)
    }

    /// The output delay [`Host::output_delay_ms`] gives `client_id`.
    pub fn set_output_delay(&self, client_id: &str, ms: u32) {
        lock(&self.delays).insert(client_id.to_owned(), ms);
    }
}

impl Host for RecordingHost {
    fn player_connected(&self, player: SendspinPlayer) {
        let conn = player.connection();
        let recorded = Recorded {
            name: conn.name().to_owned(),
            status: conn.player_status(),
        };
        lock(&self.players).insert(conn.client_id().to_owned(), recorded);
    }

    fn player_status(&self, client_id: &str, status: &PlayerStatus) {
        if let Some(p) = lock(&self.players).get_mut(client_id) {
            p.status = status.clone();
        }
    }

    fn player_gone(&self, client_id: &str) {
        lock(&self.players).remove(client_id);
    }

    fn output_delay_ms(&self, client_id: &str) -> Option<u32> {
        lock(&self.delays).get(client_id).copied()
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Runs a [`Trust`] through what the server needs of it: a kept
/// identity, approval, pairing requests by each method, a pairing that
/// ends both, and unpairing. Panics where it falls short.
pub fn check_trust(trust: &dyn Trust) {
    let id = trust.identity();
    assert_eq!(trust.identity().id(), id.id(), "the identity is kept");

    let client = Identity::generate().id();
    assert!(trust.long_term_psk(&client).is_none());
    trust.approve(&client).unwrap();
    assert!(trust.is_approved(&client));

    let token = PairingToken::PairingPsk {
        client_key: b64url_array(&client).unwrap(),
        pairing_psk: [4; 32],
    }
    .encode();
    let (named, request) = PairingRequest::from_token(&token).unwrap();
    assert_eq!(named, client);
    trust.request_pairing(&client, request.clone()).unwrap();
    assert_eq!(trust.pairing_request(&client), Some(request));

    trust.pair(&client, [9; 32]).unwrap();
    assert_eq!(trust.long_term_psk(&client), Some([9; 32]));
    assert!(!trust.is_approved(&client), "pairing ends approval");
    assert_eq!(trust.pairing_request(&client), None, "and the request");

    trust
        .request_pairing(&client, PairingRequest::DynamicCode { code: None })
        .unwrap();
    assert!(enter_code(trust, &client, "123-456").unwrap());
    assert_eq!(
        trust.pairing_request(&client),
        Some(PairingRequest::DynamicCode {
            code: Some("123456".into())
        })
    );
    assert!(enter_code(trust, &client, "12345").is_err());
    trust
        .request_pairing(&client, PairingRequest::static_code("1234 5678").unwrap())
        .unwrap();
    assert!(!enter_code(trust, &client, "123456").unwrap());
    assert_eq!(
        trust.pairing_request(&client),
        Some(PairingRequest::StaticCode {
            code: "12345678".into()
        })
    );
    trust.end_pairing_request(&client).unwrap();
    assert_eq!(trust.pairing_request(&client), None);

    assert!(trust.unpair(&client).unwrap());
    assert!(!trust.unpair(&client).unwrap());
    assert!(trust.long_term_psk(&client).is_none());
}
