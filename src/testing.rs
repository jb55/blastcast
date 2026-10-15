//! A Sendspin speaker that lives in memory, for other crates' tests (the
//! `testing` feature): a [`Connection`] an [`AudioStream`] can play to,
//! with what was sent to it read back off its queues. There is no socket
//! and no time sync, so each chunk's timestamp is the server-clock time
//! the stream sent it for.
//!
//! [`AudioStream`]: crate::AudioStream

use std::sync::Arc;

use blastcast::ServerClock;

use crate::connection::{Connection, Queues};
use crate::protocol::binary::AUDIO_CHUNK;
use crate::protocol::{
    BinaryFrame, ClientHello, ClientState, Codec, ConnectionReason, HeaderLayout, PlayerSupport,
    ServerMessage, StreamFormat,
};

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
