//! Golden tests against real Sendspin traffic.
//!
//! `fixtures/sendspin-go-capture.jsonl` is a session with sendspin-go's
//! `sendspin-server` (Sendspin/sendspin-go c61a405), recorded with
//! `scripts/capture-sendspin.py`: our client messages, then everything the
//! server sent, binary frames cut to their first 16 bytes.
//! `fixtures/messages.jsonl` adds the shapes that session didn't produce,
//! from sendspin-go's own tests and aiosendspin 9.x's models.
//!
//! Every text frame must decode, and encode back to the same bytes.

use blastcast_sendspin::protocol::{
    BinaryFrame, ClientMessage, Codec, ConnectionReason, DecodeError, HeaderLayout, MediaCommand,
    PlaybackState, PlayerCommand, ServerMessage, StreamFormat, binary,
};
use serde_json::Value;

struct Record {
    dir: String,
    kind: String,
    data: String,
    note: String,
    hex: String,
    len: usize,
}

fn records(name: &str) -> Vec<Record> {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{path}: {e}"))
        .lines()
        .map(|line| {
            let v: Value = serde_json::from_str(line).unwrap();
            let s = |k: &str| v[k].as_str().unwrap_or_default().to_owned();
            Record {
                dir: s("dir"),
                kind: v["kind"].as_str().unwrap_or("text").to_owned(),
                data: s("data"),
                note: s("note"),
                hex: s("hex"),
                len: v["len"].as_u64().unwrap_or(0) as usize,
            }
        })
        .collect()
}

fn round_trip(r: &Record) {
    let encoded = match r.dir.as_str() {
        "c2s" => ClientMessage::decode(&r.data)
            .unwrap_or_else(|e| panic!("{}: {e}\n{}", r.note, r.data))
            .to_json(),
        "s2c" => ServerMessage::decode(&r.data)
            .unwrap_or_else(|e| panic!("{}: {e}\n{}", r.note, r.data))
            .to_json(),
        d => panic!("unknown dir {d}"),
    };
    assert_eq!(encoded, r.data, "{}", r.note);
}

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn capture_text_frames_round_trip() {
    let recs = records("sendspin-go-capture.jsonl");
    let texts: Vec<_> = recs.iter().filter(|r| r.kind == "text").collect();
    assert_eq!(texts.len(), 8);
    for r in texts {
        round_trip(r);
    }
}

#[test]
fn hand_fixtures_round_trip() {
    for r in records("messages.jsonl") {
        round_trip(&r);
    }
}

#[test]
fn capture_handshake_reads_as_expected() {
    let recs = records("sendspin-go-capture.jsonl");
    let server: Vec<_> = recs
        .iter()
        .filter(|r| r.dir == "s2c" && r.kind == "text")
        .map(|r| ServerMessage::decode(&r.data).unwrap())
        .collect();

    let ServerMessage::Hello(hello) = &server[0] else {
        panic!("first server message is {:?}", server[0]);
    };
    assert_eq!(hello.version, 1);
    assert_eq!(hello.connection_reason, ConnectionReason::Playback);
    // The Go server took player and metadata but not controller, and kept
    // the client's versioned spelling.
    assert_eq!(hello.active_roles, ["player@v1", "metadata@v1"]);

    let ServerMessage::GroupUpdate(g) = &server[1] else {
        panic!("{:?}", server[1])
    };
    assert_eq!(g.playback_state, Some(PlaybackState::Playing));
    assert_eq!(g.group_name, None);

    let ServerMessage::State(state) = &server[2] else {
        panic!("{:?}", server[2])
    };
    let meta = state.metadata.as_ref().unwrap().as_ref().unwrap();
    assert_eq!(meta.title, Some(Some("Test Tone".into())));
    assert_eq!(meta.artwork_url, None, "omitted, not null");

    let ServerMessage::StreamStart(start) = &server[3] else {
        panic!("{:?}", server[3])
    };
    let format = start.player.as_ref().unwrap().format();
    assert_eq!(
        format,
        StreamFormat {
            codec: Codec::Pcm,
            channels: 2,
            sample_rate: 192_000,
            bit_depth: 24
        }
    );

    let client_time = recs
        .iter()
        .find_map(|r| match ClientMessage::decode(&r.data) {
            Ok(ClientMessage::Time(t)) => Some(t),
            _ => None,
        })
        .unwrap();
    let ServerMessage::Time(t) = &server[4] else {
        panic!("{:?}", server[4])
    };
    assert_eq!(t.client_transmitted, client_time.client_transmitted);
    assert!(t.server_transmitted >= t.server_received);
}

#[test]
fn capture_audio_frames_use_the_legacy_header() {
    let recs = records("sendspin-go-capture.jsonl");
    let frames: Vec<_> = recs.iter().filter(|r| r.kind == "binary").collect();
    assert_eq!(frames.len(), 4);

    let mut last = None;
    for r in frames {
        let bytes = hex(&r.hex);
        let f = BinaryFrame::decode(&bytes, HeaderLayout::Legacy).unwrap();
        assert_eq!(f.kind, binary::AUDIO_CHUNK);
        // 9-byte header + 20 ms of 192 kHz / 24-bit / stereo PCM.
        assert_eq!(r.len - 9, 192_000 / 50 * 3 * 2);
        if let Some(prev) = last {
            let step = f.timestamp_us - prev;
            assert!((19_000..21_000).contains(&step), "step {step} µs");
        }
        last = Some(f.timestamp_us);
        // Our encoder writes the same header.
        let ours = binary::audio_chunk(HeaderLayout::Legacy, f.timestamp_us, 0, f.payload);
        assert_eq!(ours, bytes);
    }
}

#[test]
fn legacy_player_support_expands_to_formats() {
    let r = records("messages.jsonl")
        .into_iter()
        .find(|r| r.note.starts_with("MA legacy"))
        .unwrap();
    let ClientMessage::Hello(hello) = ClientMessage::decode(&r.data).unwrap() else {
        panic!()
    };
    assert_eq!(hello.role("player"), Some("player"));
    let player = hello.player().expect("legacy key read");
    let formats = player.formats();
    assert_eq!(formats.len(), 4);
    assert_eq!(
        formats[0],
        StreamFormat {
            codec: Codec::Flac,
            channels: 2,
            sample_rate: 48_000,
            bit_depth: 16
        }
    );
    assert_eq!(formats[3].codec, Codec::Pcm);
    assert_eq!(formats[3].sample_rate, 44_100);
}

#[test]
fn versioned_support_key_wins_over_legacy() {
    let text = r#"{"type":"client/hello","payload":{"client_id":"c","name":"n","version":1,
        "supported_roles":["player@v1"],
        "player@v1_support":{"supported_formats":[{"codec":"opus","channels":2,"sample_rate":48000,"bit_depth":16}],"buffer_capacity":1,"supported_commands":[]},
        "player_support":{"supported_formats":[{"codec":"pcm","channels":2,"sample_rate":48000,"bit_depth":16}],"buffer_capacity":1,"supported_commands":[]}}}"#;
    let ClientMessage::Hello(hello) = ClientMessage::decode(text).unwrap() else {
        panic!()
    };
    assert_eq!(hello.player().unwrap().formats()[0].codec, Codec::Opus);
    assert_eq!(hello.role("player"), Some("player@v1"));
    assert_eq!(hello.role("controller"), None);
}

#[test]
fn availability_across_client_generations() {
    let available = |payload: &str| {
        let text = format!(r#"{{"type":"client/state","payload":{payload}}}"#);
        let ClientMessage::State(s) = ClientMessage::decode(&text).unwrap() else {
            panic!()
        };
        s.available()
    };
    assert_eq!(available(r#"{"available":false}"#), Some(false));
    assert_eq!(available(r#"{"state":"external_source"}"#), Some(false));
    assert_eq!(
        available(r#"{"player":{"state":"synchronized"}}"#),
        Some(true)
    );
    assert_eq!(available(r#"{"player":{"state":"error"}}"#), Some(true));
    assert_eq!(available(r#"{"player":{"volume":5}}"#), None);
}

#[test]
fn metadata_null_and_omitted_differ() {
    let r = records("messages.jsonl")
        .into_iter()
        .find(|r| r.note.starts_with("metadata diff"))
        .unwrap();
    let ServerMessage::State(s) = ServerMessage::decode(&r.data).unwrap() else {
        panic!()
    };
    let m = s.metadata.unwrap().unwrap();
    assert_eq!(m.title, Some(None), "null clears");
    assert_eq!(m.album, None, "omitted keeps");
    assert_eq!(m.artist, Some(Some("Sendspin".into())));
}

#[test]
fn unknown_values_and_bad_input() {
    // An unknown command decodes, so the handler can ignore it.
    let ClientMessage::Command(c) = ClientMessage::decode(
        r#"{"type":"client/command","payload":{"controller":{"command":"teleport"}}}"#,
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(c.controller.unwrap().command, MediaCommand::Unknown);

    // A missing or null payload is an empty one.
    assert!(ClientMessage::decode(r#"{"type":"client/goodbye"}"#).is_ok());
    assert!(ClientMessage::decode(r#"{"type":"client/goodbye","payload":null}"#).is_ok());

    assert!(matches!(
        ClientMessage::decode(r#"{"type":"client/teleport","payload":{}}"#),
        Err(DecodeError::UnknownType(t)) if t == "client/teleport"
    ));
    assert!(matches!(
        ClientMessage::decode(r#"{"type":"client/time","payload":{"client_transmitted":"soon"}}"#),
        Err(DecodeError::Payload { kind, .. }) if kind == "client/time"
    ));
    assert!(matches!(
        ClientMessage::decode("not json"),
        Err(DecodeError::Envelope(_))
    ));
    // A server type is not a client message.
    assert!(matches!(
        ClientMessage::decode(r#"{"type":"server/time","payload":{}}"#),
        Err(DecodeError::UnknownType(_))
    ));
}

#[test]
fn player_command_constructors() {
    let msg = ServerMessage::Command(blastcast_sendspin::protocol::ServerCommand {
        player: Some(PlayerCommand::volume(150)),
    });
    assert_eq!(
        msg.to_json(),
        r#"{"type":"server/command","payload":{"player":{"command":"volume","volume":100}}}"#
    );
    let msg = ServerMessage::Command(blastcast_sendspin::protocol::ServerCommand {
        player: Some(PlayerCommand::mute(false)),
    });
    assert_eq!(
        msg.to_json(),
        r#"{"type":"server/command","payload":{"player":{"command":"mute","mute":false}}}"#
    );
    let msg = ServerMessage::Command(blastcast_sendspin::protocol::ServerCommand {
        player: Some(PlayerCommand::set_output_delay(6000)),
    });
    assert_eq!(
        msg.to_json(),
        r#"{"type":"server/command","payload":{"player":{"command":"set_output_delay","output_delay_ms":5000}}}"#
    );
    let msg = ServerMessage::Command(blastcast_sendspin::protocol::ServerCommand {
        player: Some(PlayerCommand::set_static_delay(30)),
    });
    assert_eq!(
        msg.to_json(),
        r#"{"type":"server/command","payload":{"player":{"command":"set_static_delay","static_delay_ms":30}}}"#
    );
}
