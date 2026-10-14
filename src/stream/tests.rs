//! The stream against a fake connection, on tokio's paused clock: the
//! stream task sees time move only when everything is idle, so every
//! timestamp can be checked exactly.

use std::time::Duration;

use async_trait::async_trait;
use tas_audio::{AudioFormat, PcmChunk, SineSource};

use super::*;
use crate::connection::Queues;
use crate::protocol::{
    BinaryFrame, ClientHello, ClientState, Codec, ConnectionReason, HeaderLayout, PlayerSupport,
    RequestFormatPlayer, StreamFormat,
};

/// Bytes in a 20 ms chunk of 48 kHz stereo s16le.
const CHUNK_BYTES: usize = 960 * 4;

fn pcm(sample_rate: u32, bit_depth: u32) -> StreamFormat {
    StreamFormat {
        codec: Codec::Pcm,
        channels: 2,
        sample_rate,
        bit_depth,
    }
}

fn flac(bit_depth: u32) -> StreamFormat {
    StreamFormat {
        codec: Codec::Flac,
        ..pcm(48_000, bit_depth)
    }
}

fn opus(sample_rate: u32) -> StreamFormat {
    StreamFormat {
        codec: Codec::Opus,
        ..pcm(sample_rate, 16)
    }
}

fn player(
    clock: ServerClock,
    formats: Vec<StreamFormat>,
    capacity: u64,
) -> (Arc<Connection>, Queues) {
    named_player(clock, "kitchen", formats, capacity)
}

fn named_player(
    clock: ServerClock,
    client_id: &str,
    formats: Vec<StreamFormat>,
    capacity: u64,
) -> (Arc<Connection>, Queues) {
    let hello = ClientHello {
        client_id: client_id.into(),
        name: client_id.into(),
        player_v1_support: Some(PlayerSupport {
            supported_formats: formats,
            buffer_capacity: capacity,
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
    (Arc::new(conn), queues)
}

fn available(conn: &Connection, available: bool) {
    conn.apply_state(&ClientState {
        available: Some(available),
        ..Default::default()
    });
}

fn state(conn: &Connection, payload: &str) {
    let text = format!(r#"{{"type":"client/state","payload":{payload}}}"#);
    let crate::protocol::ClientMessage::State(s) =
        crate::protocol::ClientMessage::decode(&text).unwrap()
    else {
        panic!("not a client/state");
    };
    conn.apply_state(&s);
}

/// Control messages queued so far, by type.
fn control(q: &mut Queues) -> Vec<&'static str> {
    let mut out = Vec::new();
    while let Ok(msg) = q.control.try_recv() {
        out.push(match msg {
            ServerMessage::StreamStart(_) => "start",
            ServerMessage::StreamClear(_) => "clear",
            ServerMessage::StreamEnd(_) => "end",
            other => panic!("unexpected {other:?}"),
        });
    }
    out
}

/// Audio chunks queued so far: (timestamp, payload bytes).
fn audio(q: &mut Queues) -> Vec<(i64, usize)> {
    payloads(q)
        .into_iter()
        .map(|(ts, payload)| (ts, payload.len()))
        .collect()
}

/// Audio chunks queued so far: (timestamp, payload).
fn payloads(q: &mut Queues) -> Vec<(i64, Vec<u8>)> {
    let mut out = Vec::new();
    while let Ok((_, frame)) = q.audio.try_recv() {
        let f = BinaryFrame::decode(&frame, HeaderLayout::Legacy).unwrap();
        assert_eq!(f.kind, binary::AUDIO_CHUNK);
        out.push((f.timestamp_us, f.payload.to_vec()));
    }
    out
}

/// The player object of the next control message, a `stream/start`.
fn stream_start(q: &mut Queues) -> StreamStartPlayer {
    match q.control.try_recv() {
        Ok(ServerMessage::StreamStart(start)) => start.player.unwrap(),
        other => panic!("expected stream/start, got {other:?}"),
    }
}

/// Decodes FLAC chunks with claxon, behind the `stream/start` header.
fn decode_flac(start: &StreamStartPlayer, chunks: &[(i64, Vec<u8>)]) -> Vec<i32> {
    use base64::Engine;
    let mut bytes = base64::engine::general_purpose::STANDARD
        .decode(
            start
                .codec_header
                .as_ref()
                .expect("FLAC has a codec header"),
        )
        .unwrap();
    for (_, payload) in chunks {
        bytes.extend_from_slice(payload);
    }
    let mut reader = claxon::FlacReader::new(&bytes[..]).unwrap();
    reader.samples().map(Result::unwrap).collect()
}

/// Each chunk starts where the frames before it end, to the microsecond.
fn assert_contiguous(chunks: &[(i64, usize)], frame_bytes: usize, rate: i64) {
    let mut frames = 0;
    for &(ts, len) in chunks {
        assert_eq!(ts, chunks[0].0 + frames * 1_000_000 / rate, "{chunks:?}");
        frames += (len / frame_bytes) as i64;
    }
}

async fn sleep_ms(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

/// A live source of 20 ms silent chunks, each held back by its delay.
struct Delayed {
    delays_ms: std::vec::IntoIter<u64>,
}

impl Delayed {
    fn new(delays_ms: Vec<u64>) -> Box<Self> {
        Box::new(Self {
            delays_ms: delays_ms.into_iter(),
        })
    }
}

#[async_trait]
impl LiveSource for Delayed {
    fn format(&self) -> AudioFormat {
        SESSION_FORMAT
    }

    fn is_live(&self) -> bool {
        true
    }

    async fn next_chunk(&mut self) -> Result<Option<PcmChunk>, AudioError> {
        let Some(ms) = self.delays_ms.next() else {
            return Ok(None);
        };
        sleep_ms(ms).await;
        Ok(Some(PcmChunk::new(
            SESSION_FORMAT,
            vec![0.0; CHUNK_FRAMES * 2],
        )))
    }
}

#[test]
fn send_ahead_follows_the_players_timing() {
    let timing = |min, lead, delay| PlayerTiming {
        min_buffer_ms: min,
        required_lead_time_ms: lead,
        output_delay_ms: delay,
        buffer_capacity: 0,
    };
    assert_eq!(send_ahead_us(&timing(None, None, 0), true), 250_000);
    assert_eq!(send_ahead_us(&timing(None, None, 100), false), 350_000);
    assert_eq!(
        send_ahead_us(&timing(Some(400), Some(1000), 100), true),
        500_000
    );
    assert_eq!(
        send_ahead_us(&timing(Some(400), Some(1000), 100), false),
        1_100_000
    );
    assert_eq!(send_ahead_us(&timing(None, Some(1000), 0), true), 250_000);
    assert_eq!(send_ahead_us(&timing(Some(80), None, 0), false), 80_000);
}

#[tokio::test(start_paused = true)]
async fn streams_a_live_source_on_one_timeline() {
    let clock = ServerClock::new();
    let (conn, mut q) = player(clock, vec![pcm(96_000, 24), pcm(48_000, 16)], 0);
    available(&conn, true);
    let sine = SineSource::new(SESSION_FORMAT, 440.0).paced(clock);
    let stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    stream.add_player(conn.clone());
    sleep_ms(205).await;

    let Ok(ServerMessage::StreamStart(start)) = q.control.try_recv() else {
        panic!("expected stream/start first");
    };
    assert_eq!(start.player.unwrap().format(), pcm(48_000, 16));
    let chunks = audio(&mut q);
    assert_eq!(chunks.len(), 10, "one per 20 ms");
    // The first chunk was ready at 20 ms and plays 250 ms after.
    assert_eq!(chunks[0], (270_000, CHUNK_BYTES));
    assert_contiguous(&chunks, 4, 48_000);
    stream.stop().await;
    assert_eq!(control(&mut q), ["end"]);
}

#[tokio::test(start_paused = true)]
async fn a_stalled_source_shifts_the_timeline_forward() {
    let clock = ServerClock::new();
    let (conn, mut q) = player(clock, vec![pcm(48_000, 16)], 0);
    available(&conn, true);
    let stream = AudioStream::start(clock, Delayed::new(vec![20, 20, 20, 1000, 20, 20])).unwrap();
    stream.add_player(conn.clone());
    sleep_ms(1400).await;

    let ts: Vec<i64> = audio(&mut q).iter().map(|c| c.0).collect();
    // 20, 40, 60 ms arrivals play from 270 ms. The fourth arrives at
    // 1060 ms, long after 330 ms has played, so it goes 250 ms from then
    // and the rest follow it.
    assert_eq!(
        ts,
        [270_000, 290_000, 310_000, 1_310_000, 1_330_000, 1_350_000]
    );
    assert!(stream.is_finished(), "played out by 1.37 s");
    assert_eq!(control(&mut q), ["start", "end"]);
}

#[tokio::test(start_paused = true)]
async fn a_late_chunk_within_the_tolerance_keeps_the_timeline() {
    let clock = ServerClock::new();
    let (conn, mut q) = player(clock, vec![pcm(48_000, 16)], 0);
    available(&conn, true);
    // The third chunk comes 15 ms late, the fourth 25 ms after that.
    let stream = AudioStream::start(clock, Delayed::new(vec![20, 20, 35, 45, 20])).unwrap();
    stream.add_player(conn.clone());
    sleep_ms(500).await;

    let ts: Vec<i64> = audio(&mut q).iter().map(|c| c.0).collect();
    // At 75 ms the third goes out 245 ms ahead and keeps its place. At
    // 120 ms the floor is 370 ms and the fourth's place 330 ms: 40 ms
    // behind, so it shifts to the floor.
    assert_eq!(ts, [270_000, 290_000, 310_000, 370_000, 390_000]);
    drop(stream);
}

#[tokio::test(start_paused = true)]
async fn send_ahead_comes_from_client_state() {
    let clock = ServerClock::new();
    let (conn, mut q) = player(clock, vec![pcm(48_000, 16)], 0);
    state(
        &conn,
        r#"{"player":{"min_buffer_ms":400,"required_lead_time_ms":1000,"output_delay_ms":100}}"#,
    );
    let sine = SineSource::new(SESSION_FORMAT, 440.0).paced(clock);
    let stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    stream.add_player(conn.clone());
    sleep_ms(25).await;
    // Live, so the required lead is left out: 400 + 100 ms.
    assert_eq!(audio(&mut q)[0].0, 20_000 + 500_000);
    stream.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_source_that_is_not_live_fills_only_the_players_buffer() {
    let clock = ServerClock::new();
    // 400 ms of buffer: more than the 250 ms send-ahead, as any real player.
    let (conn, mut q) = player(clock, vec![pcm(48_000, 16)], 20 * CHUNK_BYTES as u64);
    available(&conn, true);
    let sine = SineSource::new(SESSION_FORMAT, 440.0);
    let stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    stream.add_player(conn.clone());

    sleep_ms(100).await;
    let chunks = audio(&mut q);
    assert_eq!(chunks.len(), 20, "as much as the buffer holds, at once");
    assert_eq!(chunks[0].0, 250_000);
    assert_contiguous(&chunks, 4, 48_000);

    // Room comes back as each chunk finishes playing, the first at 270 ms.
    sleep_ms(175).await;
    assert_eq!(audio(&mut q), [(650_000, CHUNK_BYTES)]);
    sleep_ms(40).await;
    assert_eq!(audio(&mut q).len(), 2);
    stream.stop().await;
}

#[tokio::test(start_paused = true)]
async fn without_players_a_source_is_paced_not_drained() {
    let clock = ServerClock::new();
    let sine = SineSource::new(SESSION_FORMAT, 440.0).limit_frames(48_000);
    let mut stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    stream.finished().await;
    // One second of audio, starting 250 ms out, ends at 1.25 s.
    assert_eq!(clock.now_us(), 1_250_000);
}

#[tokio::test(start_paused = true)]
async fn the_last_source_plays_out_before_stream_end() {
    let clock = ServerClock::new();
    let (conn, mut q) = player(clock, vec![pcm(48_000, 16)], 0);
    available(&conn, true);
    // Long enough that the second is queued while the first still plays.
    let a = SineSource::new(SESSION_FORMAT, 440.0).limit_frames(10_000);
    let mut stream = AudioStream::start(clock, Box::new(a)).unwrap();
    stream.add_player(conn.clone());
    let b = SineSource::new(SESSION_FORMAT, 880.0).limit_frames(10_000);
    stream.queue_next(Box::new(b)).unwrap();
    stream.finished().await;

    // Gapless: the second source's frames fill out the first's last chunk,
    // so 20 000 frames are 20 whole chunks and 800 frames.
    let chunks = audio(&mut q);
    assert_eq!(chunks.len(), 21);
    assert!(chunks[..20].iter().all(|c| c.1 == CHUNK_BYTES));
    assert_eq!(chunks[20], (650_000, 800 * 4));
    assert_contiguous(&chunks, 4, 48_000);
    assert_eq!(control(&mut q), ["start", "end"], "nothing between them");
    // The last chunk ends at 666 666 µs; tokio's timer rounds up to the ms.
    assert_eq!(clock.now_us(), 667_000);
}

#[tokio::test(start_paused = true)]
async fn seek_clears_and_restarts_the_timeline() {
    let clock = ServerClock::new();
    let (conn, mut q) = player(clock, vec![pcm(48_000, 16)], 0);
    available(&conn, true);
    let sine = SineSource::new(SESSION_FORMAT, 440.0).paced(clock);
    let stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    stream.add_player(conn.clone());
    sleep_ms(105).await;
    assert_eq!(audio(&mut q).last().unwrap().0, 350_000);

    let sine = SineSource::new(SESSION_FORMAT, 220.0).paced(clock);
    stream.seek(Box::new(sine)).unwrap();
    sleep_ms(30).await;
    assert_eq!(control(&mut q), ["start", "clear"]);
    // The new source's first chunk is ready at 125 ms.
    assert_eq!(audio(&mut q), [(375_000, CHUNK_BYTES)]);
    stream.stop().await;
    assert_eq!(control(&mut q), ["end"]);
}

#[tokio::test(start_paused = true)]
async fn a_player_gets_stream_start_only_while_available() {
    let clock = ServerClock::new();
    let (conn, mut q) = player(clock, vec![pcm(48_000, 16)], 0);
    let sine = SineSource::new(SESSION_FORMAT, 440.0).paced(clock);
    let stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    stream.add_player(conn.clone());
    sleep_ms(50).await;
    assert!(control(&mut q).is_empty(), "no client/state yet");
    assert!(audio(&mut q).is_empty());

    available(&conn, true);
    sleep_ms(20).await;
    assert_eq!(control(&mut q), ["start"]);
    // It joins the timeline already running: the 60 ms chunk, the third
    // since it was placed at 20 ms + 250 ms.
    assert_eq!(audio(&mut q), [(310_000, CHUNK_BYTES)]);

    available(&conn, false);
    sleep_ms(40).await;
    assert!(audio(&mut q).is_empty());
    assert!(control(&mut q).is_empty(), "the client stops itself");

    available(&conn, true);
    sleep_ms(20).await;
    assert_eq!(control(&mut q), ["start"]);
    assert_eq!(audio(&mut q).len(), 1);

    stream.remove_player("kitchen");
    sleep_ms(20).await;
    assert_eq!(control(&mut q), ["end"]);
    assert!(audio(&mut q).is_empty());
    stream.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_player_without_the_session_rate_is_resampled() {
    let clock = ServerClock::new();
    let (conn, mut q) = player(clock, vec![pcm(44_100, 24)], 0);
    available(&conn, true);
    let sine = SineSource::new(SESSION_FORMAT, 440.0).paced(clock);
    let stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    stream.add_player(conn.clone());
    sleep_ms(1000).await;

    let Ok(ServerMessage::StreamStart(start)) = q.control.try_recv() else {
        panic!("expected stream/start");
    };
    assert_eq!(start.player.unwrap().format(), pcm(44_100, 24));
    let chunks = audio(&mut q);
    assert_eq!(chunks[0].0, 270_000);
    assert_contiguous(&chunks, 6, 44_100);
    let frames: usize = chunks.iter().map(|c| c.1 / 6).sum();
    // 49 chunks of 882 frames, less what the resampler holds back.
    assert!((49 * 882 - 1000..=49 * 882).contains(&frames), "{frames}");
    stream.stop().await;
}

#[tokio::test(start_paused = true)]
async fn players_it_cannot_serve_are_skipped() {
    let clock = ServerClock::new();
    let mono = StreamFormat {
        channels: 1,
        ..pcm(48_000, 16)
    };
    let (conn, mut q) = player(clock, vec![opus(44_100), flac(32), mono], 0);
    available(&conn, true);
    let sine = SineSource::new(SESSION_FORMAT, 440.0).paced(clock);
    let stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    stream.add_player(conn.clone());
    sleep_ms(50).await;
    assert!(control(&mut q).is_empty());
    assert!(audio(&mut q).is_empty());
    stream.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_flac_player_gets_its_header_and_a_frame_a_chunk() {
    let clock = ServerClock::new();
    let (conn, mut q) = player(clock, vec![flac(24), pcm(48_000, 16)], 0);
    available(&conn, true);
    let sine = SineSource::new(SESSION_FORMAT, 440.0).paced(clock);
    let stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    stream.add_player(conn.clone());
    sleep_ms(105).await;

    let start = stream_start(&mut q);
    assert_eq!(start.format(), flac(24));
    let chunks = payloads(&mut q);
    let ts: Vec<i64> = chunks.iter().map(|c| c.0).collect();
    assert_eq!(ts, [270_000, 290_000, 310_000, 330_000, 350_000]);
    // Compressed, and lossless: the sine from phase zero, at 24 bits.
    assert!(chunks.iter().all(|c| c.1.len() < 960 * 6 / 2));
    let samples = decode_flac(&start, &chunks);
    assert_eq!(samples.len(), 5 * 960 * 2);
    assert_eq!(&samples[..2], [0, 0]);
    assert!(samples.iter().any(|s| s.unsigned_abs() > 4_000_000));
    stream.stop().await;
}

#[tokio::test(start_paused = true)]
async fn opus_chunks_are_stamped_early_by_the_encoders_lookahead() {
    let clock = ServerClock::new();
    let (conn, mut q) = player(clock, vec![opus(48_000)], 0);
    available(&conn, true);
    let sine = SineSource::new(SESSION_FORMAT, 440.0).paced(clock);
    let stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    stream.add_player(conn.clone());
    sleep_ms(65).await;

    let start = stream_start(&mut q);
    assert_eq!((start.format(), start.codec_header), (opus(48_000), None));
    let delay = OpusEncoder::new(opus(48_000)).unwrap().delay_us();
    let chunks = payloads(&mut q);
    let ts: Vec<i64> = chunks.iter().map(|c| c.0).collect();
    assert_eq!(ts, [270_000 - delay, 290_000 - delay, 310_000 - delay]);
    let mut decoder = opus::Decoder::new(48_000, opus::Channels::Stereo).unwrap();
    for (_, packet) in &chunks {
        let mut out = vec![0.0; 960 * 2];
        assert_eq!(decoder.decode_float(packet, &mut out, false).unwrap(), 960);
    }
    stream.stop().await;
}

#[tokio::test(start_paused = true)]
async fn players_on_one_format_share_its_encoder() {
    let clock = ServerClock::new();
    // Resampled, so an encoder of its own would hold back different
    // frames, and stamp them differently, from the one already running.
    let (kitchen, mut kq) = named_player(clock, "kitchen", vec![pcm(44_100, 16)], 0);
    let (den, mut dq) = named_player(clock, "den", vec![pcm(44_100, 16)], 0);
    let (attic, mut aq) = named_player(clock, "attic", vec![flac(16)], 0);
    available(&kitchen, true);
    available(&attic, true);
    let sine = SineSource::new(SESSION_FORMAT, 440.0).paced(clock);
    let stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    for conn in [&kitchen, &den, &attic] {
        stream.add_player((*conn).clone());
    }
    sleep_ms(65).await;
    available(&den, true);
    sleep_ms(100).await;

    let k = payloads(&mut kq);
    let d = payloads(&mut dq);
    assert!(d.len() >= 4, "{}", d.len());
    // The den's chunks are the kitchen's latest, to the byte and the µs.
    assert_eq!(d[..], k[k.len() - d.len()..]);
    assert_eq!(stream_start(&mut dq).format(), pcm(44_100, 16));
    assert_eq!(payloads(&mut aq).len(), 8, "FLAC encoded beside it");
    stream.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_format_request_gets_a_fresh_stream_start() {
    let clock = ServerClock::new();
    let (conn, mut q) = player(clock, vec![pcm(48_000, 16), flac(16), opus(48_000)], 0);
    available(&conn, true);
    let sine = SineSource::new(SESSION_FORMAT, 440.0).paced(clock);
    let stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    stream.add_player(conn.clone());
    sleep_ms(45).await;
    assert_eq!(stream_start(&mut q).format(), pcm(48_000, 16));
    assert_eq!(
        audio(&mut q),
        [(270_000, CHUNK_BYTES), (290_000, CHUNK_BYTES)]
    );

    conn.request_format(RequestFormatPlayer {
        codec: Some(Codec::Flac),
        ..Default::default()
    });
    sleep_ms(40).await;
    let start = stream_start(&mut q);
    assert_eq!(start.format(), flac(16));
    // FLAC from the next chunk on, on the same timeline.
    let chunks = payloads(&mut q);
    assert_eq!(
        chunks.iter().map(|c| c.0).collect::<Vec<_>>(),
        [310_000, 330_000]
    );
    assert_eq!(decode_flac(&start, &chunks).len(), 2 * 960 * 2);

    // One it can't send changes nothing.
    conn.request_format(RequestFormatPlayer {
        sample_rate: Some(44_100),
        ..Default::default()
    });
    sleep_ms(20).await;
    assert!(control(&mut q).is_empty());
    assert_eq!(payloads(&mut q).len(), 1);
    stream.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_request_made_before_the_stream_picks_the_first_format() {
    let clock = ServerClock::new();
    let (conn, mut q) = player(clock, vec![pcm(48_000, 16), opus(48_000)], 0);
    available(&conn, true);
    conn.request_format(RequestFormatPlayer {
        codec: Some(Codec::Opus),
        ..Default::default()
    });
    let sine = SineSource::new(SESSION_FORMAT, 440.0).paced(clock);
    let stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    stream.add_player(conn.clone());
    sleep_ms(25).await;
    assert_eq!(stream_start(&mut q).format(), opus(48_000));
    assert!(control(&mut q).is_empty(), "one stream/start, not two");
    stream.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_late_joiner_is_sent_the_chunks_still_ahead() {
    let clock = ServerClock::new();
    let capacity = 20 * CHUNK_BYTES as u64;
    let (kitchen, mut kq) = named_player(clock, "kitchen", vec![pcm(48_000, 16)], capacity);
    let (den, mut dq) = named_player(clock, "den", vec![pcm(48_000, 16)], capacity);
    available(&kitchen, true);
    available(&den, true);
    let sine = SineSource::new(SESSION_FORMAT, 440.0);
    let stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    stream.add_player(kitchen.clone());
    sleep_ms(100).await;
    stream.add_player(den.clone());
    sleep_ms(1).await;

    // The kitchen's buffer was filled at once, 250 to 650 ms. At 100 ms
    // the den is sent the part of it at least its 250 ms send-ahead out.
    assert_eq!(control(&mut dq), ["start"]);
    let k = payloads(&mut kq);
    let d = payloads(&mut dq);
    assert_eq!((k[0].0, k.len()), (250_000, 20));
    assert_eq!(d[0].0, 350_000);
    assert_eq!(d[..], k[5..], "the same chunks, to the byte and the µs");

    // Then both are sent the rest together.
    sleep_ms(200).await;
    let k = payloads(&mut kq);
    assert_eq!(k[0].0, 650_000);
    assert_eq!(payloads(&mut dq), k);
    stream.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_late_joiner_takes_only_the_latest_chunks_its_buffer_holds() {
    let clock = ServerClock::new();
    let (kitchen, mut kq) = named_player(
        clock,
        "kitchen",
        vec![pcm(48_000, 16)],
        20 * CHUNK_BYTES as u64,
    );
    let (den, mut dq) = named_player(clock, "den", vec![pcm(48_000, 16)], 4 * CHUNK_BYTES as u64);
    available(&kitchen, true);
    available(&den, true);
    let sine = SineSource::new(SESSION_FORMAT, 440.0);
    let stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    stream.add_player(kitchen.clone());
    sleep_ms(100).await;
    stream.add_player(den.clone());
    sleep_ms(1).await;

    // Four chunks fit, so the four that end where the stream's cursor is:
    // what comes next follows them without a gap.
    let k = payloads(&mut kq);
    let d = payloads(&mut dq);
    assert_eq!(d[..], k[16..]);
    assert_eq!(d[0].0, 570_000);
    stream.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_late_joiner_on_a_new_format_is_encoded_from_kept_audio() {
    let clock = ServerClock::new();
    let (kitchen, mut kq) = named_player(
        clock,
        "kitchen",
        vec![pcm(48_000, 16)],
        20 * CHUNK_BYTES as u64,
    );
    let (attic, mut aq) = named_player(clock, "attic", vec![flac(16)], 0);
    available(&kitchen, true);
    available(&attic, true);
    let sine = SineSource::new(SESSION_FORMAT, 440.0);
    let stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    stream.add_player(kitchen.clone());
    sleep_ms(100).await;
    stream.add_player(attic.clone());
    sleep_ms(1).await;

    let start = stream_start(&mut aq);
    let k = payloads(&mut kq);
    let a = payloads(&mut aq);
    assert_eq!(a[0].0, 350_000);
    let ts: Vec<i64> = a.iter().map(|c| c.0).collect();
    assert_eq!(ts, k[5..].iter().map(|c| c.0).collect::<Vec<_>>());
    // Lossless: the same samples the kitchen plays over the same span.
    let pcm: Vec<i32> = k[5..]
        .iter()
        .flat_map(|(_, p)| {
            p.chunks(2)
                .map(|s| i32::from(i16::from_le_bytes([s[0], s[1]])))
        })
        .collect();
    assert_eq!(decode_flac(&start, &a), pcm);
    stream.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_removed_player_gets_stream_end_even_while_paused() {
    let clock = ServerClock::new();
    let (conn, mut q) = player(clock, vec![pcm(48_000, 16)], 0);
    available(&conn, true);
    let sine = SineSource::new(SESSION_FORMAT, 440.0).paced(clock);
    let stream = AudioStream::start(clock, Box::new(sine)).unwrap();
    stream.add_player(conn.clone());
    sleep_ms(30).await;
    available(&conn, false);
    sleep_ms(30).await;
    // Awaiting the removal means the stream has queued all it will.
    stream.remove_player("kitchen").await;
    assert_eq!(control(&mut q), ["start", "end"]);

    let ended = stream.ended();
    stream.stop().await;
    tokio::time::timeout(Duration::from_secs(1), ended)
        .await
        .expect("ended() resolves once the task is gone");
}
