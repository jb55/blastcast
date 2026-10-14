//! Streaming session audio to Sendspin players: one shared timeline, each
//! player's chunks sent far enough ahead for its buffer and no further than
//! it can hold.
//!
//! An [`AudioStream`] runs one source on its own task and sends it to
//! every player added to it. A source is read in 20 ms chunks and each chunk
//! is placed once on the stream's [`Timeline`](tas_audio::Timeline), so
//! every player gets the same play time for the same audio. That is what
//! keeps a group in sync later.
//!
//! **Send-ahead.** A chunk must reach a player before it plays, with time to
//! spare for the player's buffer. Each player's floor is
//! [`send_ahead_us`]; the timeline is kept the largest floor ahead of now.
//! When the source stalls (AirPlay drops out, a decoder hiccups), the
//! timeline would fall behind now. It is shifted forward instead, so the
//! next chunk again plays a send-ahead from now. This is aiosendspin's
//! rebase (`push_stream.py`, `_resolve_channel_play_start`), with one
//! difference: a chunk up to [`SHIFT_TOLERANCE_US`] late leaves the timeline
//! alone. A paced source wakes a few µs late all the time, and aiosendspin's
//! exact floor turns each of those into a gap of a few µs in the timestamps.
//! It is not sendspin-go's fixed 500 ms lead off a fresh `now()` every tick,
//! which jitters every chunk's timestamp by the scheduler's lateness.
//!
//! **Buffer tracking.** A source that isn't live may be read ahead as far as
//! the players hold: a [`BufferTracker`] per player counts the bytes sent
//! and not yet played against its `buffer_capacity`, capped at 30 s. A live
//! source is sent as it arrives; a chunk that wouldn't fit is dropped for
//! that player rather than holding up the rest.
//!
//! **Lifecycle.** A player gets `stream/start` when it is added, or when it
//! becomes available again. [`AudioStream::seek`] sends `stream/clear` and
//! restarts the timeline. [`AudioStream::queue_next`] sends nothing: the
//! next source's first frame follows the last one's in the same chunk, which
//! is a gapless track change. `stream/end` goes out only on a real stop:
//! [`AudioStream::stop`], removing a player, or the last source running out
//! once its audio has played.
//!
//! **Late joiners.** A player that starts while the stream runs is not
//! made to wait for the cursor, which a source that isn't live may have
//! read seconds ahead. It is sent again the chunks already sent to others
//! that start at least its send-ahead from now, and never less than
//! [`LATE_JOIN_MIN_LEAD_US`]: as many of the latest as its buffer takes,
//! then the rest of the stream with everyone. The chunks come from its
//! format's [`Encoding`], or, for a format nobody was on, are encoded
//! afresh from the session audio kept for that. This is aiosendspin's
//! catch-up (`push_stream.py`, `LATE_JOINER_MIN_LEAD_US`).
//!
//! **Formats.** Each player is sent the first format in its hello's list
//! that the server encodes, preferring one at the session rate (see
//! [`pick_format`]): PCM, FLAC or Opus. Players on the same format share
//! one [`Encoder`], so a chunk is encoded once per format, not per player,
//! and a player joining mid-stream gets the same timestamps as the rest.
//! A `stream/request-format` moves the player to the format it asks for,
//! answered with a fresh `stream/start`.

mod buffer;
mod encode;
mod feed;
mod format;

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use tas_audio::{
    AudioError, LiveSource, SESSION_FORMAT, ServerClock, Span, Timeline, to_session_format,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

pub use buffer::{BufferTracker, DEFAULT_CAPACITY_BYTES, MAX_BUFFERED_US};
pub use encode::{Encoder, FlacEncoder, OpusEncoder, PcmEncoder, can_encode};
pub use format::{pick_format, requested_format};

use crate::connection::{Connection, SendError};
use crate::protocol::{
    RequestFormatPlayer, ServerMessage, StreamClear, StreamEnd, StreamFormat, StreamStart,
    StreamStartPlayer, binary,
};
use crate::status::PlayerTiming;
use feed::{Feed, FeedItem};

/// Frames per chunk: 20 ms of session audio. The spec allows 15-150 ms.
pub const CHUNK_FRAMES: usize = SESSION_FORMAT.sample_rate as usize / 50;

/// The send-ahead for a player that reports no buffer needs, and for a
/// stream with no players: aiosendspin's `DEFAULT_INITIAL_DELAY_US`.
pub const DEFAULT_SEND_AHEAD_US: i64 = 250_000;

/// How far the timeline may fall behind the send-ahead floor before it is
/// shifted: one chunk. Less is scheduler jitter, which a player's buffer
/// absorbs; shifting for it would put a gap in the audio.
pub const SHIFT_TOLERANCE_US: i64 = 20_000;

/// How far ahead of its play time a player needs each chunk:
/// `max(min_buffer, required_lead) + output_delay`, leaving out the
/// required lead for a live source. A live source can't grow the player's
/// queue once playback starts, so extra lead would be permanent latency.
/// A player that reports neither buffer figure gets 250 ms plus its delay.
pub fn send_ahead_us(timing: &PlayerTiming, live: bool) -> i64 {
    let lead = timing.required_lead_time_ms.filter(|_| !live);
    let buffer_ms = match (timing.min_buffer_ms, lead) {
        (None, None) => DEFAULT_SEND_AHEAD_US / 1000,
        (min, lead) => i64::from(min.unwrap_or(0).max(lead.unwrap_or(0))),
    };
    (buffer_ms + i64::from(timing.output_delay_ms)) * 1000
}

/// The least lead a late joiner's first chunk is sent with:
/// aiosendspin's `LATE_JOINER_MIN_LEAD_US`. It is below
/// [`DEFAULT_SEND_AHEAD_US`] so a stream running at that send-ahead always
/// has a chunk this far ahead to replay.
pub const LATE_JOIN_MIN_LEAD_US: i64 = 100_000;

const PLAYER_ROLE: &str = "player";

/// A running stream. Commands are queued to its task; dropping the handle
/// stops it the way [`AudioStream::stop`] does.
pub struct AudioStream {
    commands: mpsc::UnboundedSender<Command>,
    task: JoinHandle<()>,
    /// Closed when the task ends.
    ended: watch::Receiver<()>,
}

enum Command {
    Add(Arc<Connection>),
    Remove(String, oneshot::Sender<()>),
    Seek(Box<dyn LiveSource>),
    Queue(Box<dyn LiveSource>),
    Stop,
}

impl AudioStream {
    /// Starts streaming `source`, timed on `clock`. The clock must be the
    /// one the players sync to, the server's `ServerConfig::clock`.
    pub fn start(clock: ServerClock, source: Box<dyn LiveSource>) -> Result<Self, AudioError> {
        let source = to_session_format(source)?;
        let (commands, rx) = mpsc::unbounded_channel();
        let (ended_tx, ended) = watch::channel(());
        let streamer = Streamer {
            _ended: ended_tx,
            history: VecDeque::new(),
            clock,
            live: source.is_live(),
            feed: Some(Feed::spawn(source)),
            queued: VecDeque::new(),
            timeline: None,
            outputs: Vec::new(),
            encodings: Vec::new(),
            pending: None,
            wake_us: None,
        };
        let task = tokio::spawn(streamer.run(rx));
        Ok(Self {
            commands,
            task,
            ended,
        })
    }

    /// Streams to `conn` from the next chunk on. A player already in the
    /// stream under the same `client_id` (a reconnect) is replaced.
    pub fn add_player(&self, conn: Arc<Connection>) {
        let _ = self.commands.send(Command::Add(conn));
    }

    /// Stops streaming to a player, sending it `stream/end` if it had a
    /// `stream/start`. Await the result to know the stream will queue
    /// nothing more for it, before another stream starts on it.
    pub fn remove_player(&self, client_id: impl Into<String>) -> Removed {
        let (done, rx) = oneshot::channel();
        let _ = self.commands.send(Command::Remove(client_id.into(), done));
        Removed(rx)
    }

    /// Jumps to `source`: the players drop what they hold (`stream/clear`)
    /// and the timeline starts again a send-ahead from now. A seek and a
    /// track skip are both this.
    pub fn seek(&self, source: Box<dyn LiveSource>) -> Result<(), AudioError> {
        let source = to_session_format(source)?;
        let _ = self.commands.send(Command::Seek(source));
        Ok(())
    }

    /// Plays `source` when the current one runs out, with no gap and
    /// nothing sent to the players.
    pub fn queue_next(&self, source: Box<dyn LiveSource>) -> Result<(), AudioError> {
        let source = to_session_format(source)?;
        let _ = self.commands.send(Command::Queue(source));
        Ok(())
    }

    /// Whether the stream has ended: stopped, or its last source played out.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    /// Waits for the stream to end on its own, its last source played out.
    pub async fn finished(&mut self) {
        let _ = (&mut self.task).await;
    }

    /// Resolves once the stream has ended, however it ended, without
    /// holding the stream.
    pub fn ended(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut ended = self.ended.clone();
        async move { while ended.changed().await.is_ok() {} }
    }

    /// Stops now: every player gets `stream/end` and drops what it holds.
    pub async fn stop(mut self) {
        let _ = self.commands.send(Command::Stop);
        self.finished().await;
    }
}

/// What [`AudioStream::remove_player`] returns: resolves once the stream
/// has let go of the player, or has ended.
pub struct Removed(oneshot::Receiver<()>);

impl Future for Removed {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        std::pin::Pin::new(&mut self.0).poll(cx).map(|_| ())
    }
}

/// One player in the stream.
struct Output {
    conn: Arc<Connection>,
    /// The format it negotiated, which names the [`Encoding`] it is sent.
    format: StreamFormat,
    tracker: BufferTracker,
    /// Its `stream/request-format`s, answered as they come.
    requests: watch::Receiver<Option<RequestFormatPlayer>>,
    /// It has had `stream/start` and is being sent audio.
    started: bool,
    /// It has had `stream/start` and no `stream/end` since. A player that
    /// went unavailable is paused, not started, but still open.
    open: bool,
}

impl Output {
    fn send_control(&self, msg: ServerMessage) {
        if let Err(e) = self.conn.send(msg) {
            tracing::debug!(client_id = self.conn.client_id(), error = %e, "stream message not sent");
        }
    }

    fn start(&mut self, encoding: &Encoding) {
        let encoder = &encoding.encoder;
        self.send_control(ServerMessage::StreamStart(StreamStart {
            server_transmitted: None,
            player: Some(StreamStartPlayer::new(
                encoder.format(),
                encoder.codec_header(),
            )),
            artwork: None,
        }));
        self.tracker = BufferTracker::new(self.conn.player_status().timing.buffer_capacity);
        self.started = true;
        self.open = true;
        tracing::info!(
            client_id = self.conn.client_id(),
            format = ?encoder.format(),
            "stream started"
        );
    }

    fn end(&mut self) {
        if self.open {
            self.send_control(ServerMessage::StreamEnd(StreamEnd {
                server_transmitted: None,
                roles: Some(vec![PLAYER_ROLE.into()]),
            }));
        }
        self.started = false;
        self.open = false;
    }

    /// Answers a `stream/request-format` that came since the last look.
    /// A started player is stopped so the caller restarts it, which sends
    /// the `stream/start` the spec answers with.
    fn take_request(&mut self, encodings: &mut Vec<Encoding>) {
        if !self.requests.has_changed().unwrap_or(false) {
            return;
        }
        let Some(request) = *self.requests.borrow_and_update() else {
            return;
        };
        let client_id = self.conn.client_id();
        let support = self.conn.hello().player();
        let Some(format) = support.and_then(|s| requested_format(s, Some(self.format), &request))
        else {
            tracing::warn!(
                client_id,
                ?request,
                "can't send the format the player asked for"
            );
            return;
        };
        if let Err(e) = Encoding::find_or_add(encodings, format) {
            tracing::warn!(client_id, error = %e, "can't encode the format the player asked for");
            return;
        }
        self.format = format;
        self.started = false;
    }
}

/// The stream in one format, encoded once a chunk and sent to every
/// player that negotiated it.
struct Encoding {
    encoder: Encoder,
    /// The timeline at the format's rate, which a resampled format needs
    /// since its chunks hold a different number of frames. It starts where
    /// the shared one is, less the encoder's delay, and shifts with it.
    timeline: Timeline,
    /// It has a started player and is being encoded. One that stops is
    /// reset and placed afresh when a player starts on it again.
    running: bool,
    /// The chunks encoded that start at least [`LATE_JOIN_MIN_LEAD_US`]
    /// from now, oldest first, for late joiners.
    sent: VecDeque<(Span, Bytes)>,
}

impl Encoding {
    fn format(&self) -> StreamFormat {
        self.encoder.format()
    }

    /// The index of `format`'s encoding, made if there is none yet.
    fn find_or_add(encodings: &mut Vec<Self>, format: StreamFormat) -> Result<usize, AudioError> {
        if let Some(i) = encodings.iter().position(|e| e.format() == format) {
            return Ok(i);
        }
        encodings.push(Self {
            encoder: Encoder::new(format)?,
            timeline: Timeline::new(0, format.sample_rate),
            running: false,
            sent: VecDeque::new(),
        });
        Ok(encodings.len() - 1)
    }

    /// Starts encoding with the shared timeline at `at_us`.
    fn run(&mut self, at_us: i64) {
        self.encoder.reset();
        self.timeline = Timeline::new(at_us - self.encoder.delay_us(), self.format().sample_rate);
        self.running = true;
        self.sent.clear();
    }

    /// Encodes a chunk of session audio and keeps it for late joiners.
    /// `None` when the encoder had nothing whole to give yet, or failed.
    fn encode(&mut self, samples: &[f32]) -> Option<(Bytes, Span)> {
        let (bytes, frames) = match self.encoder.encode(samples) {
            Ok(encoded) => encoded,
            Err(e) => {
                tracing::warn!(format = ?self.format(), error = %e, "encoding failed, chunk dropped");
                return None;
            }
        };
        if frames == 0 {
            return None;
        }
        let span = self.timeline.advance(frames as u64);
        let bytes = Bytes::from(bytes);
        self.sent.push_back((span, bytes.clone()));
        Some((bytes, span))
    }
}

/// How many of the newest of `chunks` (bytes, span), given oldest first,
/// fit together into an empty buffer of `capacity_bytes`.
fn fitting_tail<'a>(
    chunks: impl DoubleEndedIterator<Item = (u64, &'a Span)>,
    capacity_bytes: u64,
) -> usize {
    let mut room = BufferTracker::new(capacity_bytes);
    let mut n = 0;
    for (bytes, span) in chunks.rev() {
        if !room.can_send(bytes, span.duration_us) {
            break;
        }
        room.register(span.end_us(), bytes, span.duration_us);
        n += 1;
    }
    n
}

enum Event {
    Command(Option<Command>),
    Feed(FeedItem),
    Wake,
}

/// The stream task's state.
struct Streamer {
    /// Dropped with the task, which is how [`AudioStream::ended`] hears.
    _ended: watch::Sender<()>,
    /// The session audio sent that starts at least
    /// [`LATE_JOIN_MIN_LEAD_US`] from now, oldest first, for a late joiner
    /// on a format no other player is on.
    history: VecDeque<(Span, Vec<f32>)>,
    clock: ServerClock,
    /// Whether the source being read is live, from its latest chunk.
    live: bool,
    /// `None` once every source has run out.
    feed: Option<Feed>,
    /// Sources queued too late for the feed, which had already finished.
    queued: VecDeque<Box<dyn LiveSource>>,
    /// The shared timeline, at the session rate. `None` until the first
    /// chunk after a start or a seek, which places it.
    timeline: Option<Timeline>,
    outputs: Vec<Output>,
    /// One per format some player negotiated.
    encodings: Vec<Encoding>,
    /// A chunk read but not yet sent: a source that isn't live waits here
    /// until every player has room for it.
    pending: Option<Vec<f32>>,
    /// When to look again at the pending chunk, or, with the feed ended,
    /// when the last chunk has played.
    wake_us: Option<i64>,
}

impl Streamer {
    async fn run(mut self, mut commands: mpsc::UnboundedReceiver<Command>) {
        loop {
            let wake = self.wake_us.map(|us| self.clock.instant_at(us));
            let can_read = self.pending.is_none();
            let event = tokio::select! {
                biased;
                cmd = commands.recv() => Event::Command(cmd),
                item = async { self.feed.as_mut().expect("guarded").recv().await },
                    if can_read && self.feed.is_some() => Event::Feed(item),
                _ = async { tokio::time::sleep_until(wake.expect("guarded")).await },
                    if wake.is_some() => Event::Wake,
            };
            match event {
                Event::Command(None | Some(Command::Stop)) => break,
                Event::Command(Some(cmd)) => self.handle(cmd),
                Event::Feed(FeedItem::Chunk { samples, live }) => {
                    self.live = live;
                    self.pending = Some(samples);
                    self.pump();
                }
                Event::Feed(FeedItem::End) => {
                    self.feed = None;
                    self.next_feed();
                }
                Event::Wake if self.pending.is_some() => self.pump(),
                Event::Wake => {
                    tracing::info!("stream played out");
                    break;
                }
            }
        }
        for out in &mut self.outputs {
            out.end();
        }
    }

    fn handle(&mut self, cmd: Command) {
        match cmd {
            Command::Add(conn) => {
                self.add(conn);
                // A chunk waiting for room may wait a while; the new player
                // starts and catches up now.
                if self.pending.is_some() {
                    self.pump();
                }
            }
            Command::Remove(client_id, done) => {
                if let Some(i) = self.position(&client_id) {
                    self.outputs.remove(i).end();
                }
                let _ = done.send(());
            }
            Command::Seek(source) => {
                self.queued.clear();
                self.feed = Some(Feed::spawn(source));
                self.pending = None;
                self.wake_us = None;
                self.timeline = None;
                self.history.clear();
                for out in self.outputs.iter_mut().filter(|o| o.started) {
                    out.send_control(ServerMessage::StreamClear(StreamClear {
                        server_transmitted: None,
                        roles: Some(vec![PLAYER_ROLE.into()]),
                    }));
                    out.tracker.clear();
                }
                for encoding in &mut self.encodings {
                    encoding.encoder.reset();
                    encoding.sent.clear();
                }
            }
            Command::Queue(source) => {
                let source = match &self.feed {
                    Some(feed) => feed.queue(source).err(),
                    None => Some(source),
                };
                // The reader has finished, but chunks it read may still be
                // in its channel: the source waits for its `End`.
                if let Some(source) = source {
                    self.queued.push_back(source);
                    if self.feed.is_none() {
                        self.next_feed();
                    }
                }
            }
            Command::Stop => unreachable!("handled by run"),
        }
    }

    /// Reads the sources queued after the feed finished, if any; with none,
    /// waits for the last chunk to play. The timeline carries on either
    /// way, shifted forward if the gap left it behind.
    fn next_feed(&mut self) {
        match self.queued.pop_front() {
            Some(source) => {
                let feed = Feed::spawn(source);
                for source in self.queued.drain(..) {
                    let _ = feed.queue(source);
                }
                self.feed = Some(feed);
                self.wake_us = None;
            }
            None => self.wake_us = Some(self.timeline.as_ref().map_or(0, Timeline::cursor_us)),
        }
    }

    fn position(&self, client_id: &str) -> Option<usize> {
        self.outputs
            .iter()
            .position(|o| o.conn.client_id() == client_id)
    }

    fn add(&mut self, conn: Arc<Connection>) {
        let client_id = conn.client_id().to_owned();
        if !conn.has_role(PLAYER_ROLE) {
            tracing::warn!(
                client_id,
                "not streaming to a client without the player role"
            );
            return;
        }
        // A request sent before the stream began counts from the start.
        let requests = conn.watch_format_request();
        let request = *requests.borrow();
        let Some(format) = conn.hello().player().and_then(|support| {
            request
                .and_then(|r| requested_format(support, None, &r))
                .or_else(|| pick_format(support))
        }) else {
            tracing::warn!(client_id, "player lists no format this server sends");
            return;
        };
        if let Err(e) = Encoding::find_or_add(&mut self.encodings, format) {
            tracing::warn!(client_id, error = %e, "can't encode for player");
            return;
        }
        let output = Output {
            tracker: BufferTracker::new(0),
            format,
            requests,
            conn,
            started: false,
            open: false,
        };
        match self.position(&client_id) {
            Some(i) => {
                if !Arc::ptr_eq(&self.outputs[i].conn, &output.conn) {
                    self.outputs[i] = output;
                }
            }
            None => self.outputs.push(output),
        }
    }

    /// The largest send-ahead among players that can play now.
    fn send_ahead_us(&self) -> i64 {
        self.outputs
            .iter()
            .filter(|o| o.conn.is_available())
            .map(|o| send_ahead_us(&o.conn.player_status().timing, self.live))
            .max()
            .unwrap_or(DEFAULT_SEND_AHEAD_US)
    }

    /// Sends the pending chunk, or sets `wake_us` for when to try again.
    fn pump(&mut self) {
        let Some(frames) = self
            .pending
            .as_ref()
            .map(|s| s.len() / usize::from(SESSION_FORMAT.channels))
        else {
            return;
        };
        let now = self.clock.now_us();
        let send_ahead = self.send_ahead_us();
        let floor = now + send_ahead;
        self.place_timeline(floor);
        self.sync_outputs(now);

        if !self.live
            && let Some(wait_us) = self.room_wait(frames, now, send_ahead)
        {
            self.wake_us = Some(wait_us);
            return;
        }
        self.wake_us = None;
        let samples = self.pending.take().expect("checked above");
        let session_span = self
            .timeline
            .as_mut()
            .expect("placed above")
            .advance(frames as u64);
        let live = self.live;
        for encoding in self.encodings.iter_mut().filter(|e| e.running) {
            let format = encoding.format();
            let Some((bytes, span)) = encoding.encode(&samples) else {
                continue;
            };
            for out in self
                .outputs
                .iter_mut()
                .filter(|o| o.started && o.format == format)
            {
                send_chunk(out, &bytes, span, now, live);
            }
        }
        self.history.push_back((session_span, samples));
    }

    /// Starts the timeline at `floor`, or shifts it, and every running
    /// encoding's with it, forward to `floor` if the source fell more than
    /// [`SHIFT_TOLERANCE_US`] behind.
    fn place_timeline(&mut self, floor: i64) {
        let Some(timeline) = &mut self.timeline else {
            self.timeline = Some(Timeline::new(floor, SESSION_FORMAT.sample_rate));
            for encoding in self.encodings.iter_mut().filter(|e| e.running) {
                encoding
                    .timeline
                    .rebase(floor - encoding.encoder.delay_us());
            }
            return;
        };
        let shift = floor - timeline.cursor_us();
        if shift > SHIFT_TOLERANCE_US {
            tracing::debug!(
                shift_us = shift,
                "source fell behind, shifting the timeline"
            );
            timeline.rebase(floor);
            for encoding in self.encodings.iter_mut().filter(|e| e.running) {
                let cursor = encoding.timeline.cursor_us();
                encoding.timeline.rebase(cursor + shift);
            }
        }
    }

    /// Drops closed players, answers format requests, starts players that
    /// can now play, and stops sending to ones that can't. Encodings follow
    /// their players: one runs while a player on it is started, and goes
    /// once no player has its format.
    fn sync_outputs(&mut self, now: i64) {
        self.outputs.retain(|o| !o.conn.is_closed());
        // Nothing older can be replayed: a late joiner's lead is at least this.
        let replayable = now + LATE_JOIN_MIN_LEAD_US;
        while self
            .history
            .front()
            .is_some_and(|(span, _)| span.start_us < replayable)
        {
            self.history.pop_front();
        }
        for encoding in &mut self.encodings {
            while encoding
                .sent
                .front()
                .is_some_and(|(span, _)| span.start_us < replayable)
            {
                encoding.sent.pop_front();
            }
        }
        let cursor = self.timeline.as_ref().map_or(now, Timeline::cursor_us);
        for i in 0..self.outputs.len() {
            let out = &mut self.outputs[i];
            out.take_request(&mut self.encodings);
            let available = out.conn.is_available();
            if available && !out.started {
                self.start_output(i, cursor, now);
            } else if !available && out.started {
                let out = &mut self.outputs[i];
                // Its output was taken by something else; the client
                // stopped on its own. It gets stream/start if it returns.
                tracing::info!(
                    client_id = out.conn.client_id(),
                    "player unavailable, pausing its stream"
                );
                out.started = false;
            }
            self.outputs[i].tracker.prune(now);
        }
        let outputs = &self.outputs;
        self.encodings
            .retain(|e| outputs.iter().any(|o| o.format == e.format()));
        for encoding in &mut self.encodings {
            encoding.running &= outputs
                .iter()
                .any(|o| o.started && o.format == encoding.format());
        }
    }

    /// Sends `stream/start` to output `i` and catches it up: the chunks
    /// already encoded in its format that start at least its send-ahead
    /// from now, as many of the latest as its buffer holds. A format that
    /// isn't running starts at the first such chunk of session audio kept,
    /// or at the shared `cursor` when there is none, and encodes the kept
    /// audio from there.
    fn start_output(&mut self, i: usize, cursor: i64, now: i64) {
        let Self {
            outputs,
            encodings,
            history,
            live,
            ..
        } = self;
        let live = *live;
        let out = &mut outputs[i];
        let encoding = encodings
            .iter_mut()
            .find(|e| e.format() == out.format)
            .expect("added with its output");
        let lead = send_ahead_us(&out.conn.player_status().timing, live);
        let target = now + lead.max(LATE_JOIN_MIN_LEAD_US);

        if encoding.running {
            out.start(encoding);
            let first = encoding.sent.partition_point(|(s, _)| s.start_us < target);
            let chunks = encoding.sent.range(first..);
            let n = fitting_tail(
                chunks.map(|(span, bytes)| (bytes.len() as u64, span)),
                out.tracker.capacity_bytes(),
            );
            for (span, bytes) in encoding.sent.range(encoding.sent.len() - n..) {
                send_chunk(out, bytes, *span, now, live);
            }
            return;
        }

        let first = history.partition_point(|(s, _)| s.start_us < target);
        let capacity = out.conn.player_status().timing.buffer_capacity;
        let n = fitting_tail(
            history.range(first..).map(|(span, samples)| {
                let frames = samples.len() / usize::from(SESSION_FORMAT.channels);
                (encoding.encoder.estimate_bytes(frames) as u64, span)
            }),
            capacity,
        );
        let replay = history.range(history.len() - n..);
        let mut at = history
            .get(history.len() - n)
            .map_or(cursor, |(s, _)| s.start_us);
        encoding.run(at);
        out.start(encoding);
        for (span, samples) in replay {
            // A shift left a gap in the kept audio; the format's timeline
            // takes the same gap.
            if span.start_us != at {
                let cursor = encoding.timeline.cursor_us();
                encoding.timeline.rebase(cursor + span.start_us - at);
            }
            at = span.end_us();
            if let Some((bytes, span)) = encoding.encode(samples) {
                send_chunk(out, &bytes, span, now, live);
            }
        }
        if at != cursor {
            let c = encoding.timeline.cursor_us();
            encoding.timeline.rebase(c + cursor - at);
        }
    }

    /// For a source that isn't live: when to try again if some player has
    /// no room for the next chunk. With no player to send to, the stream
    /// is paced at the send-ahead instead of read as fast as it goes.
    fn room_wait(&self, frames: usize, now: i64, send_ahead: i64) -> Option<i64> {
        let cursor = self.timeline.as_ref()?.cursor_us();
        let duration = (frames as i64 * 1_000_000) / i64::from(SESSION_FORMAT.sample_rate);
        let mut started = self.outputs.iter().filter(|o| o.started).peekable();
        if started.peek().is_none() {
            let at = cursor - send_ahead;
            return (at > now).then_some(at);
        }
        started
            .filter_map(|o| {
                if o.conn.audio_queue_room() == 0 {
                    return Some(now + duration);
                }
                let bytes =
                    self.encodings
                        .iter()
                        .find(|e| e.format() == o.format)
                        .map_or(0, |e| e.encoder.estimate_bytes(frames)) as u64;
                (!o.tracker.can_send(bytes, duration))
                    .then(|| o.tracker.next_release_us().unwrap_or(now + duration))
            })
            .min()
    }
}

/// Queues one encoded chunk for `out`, playing over `span`.
fn send_chunk(out: &mut Output, bytes: &[u8], span: Span, now: i64, live: bool) {
    let client_id = out.conn.client_id();
    let len = bytes.len() as u64;
    if live && !out.tracker.can_send(len, span.duration_us) {
        tracing::debug!(client_id, "player buffer full, chunk dropped");
        return;
    }
    let frame = binary::audio_chunk(
        out.conn.header_layout(),
        span.start_us,
        binary::send_ahead(now, span.start_us),
        bytes,
    );
    match out.conn.send_audio(Bytes::from(frame)) {
        Ok(()) => out.tracker.register(span.end_us(), len, span.duration_us),
        Err(SendError::Full) => tracing::debug!(client_id, "audio queue full, chunk dropped"),
        Err(SendError::NotReady | SendError::Unavailable) => out.started = false,
        Err(SendError::Closed) => {}
    }
}

#[cfg(test)]
mod tests;
