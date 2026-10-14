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

mod buffer;
mod feed;
mod format;

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use tas_audio::{AudioError, LiveSource, SESSION_FORMAT, ServerClock, Timeline, to_session_format};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub use buffer::{BufferTracker, DEFAULT_CAPACITY_BYTES, MAX_BUFFERED_US};
pub use format::{PcmEncoder, pick_pcm_format};

use crate::connection::{Connection, SendError};
use crate::protocol::{
    ServerMessage, StreamClear, StreamEnd, StreamStart, StreamStartPlayer, binary,
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

const PLAYER_ROLE: &str = "player";

/// A running stream. Commands are queued to its task; dropping the handle
/// stops it the way [`AudioStream::stop`] does.
pub struct AudioStream {
    commands: mpsc::UnboundedSender<Command>,
    task: JoinHandle<()>,
}

enum Command {
    Add(Arc<Connection>),
    Remove(String),
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
        let streamer = Streamer {
            clock,
            live: source.is_live(),
            feed: Some(Feed::spawn(source)),
            queued: VecDeque::new(),
            timeline: None,
            outputs: Vec::new(),
            pending: None,
            wake_us: None,
        };
        let task = tokio::spawn(streamer.run(rx));
        Ok(Self { commands, task })
    }

    /// Streams to `conn` from the next chunk on. A player already in the
    /// stream under the same `client_id` (a reconnect) is replaced.
    pub fn add_player(&self, conn: Arc<Connection>) {
        let _ = self.commands.send(Command::Add(conn));
    }

    /// Stops streaming to a player, sending it `stream/end`.
    pub fn remove_player(&self, client_id: impl Into<String>) {
        let _ = self.commands.send(Command::Remove(client_id.into()));
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

    /// Stops now: every player gets `stream/end` and drops what it holds.
    pub async fn stop(mut self) {
        let _ = self.commands.send(Command::Stop);
        self.finished().await;
    }
}

/// One player in the stream.
struct Output {
    conn: Arc<Connection>,
    encoder: PcmEncoder,
    tracker: BufferTracker,
    /// The player's own timeline, at its wire rate, which a resampled
    /// player needs since its chunks hold a different number of frames.
    /// It starts where the shared one is and shifts with it.
    timeline: Timeline,
    /// It has had `stream/start` and is being sent audio.
    started: bool,
}

impl Output {
    fn send_control(&self, msg: ServerMessage) {
        if let Err(e) = self.conn.send(msg) {
            tracing::debug!(client_id = self.conn.client_id(), error = %e, "stream message not sent");
        }
    }

    fn start(&mut self, at_us: i64) {
        self.send_control(ServerMessage::StreamStart(StreamStart {
            server_transmitted: None,
            player: Some(StreamStartPlayer::new(self.encoder.format(), None)),
            artwork: None,
        }));
        self.tracker = BufferTracker::new(self.conn.player_status().timing.buffer_capacity);
        self.encoder.reset();
        self.timeline = Timeline::new(at_us, self.encoder.format().sample_rate);
        self.started = true;
        tracing::info!(
            client_id = self.conn.client_id(),
            format = ?self.encoder.format(),
            "stream started"
        );
    }

    fn end(&mut self) {
        if self.started {
            self.send_control(ServerMessage::StreamEnd(StreamEnd {
                server_transmitted: None,
                roles: Some(vec![PLAYER_ROLE.into()]),
            }));
            self.started = false;
        }
    }
}

enum Event {
    Command(Option<Command>),
    Feed(FeedItem),
    Wake,
}

/// The stream task's state.
struct Streamer {
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
            Command::Add(conn) => self.add(conn),
            Command::Remove(client_id) => {
                if let Some(i) = self.position(&client_id) {
                    self.outputs.remove(i).end();
                }
            }
            Command::Seek(source) => {
                self.queued.clear();
                self.feed = Some(Feed::spawn(source));
                self.pending = None;
                self.wake_us = None;
                self.timeline = None;
                for out in self.outputs.iter_mut().filter(|o| o.started) {
                    out.send_control(ServerMessage::StreamClear(StreamClear {
                        server_transmitted: None,
                        roles: Some(vec![PLAYER_ROLE.into()]),
                    }));
                    out.tracker.clear();
                    out.encoder.reset();
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
        let Some(format) = conn.hello().player().and_then(pick_pcm_format) else {
            tracing::warn!(client_id, "player lists no PCM format this server sends");
            return;
        };
        let encoder = match PcmEncoder::new(format) {
            Ok(encoder) => encoder,
            Err(e) => {
                tracing::warn!(client_id, error = %e, "can't encode for player");
                return;
            }
        };
        let output = Output {
            tracker: BufferTracker::new(0),
            timeline: Timeline::new(0, format.sample_rate),
            encoder,
            conn,
            started: false,
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
        self.timeline
            .as_mut()
            .expect("placed above")
            .advance(frames as u64);
        let live = self.live;
        for out in self.outputs.iter_mut().filter(|o| o.started) {
            send_chunk(out, &samples, now, live);
        }
    }

    /// Starts the timeline at `floor`, or shifts it, and every started
    /// player's with it, forward to `floor` if the source fell more than
    /// [`SHIFT_TOLERANCE_US`] behind.
    fn place_timeline(&mut self, floor: i64) {
        let Some(timeline) = &mut self.timeline else {
            self.timeline = Some(Timeline::new(floor, SESSION_FORMAT.sample_rate));
            for out in self.outputs.iter_mut().filter(|o| o.started) {
                out.timeline.rebase(floor);
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
            for out in self.outputs.iter_mut().filter(|o| o.started) {
                let cursor = out.timeline.cursor_us();
                out.timeline.rebase(cursor + shift);
            }
        }
    }

    /// Drops closed players, starts ones that can now play, and stops
    /// sending to ones that can't.
    fn sync_outputs(&mut self, now: i64) {
        self.outputs.retain(|o| !o.conn.is_closed());
        let cursor = self.timeline.as_ref().map_or(now, Timeline::cursor_us);
        for out in &mut self.outputs {
            let available = out.conn.is_available();
            if available && !out.started {
                out.start(cursor);
            } else if !available && out.started {
                // Its output was taken by something else; the client
                // stopped on its own. It gets stream/start if it returns.
                tracing::info!(
                    client_id = out.conn.client_id(),
                    "player unavailable, pausing its stream"
                );
                out.started = false;
            }
            out.tracker.prune(now);
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
                let bytes = o.encoder.estimate_bytes(frames) as u64;
                (!o.tracker.can_send(bytes, duration))
                    .then(|| o.tracker.next_release_us().unwrap_or(now + duration))
            })
            .min()
    }
}

/// Encodes one chunk for `out` and queues it.
fn send_chunk(out: &mut Output, samples: &[f32], now: i64, live: bool) {
    let client_id = out.conn.client_id();
    let (bytes, frames) = match out.encoder.encode(samples) {
        Ok(encoded) => encoded,
        Err(e) => {
            tracing::warn!(client_id, error = %e, "encoding failed, chunk dropped");
            return;
        }
    };
    if frames == 0 {
        return;
    }
    let span = out.timeline.advance(frames as u64);
    let len = bytes.len() as u64;
    if live && !out.tracker.can_send(len, span.duration_us) {
        tracing::debug!(client_id, "player buffer full, chunk dropped");
        return;
    }
    let frame = binary::audio_chunk(
        out.conn.header_layout(),
        span.start_us,
        binary::send_ahead(now, span.start_us),
        &bytes,
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
