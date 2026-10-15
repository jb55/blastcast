//! What the server can measure about one connection, for `/debug`: how well
//! the client's clock sync is going, the stream it is being sent, and the
//! last thing that went wrong.
//!
//! **Clock sync.** The client runs the time filter, so the server never
//! learns the offset the client settled on. What it sees is each
//! `client/time`'s `client_transmitted` beside its own `server_received`.
//! Their difference is the clock offset plus that message's one-way delay,
//! so the spread of successive differences is the delay jitter the client's
//! filter has to see through. It is smoothed as RTP's interarrival jitter
//! is (RFC 3550, 6.4.1): the change between successive samples, so a slow
//! drift between the two clocks doesn't count as jitter.
//!
//! **Round trip.** The writer's websocket pings carry the server clock, and
//! a pong echoes it back, so each pong is one RTT sample.
//!
//! **Telemetry.** Besides the aggregates `/debug` reads, each measurement
//! goes to the connection's [`Probe`] as it is taken: every `client/time`'s
//! offset, every pong's RTT, and per chunk its lead, size, the audio
//! buffered after it and its span on the timeline, sent or dropped.

use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard, PoisonError};

use serde::Serialize;
use tas_proto::Series;
use tas_telemetry::Probe;

use crate::protocol::{Codec, StreamFormat};

/// The RTT samples kept, at one per ping interval.
const RTT_WINDOW: usize = 16;
/// The `client/time` arrivals the request interval is averaged over.
const TIME_WINDOW: usize = 32;
/// RFC 3550's jitter gain.
const JITTER_GAIN: i64 = 16;
/// The bytes of a ping payload that carries the server clock.
const PING_STAMP: usize = 8;

/// One connection's measurements. Every method takes the server clock's
/// `now_us`, so tests can drive it without waiting.
#[derive(Debug)]
pub struct LinkStats {
    inner: Mutex<Inner>,
    probe: Probe,
}

#[derive(Debug, Default)]
struct Inner {
    connected_us: i64,
    time: TimeSync,
    rtts: VecDeque<i64>,
    stream: Option<StreamStats>,
    last_error: Option<LastError>,
}

#[derive(Debug, Default)]
struct TimeSync {
    samples: u64,
    /// `server_received - client_transmitted` of the latest `client/time`.
    offset_us: Option<i64>,
    jitter_us: i64,
    /// When the latest few arrived, on the server clock.
    arrivals: VecDeque<i64>,
}

#[derive(Debug, Clone)]
struct StreamStats {
    format: StreamFormat,
    send_ahead_us: i64,
    playing: bool,
    chunks_sent: u64,
    chunks_dropped: u64,
    /// When the last chunk sent finishes playing.
    end_us: Option<i64>,
    /// How far ahead of its play time the last chunk was sent.
    lead_us: Option<i64>,
    /// The latency the stream added to its timed source's play times.
    added_latency_us: i64,
    /// The surplus lead the stream cut from them as its timeline started.
    trimmed_lead_us: i64,
}

/// Something that went wrong on a connection, kept after it closes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastError {
    pub message: String,
    /// On the server clock.
    pub at_us: i64,
}

impl LinkStats {
    /// Stats that emit no telemetry.
    pub fn new(now_us: i64) -> Self {
        Self::with_probe(now_us, Probe::default())
    }

    /// Stats that emit each measurement to `probe` as well.
    pub fn with_probe(now_us: i64, probe: Probe) -> Self {
        Self {
            inner: Mutex::new(Inner {
                connected_us: now_us,
                ..Default::default()
            }),
            probe,
        }
    }

    /// Where this connection's telemetry goes.
    pub fn probe(&self) -> &Probe {
        &self.probe
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A `client/time` that arrived at `received_us`.
    pub fn time_request(&self, client_transmitted: i64, received_us: i64) {
        let offset = received_us - client_transmitted;
        self.probe
            .sample(Series::ClockOffset, received_us, offset as f64);
        let t = &mut self.lock().time;
        if let Some(prev) = t.offset_us {
            let d = (offset - prev).abs();
            t.jitter_us += (d - t.jitter_us) / JITTER_GAIN;
        }
        t.offset_us = Some(offset);
        t.samples += 1;
        if t.arrivals.len() == TIME_WINDOW {
            t.arrivals.pop_front();
        }
        t.arrivals.push_back(received_us);
    }

    /// The payload of a ping sent at `now_us`.
    pub fn ping_payload(now_us: i64) -> [u8; PING_STAMP] {
        now_us.to_be_bytes()
    }

    /// A pong that came back at `now_us`. One that doesn't echo a stamped
    /// ping (an unsolicited pong) is ignored.
    pub fn pong(&self, payload: &[u8], now_us: i64) {
        let Ok(stamp) = <[u8; PING_STAMP]>::try_from(payload) else {
            return;
        };
        let rtt = now_us - i64::from_be_bytes(stamp);
        if rtt < 0 {
            return;
        }
        self.probe.sample(Series::Rtt, now_us, rtt as f64);
        let rtts = &mut self.lock().rtts;
        if rtts.len() == RTT_WINDOW {
            rtts.pop_front();
        }
        rtts.push_back(rtt);
    }

    /// The player was sent `stream/start` in `format`, to be sent each
    /// chunk `send_ahead_us` before it plays.
    pub fn stream_started(&self, format: StreamFormat, send_ahead_us: i64) {
        self.lock().stream = Some(StreamStats {
            format,
            send_ahead_us,
            playing: true,
            chunks_sent: 0,
            chunks_dropped: 0,
            end_us: None,
            lead_us: None,
            added_latency_us: 0,
            trimmed_lead_us: 0,
        });
    }

    /// The stream now adds `us` to its timed source's play times.
    pub fn added_latency(&self, us: i64) {
        if let Some(s) = &mut self.lock().stream {
            s.added_latency_us = us;
        }
    }

    /// The stream now plays its timed source `us` sooner than stamped.
    pub fn trimmed_lead(&self, us: i64) {
        if let Some(s) = &mut self.lock().stream {
            s.trimmed_lead_us = us;
        }
    }

    /// The player's stream ended or paused.
    pub fn stream_stopped(&self) {
        if let Some(s) = &mut self.lock().stream {
            s.playing = false;
        }
    }

    /// A chunk of `bytes` playing from `start_us` to `end_us` was queued
    /// at `now_us`.
    pub fn chunk_sent(&self, start_us: i64, end_us: i64, bytes: usize, now_us: i64) {
        let p = &self.probe;
        p.span(Series::ChunkSent, now_us, start_us, end_us - start_us);
        p.sample(Series::Lead, now_us, (start_us - now_us) as f64);
        p.sample(Series::ChunkBytes, now_us, bytes as f64);
        p.sample(Series::Buffered, now_us, (end_us - now_us).max(0) as f64);
        if let Some(s) = &mut self.lock().stream {
            s.chunks_sent += 1;
            s.end_us = Some(end_us);
            s.lead_us = Some(start_us - now_us);
        }
    }

    /// A chunk that would have played from `start_us` to `end_us` was
    /// dropped at `now_us`.
    pub fn chunk_dropped(&self, start_us: i64, end_us: i64, now_us: i64) {
        self.probe
            .span(Series::ChunkDropped, now_us, start_us, end_us - start_us);
        if let Some(s) = &mut self.lock().stream {
            s.chunks_dropped += 1;
        }
    }

    pub fn error(&self, message: impl ToString, now_us: i64) {
        self.lock().last_error = Some(LastError {
            message: message.to_string(),
            at_us: now_us,
        });
    }

    pub fn last_error(&self) -> Option<LastError> {
        self.lock().last_error.clone()
    }

    /// What the stats say as of `now_us`. `queued` is how many frames wait
    /// in the connection's audio queue.
    pub fn snapshot(&self, now_us: i64, queued: usize) -> StatsSnapshot {
        let inner = self.lock();
        let t = &inner.time;
        let interval_ms = match (t.arrivals.front(), t.arrivals.back()) {
            (Some(first), Some(last)) if t.arrivals.len() > 1 => {
                Some(ms(last - first) / (t.arrivals.len() - 1) as f64)
            }
            _ => None,
        };
        let clock_sync = ClockSync {
            samples: t.samples,
            offset_us: t.offset_us,
            jitter_us: t.offset_us.map(|_| t.jitter_us),
            interval_ms,
            last_ms_ago: t.arrivals.back().map(|at| ms(now_us - at)),
            rtt_ms: inner.rtts.back().copied().map(ms),
            rtt_min_ms: inner.rtts.iter().min().copied().map(ms),
            rtt_max_ms: inner.rtts.iter().max().copied().map(ms),
        };
        let stream = inner.stream.as_ref().map(|s| StreamSnapshot {
            codec: s.format.codec,
            sample_rate: s.format.sample_rate,
            channels: s.format.channels,
            bit_depth: s.format.bit_depth,
            playing: s.playing,
            send_ahead_ms: ms(s.send_ahead_us),
            added_latency_ms: ms(s.added_latency_us),
            trimmed_lead_ms: ms(s.trimmed_lead_us),
            lead_ms: s.lead_us.map(ms),
            buffered_ms: match s.end_us {
                Some(end) if s.playing => ms((end - now_us).max(0)),
                _ => 0.0,
            },
            queued_frames: queued,
            chunks_sent: s.chunks_sent,
            chunks_dropped: s.chunks_dropped,
        });
        StatsSnapshot {
            connected_s: (now_us - inner.connected_us) as f64 / 1e6,
            clock_sync,
            stream,
            last_error: inner
                .last_error
                .as_ref()
                .map(|e| ErrorSnapshot::new(e, now_us)),
        }
    }
}

fn ms(us: i64) -> f64 {
    us as f64 / 1000.0
}

/// One connection, as `/debug` shows it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StatsSnapshot {
    pub connected_s: f64,
    pub clock_sync: ClockSync,
    /// The latest stream the player was sent, `None` if it never was.
    pub stream: Option<StreamSnapshot>,
    pub last_error: Option<ErrorSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ClockSync {
    /// `client/time` requests answered.
    pub samples: u64,
    /// The latest `server_received - client_transmitted`: the clock offset
    /// plus the request's one-way delay. Only its changes mean anything;
    /// the client's clock may count from anywhere.
    pub offset_us: Option<i64>,
    /// The smoothed change in `offset_us` between requests: delay jitter.
    pub jitter_us: Option<i64>,
    /// The mean time between the latest requests.
    pub interval_ms: Option<f64>,
    pub last_ms_ago: Option<f64>,
    /// Websocket ping round trips, the latest few.
    pub rtt_ms: Option<f64>,
    pub rtt_min_ms: Option<f64>,
    pub rtt_max_ms: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StreamSnapshot {
    pub codec: Codec,
    pub sample_rate: u32,
    pub channels: u32,
    pub bit_depth: u32,
    /// False once the stream ended or the player went unavailable.
    pub playing: bool,
    /// The player's own send-ahead floor; the stream runs at the largest
    /// of its players'.
    pub send_ahead_ms: f64,
    /// The latency the stream added to its timed source's play times, so a
    /// sender's chunks with less lead than the send-ahead still play: 0 for
    /// a source that isn't timed. It only grows while the stream runs.
    pub added_latency_ms: f64,
    /// How much sooner than stamped the stream plays a timed source whose
    /// lead may be trimmed (an AirPlay sender's), so its audio plays a
    /// send-ahead after it arrives, not two seconds: set as each timeline
    /// starts, 0 for any other source. `added_latency_ms` grows on top.
    pub trimmed_lead_ms: f64,
    /// How far ahead of its play time the last chunk was sent.
    pub lead_ms: Option<f64>,
    /// Audio sent that hasn't played yet, as far as the server knows.
    pub buffered_ms: f64,
    /// Frames still waiting in the server's queue for the socket.
    pub queued_frames: usize,
    pub chunks_sent: u64,
    pub chunks_dropped: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ErrorSnapshot {
    pub message: String,
    pub s_ago: f64,
}

impl ErrorSnapshot {
    pub fn new(e: &LastError, now_us: i64) -> Self {
        Self {
            message: e.message.clone(),
            s_ago: (now_us - e.at_us) as f64 / 1e6,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pcm() -> StreamFormat {
        StreamFormat {
            codec: Codec::Pcm,
            channels: 2,
            sample_rate: 48_000,
            bit_depth: 16,
        }
    }

    #[test]
    fn steady_requests_have_no_jitter_and_drift_barely_counts() {
        let s = LinkStats::new(0);
        // The client clock runs 100 ppm slow: the offset creeps 100 µs a
        // second, but every step is the same, so jitter stays tiny.
        for i in 0..40 {
            let at = 1_000_000 * i;
            s.time_request(at - 100 * i - 5_000_000, at);
        }
        let c = s.snapshot(40_000_000, 0).clock_sync;
        assert_eq!(c.samples, 40);
        assert_eq!(c.interval_ms, Some(1000.0));
        assert_eq!(c.last_ms_ago, Some(1000.0));
        assert!(c.jitter_us.unwrap() <= 100, "{c:?}");
    }

    #[test]
    fn delay_spikes_show_as_jitter() {
        let s = LinkStats::new(0);
        for i in 0..40i64 {
            let delay = if i % 2 == 0 { 0 } else { 8_000 };
            let sent = 1_000_000 * i;
            s.time_request(sent, sent + delay);
        }
        let jitter = s.snapshot(0, 0).clock_sync.jitter_us.unwrap();
        assert!((6_000..=8_000).contains(&jitter), "{jitter}");
    }

    #[test]
    fn a_pong_echoing_a_ping_is_an_rtt() {
        let s = LinkStats::new(0);
        s.pong(&LinkStats::ping_payload(1_000), 4_000);
        s.pong(&LinkStats::ping_payload(10_000), 11_500);
        s.pong(b"not ours", 20_000);
        let c = s.snapshot(20_000, 0).clock_sync;
        assert_eq!(c.rtt_ms, Some(1.5));
        assert_eq!(c.rtt_min_ms, Some(1.5));
        assert_eq!(c.rtt_max_ms, Some(3.0));
    }

    #[test]
    fn buffered_is_what_is_sent_and_not_yet_played() {
        let s = LinkStats::new(0);
        assert_eq!(s.snapshot(0, 0).stream, None);
        s.stream_started(pcm(), 250_000);
        s.chunk_sent(250_000, 270_000, 3840, 0);
        s.chunk_sent(270_000, 290_000, 3840, 20_000);
        s.chunk_dropped(290_000, 310_000, 40_000);
        let st = s.snapshot(100_000, 3).stream.unwrap();
        assert_eq!(st.buffered_ms, 190.0);
        assert_eq!(st.lead_ms, Some(250.0));
        assert_eq!(st.send_ahead_ms, 250.0);
        assert_eq!((st.chunks_sent, st.chunks_dropped), (2, 1));
        assert_eq!(st.queued_frames, 3);
        s.stream_stopped();
        assert_eq!(s.snapshot(100_000, 0).stream.unwrap().buffered_ms, 0.0);
    }

    #[test]
    fn each_time_request_and_pong_is_a_sample() {
        let hub = tas_telemetry::Telemetry::new();
        let s = LinkStats::with_probe(0, hub.probe("kitchen"));
        s.time_request(1_000, 6_000);
        s.pong(&LinkStats::ping_payload(10_000), 11_500);
        let history = hub.subscribe(20_000).history;
        let got: Vec<(Series, i64, f64)> = history
            .iter()
            .map(|t| (t.series, t.points[0].t_us, t.points[0].value))
            .collect();
        assert_eq!(
            got,
            [
                (Series::ClockOffset, 6_000, 5_000.0),
                (Series::Rtt, 11_500, 1_500.0)
            ]
        );
    }

    #[test]
    fn the_last_error_is_kept_with_its_age() {
        let s = LinkStats::new(0);
        s.error("write timed out", 1_000_000);
        let e = s.snapshot(3_500_000, 0).last_error.unwrap();
        assert_eq!(e.message, "write timed out");
        assert_eq!(e.s_ago, 2.5);
    }
}
