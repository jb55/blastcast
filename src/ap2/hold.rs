//! A buffered stream's audio, held by when it plays and handed over a
//! little ahead of that: AirPlay 2's SETRATEANCHORTIME, FLUSHBUFFERED and
//! back-pressure, on blastcast's timeline.
//!
//! **Holding.** A phone sends buffered audio seconds, even minutes, ahead
//! of when it plays, as fast as we read it. Each decoded packet is held
//! by its RTP time (unwrapped across 2^32), up to
//! [`AUDIO_BUFFER_SIZE`] bytes of decoded audio. Past that the data
//! channel isn't read, so TCP holds the phone back: the socket is never
//! paced by the clock.
//!
//! **Handing over.** SETRATEANCHORTIME says frame `rtpTime` plays at
//! `networkTime` on the PTP timeline `networkTimeTimelineID`: a blastcast
//! [`Anchor`]. With rate 1, each packet is handed to the receiver's
//! [`Sink`] [`HAND_OVER_LEAD_US`] before it plays, stamped
//! [`Anchor::play_at_us`] through the follower's map of that timeline, so
//! the Sendspin stream gets seconds of lead and adds no latency. What is
//! ready at once goes as one chunk, since the stream's queue counts chunks,
//! not frames. The sink wraps the stream in blastcast's
//! [`RateMatch`](blastcast::RateMatch), as it does AirPlay 1's. A packet
//! whose time has passed is dropped, not handed over late, and a gap of
//! under a second after audio handed over is handed over as silence, so
//! the audio stays as contiguous as its stamps.
//!
//! **Keeping what hasn't played.** A packet handed over stays held until
//! it has played. So whenever the sink must drop what it was given
//! ([`Sink::flush`], which is [`Item::Flush`](blastcast::Item::Flush) and
//! Sendspin's `stream/clear`), the hold starts again from the first packet
//! that hasn't played, and nothing the phone still means to play is lost:
//!
//! - **rate 0 pauses**: the sink is flushed and nothing goes until an
//!   anchor with rate 1. That anchor's `rtpTime` is where the phone
//!   resumes, so what is held from there plays at its new time, with no
//!   gap in the frames or the stamps. No silence is sent while paused.
//! - **an anchor that moves the timeline** (rate 1 again, at a time that
//!   doesn't follow from the last anchor) flushes the sink and hands over
//!   again at the new times. One that agrees with the last changes nothing.
//! - **FLUSHBUFFERED** drops `flushFromTS` (the start, when there is one)
//!   to `flushUntilTS` from what is held, and the packets in that range
//!   still to come. Without `flushFromTS` it drops everything before
//!   `flushUntilTS`. The sink is flushed when any of it was handed over.
//!
//! TEARDOWN drops the stream, and the receiver ends the session the way it
//! ends AirPlay 1's ([`crate::raop`]): [`Item::End`](blastcast::Item::End)
//! after its end delay unless a sender plays again.
//!
//! **Telemetry**, under the receiver's source id: each SETRATEANCHORTIME
//! as a [`Series::Rate`] mark with its rate, and a [`Series::Sync`] mark
//! where its frame plays; each FLUSHBUFFERED as a [`Series::Flushed`] mark
//! of how much it dropped, and [`Series::Flush`] when the sink was
//! flushed; each chunk's [`Series::Lead`] as it is handed over, and what
//! is held as [`Series::Buffered`].
//!
//! The order of things is shairport-sync's (rtsp.c's
//! `handle_setrateanchori` and `handle_flushbuffered`, MIT-style), and
//! openairplay2 0.5's (MIT) reading of the same requests; neither keeps
//! what was handed over.

use std::collections::BTreeMap;
use std::io;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use blastcast::{Anchor, ClockMap};
use plist::{Dictionary, Value};
use tas_audio::{PcmChunk, ServerClock};
use tas_proto::Series;
use tas_telemetry::Probe;
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::buffered::Decoded;
use super::ptp::Ptp;
use super::setup::AUDIO_BUFFER_SIZE;
use crate::audio::AIRPLAY_FORMAT;
use crate::raop::announce::{CHANNELS, SAMPLE_RATE};

/// How long before it plays a packet is handed over: as much lead as the
/// Sendspin stream can use. It covers the AirPlay 1 bridge's
/// `required_lead_time_ms` (2.5 s), so a phone that leaves that much has
/// it honoured, and fits in a player's default buffer (1 MiB, about 5 s of
/// PCM).
pub const HAND_OVER_LEAD_US: i64 = 3_000_000;

/// The most held, in frames: [`AUDIO_BUFFER_SIZE`] bytes of decoded s16
/// stereo, about 47 s.
pub const MAX_HELD_FRAMES: u64 = AUDIO_BUFFER_SIZE / (2 * CHANNELS as u64);

/// The longest gap after audio handed over that is handed over as
/// silence: a second. A longer one is the sender skipping, which the
/// stamps say.
pub const MAX_GAP_FRAMES: i64 = SAMPLE_RATE as i64;

/// How far a new anchor may stray from where the last one puts its frame
/// and still be the same timeline: a millisecond.
const AGREE_US: i64 = 1_000;

/// How often a playing stream whose timeline has no clock map yet looks
/// for one.
const MAP_POLL: Duration = Duration::from_millis(20);

/// Ranged flushes remembered, for packets in them still to come.
const RANGES: usize = 8;

/// Where the hold hands its audio: the receiver, which passes it to
/// whoever plays it as a [`TimedSource`](blastcast::TimedSource).
pub trait Sink: Send + Sync + 'static {
    /// `chunk`'s first frame plays at `play_at_us` on the server clock.
    fn audio(&self, chunk: &PcmChunk, play_at_us: Option<i64>);
    /// Drop everything handed over that hasn't played.
    fn flush(&self);
}

/// SETRATEANCHORTIME's anchor: frame `rtp` plays at `remote_us` on PTP
/// timeline `timeline`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateAnchor {
    /// The grandmaster's clock id: `networkTimeTimelineID`.
    pub timeline: u64,
    pub rtp: u32,
    /// `networkTimeSecs` and `networkTimeFrac`, in µs.
    pub remote_us: i64,
}

/// What the RTSP connection tells a stream's hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// SETRATEANCHORTIME: playing (rate 1) or paused (0), as before when
    /// it gives no rate, from `anchor` when it names one, else from the
    /// last.
    Rate {
        playing: Option<bool>,
        anchor: Option<RateAnchor>,
    },
    /// FLUSHBUFFERED: drop RTP times `from` (everything before, when
    /// `None`) up to `until`.
    Flush { from: Option<u32>, until: u32 },
}

impl Command {
    /// A SETRATEANCHORTIME body, or `None` if it isn't one.
    pub fn rate(body: &[u8]) -> Option<Self> {
        let dict = dictionary(body)?;
        let number = |key: &str| {
            let v = dict.get(key)?;
            v.as_unsigned_integer()
                .or_else(|| v.as_real().map(|r| r as u64))
        };
        let anchor = match (
            number("networkTimeSecs"),
            number("networkTimeFrac"),
            number("networkTimeTimelineID"),
            number("rtpTime"),
        ) {
            (Some(secs), frac, Some(timeline), Some(rtp)) => Some(RateAnchor {
                timeline,
                rtp: rtp as u32,
                remote_us: network_time_us(secs, frac.unwrap_or(0)),
            }),
            _ => None,
        };
        let playing = number("rate").map(|r| r & 1 == 1);
        Some(Self::Rate { playing, anchor })
    }

    /// A FLUSHBUFFERED body, or `None` if it has no `flushUntilTS`.
    pub fn flush(body: &[u8]) -> Option<Self> {
        let dict = dictionary(body)?;
        let ts = |key: &str| dict.get(key)?.as_unsigned_integer().map(|t| t as u32);
        Some(Self::Flush {
            from: ts("flushFromTS"),
            until: ts("flushUntilTS")?,
        })
    }
}

fn dictionary(body: &[u8]) -> Option<Dictionary> {
    Value::from_reader(io::Cursor::new(body))
        .ok()?
        .into_dictionary()
}

/// PTP's seconds and 64-bit binary fraction, in µs, as shairport-sync
/// reads them: the fraction's top 32 bits, scaled to nanoseconds.
pub fn network_time_us(secs: u64, frac: u64) -> i64 {
    let nanos = ((frac >> 32) * 1_000_000_000) >> 32;
    (i128::from(secs) * 1_000_000 + i128::from(nanos / 1000)) as i64
}

/// The seconds and fraction a sender writes for `ns`.
pub fn network_time(ns: i128) -> (u64, u64) {
    let secs = (ns / 1_000_000_000) as u64;
    let nanos = (ns % 1_000_000_000) as u64;
    // Its top 32 bits, rounded up, so reading them back truncates to the
    // same nanosecond.
    let top = (nanos << 32).div_ceil(1_000_000_000);
    (secs, top << 32)
}

/// What became of a packet offered to the hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inserted {
    Held,
    /// A FLUSHBUFFERED dropped its RTP time before it came.
    Flushed,
    /// It is behind what was handed over already.
    Late,
    Duplicate,
}

/// Frames handed over at once: contiguous, playing from `play_at_us`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ready {
    pub play_at_us: i64,
    /// Interleaved s16 stereo.
    pub frames: Vec<i16>,
}

/// The audio held, and where handing it over has got to. Time is given,
/// never read, so it is tested without a clock. See the
/// [module docs](self).
#[derive(Debug, Default)]
pub struct Hold {
    /// Packets by unwrapped RTP time; those before `next` were handed over
    /// and haven't played.
    packets: BTreeMap<i64, Vec<i16>>,
    frames: u64,
    /// The RTP time the next one is unwrapped near.
    reference: Option<i64>,
    /// The latest anchor, and the timeline it is on.
    anchor: Option<(u64, Anchor)>,
    /// The latest rate was 1.
    playing: bool,
    /// The next frame to hand over; `None` starts afresh from the first
    /// held.
    next: Option<i64>,
    /// Something was handed over since the sink was last flushed.
    handed: bool,
    /// Everything before this was flushed.
    flushed_before: Option<i64>,
    /// Ranges flushed, for packets in them still to come.
    flushed: Vec<Range<i64>>,
    /// Frames dropped because their time had passed, since last taken.
    late: u64,
}

fn frames_in(samples: &[i16]) -> i64 {
    (samples.len() / CHANNELS) as i64
}

impl Hold {
    /// Frames held, handed over or not, that haven't played.
    pub fn held_frames(&self) -> u64 {
        self.frames
    }

    /// Nothing more is read until some has played or is dropped.
    pub fn is_full(&self) -> bool {
        self.frames >= MAX_HELD_FRAMES
    }

    pub fn is_playing(&self) -> bool {
        self.playing && self.anchor.is_some()
    }

    /// The PTP timeline the latest anchor is on.
    pub fn timeline(&self) -> Option<u64> {
        self.anchor.map(|(t, _)| t)
    }

    /// Frames dropped as late since the last call.
    pub fn take_late(&mut self) -> u64 {
        std::mem::take(&mut self.late)
    }

    /// When the frame at RTP time `rtp` plays, through `map`, by the
    /// latest anchor.
    pub fn play_at_us(&self, rtp: u32, map: &ClockMap) -> Option<i64> {
        let (_, anchor) = self.anchor?;
        Some(anchor.play_at_us(self.near(rtp) as u64, map))
    }

    /// `rtp` unwrapped near the last packet's.
    fn near(&self, rtp: u32) -> i64 {
        match self.reference {
            Some(r) => r + i64::from(rtp.wrapping_sub(r as u32) as i32),
            None => i64::from(rtp),
        }
    }

    fn unwrap(&mut self, rtp: u32) -> i64 {
        let at = self.near(rtp);
        self.reference = Some(at);
        at
    }

    /// Holds `frames` (interleaved s16 stereo), the packet at `rtp`.
    pub fn insert(&mut self, rtp: u32, frames: Vec<i16>) -> Inserted {
        let at = self.unwrap(rtp);
        if self.flushed_before.is_some_and(|b| at < b)
            || self.flushed.iter().any(|r| r.contains(&at))
        {
            return Inserted::Flushed;
        }
        if self.next.is_some_and(|n| at < n) {
            return Inserted::Late;
        }
        if self.packets.contains_key(&at) {
            return Inserted::Duplicate;
        }
        self.frames += frames_in(&frames) as u64;
        self.packets.insert(at, frames);
        Inserted::Held
    }

    /// SETRATEANCHORTIME: playing or paused (as before, when `None`),
    /// from `anchor` if there is one. Returns whether the sink must be
    /// flushed.
    pub fn set_rate(&mut self, playing: Option<bool>, anchor: Option<RateAnchor>) -> bool {
        let playing = playing.unwrap_or(self.playing);
        if self.reference.is_none()
            && let Some(a) = anchor
        {
            self.reference = Some(i64::from(a.rtp));
        }
        let new = anchor.map(|a| {
            let anchor = Anchor {
                frame: self.near(a.rtp) as u64,
                remote_us: a.remote_us,
                rate: SAMPLE_RATE,
            };
            (a.timeline, anchor)
        });
        let keeps = playing
            && self.playing
            && match (&self.anchor, &new) {
                (Some(old), Some(new)) => agrees(old, new),
                (Some(_), None) => true,
                (None, _) => false,
            };
        if let Some(new) = new {
            self.anchor = Some(new);
        }
        self.playing = playing;
        if keeps {
            return false;
        }
        let flush = self.rewind();
        if playing && let Some((_, anchor)) = new {
            // The phone plays from here: anything before has played.
            let frames = self.cut(i64::MIN..anchor.frame as i64);
            if frames > 0 {
                tracing::trace!(frames, "AirPlay 2 audio from before the anchor dropped");
            }
        }
        flush
    }

    /// FLUSHBUFFERED. Returns the frames dropped, and whether the sink
    /// must be flushed.
    pub fn flush(&mut self, from: Option<u32>, until: u32) -> (u64, bool) {
        let until = self.near(until);
        match from.map(|f| self.near(f)) {
            None => {
                let dropped = self.cut(i64::MIN..until);
                self.flushed_before = self.flushed_before.max(Some(until));
                (dropped, self.rewind())
            }
            Some(from) => {
                let dropped = self.cut(from..until);
                if self.flushed.len() == RANGES {
                    self.flushed.remove(0);
                }
                self.flushed.push(from..until);
                let handed = self.next.is_some_and(|n| n > from);
                (dropped, handed && self.rewind())
            }
        }
    }

    /// Starts handing over afresh from the first packet held; whether
    /// anything handed over must be flushed from the sink.
    fn rewind(&mut self) -> bool {
        self.next = None;
        std::mem::take(&mut self.handed)
    }

    /// Drops the frames in `range`, keeping the parts of the packets on
    /// its edges outside it. Returns how many.
    fn cut(&mut self, range: Range<i64>) -> u64 {
        let hit: Vec<i64> = self
            .packets
            .range(..range.end)
            .rev()
            .take_while(|(k, f)| **k + frames_in(f) > range.start)
            .map(|(k, _)| *k)
            .collect();
        let mut dropped = 0;
        for at in hit {
            let samples = self.packets.remove(&at).expect("just found");
            let end = at + frames_in(&samples);
            if at < range.start {
                let keep = (range.start - at) as usize * CHANNELS;
                self.packets.insert(at, samples[..keep].to_vec());
            }
            if end > range.end {
                let skip = (range.end - at) as usize * CHANNELS;
                self.packets.insert(range.end, samples[skip..].to_vec());
            }
            dropped += (end.min(range.end) - at.max(range.start)) as u64;
        }
        self.frames -= dropped;
        dropped
    }

    /// What is due by `now_us` with `lead_us` to spare, through `map`;
    /// and what has played is let go.
    pub fn ready(&mut self, now_us: i64, map: &ClockMap, lead_us: i64) -> Vec<Ready> {
        let mut out: Vec<(i64, Ready)> = Vec::new();
        let Some((_, anchor)) = self.anchor.filter(|_| self.playing) else {
            return Vec::new();
        };
        let at = |frame: i64| anchor.play_at_us(frame as u64, map);
        if let Some(next) = self.next {
            let played: Vec<(i64, i64)> = self
                .packets
                .range(..next)
                .map(|(k, f)| (*k, frames_in(f)))
                .take_while(|(k, n)| at(k + n) <= now_us)
                .collect();
            for (k, n) in played {
                self.packets.remove(&k);
                self.frames -= n as u64;
            }
        }
        loop {
            let from = self.next.unwrap_or(i64::MIN);
            let Some((&k, samples)) = self.packets.range(from..).next() else {
                break;
            };
            let n = frames_in(samples);
            let start = at(k);
            if start > now_us + lead_us {
                break;
            }
            if start < now_us {
                self.packets.remove(&k);
                self.frames -= n as u64;
                self.late += n as u64;
                self.next = Some(k + n);
                continue;
            }
            // A short gap after what was handed over goes as silence.
            let gap = self
                .next
                .filter(|&c| c < k && k - c <= MAX_GAP_FRAMES && at(c) >= now_us);
            let begin = gap.unwrap_or(k);
            let mut frames = vec![0; (k - begin) as usize * CHANNELS];
            frames.extend_from_slice(samples);
            match out.last_mut() {
                Some((end, ready)) if *end == begin => {
                    ready.frames.extend(frames);
                    *end = k + n;
                }
                _ => out.push((
                    k + n,
                    Ready {
                        play_at_us: at(begin),
                        frames,
                    },
                )),
            }
            self.next = Some(k + n);
            self.handed = true;
        }
        out.into_iter().map(|(_, r)| r).collect()
    }

    /// When the next packet is due, with `lead_us` to spare, through
    /// `map`; `None` while there is nothing to hand over.
    pub fn next_due(&self, map: &ClockMap, lead_us: i64) -> Option<i64> {
        let (_, anchor) = self.anchor.filter(|_| self.playing)?;
        let (&k, _) = self.packets.range(self.next.unwrap_or(i64::MIN)..).next()?;
        Some(anchor.play_at_us(k as u64, map) - lead_us)
    }
}

/// Whether `new` puts its frame where `old` does, on the same timeline.
fn agrees(old: &(u64, Anchor), new: &(u64, Anchor)) -> bool {
    let frames = i128::from(new.1.frame.wrapping_sub(old.1.frame) as i64);
    let remote = old.1.remote_us + (frames * 1_000_000 / i128::from(SAMPLE_RATE)) as i64;
    old.0 == new.0 && (remote - new.1.remote_us).abs() <= AGREE_US
}

/// What a stream's hold hands to, and what it reads the time and its
/// timeline with.
pub struct Out {
    pub sink: Arc<dyn Sink>,
    pub ptp: Ptp,
    pub clock: ServerClock,
    pub probe: Probe,
}

/// Holds what comes on `decoded` and hands it to `out` as it falls due,
/// as `commands` say, until the commands stop.
pub async fn run(
    mut decoded: mpsc::Receiver<Decoded>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    out: Out,
) {
    let mut hold = Hold::default();
    let mut reading = true;
    loop {
        let now = out.clock.now_us();
        let map = hold.timeline().and_then(|t| out.ptp.map(t));
        let wake = match &map {
            Some(map) => {
                hand_over(&mut hold, &out, now, map);
                hold.next_due(map, HAND_OVER_LEAD_US)
                    .map(|us| out.clock.instant_at(us))
            }
            None if hold.is_playing() => Some(Instant::now() + MAP_POLL),
            None => None,
        };
        let far = Instant::now() + Duration::from_secs(3600);
        tokio::select! {
            packet = decoded.recv(), if reading && !hold.is_full() => match packet {
                Some(p) => {
                    if hold.insert(p.rtp, p.frames) == Inserted::Held {
                        let now = out.clock.now_us();
                        out.probe.sample(Series::Buffered, now, frames_us(hold.held_frames()));
                    }
                }
                None => reading = false,
            },
            command = commands.recv() => match command {
                Some(command) => apply(&mut hold, &out, command),
                None => return,
            },
            () = tokio::time::sleep_until(wake.unwrap_or(far)), if wake.is_some() => {}
        }
    }
}

fn hand_over(hold: &mut Hold, out: &Out, now: i64, map: &ClockMap) {
    for ready in hold.ready(now, map, HAND_OVER_LEAD_US) {
        out.probe
            .sample(Series::Lead, now, (ready.play_at_us - now) as f64);
        let samples = ready
            .frames
            .iter()
            .map(|&s| f32::from(s) / 32_768.0)
            .collect();
        out.sink.audio(
            &PcmChunk::new(AIRPLAY_FORMAT, samples),
            Some(ready.play_at_us),
        );
    }
    let late = hold.take_late();
    if late > 0 {
        tracing::debug!(
            late_ms = late * 1000 / u64::from(SAMPLE_RATE),
            "AirPlay 2 audio came after its time; dropped"
        );
    }
}

fn apply(hold: &mut Hold, out: &Out, command: Command) {
    let now = out.clock.now_us();
    let flush = match command {
        Command::Rate { playing, anchor } => {
            let flush = hold.set_rate(playing, anchor);
            let playing = hold.is_playing();
            out.probe
                .mark(Series::Rate, now, if playing { 1.0 } else { 0.0 });
            let at = anchor.and_then(|a| {
                let map = out.ptp.map(a.timeline)?;
                hold.play_at_us(a.rtp, &map)
            });
            if let Some(at) = at {
                out.probe.mark(Series::Sync, now, at as f64);
            }
            tracing::debug!(
                playing,
                ?anchor,
                lead_ms = at.map(|at| (at - now) / 1000),
                "AirPlay 2 rate and anchor"
            );
            flush
        }
        Command::Flush { from, until } => {
            let (dropped, flush) = hold.flush(from, until);
            out.probe.mark(Series::Flushed, now, frames_us(dropped));
            tracing::debug!(from, until, dropped, "AirPlay 2 buffered audio flushed");
            flush
        }
    };
    if flush {
        out.sink.flush();
        out.probe.mark(Series::Flush, now, 0.0);
    }
}

/// `frames` at the stream's rate, in µs.
fn frames_us(frames: u64) -> f64 {
    frames as f64 * 1e6 / f64::from(SAMPLE_RATE)
}

#[cfg(test)]
mod tests;
