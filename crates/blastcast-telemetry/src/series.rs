//! What is measured: each [`Series`], how its points read ([`Kind`]) and
//! what they count ([`Unit`]), and a [`Point`] of one.

use serde::{Deserialize, Serialize};

/// How far back a series' history goes: a minute, on the server clock.
/// The hub keeps as much, and so does a client that reads it.
pub const WINDOW_US: i64 = 60_000_000;

/// One point of a series. A sample and a mark use `value` alone; a span
/// plays from `value` (server-clock µs) for `duration_us`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    /// When it was measured, in server-clock µs.
    pub t_us: i64,
    pub value: f64,
    /// A span's length; 0 for anything else.
    pub duration_us: i64,
}

/// Something measured over time, on a player (its client id), a stream
/// (its [`Stream::id`](crate::Stream::id)), an AirPlay receiver (its [`Receiver::id`](crate::Receiver::id)), an
/// AirPlay speaker (its [`Speaker::id`](crate::Speaker::id), the player's) or an AirPlay 2 PTP
/// timeline (`ptp:` and its grandmaster's clock id, 16 hex digits). Each
/// has a [`Kind`] and a [`Unit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Series {
    /// Sendspin, per player: a `client/time`'s `server_received -
    /// client_transmitted`, the clock offset plus that request's one-way
    /// delay. Only its changes mean anything. AirPlay, per receiver: a
    /// timing reply's offset, the sender's NTP clock less the server
    /// clock.
    ClockOffset,
    /// Sendspin, per player: a websocket ping's round trip. AirPlay, per
    /// receiver: a timing reply's.
    Rtt,
    /// Sendspin, per player: how far ahead of its play time a chunk was
    /// queued. AirPlay, per receiver: how far ahead of the time the sender
    /// means it to play a packet arrived; AirPlay 2, how far ahead of its
    /// play time a chunk was handed over.
    Lead,
    /// Sendspin, per player: a chunk's encoded size.
    ChunkBytes,
    /// Sendspin, per player: the audio sent and not yet played, as of a
    /// chunk queued. AirPlay, per receiver: the audio in the reorder buffer
    /// behind a gap, not yet handed over, as of a packet queued. AirPlay 2:
    /// the audio held and not yet played, as of a packet held.
    Buffered,
    /// Sendspin, per player, a span: a chunk queued at `t`, playing from
    /// `v` for `d`.
    ChunkSent,
    /// Sendspin, per player, a span: a chunk dropped at `t` (the player's
    /// buffer or queue was full) that would have played from `v` for `d`.
    ChunkDropped,
    /// Sendspin, per stream: where the next chunk goes on the timeline, as
    /// of each chunk.
    Cursor,
    /// Sendspin, per stream: the send-ahead floor as of each chunk, the
    /// earliest the next chunk may play.
    Floor,
    /// A mark, per stream or per player: a stream started, or `stream/start`
    /// went to the player. `v` is its send-ahead.
    StreamStart,
    /// A mark, per stream: a seek, `stream/clear` to every player.
    StreamClear,
    /// A mark, per stream: the source fell behind and the timeline jumped
    /// forward by `v`.
    TimelineShift,
    /// A mark, per stream or per player: the stream ended, or
    /// `stream/end` went to the player.
    StreamEnd,
    /// AirPlay, per receiver: the clock map's offset after each timing
    /// reply, the one of the quickest recent round trip.
    ClockMap,
    /// A mark, per receiver: a sync packet, or AirPlay 2's
    /// SETRATEANCHORTIME, the sender's anchor. `v` is when its frame plays,
    /// mapped to the server clock.
    Sync,
    /// A mark, per receiver: a lost packet's resend was given up on and
    /// it went as silence for `v`. Per speaker: a gap in the stream filled with `v`
    /// of silence.
    Silence,
    /// A mark, per receiver: nothing came for a second, and the reorder
    /// buffer went idle until the next packet.
    RanDry,
    /// A mark, per receiver: the sender's timestamps jumped by `v`, which
    /// was skipped over.
    TimestampJump,
    /// A mark, per receiver: the receiver asked the sender for `v` lost
    /// packets again.
    ResendAsked,
    /// A mark, per receiver: the sender flushed, or AirPlay 2 dropped what
    /// it had handed over (a pause, a new timeline, or a flush). Per
    /// speaker: tas flushed the speaker.
    Flush,
    /// A mark, per receiver: a new sender took it over from the one
    /// playing.
    Takeover,
    /// A mark, per receiver: it started playing, at a sender's first
    /// `RECORD`.
    PlayBegin,
    /// A mark, per receiver: it went quiet, a while after the last sender
    /// left.
    PlayEnd,
    /// A mark, per speaker: the RAOP session was anchored, the next frame
    /// to play at `v`.
    Anchor,
    /// A mark, per speaker: the stream jumped by `v`, and the session was
    /// flushed and anchored again.
    Reanchor,
    /// Per speaker: its `Audio-Latency`, as each RAOP session comes up.
    AudioLatency,
    /// Per speaker: the packets sent again on its asking, this session, as
    /// of each resend request.
    Resent,
    /// Sendspin, per stream: the latency added to a timed source's play
    /// times, as of each of its chunks, so they reach the players in time.
    AddedLatency,
    /// A mark, per stream: a timed source's chunk came with too little
    /// lead, and the latency added to it grew by `v`.
    LatencyAdded,
    /// A mark, per stream: a timed source whose lead may be trimmed
    /// (an AirPlay sender's) started its timeline `v` sooner than its
    /// stamps, so its first chunk plays its keep past the send-ahead
    /// floor; the rest keep the stamps' spacing. Marked again with what
    /// is trimmed when its stamps jump, and it carries on from where its
    /// audio was.
    LeadTrimmed,
    /// A mark, per stream: a timed source's play times jumped from the
    /// timeline by `v` (it skipped, or resumed after a gap without a
    /// flush), and the timeline moved to them.
    TimelineRebase,
    /// Sendspin, per stream: how far ahead of its arrival a timed source's
    /// chunk is stamped to play.
    SourceLead,
    /// AirPlay, per receiver: a chunk's stamp less where blastcast's rate
    /// match puts it, as of each chunk. Past 10 ms, frames are dropped or
    /// repeated until it crosses zero.
    RateError,
    /// AirPlay, per receiver: frames the rate match has dropped this
    /// stream because the sender's clock runs fast, as of each one.
    FramesDropped,
    /// AirPlay, per receiver: frames the rate match has repeated this
    /// stream because the sender's clock runs slow, as of each one.
    FramesRepeated,
    /// AirPlay 2, per PTP timeline (`ptp:` and the grandmaster's clock id
    /// in hex): a Sync's offset, the grandmaster's time less the server
    /// clock, less that packet's one-way delay. Only its changes mean
    /// anything.
    PtpOffset,
    /// AirPlay 2, per PTP timeline: the clock map's drift, how much faster
    /// the grandmaster's clock runs than ours, as of each Sync.
    PtpDrift,
    /// AirPlay 2, per PTP timeline: how far off the clock map may be, the
    /// spread of its envelope, as of each Sync.
    PtpError,
    /// A mark, per PTP timeline: it became the grandmaster a sender plays
    /// on.
    Grandmaster,
    /// A mark, per receiver: an AirPlay 2 SETRATEANCHORTIME. `v` is the
    /// rate it leaves, 1 playing and 0 paused.
    Rate,
    /// A mark, per receiver: an AirPlay 2 FLUSHBUFFERED dropped `v` of the
    /// audio held.
    Flushed,
    /// One a newer server sends.
    #[serde(other)]
    Unknown,
}

/// How a [`Series`]' points are read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// A value at a time, drawn as a line.
    Sample,
    /// A stretch of the timeline from `v` for `d`, drawn as a bar.
    Span,
    /// Something that happened at `t`, drawn as a tick.
    Mark,
}

/// What a [`Series`]' values count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Unit {
    /// A duration in µs.
    Us,
    /// A duration in ms.
    Ms,
    /// A time on the server clock, in µs: a point on the timeline.
    ClockUs,
    /// Parts per million, a rate.
    Ppm,
    Bytes,
    Count,
    /// The value carries nothing.
    None,
}

impl Series {
    pub const ALL: [Series; 41] = [
        Series::ClockOffset,
        Series::Rtt,
        Series::Lead,
        Series::ChunkBytes,
        Series::Buffered,
        Series::ChunkSent,
        Series::ChunkDropped,
        Series::Cursor,
        Series::Floor,
        Series::StreamStart,
        Series::StreamClear,
        Series::TimelineShift,
        Series::StreamEnd,
        Series::ClockMap,
        Series::Sync,
        Series::Silence,
        Series::RanDry,
        Series::TimestampJump,
        Series::ResendAsked,
        Series::Flush,
        Series::Takeover,
        Series::PlayBegin,
        Series::PlayEnd,
        Series::Anchor,
        Series::Reanchor,
        Series::AudioLatency,
        Series::Resent,
        Series::AddedLatency,
        Series::LatencyAdded,
        Series::LeadTrimmed,
        Series::TimelineRebase,
        Series::SourceLead,
        Series::RateError,
        Series::FramesDropped,
        Series::FramesRepeated,
        Series::PtpOffset,
        Series::PtpDrift,
        Series::PtpError,
        Series::Grandmaster,
        Series::Rate,
        Series::Flushed,
    ];

    pub fn kind(self) -> Kind {
        match self {
            Series::ChunkSent | Series::ChunkDropped => Kind::Span,
            Series::StreamStart
            | Series::StreamClear
            | Series::TimelineShift
            | Series::StreamEnd
            | Series::Sync
            | Series::Silence
            | Series::RanDry
            | Series::TimestampJump
            | Series::ResendAsked
            | Series::Flush
            | Series::Takeover
            | Series::PlayBegin
            | Series::PlayEnd
            | Series::Anchor
            | Series::Reanchor
            | Series::LatencyAdded
            | Series::LeadTrimmed
            | Series::TimelineRebase
            | Series::Grandmaster
            | Series::Rate
            | Series::Flushed
            | Series::Unknown => Kind::Mark,
            Series::ClockOffset
            | Series::Rtt
            | Series::Lead
            | Series::ChunkBytes
            | Series::Buffered
            | Series::Cursor
            | Series::Floor
            | Series::ClockMap
            | Series::AudioLatency
            | Series::Resent
            | Series::AddedLatency
            | Series::SourceLead
            | Series::RateError
            | Series::FramesDropped
            | Series::FramesRepeated
            | Series::PtpOffset
            | Series::PtpDrift
            | Series::PtpError => Kind::Sample,
        }
    }

    pub fn unit(self) -> Unit {
        match self {
            Series::ClockOffset
            | Series::Rtt
            | Series::Lead
            | Series::Buffered
            | Series::StreamStart
            | Series::TimelineShift
            | Series::ClockMap
            | Series::Silence
            | Series::TimestampJump
            | Series::Reanchor
            | Series::AudioLatency
            | Series::AddedLatency
            | Series::LatencyAdded
            | Series::LeadTrimmed
            | Series::TimelineRebase
            | Series::SourceLead
            | Series::RateError
            | Series::PtpOffset
            | Series::PtpError
            | Series::Flushed => Unit::Us,
            Series::ChunkBytes => Unit::Bytes,
            Series::ChunkSent
            | Series::ChunkDropped
            | Series::Cursor
            | Series::Floor
            | Series::Sync
            | Series::Anchor => Unit::ClockUs,
            Series::ResendAsked
            | Series::Resent
            | Series::FramesDropped
            | Series::FramesRepeated
            | Series::Rate => Unit::Count,
            Series::PtpDrift => Unit::Ppm,
            Series::StreamClear
            | Series::StreamEnd
            | Series::RanDry
            | Series::Flush
            | Series::Takeover
            | Series::PlayBegin
            | Series::PlayEnd
            | Series::Grandmaster
            | Series::Unknown => Unit::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn every_series_has_a_kind_and_a_unit() {
        for series in Series::ALL {
            let name = serde_json::to_value(series).unwrap();
            assert_eq!(serde_json::from_value::<Series>(name).unwrap(), series);
            if series.kind() == Kind::Span {
                assert_eq!(series.unit(), Unit::ClockUs, "{series:?}");
            }
        }
        assert_eq!(
            serde_json::from_value::<Series>(json!("warp_drift")).unwrap(),
            Series::Unknown
        );
    }
}
