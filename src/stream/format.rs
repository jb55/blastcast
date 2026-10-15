//! Choosing the format a player is sent: from its hello's list, and again
//! when it asks for another with `stream/request-format`.

use blastcast::SESSION_FORMAT;

use super::encode::can_encode;
use crate::protocol::{Codec, PlayerSupport, RequestFormatPlayer, StreamFormat};

/// The format to send a player: the first in its preference order that
/// this server encodes at the session's rate, so nothing is resampled,
/// and failing that the first it encodes at any rate. `None` when the
/// player lists nothing this server can send.
///
/// The spec has the server take the highest-priority entry it can
/// produce, and allows a lower one to avoid resampling. sendspin-go lists
/// 192 kHz PCM first, and taking that outright would upsample every
/// chunk fourfold for nothing.
pub fn pick_format(support: &PlayerSupport) -> Option<StreamFormat> {
    let candidates: Vec<StreamFormat> = support.formats().into_iter().filter(can_encode).collect();
    candidates
        .iter()
        .find(|f| f.sample_rate == SESSION_FORMAT.sample_rate)
        .or(candidates.first())
        .copied()
}

/// The format a `stream/request-format` asks for. A field the request
/// leaves out keeps its `current` value, as the spec has it.
///
/// The answer is an entry of the player's list that has every field the
/// request names: the one that also keeps the current values if listed,
/// else the first. A request naming a codec alone can thus move from
/// 44.1 kHz PCM to 48 kHz Opus, where the current rate can't be kept. A
/// player that lists nothing matching still gets what it asked for if it
/// can be sent. `None` when it can't, and the player keeps its format.
pub fn requested_format(
    support: &PlayerSupport,
    current: Option<StreamFormat>,
    request: &RequestFormatPlayer,
) -> Option<StreamFormat> {
    let target = current
        .or_else(|| pick_format(support))
        .map(|base| StreamFormat {
            codec: request.codec.unwrap_or(base.codec),
            channels: request.channels.unwrap_or(base.channels),
            sample_rate: request.sample_rate.unwrap_or(base.sample_rate),
            bit_depth: request.bit_depth.unwrap_or(base.bit_depth),
        });
    let candidates: Vec<StreamFormat> = support
        .formats()
        .into_iter()
        .filter(|f| names(request, f) && can_encode(f))
        .collect();
    if let Some(listed) = target.and_then(|t| candidates.iter().find(|f| same(f, &t))) {
        return Some(*listed);
    }
    candidates
        .first()
        .copied()
        .or(target.filter(|t| names(request, t) && can_encode(t)))
}

/// Whether `format` has every field `request` names.
fn names(request: &RequestFormatPlayer, format: &StreamFormat) -> bool {
    request.codec.is_none_or(|c| c == format.codec)
        && request.channels.is_none_or(|c| c == format.channels)
        && request.sample_rate.is_none_or(|r| r == format.sample_rate)
        && request
            .bit_depth
            .is_none_or(|b| b == format.bit_depth || format.codec == Codec::Opus)
}

/// The same format, but for an Opus bit depth, which means nothing.
fn same(a: &StreamFormat, b: &StreamFormat) -> bool {
    a.codec == b.codec
        && a.channels == b.channels
        && a.sample_rate == b.sample_rate
        && (a.bit_depth == b.bit_depth || a.codec == Codec::Opus)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::encode::tests::fmt;

    fn pcm(sample_rate: u32, bit_depth: u32) -> StreamFormat {
        fmt(Codec::Pcm, sample_rate, bit_depth)
    }

    fn support(formats: Vec<StreamFormat>) -> PlayerSupport {
        PlayerSupport {
            supported_formats: formats,
            ..Default::default()
        }
    }

    #[test]
    fn prefers_the_session_rate_in_the_players_order() {
        // sendspin-go's default list.
        let s = support(vec![
            pcm(192_000, 24),
            pcm(96_000, 24),
            pcm(48_000, 16),
            pcm(44_100, 16),
            fmt(Codec::Flac, 48_000, 24),
        ]);
        assert_eq!(pick_format(&s), Some(pcm(48_000, 16)));
        let s = support(vec![pcm(48_000, 24), pcm(48_000, 16)]);
        assert_eq!(pick_format(&s), Some(pcm(48_000, 24)));
    }

    #[test]
    fn takes_a_compressed_codec_the_player_lists_first() {
        let s = support(vec![
            fmt(Codec::Opus, 48_000, 16),
            fmt(Codec::Flac, 48_000, 24),
            pcm(48_000, 16),
        ]);
        assert_eq!(pick_format(&s), Some(fmt(Codec::Opus, 48_000, 16)));
        // FLAC at 44.1 kHz would need resampling in variable blocks, so
        // it's skipped, and PCM at the same rate is resampled instead.
        let s = support(vec![fmt(Codec::Flac, 44_100, 16), pcm(44_100, 16)]);
        assert_eq!(pick_format(&s), Some(pcm(44_100, 16)));
    }

    #[test]
    fn falls_back_to_another_rate() {
        let s = support(vec![
            fmt(Codec::Opus, 44_100, 16),
            pcm(44_100, 32),
            pcm(44_100, 16),
        ]);
        assert_eq!(pick_format(&s), Some(pcm(44_100, 16)));
        let s = support(vec![pcm(44_100, 16), fmt(Codec::Opus, 24_000, 16)]);
        assert_eq!(pick_format(&s), Some(pcm(44_100, 16)));
    }

    #[test]
    fn refuses_what_it_cannot_send() {
        let mono = StreamFormat {
            channels: 1,
            ..pcm(48_000, 16)
        };
        assert_eq!(pick_format(&support(vec![mono, pcm(48_000, 8)])), None);
        assert_eq!(pick_format(&support(vec![])), None);
    }

    #[test]
    fn reads_the_oldest_per_dimension_lists() {
        let s = PlayerSupport {
            support_codecs: vec![Codec::Pcm],
            support_channels: vec![2],
            support_sample_rates: vec![44_100, 48_000],
            support_bit_depth: vec![16],
            ..Default::default()
        };
        assert_eq!(pick_format(&s), Some(pcm(48_000, 16)));
    }

    fn request(codec: Option<Codec>, sample_rate: Option<u32>) -> RequestFormatPlayer {
        RequestFormatPlayer {
            codec,
            sample_rate,
            ..Default::default()
        }
    }

    #[test]
    fn a_request_keeps_what_it_leaves_out() {
        let s = support(vec![
            pcm(48_000, 24),
            fmt(Codec::Flac, 48_000, 16),
            fmt(Codec::Flac, 48_000, 24),
            fmt(Codec::Opus, 48_000, 16),
        ]);
        let now = Some(pcm(48_000, 24));
        // Keeps 24 bits, though the 16-bit FLAC entry comes first.
        assert_eq!(
            requested_format(&s, now, &request(Some(Codec::Flac), None)),
            Some(fmt(Codec::Flac, 48_000, 24))
        );
        // Opus's bit depth is the listed entry's, not the current 24.
        assert_eq!(
            requested_format(&s, now, &request(Some(Codec::Opus), None)),
            Some(fmt(Codec::Opus, 48_000, 16))
        );
    }

    #[test]
    fn a_request_that_cannot_keep_the_rate_takes_the_first_match() {
        let s = support(vec![
            pcm(44_100, 16),
            fmt(Codec::Opus, 24_000, 16),
            fmt(Codec::Opus, 48_000, 16),
        ]);
        let now = Some(pcm(44_100, 16));
        assert_eq!(
            requested_format(&s, now, &request(Some(Codec::Opus), None)),
            Some(fmt(Codec::Opus, 24_000, 16))
        );
        assert_eq!(
            requested_format(&s, now, &request(Some(Codec::Opus), Some(48_000))),
            Some(fmt(Codec::Opus, 48_000, 16))
        );
    }

    #[test]
    fn an_unlisted_request_is_sent_if_it_can_be() {
        let s = support(vec![pcm(48_000, 16)]);
        let now = Some(pcm(48_000, 16));
        assert_eq!(
            requested_format(&s, now, &request(Some(Codec::Flac), None)),
            Some(fmt(Codec::Flac, 48_000, 16))
        );
        assert_eq!(
            requested_format(&s, now, &request(Some(Codec::Flac), Some(44_100))),
            None
        );
        assert_eq!(
            requested_format(&s, now, &request(Some(Codec::Unknown), None)),
            None
        );
    }

    #[test]
    fn a_request_before_any_stream_starts_from_the_default() {
        let s = support(vec![pcm(48_000, 16), fmt(Codec::Opus, 48_000, 16)]);
        assert_eq!(
            requested_format(&s, None, &request(Some(Codec::Opus), None)),
            Some(fmt(Codec::Opus, 48_000, 16))
        );
    }
}
