//! What a sender's `SET_PARAMETER`s say, as [`MetadataEvent`]s:
//!
//! - `text/parameters`: `volume: <dB>` and `progress: start/current/end`
//!   lines.
//! - `application/x-dmap-tagged`: a DMAP listing item (`mlit`) with the
//!   track's `minm` (title), `asar` (artist), `asal` (album) and `astm`
//!   (length, ms).
//! - `image/*`: the cover art. An empty body, or `image/none`, is no art
//!   and says nothing.

use std::time::Duration;

use crate::metadata::{Artwork, MetadataEvent, Progress, SenderVolume, TrackInfo};

/// What a `SET_PARAMETER` with this content type and body says.
pub fn events(content_type: &str, body: &[u8]) -> Vec<MetadataEvent> {
    let content_type = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    match content_type.as_str() {
        "text/parameters" => text_parameters(&String::from_utf8_lossy(body)),
        "application/x-dmap-tagged" => track(body)
            .filter(|t| *t != TrackInfo::default())
            .map(MetadataEvent::Track)
            .into_iter()
            .collect(),
        "image/none" => Vec::new(),
        t if t.starts_with("image/") && !body.is_empty() => {
            vec![MetadataEvent::Artwork(Artwork::new(body.to_vec()))]
        }
        _ => Vec::new(),
    }
}

fn text_parameters(text: &str) -> Vec<MetadataEvent> {
    text.lines()
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            match key.trim() {
                "volume" => {
                    let db = value.trim().parse::<f64>().ok()?;
                    Some(MetadataEvent::Volume(SenderVolume::from_db(db)))
                }
                "progress" => Progress::parse(value).map(MetadataEvent::Progress),
                _ => None,
            }
        })
        .collect()
}

/// The track in a DMAP body: its items, or those of the `mlit` it holds.
fn track(body: &[u8]) -> Option<TrackInfo> {
    let mut items = dmap_items(body)?;
    if let [(code, inner)] = items[..]
        && code == *b"mlit"
    {
        items = dmap_items(inner)?;
    }
    let text = |v: &[u8]| Some(String::from_utf8_lossy(v).trim_end_matches('\0').to_owned());
    let mut track = TrackInfo::default();
    for (code, value) in items {
        match &code {
            b"minm" => track.title = text(value),
            b"asar" => track.artist = text(value),
            b"asal" => track.album = text(value),
            b"astm" => {
                track.duration = value
                    .first_chunk::<4>()
                    .map(|ms| Duration::from_millis(u32::from_be_bytes(*ms).into()));
            }
            _ => {}
        }
    }
    Some(track)
}

/// DMAP's items: a four-letter code, a big-endian u32 length and that many
/// bytes, back to back. `None` if one runs past the end.
fn dmap_items(mut body: &[u8]) -> Option<Vec<([u8; 4], &[u8])>> {
    let mut items = Vec::new();
    while !body.is_empty() {
        let code: [u8; 4] = body.get(..4)?.try_into().ok()?;
        let len = u32::from_be_bytes(body.get(4..8)?.try_into().ok()?) as usize;
        let value = body.get(8..8 + len)?;
        items.push((code, value));
        body = &body[8 + len..];
    }
    Some(items)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn dmap(code: &[u8; 4], value: &[u8]) -> Vec<u8> {
        let mut out = code.to_vec();
        out.extend_from_slice(&(value.len() as u32).to_be_bytes());
        out.extend_from_slice(value);
        out
    }

    /// A track as iTunes sends it: an `mlit` of items, some we don't use.
    pub(crate) fn listing(title: &str, artist: &str, album: &str, ms: u32) -> Vec<u8> {
        let items = [
            dmap(b"mikd", &[2]),
            dmap(b"minm", title.as_bytes()),
            dmap(b"asar", artist.as_bytes()),
            dmap(b"asal", album.as_bytes()),
            dmap(b"astm", &ms.to_be_bytes()),
            dmap(b"asgn", b"Trip Hop"),
        ]
        .concat();
        dmap(b"mlit", &items)
    }

    #[test]
    fn a_dmap_listing_is_a_track() {
        let body = listing(
            "Unfinished Sympathy",
            "Massive Attack",
            "Blue Lines",
            308_000,
        );
        assert_eq!(
            events("application/x-dmap-tagged", &body),
            [MetadataEvent::Track(TrackInfo {
                title: Some("Unfinished Sympathy".into()),
                artist: Some("Massive Attack".into()),
                album: Some("Blue Lines".into()),
                duration: Some(Duration::from_secs(308)),
            })]
        );
        // Bare items, without the listing around them.
        let bare = dmap(b"minm", b"Teardrop");
        let [MetadataEvent::Track(t)] = &events("application/x-dmap-tagged", &bare)[..] else {
            panic!()
        };
        assert_eq!(t.title.as_deref(), Some("Teardrop"));
        // Cut short, or about nothing we show: nothing.
        assert!(events("application/x-dmap-tagged", &body[..body.len() - 2]).is_empty());
        assert!(events("application/x-dmap-tagged", &dmap(b"mikd", &[2])).is_empty());
    }

    #[test]
    fn volume_and_progress() {
        assert_eq!(
            events("text/parameters", b"volume: -15.000000\r\n"),
            [MetadataEvent::Volume(SenderVolume {
                level: 50,
                muted: false
            })]
        );
        assert_eq!(
            events("text/parameters", b"volume: -144.000000\r\n"),
            [MetadataEvent::Volume(SenderVolume {
                level: 0,
                muted: true
            })]
        );
        let [MetadataEvent::Progress(p)] =
            &events("text/parameters", b"progress: 1000/45100/442000\r\n")[..]
        else {
            panic!()
        };
        assert_eq!(p.elapsed, Duration::from_secs(1));
        assert_eq!(p.duration, Some(Duration::from_secs(10)));
        assert!(events("text/parameters", b"volume: loud\r\n").is_empty());
    }

    #[test]
    fn cover_art() {
        let jpeg = vec![0xFF, 0xD8, 0xFF, 0xE0, 1, 2, 3];
        let [MetadataEvent::Artwork(art)] = &events("image/jpeg", &jpeg)[..] else {
            panic!()
        };
        assert_eq!(art.content_type(), Some("image/jpeg"));
        assert!(events("image/jpeg", b"").is_empty());
        assert!(events("image/none", b"").is_empty());
        assert!(events("application/octet-stream", &jpeg).is_empty());
    }
}
