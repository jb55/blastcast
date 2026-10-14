//! Binary frames: a type byte, a big-endian i64 timestamp in server-clock
//! microseconds, and the payload.
//!
//! Audio chunks (type 4) grew a field. sendspin-go and aiosendspin 9.x send
//! the 9-byte header; the current spec (commit bda23f3) puts a big-endian
//! u32 `send_ahead` after the timestamp, 13 bytes. A client reads the header
//! its own version knows, so the server picks the [`HeaderLayout`] per
//! client: every plaintext v1 client gets `Legacy`, and `SendAhead` waits for
//! the encrypted (Noise) connections that come with the newer spec. Other
//! binary types never carry `send_ahead`.

/// Audio chunk, the player role's slot 0.
pub const AUDIO_CHUNK: u8 = 4;
/// Artwork channels 0-3 are types 8-11.
pub const ARTWORK_CHANNEL_0: u8 = 8;
pub const ARTWORK_CHANNELS: u8 = 4;

const TYPE_AND_TIMESTAMP: usize = 1 + 8;

/// Which audio chunk header a client reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HeaderLayout {
    /// Type and timestamp: 9 bytes.
    #[default]
    Legacy,
    /// Type, timestamp and `send_ahead`: 13 bytes.
    SendAhead,
}

impl HeaderLayout {
    /// The header length of a frame of type `kind`.
    pub fn header_len(self, kind: u8) -> usize {
        match self {
            Self::SendAhead if kind == AUDIO_CHUNK => TYPE_AND_TIMESTAMP + 4,
            _ => TYPE_AND_TIMESTAMP,
        }
    }
}

/// A decoded binary frame, borrowing its payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinaryFrame<'a> {
    pub kind: u8,
    /// Server clock, microseconds: when the first sample plays, or the image
    /// shows.
    pub timestamp_us: i64,
    /// Only on audio chunks in the `SendAhead` layout.
    pub send_ahead_us: Option<u32>,
    pub payload: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FrameError {
    #[error("binary frame of type {kind} is {len} bytes, shorter than its {need}-byte header")]
    Short { kind: u8, len: usize, need: usize },
    #[error("empty binary frame")]
    Empty,
}

impl<'a> BinaryFrame<'a> {
    pub fn decode(frame: &'a [u8], layout: HeaderLayout) -> Result<Self, FrameError> {
        let &kind = frame.first().ok_or(FrameError::Empty)?;
        let need = layout.header_len(kind);
        if frame.len() < need {
            return Err(FrameError::Short {
                kind,
                len: frame.len(),
                need,
            });
        }
        let timestamp_us = i64::from_be_bytes(frame[1..9].try_into().expect("8 bytes"));
        let send_ahead_us = (need > TYPE_AND_TIMESTAMP)
            .then(|| u32::from_be_bytes(frame[9..13].try_into().expect("4 bytes")));
        Ok(Self {
            kind,
            timestamp_us,
            send_ahead_us,
            payload: &frame[need..],
        })
    }

    /// The artwork channel (0-3) for artwork frames.
    pub fn artwork_channel(&self) -> Option<u8> {
        self.kind
            .checked_sub(ARTWORK_CHANNEL_0)
            .filter(|&c| c < ARTWORK_CHANNELS)
    }
}

/// Encode an audio chunk. `send_ahead_us` is written only in the
/// `SendAhead` layout; get it from [`send_ahead`].
pub fn audio_chunk(
    layout: HeaderLayout,
    timestamp_us: i64,
    send_ahead_us: u32,
    payload: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(layout.header_len(AUDIO_CHUNK) + payload.len());
    out.push(AUDIO_CHUNK);
    out.extend_from_slice(&timestamp_us.to_be_bytes());
    if layout == HeaderLayout::SendAhead {
        out.extend_from_slice(&send_ahead_us.to_be_bytes());
    }
    out.extend_from_slice(payload);
    out
}

/// Encode an artwork image for `channel` (0-3).
pub fn artwork(channel: u8, timestamp_us: i64, image: &[u8]) -> Vec<u8> {
    assert!(channel < ARTWORK_CHANNELS, "artwork channel {channel} > 3");
    let mut out = Vec::with_capacity(TYPE_AND_TIMESTAMP + image.len());
    out.push(ARTWORK_CHANNEL_0 + channel);
    out.extend_from_slice(&timestamp_us.to_be_bytes());
    out.extend_from_slice(image);
    out
}

/// The `send_ahead` field for a chunk sent at `transmit_us` that plays at
/// `timestamp_us`. It saturates as the spec says: 0 when sent at or after
/// the play time, `u32::MAX` when the lead doesn't fit (about 71 minutes).
pub fn send_ahead(transmit_us: i64, timestamp_us: i64) -> u32 {
    let lead = timestamp_us.saturating_sub(transmit_us);
    if lead <= 0 {
        0
    } else {
        u32::try_from(lead).unwrap_or(u32::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_audio_chunk_round_trip() {
        let frame = audio_chunk(HeaderLayout::Legacy, 2_061_315, 99, &[1, 2, 3]);
        assert_eq!(
            frame,
            [4, 0, 0, 0, 0, 0, 0x1f, 0x74, 0x03, 1, 2, 3],
            "9-byte header, send_ahead dropped"
        );
        let f = BinaryFrame::decode(&frame, HeaderLayout::Legacy).unwrap();
        assert_eq!(f.kind, AUDIO_CHUNK);
        assert_eq!(f.timestamp_us, 2_061_315);
        assert_eq!(f.send_ahead_us, None);
        assert_eq!(f.payload, [1, 2, 3]);
    }

    #[test]
    fn send_ahead_audio_chunk_round_trip() {
        let frame = audio_chunk(HeaderLayout::SendAhead, -5, 250_000, &[9]);
        assert_eq!(frame.len(), 13 + 1);
        assert_eq!(&frame[9..13], &250_000u32.to_be_bytes());
        let f = BinaryFrame::decode(&frame, HeaderLayout::SendAhead).unwrap();
        assert_eq!(f.timestamp_us, -5);
        assert_eq!(f.send_ahead_us, Some(250_000));
        assert_eq!(f.payload, [9]);
    }

    #[test]
    fn send_ahead_layout_leaves_artwork_alone() {
        let frame = artwork(2, 7, b"png");
        assert_eq!(frame[0], 10);
        let f = BinaryFrame::decode(&frame, HeaderLayout::SendAhead).unwrap();
        assert_eq!(f.artwork_channel(), Some(2));
        assert_eq!(f.send_ahead_us, None);
        assert_eq!(f.payload, b"png");
    }

    #[test]
    fn short_frames_are_errors() {
        assert_eq!(
            BinaryFrame::decode(&[], HeaderLayout::Legacy),
            Err(FrameError::Empty)
        );
        let header_only = audio_chunk(HeaderLayout::Legacy, 1, 0, &[]);
        assert!(BinaryFrame::decode(&header_only, HeaderLayout::Legacy).is_ok());
        assert_eq!(
            BinaryFrame::decode(&header_only, HeaderLayout::SendAhead),
            Err(FrameError::Short {
                kind: 4,
                len: 9,
                need: 13
            })
        );
    }

    #[test]
    fn send_ahead_saturates() {
        assert_eq!(send_ahead(100, 350), 250);
        assert_eq!(send_ahead(350, 350), 0);
        assert_eq!(send_ahead(400, 350), 0);
        assert_eq!(send_ahead(0, i64::from(u32::MAX) + 1), u32::MAX);
        assert_eq!(send_ahead(i64::MIN, i64::MAX), u32::MAX);
    }

    #[test]
    fn non_artwork_has_no_channel() {
        let f = BinaryFrame::decode(&[4; 9], HeaderLayout::Legacy).unwrap();
        assert_eq!(f.artwork_channel(), None);
        let f = BinaryFrame::decode(&[12; 9], HeaderLayout::Legacy).unwrap();
        assert_eq!(f.artwork_channel(), None);
    }
}
