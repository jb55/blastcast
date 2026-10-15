//! FLAC: each 20 ms chunk is one fixed-size FLAC frame, after a codec
//! header of `fLaC` and STREAMINFO that goes in `stream/start`.
//!
//! Frames are independent, so a player joining mid-stream decodes from
//! the first one it gets. Encoded with flacenc, which is pure Rust.

use blastcast::{AudioError, SESSION_FORMAT};
use blastcast_audio::pcm;
use flacenc::bitsink::MemSink;
use flacenc::component::{BitRepr, Stream, StreamInfo};
use flacenc::config;
use flacenc::error::{Verified, Verify};
use flacenc::source::{Fill, FrameBuf};

use super::{BIT_DEPTHS, whole_chunk};
use crate::protocol::{Codec, StreamFormat};
use crate::stream::CHUNK_FRAMES;

/// Frame numbers are 31 bits in a fixed-blocksize stream: 497 days of
/// 20 ms frames before they wrap.
const FRAME_NUMBER_LIMIT: usize = 1 << 31;

/// Bytes a frame can take beyond its samples stored verbatim: the frame
/// header and footer, and a byte per subframe header.
const FRAME_OVERHEAD: usize = 32;

pub struct FlacEncoder {
    format: StreamFormat,
    config: Verified<config::Encoder>,
    info: StreamInfo,
    header: Vec<u8>,
    frame: FrameBuf,
    ints: Vec<i32>,
    frame_number: usize,
}

impl FlacEncoder {
    pub fn new(format: StreamFormat) -> Result<Self, AudioError> {
        if format.codec != Codec::Flac
            || !BIT_DEPTHS.contains(&format.bit_depth)
            || format.sample_rate != SESSION_FORMAT.sample_rate
            || format.channels != u32::from(SESSION_FORMAT.channels)
        {
            return Err(AudioError::Format(format!(
                "FLAC is sent only as {} kHz stereo at 16 or 24 bits, not {format:?}",
                SESSION_FORMAT.sample_rate / 1000
            )));
        }
        let err = |e: flacenc::error::VerifyError| AudioError::Encoder(e.to_string());
        let channels = format.channels as usize;
        let mut config = config::Encoder::default();
        config.block_size = CHUNK_FRAMES;
        config.multithread = false;
        let config = config.into_verified().map_err(|(_, e)| err(e))?;
        let mut info = StreamInfo::new(
            format.sample_rate as usize,
            channels,
            format.bit_depth as usize,
        )
        .map_err(err)?;
        info.set_block_sizes(CHUNK_FRAMES, CHUNK_FRAMES)
            .map_err(err)?;
        // A stream with no frames writes as `fLaC` and its STREAMINFO,
        // marked as the last metadata block.
        let header = to_bytes(&Stream::with_stream_info(info.clone()))?;
        Ok(Self {
            format,
            config,
            info,
            header,
            frame: FrameBuf::with_size(channels, CHUNK_FRAMES).map_err(err)?,
            ints: Vec::new(),
            frame_number: 0,
        })
    }

    pub fn format(&self) -> StreamFormat {
        self.format
    }

    /// `fLaC` and the STREAMINFO block.
    pub fn header(&self) -> &[u8] {
        &self.header
    }

    /// The most a chunk of `session_frames` can take: the samples stored
    /// verbatim, which FLAC falls back to when prediction doesn't pay.
    pub fn estimate_bytes(&self, session_frames: usize) -> usize {
        let frames = session_frames.max(CHUNK_FRAMES);
        frames * self.format.channels as usize * (self.format.bit_depth as usize / 8)
            + FRAME_OVERHEAD
    }

    /// One frame of [`CHUNK_FRAMES`]; a short last chunk is padded with
    /// silence.
    pub fn encode(&mut self, samples: &[f32]) -> Result<(Vec<u8>, usize), AudioError> {
        let samples = whole_chunk(samples)?;
        self.ints.clear();
        pcm::f32_to_int(&samples, self.format.bit_depth, &mut self.ints);
        self.frame
            .fill_interleaved(&self.ints)
            .map_err(|e| AudioError::Encoder(e.to_string()))?;
        let frame = flacenc::encode_fixed_size_frame(
            &self.config,
            &self.frame,
            self.frame_number,
            &self.info,
        )
        .map_err(|e| AudioError::Encoder(e.to_string()))?;
        self.frame_number = (self.frame_number + 1) % FRAME_NUMBER_LIMIT;
        Ok((to_bytes(&frame)?, CHUNK_FRAMES))
    }
}

fn to_bytes(component: &impl BitRepr) -> Result<Vec<u8>, AudioError> {
    let mut sink = MemSink::<u8>::with_capacity(component.count_bits());
    component
        .write(&mut sink)
        .map_err(|e| AudioError::Encoder(e.to_string()))?;
    Ok(sink.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::encode::tests::{fmt, sine};

    /// Decodes a header and frames with claxon, a decoder that shares no
    /// code with the encoder.
    fn decode(header: &[u8], frames: &[Vec<u8>]) -> (claxon::metadata::StreamInfo, Vec<i32>) {
        let mut bytes = header.to_vec();
        for f in frames {
            bytes.extend_from_slice(f);
        }
        let mut reader = claxon::FlacReader::new(&bytes[..]).unwrap();
        let info = reader.streaminfo();
        let samples = reader.samples().map(Result::unwrap).collect();
        (info, samples)
    }

    #[test]
    fn round_trips_through_another_decoder() {
        for bits in [16, 24] {
            let mut enc = FlacEncoder::new(fmt(Codec::Flac, 48_000, bits)).unwrap();
            assert_eq!(&enc.header()[..4], b"fLaC");
            // The STREAMINFO block's header: type 0, last, 34 bytes.
            assert_eq!(enc.header()[4..8], [0x80, 0, 0, 34]);
            let input = sine(3 * CHUNK_FRAMES);
            let mut frames = Vec::new();
            for chunk in input.chunks(CHUNK_FRAMES * 2) {
                let (bytes, n) = enc.encode(chunk).unwrap();
                assert_eq!(n, CHUNK_FRAMES);
                assert!(bytes.len() <= enc.estimate_bytes(CHUNK_FRAMES));
                frames.push(bytes);
            }
            let (info, decoded) = decode(enc.header(), &frames);
            assert_eq!(
                (info.sample_rate, info.channels, info.bits_per_sample),
                (48_000, 2, bits)
            );
            assert_eq!((info.min_block_size, info.max_block_size), (960, 960));
            let mut want = Vec::new();
            pcm::f32_to_int(&input, bits, &mut want);
            assert_eq!(decoded, want, "lossless at {bits} bits");
        }
    }

    #[test]
    fn compresses_and_pads_the_last_chunk() {
        let mut enc = FlacEncoder::new(fmt(Codec::Flac, 48_000, 16)).unwrap();
        let (silence, _) = enc.encode(&vec![0.0; CHUNK_FRAMES * 2]).unwrap();
        assert!(silence.len() < 64, "{} bytes of silence", silence.len());
        let (tail, frames) = enc.encode(&sine(100)).unwrap();
        assert_eq!(frames, CHUNK_FRAMES);
        let (_, decoded) = decode(enc.header(), &[silence, tail]);
        assert_eq!(decoded.len(), 2 * CHUNK_FRAMES * 2);
        assert!(decoded[CHUNK_FRAMES * 2 + 200..].iter().all(|&s| s == 0));
    }

    #[test]
    fn refuses_what_it_cannot_send() {
        assert!(FlacEncoder::new(fmt(Codec::Flac, 44_100, 16)).is_err());
        assert!(FlacEncoder::new(fmt(Codec::Flac, 48_000, 32)).is_err());
        assert!(FlacEncoder::new(fmt(Codec::Pcm, 48_000, 16)).is_err());
    }
}
