//! Buffered audio (stream type 103): the TCP data channel SETUP opens, its
//! packets decrypted with the stream's `shk` and decoded to PCM, each
//! tagged with its RTP time.
//!
//! On the wire, each packet is `[length: u16 BE, counting itself][packet]`.
//! The packet is a 12-byte RTP header (sequence number, 24 bits in the
//! first four bytes; RTP time; SSRC), then the ChaCha20-Poly1305
//! ciphertext and its 16-byte tag, then 8 bytes of nonce. The key is
//! `shk`, the nonce is four zero bytes and those last 8, and the AAD is the
//! header's RTP time and SSRC (`packet[4..12]`), as shairport-sync's
//! ap2_buffered_audio_processor.c reads them.
//!
//! The payload is one ALAC packet (`ct` 2) or one raw AAC-LC frame (`ct`
//! 4, no ADTS header), 44.1 kHz stereo. ALAC is decoded as the AirPlay 1
//! receiver decodes it; AAC with symphonia's pure-Rust AAC-LC.
//!
//! What comes out is a [`Decoded`] per packet, in the order sent, on a
//! bounded channel, so a consumer that holds back holds the socket back.
//!
//! Ported from `openairplay2` 0.5's buffered.rs and decode.rs (MIT,
//! Copyright (c) 2026 Stefan Arentz).

use std::io;

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Nonce};
use symphonia_codec_aac::AacDecoder;
use symphonia_core::codecs::audio::well_known::CODEC_ID_AAC;
use symphonia_core::codecs::audio::{AudioCodecParameters, AudioDecoder, AudioDecoderOptions};
use symphonia_core::packet::PacketRef;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc;

use crate::raop::announce::{self, CHANNELS, SAMPLE_RATE};

/// SETUP's stream `type` for buffered audio.
pub const TYPE_BUFFERED: u64 = 103;
/// SETUP's stream `type` for realtime audio, which we refuse.
pub const TYPE_REALTIME: u64 = 96;

const HEADER: usize = 12;
const TAG: usize = 16;
const NONCE: usize = 8;

/// The codec SETUP's `ct` names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    /// `ct` 2, `spf` frames a packet.
    Alac { spf: u32 },
    /// `ct` 4: AAC-LC, 1024 frames a packet.
    Aac,
}

impl Codec {
    /// The stream's codec from SETUP's `ct`, `spf` and `sr`; or why we
    /// can't play it.
    pub fn from_setup(ct: Option<u64>, spf: Option<u64>, sr: Option<u64>) -> Result<Self, String> {
        if let Some(sr) = sr
            && sr != u64::from(SAMPLE_RATE)
        {
            return Err(format!("a {sr} Hz stream (only {SAMPLE_RATE} Hz is)"));
        }
        match ct {
            Some(2) => {
                let spf = spf.unwrap_or(352);
                match u32::try_from(spf) {
                    Ok(spf @ 1..=16_384) => Ok(Self::Alac { spf }),
                    _ => Err(format!("ALAC with {spf} frames a packet")),
                }
            }
            Some(4) => Ok(Self::Aac),
            // shairport-sync's rtsp.c reads 8 as AAC-ELD, which symphonia
            // can't decode.
            Some(8) => Err("AAC-ELD (ct 8)".into()),
            Some(ct) => Err(format!("compression type {ct}")),
            None => Err("a stream without a compression type (ct)".into()),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Alac { .. } => "alac",
            Self::Aac => "aac",
        }
    }
}

/// One stream's `shk`, which every audio packet is sealed with.
#[derive(Clone)]
pub struct AudioKey(ChaCha20Poly1305);

impl std::fmt::Debug for AudioKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AudioKey").finish_non_exhaustive()
    }
}

/// A packet, opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    /// 24 bits.
    pub seq: u32,
    /// The RTP time of its first frame.
    pub rtp: u32,
    pub ssrc: u32,
    /// One ALAC packet or AAC frame.
    pub payload: Vec<u8>,
}

impl AudioKey {
    /// `None` unless `shk` is 32 bytes.
    pub fn new(shk: &[u8]) -> Option<Self> {
        let key: [u8; 32] = shk.try_into().ok()?;
        Some(Self(ChaCha20Poly1305::new(&key.into())))
    }

    /// Opens `packet` (what follows its length), or `None` if it is too
    /// short or doesn't authenticate.
    pub fn open(&self, packet: &[u8]) -> Option<Packet> {
        if packet.len() < HEADER + TAG + NONCE {
            return None;
        }
        let word = |at: usize| u32::from_be_bytes(packet[at..at + 4].try_into().expect("4 bytes"));
        let suffix = packet.len() - NONCE;
        let payload = self
            .0
            .decrypt(
                &nonce(&packet[suffix..]),
                Payload {
                    msg: &packet[HEADER..suffix],
                    aad: &packet[4..HEADER],
                },
            )
            .ok()?;
        Some(Packet {
            seq: word(0) & 0xff_ffff,
            rtp: word(4),
            ssrc: word(8),
            payload,
        })
    }

    /// `packet` sealed with `suffix` as its nonce, and its length in
    /// front: what a sender writes.
    #[cfg(any(test, feature = "testing"))]
    pub fn seal(&self, packet: &Packet, suffix: [u8; NONCE]) -> Vec<u8> {
        let mut header = Vec::with_capacity(HEADER);
        header.extend_from_slice(&(packet.seq & 0xff_ffff).to_be_bytes());
        header.extend_from_slice(&packet.rtp.to_be_bytes());
        header.extend_from_slice(&packet.ssrc.to_be_bytes());
        let sealed = self
            .0
            .encrypt(
                &nonce(&suffix),
                Payload {
                    msg: &packet.payload,
                    aad: &header[4..],
                },
            )
            .expect("a packet is far below ChaCha20's limit");
        let len = 2 + HEADER + sealed.len() + NONCE;
        let mut out = Vec::with_capacity(len);
        out.extend_from_slice(&(len as u16).to_be_bytes());
        out.extend_from_slice(&header);
        out.extend_from_slice(&sealed);
        out.extend_from_slice(&suffix);
        out
    }
}

fn nonce(suffix: &[u8]) -> Nonce {
    let mut nonce = [0u8; 12];
    nonce[4..].copy_from_slice(suffix);
    nonce.into()
}

/// Decodes one stream's payloads to interleaved s16 stereo.
pub enum Decoder {
    Alac(announce::Decoder),
    Aac {
        decoder: Box<AacDecoder>,
        pcm: Vec<i16>,
    },
}

impl std::fmt::Debug for Decoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Alac(_) => "Decoder::Alac",
            Self::Aac { .. } => "Decoder::Aac",
        })
    }
}

impl Decoder {
    pub fn new(codec: Codec) -> Result<Self, String> {
        match codec {
            Codec::Alac { spf } => Ok(Self::Alac(announce::Decoder::new(&announce::Codec::Alac(
                // The fmtp an AirPlay 1 sender would announce: 16-bit
                // stereo at 44.1 kHz, with ALAC's usual tuning.
                format!("{spf} 0 16 40 10 14 2 255 0 0 {SAMPLE_RATE}"),
            )))),
            Codec::Aac => {
                let mut params = AudioCodecParameters::new();
                params
                    .for_codec(CODEC_ID_AAC)
                    .with_sample_rate(SAMPLE_RATE)
                    .with_extra_data(audio_specific_config().into());
                AacDecoder::try_new(&params, &AudioDecoderOptions::default())
                    .map(|decoder| Self::Aac {
                        decoder: Box::new(decoder),
                        pcm: Vec::new(),
                    })
                    .map_err(|e| format!("can't make an AAC decoder: {e}"))
            }
        }
    }

    /// One payload's frames, interleaved.
    pub fn decode(&mut self, payload: &[u8]) -> Result<Vec<i16>, String> {
        match self {
            Self::Alac(alac) => alac.decode(payload).map_err(|e| e.to_string()),
            Self::Aac { decoder, pcm } => {
                let packet = PacketRef::new(0, 0.into(), 0u64.into(), payload);
                let decoded = decoder
                    .decode_ref(&packet)
                    .map_err(|e| format!("bad AAC: {e}"))?;
                decoded.copy_to_vec_interleaved(pcm);
                Ok(pcm.clone())
            }
        }
    }
}

/// AAC-LC's AudioSpecificConfig for 44.1 kHz stereo: object type 2 in
/// five bits, sample rate index 4 in four, channel configuration 2 in
/// four.
fn audio_specific_config() -> [u8; 2] {
    const LC: u8 = 2;
    const RATE_44100: u8 = 4;
    let channels = CHANNELS as u8;
    [
        (LC << 3) | (RATE_44100 >> 1),
        ((RATE_44100 & 1) << 7) | (channels << 3),
    ]
}

/// One packet's audio.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decoded {
    pub seq: u32,
    /// The RTP time of the first frame.
    pub rtp: u32,
    /// Interleaved s16 stereo.
    pub frames: Vec<i16>,
}

/// How a data channel ended.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Tally {
    pub decoded: u64,
    /// Packets that didn't authenticate: a wrong `shk`, or tampered.
    pub unopened: u64,
    pub undecodable: u64,
}

/// Packets that don't authenticate, with none that did, before the data
/// channel is given up on: the sender's key isn't the one SETUP gave.
const WRONG_KEY: u64 = 8;

/// Reads `data` until the sender closes it, sending each packet's audio to
/// `out` in order, and waiting while `out` is full; dropped, when there is
/// no `out`. An error when the stream's key doesn't open its packets.
pub async fn receive<R: AsyncRead + Unpin>(
    mut data: R,
    key: &AudioKey,
    decoder: &mut Decoder,
    out: Option<mpsc::Sender<Decoded>>,
) -> io::Result<Tally> {
    let mut tally = Tally::default();
    let mut packet = Vec::new();
    loop {
        let mut len = [0u8; 2];
        match data.read_exact(&mut len).await {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(tally),
            Err(e) => return Err(e),
        }
        let len = usize::from(u16::from_be_bytes(len));
        if len < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "an audio packet shorter than its length",
            ));
        }
        packet.resize(len - 2, 0);
        data.read_exact(&mut packet).await?;
        let Some(opened) = key.open(&packet) else {
            tally.unopened += 1;
            if tally.decoded == 0 && tally.unopened >= WRONG_KEY {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{} audio packets didn't authenticate: a wrong shk",
                        tally.unopened
                    ),
                ));
            }
            if tally.unopened == 1 {
                tracing::warn!(
                    len,
                    "an AirPlay 2 audio packet didn't authenticate; dropping"
                );
            }
            continue;
        };
        let frames = match decoder.decode(&opened.payload) {
            Ok(frames) => frames,
            Err(e) => {
                tally.undecodable += 1;
                if tally.undecodable == 1 {
                    tracing::warn!(error = %e, seq = opened.seq, "dropping an AirPlay 2 packet");
                }
                continue;
            }
        };
        tally.decoded += 1;
        if tally.decoded == 1 {
            tracing::debug!(
                seq = opened.seq,
                rtp = opened.rtp,
                ssrc = format_args!("{:#x}", opened.ssrc),
                "the first AirPlay 2 audio decoded"
            );
        }
        let decoded = Decoded {
            seq: opened.seq,
            rtp: opened.rtp,
            frames,
        };
        if let Some(out) = &out
            && out.send(decoded).await.is_err()
        {
            return Ok(tally);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(b: &[u8]) -> String {
        b.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Made with Python's `cryptography` (ChaCha20Poly1305): key bytes 0
    /// to 31, nonce four zero bytes and 1 to 8, AAD the RTP time and SSRC.
    const VECTOR: &str = "00000102112233440000face8d93c39ec0fecdf830de9db2e383c02460b660b684119021c8b7d566414f0102030405060708";

    #[test]
    fn the_packet_layout_matches_the_vector() {
        let key = AudioKey::new(&(0..32).collect::<Vec<u8>>()).unwrap();
        let packet = Packet {
            seq: 0x102,
            rtp: 0x1122_3344,
            ssrc: 0xface,
            payload: b"buffered audio".to_vec(),
        };
        let wire = key.seal(&packet, [1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(
            usize::from(u16::from_be_bytes([wire[0], wire[1]])),
            wire.len()
        );
        assert_eq!(hex(&wire[2..]), VECTOR);
        assert_eq!(key.open(&wire[2..]), Some(packet));
    }

    #[test]
    fn a_wrong_key_or_a_tampered_packet_doesnt_open() {
        let key = AudioKey::new(&[7; 32]).unwrap();
        let packet = Packet {
            seq: 1,
            rtp: 2,
            ssrc: 3,
            payload: vec![4; 40],
        };
        let wire = key.seal(&packet, [9; 8]);
        assert!(AudioKey::new(&[8; 32]).unwrap().open(&wire[2..]).is_none());
        // The RTP time is authenticated, though not encrypted.
        let mut moved = wire.clone();
        moved[2 + 7] ^= 1;
        assert!(key.open(&moved[2..]).is_none());
        assert!(key.open(&wire[2..20]).is_none());
        assert!(AudioKey::new(&[0; 16]).is_none());
    }

    #[test]
    fn setup_names_the_codec() {
        assert_eq!(
            Codec::from_setup(Some(2), Some(352), Some(44_100)),
            Ok(Codec::Alac { spf: 352 })
        );
        assert_eq!(Codec::from_setup(Some(4), Some(1024), None), Ok(Codec::Aac));
        for (ct, sr) in [
            (Some(8), None),
            (Some(1), None),
            (None, None),
            (Some(4), Some(48_000)),
        ] {
            assert!(Codec::from_setup(ct, None, sr).is_err(), "{ct:?} {sr:?}");
        }
        assert_eq!(audio_specific_config(), [0x12, 0x10]);
    }
}
