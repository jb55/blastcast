//! The ANNOUNCE's SDP: which codec the audio is in, and its key.
//!
//! An iPhone announces ALAC, 352 frames a packet, encrypted:
//!
//! ```text
//! m=audio 0 RTP/AVP 96
//! a=rtpmap:96 AppleLossless
//! a=fmtp:96 352 0 16 40 10 14 2 255 0 0 44100
//! a=rsaaeskey:<base64>
//! a=aesiv:<base64>
//! ```
//!
//! We also take `L16/44100/2` (big-endian PCM), which the TXT record's
//! `cn=0,1` offers, and either one unencrypted. Anything else, such as
//! FairPlay's `a=fpaeskey`, is refused.

use crate::raop::crypto::{self, AudioCipher, CryptoError};

/// What AirPlay 1 plays: 44.1 kHz stereo.
pub const SAMPLE_RATE: u32 = 44_100;
pub const CHANNELS: usize = 2;

#[derive(Debug, thiserror::Error)]
pub enum AnnounceError {
    #[error("no audio format in the SDP")]
    NoFormat,
    #[error("unsupported audio format {0:?}")]
    Format(String),
    #[error("bad ALAC parameters {0:?}")]
    Fmtp(String),
    #[error("FairPlay-encrypted audio isn't supported")]
    FairPlay,
    #[error("an AES key without an IV, or an IV without a key")]
    HalfAKey,
    #[error(transparent)]
    Crypto(#[from] CryptoError),
}

#[derive(Debug)]
pub struct Announce {
    pub codec: Codec,
    pub cipher: Option<AudioCipher>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Codec {
    /// The `fmtp` parameters.
    Alac(String),
    /// Big-endian s16.
    L16,
}

impl Announce {
    pub fn parse(sdp: &str) -> Result<Self, AnnounceError> {
        let attr = |name: &str| {
            sdp.lines()
                .map(str::trim)
                .find_map(|l| l.strip_prefix("a=")?.strip_prefix(name)?.strip_prefix(':'))
                .map(str::trim)
        };
        if attr("fpaeskey").is_some() {
            return Err(AnnounceError::FairPlay);
        }
        let rtpmap = attr("rtpmap").ok_or(AnnounceError::NoFormat)?;
        // `96 AppleLossless`: the payload type, then the encoding.
        let encoding = rtpmap.split_whitespace().nth(1).unwrap_or_default();
        let codec = if encoding.eq_ignore_ascii_case("AppleLossless") {
            // `96 352 0 16 …`: the payload type, then the parameters.
            let fmtp = attr("fmtp").ok_or(AnnounceError::NoFormat)?;
            let params = fmtp
                .split_once(char::is_whitespace)
                .map_or("", |(_, p)| p.trim());
            let info = alac::StreamInfo::from_sdp_format_parameters(params)
                .map_err(|_| AnnounceError::Fmtp(params.to_owned()))?;
            if info.sample_rate() != SAMPLE_RATE
                || usize::from(info.channels()) != CHANNELS
                || info.bit_depth() != 16
            {
                return Err(AnnounceError::Fmtp(params.to_owned()));
            }
            Codec::Alac(params.to_owned())
        } else if encoding.eq_ignore_ascii_case(&format!("L16/{SAMPLE_RATE}/{CHANNELS}")) {
            Codec::L16
        } else {
            return Err(AnnounceError::Format(encoding.to_owned()));
        };
        let cipher = match (attr("rsaaeskey"), attr("aesiv")) {
            (Some(key), Some(iv)) => Some(AudioCipher::new(
                crypto::decrypt_aes_key(key)?,
                crypto::decode_iv(iv)?,
            )),
            (None, None) => None,
            _ => return Err(AnnounceError::HalfAKey),
        };
        Ok(Self { codec, cipher })
    }
}

#[derive(Debug, thiserror::Error)]
#[error("can't decode an audio packet: {0}")]
pub struct DecodeError(&'static str);

/// Turns each packet's payload into interleaved stereo s16.
pub enum Decoder {
    Alac {
        decoder: Box<alac::Decoder>,
        buf: Vec<i16>,
    },
    L16,
}

impl Decoder {
    pub fn new(codec: &Codec) -> Self {
        match codec {
            Codec::Alac(params) => {
                let info = alac::StreamInfo::from_sdp_format_parameters(params)
                    .expect("checked when announced");
                let buf = vec![0; info.max_samples_per_packet() as usize];
                Self::Alac {
                    decoder: Box::new(alac::Decoder::new(info)),
                    buf,
                }
            }
            Codec::L16 => Self::L16,
        }
    }

    pub fn decode(&mut self, payload: &[u8]) -> Result<Vec<i16>, DecodeError> {
        match self {
            Self::Alac { decoder, buf } => decoder
                .decode_packet(payload, buf)
                .map(<[i16]>::to_vec)
                .map_err(|_| DecodeError("bad ALAC")),
            Self::L16 => {
                let whole = payload.len() - payload.len() % (2 * CHANNELS);
                let (samples, _) = payload[..whole].as_chunks::<2>();
                Ok(samples.iter().map(|&b| i16::from_be_bytes(b)).collect())
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use alac_encoder::{AlacEncoder, FormatDescription};

    use super::*;
    use crate::raop::crypto::tests::encrypt_aes_key;

    pub(crate) const FMTP: &str = "352 0 16 40 10 14 2 255 0 0 44100";

    /// One compressed ALAC packet of `samples`, as iTunes' encoder writes
    /// it.
    pub(crate) fn alac_packet(samples: &[i16]) -> Vec<u8> {
        let output = FormatDescription::alac(44_100.0, 352, 2);
        let input = FormatDescription::pcm::<i16>(44_100.0, 2);
        let mut encoder = AlacEncoder::new(&output);
        let bytes: Vec<u8> = samples.iter().flat_map(|s| s.to_le_bytes()).collect();
        let mut out = vec![0; output.max_packet_size()];
        let n = encoder.encode(&input, &bytes, &mut out);
        out.truncate(n);
        out
    }

    pub(crate) fn sdp(key: Option<(&[u8; 16], &[u8; 16])>) -> String {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD_NO_PAD;
        let mut sdp = format!(
            "v=0\r\no=iTunes 3413821438 0 IN IP4 10.0.0.9\r\ns=iTunes\r\nc=IN IP4 10.0.0.2\r\n\
t=0 0\r\nm=audio 0 RTP/AVP 96\r\na=rtpmap:96 AppleLossless\r\na=fmtp:96 {FMTP}\r\n"
        );
        if let Some((key, iv)) = key {
            sdp.push_str(&format!(
                "a=rsaaeskey:{}\r\na=aesiv:{}\r\n",
                encrypt_aes_key(key).trim_end_matches('='),
                b64.encode(iv)
            ));
        }
        sdp
    }

    #[test]
    fn an_encrypted_alac_announce_parses() {
        let (key, iv) = (*b"0123456789abcdef", *b"fedcba9876543210");
        let a = Announce::parse(&sdp(Some((&key, &iv)))).unwrap();
        assert_eq!(a.codec, Codec::Alac(FMTP.into()));
        let cipher = a.cipher.unwrap();
        let mut payload = [0u8; 16];
        crate::raop::crypto::tests::encrypt_payload(&key, &iv, &mut payload);
        cipher.decrypt(&mut payload);
        assert_eq!(payload, [0; 16]);
    }

    #[test]
    fn other_announces() {
        let a = Announce::parse(&sdp(None)).unwrap();
        assert!(a.cipher.is_none());
        let pcm = "m=audio 0 RTP/AVP 96\r\na=rtpmap:96 L16/44100/2\r\n";
        assert_eq!(Announce::parse(pcm).unwrap().codec, Codec::L16);
        for (sdp, want) in [
            ("a=rtpmap:96 mpeg4-generic/44100/2\r\n", "Format"),
            (
                "a=rtpmap:96 AppleLossless\r\na=fmtp:96 352 0 16\r\n",
                "Fmtp",
            ),
            (
                "a=rtpmap:96 AppleLossless\r\na=fmtp:96 352 0 24 40 10 14 2 255 0 0 44100\r\n",
                "Fmtp",
            ),
            (
                "a=rtpmap:96 AppleLossless\r\na=fpaeskey:AAAA\r\n",
                "FairPlay",
            ),
            ("s=nothing\r\n", "NoFormat"),
        ] {
            let err = Announce::parse(sdp).unwrap_err();
            assert!(format!("{err:?}").starts_with(want), "{sdp:?}: {err:?}");
        }
    }

    #[test]
    fn decodes_compressed_alac() {
        let samples: Vec<i16> = (0..352 * 2)
            .map(|i| ((i as f32 * 0.05).sin() * 8000.0) as i16)
            .collect();
        let packet = alac_packet(&samples);
        assert!(packet.len() < samples.len() * 2, "it is compressed");
        let mut d = Decoder::new(&Codec::Alac(FMTP.into()));
        assert_eq!(d.decode(&packet).unwrap(), samples);
        assert!(d.decode(&[0xff; 3]).is_err());
    }

    #[test]
    fn decodes_l16() {
        let mut d = Decoder::new(&Codec::L16);
        assert_eq!(
            d.decode(&[0x01, 0x02, 0xff, 0xfe, 0x00]).unwrap(),
            [0x0102, -2]
        );
    }
}
