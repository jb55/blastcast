//! AirPlay 2 on the native receiver: what an iPhone or Mac needs to see a
//! target as an AirPlay 2 device, beside its AirPlay 1 ([`crate::raop`])
//! advert on the same port.
//!
//! - [`identity`]: each target's Ed25519 key (`pk`) and `pi`, derived from
//!   one secret kept in the state file, so a phone keeps recognising the
//!   device across restarts.
//! - [`info`]: the `_airplay._tcp` advert and the `GET /info` binary
//!   plist, which carry the same fields.
//!
//! A sender's AirPlay 2 requests arrive on the AirPlay 1 receiver's
//! connection: [`handles`] tells them apart (`GET /info`, or a binary
//! plist body) and [`answer`] answers them, so the AirPlay 1 path is left
//! as it is. Pairing, the encrypted channel, PTP and buffered audio come
//! in later steps; until pairing does, a target only advertises AirPlay 2
//! when [`NativeConfig::airplay2`](crate::raop::NativeConfig::airplay2)
//! says so, since a phone that sees AirPlay 2 uses it and can't fall back.
//!
//! Ported from `openairplay2` 0.5 (MIT, Copyright (c) 2026 Stefan Arentz):
//! its info.rs (the TXT record and the /info plist) and receiver.rs (the
//! features mask), with the bits checked against shairplay 0.10's
//! features.rs.

pub mod identity;
pub mod info;

pub use identity::{Device, Identity};

use crate::raop::rtsp::{Request, Response};

/// What AirPlay 2 senders browse for.
pub const AIRPLAY_SERVICE_TYPE: &str = "_airplay._tcp.local.";

/// The content type of a binary plist body, both ways.
pub const BPLIST: &str = "application/x-apple-binary-plist";

/// What we claim to be. Nothing on the sender keys behaviour off it.
pub const MODEL: &str = "TuneAssistant,1";

/// `srcvers`: the AirPlay version shairport-sync claims in AirPlay 2 mode.
pub const SOURCE_VERSION: &str = "366.0";

/// `flags`: bit 2, audio cable attached; nothing else, so no password.
pub const STATUS_FLAGS: u32 = 0x4;

/// The bits of [`FEATURES`], each with what it tells a sender. Names from
/// <https://emanuelecozzi.net/docs/airplay2/features/>.
pub const FEATURE_BITS: &[(u32, &str)] = &[
    (9, "audio"),
    (11, "redundant audio packets"),
    // Every known-good AirPlay 2 receiver sets this one. It needs no
    // FairPlay crypto: /fp-setup is answered with fixed bytes, as
    // shairport-sync does.
    (14, "FairPlay authentication (answered with fixed bytes)"),
    (15, "metadata: artwork"),
    (16, "metadata: progress"),
    (17, "metadata: now playing, as DAAP"),
    (18, "audio formats 0"),
    (19, "audio formats 1"),
    (20, "audio formats 2"),
    (22, "audio formats 4"),
    (30, "unified advertiser info"),
    (38, "unified media control"),
    (40, "buffered audio"),
    (41, "PTP timing"),
    (47, "transient pairing (PIN 3939)"),
    (48, "CoreUtils pairing and encryption"),
];

/// The `features` mask: shairport-sync's known-good AirPlay 2 audio mask
/// with its metadata bits (15-17), and without HomeKit pairing (46).
/// Left out too: video and screen mirroring (0-7), video FairPlay (2),
/// MFi (26), legacy pairing (27), volume (32: the AirPlay 1 receiver takes
/// it from `SET_PARAMETER`), and the bplist now-playing and sync bits.
///
/// openairplay2 found an iPhone sends no track metadata without bits
/// 15-17; shairplay found some phones hide a device that sets 15 and 17.
/// We follow openairplay2, whose mask this is, and the Mac run in
/// headway:tune-assistant/actress-melt-notable checks it.
pub const FEATURES: u64 = features(FEATURE_BITS);

const fn features(bits: &[(u32, &str)]) -> u64 {
    let mut mask = 0;
    let mut i = 0;
    while i < bits.len() {
        mask |= 1 << bits[i].0;
        i += 1;
    }
    mask
}

/// Whether `req` is an AirPlay 2 request: `GET /info`, or one with a
/// binary plist body. An AirPlay 1 sender sends neither.
pub fn handles(req: &Request) -> bool {
    (req.method == "GET" && req.uri == "/info")
        || req
            .header("Content-Type")
            .is_some_and(|t| t.eq_ignore_ascii_case(BPLIST))
}

/// Answers an AirPlay 2 request to `device`.
pub fn answer(device: &Device, req: &Request) -> Response {
    match (req.method.as_str(), req.uri.as_str()) {
        ("GET", "/info") => Response::ok().body(BPLIST, info::info_plist(device)),
        (method, uri) => {
            tracing::debug!(method, uri, "AirPlay 2 request not handled yet");
            Response::new(501)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_features_are_openairplay2s_mask() {
        assert_eq!(FEATURES, 0x0001_8340_405F_CA00);
        // Neither HomeKit pairing nor screen mirroring.
        assert_eq!(FEATURES & (1 << 46), 0);
        assert_eq!(FEATURES & (1 << 7), 0);
    }

    fn request(method: &str, uri: &str, content_type: Option<&str>) -> Request {
        Request {
            method: method.into(),
            uri: uri.into(),
            headers: content_type
                .map(|t| vec![("Content-Type".into(), t.into())])
                .unwrap_or_default(),
            body: Vec::new(),
        }
    }

    #[test]
    fn airplay_2_requests_are_told_apart() {
        assert!(handles(&request("GET", "/info", None)));
        assert!(handles(&request("POST", "/pair-setup", Some(BPLIST))));
        assert!(!handles(&request("OPTIONS", "*", None)));
        assert!(!handles(&request(
            "SET_PARAMETER",
            "rtsp://x/1",
            Some("text/parameters")
        )));
    }
}
