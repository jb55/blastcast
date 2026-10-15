//! The `_airplay._tcp` advert and the `GET /info` plist: the same fields
//! twice, once for a sender browsing and once for one that connected. The
//! plist also carries the TXT record itself, packed, as `txtAirPlay`.

use blastcast_mdns::service::{Service, TxtEntry};
use plist::{Dictionary, Value};

use super::{AIRPLAY_SERVICE_TYPE, Device, FEATURES, MODEL, SOURCE_VERSION, STATUS_FLAGS};
use crate::raop::fit_label;

/// The TXT record, as shairport-sync writes it in AirPlay 2 mode.
/// `features` is two hex words, low then high; `gid` is `pi` while the
/// device is in no group.
pub fn txt(device: &Device) -> Vec<TxtEntry> {
    let (lo, hi) = (FEATURES as u32, (FEATURES >> 32) as u32);
    vec![
        TxtEntry::new("acl", "0"),
        TxtEntry::new("deviceid", device.device_id_text()),
        TxtEntry::new("features", format!("0x{lo:X},0x{hi:X}")),
        TxtEntry::new("flags", format!("0x{STATUS_FLAGS:X}")),
        TxtEntry::new("gid", device.pi.clone()),
        TxtEntry::new("gcgl", "0"),
        TxtEntry::new("igl", "0"),
        TxtEntry::new("model", MODEL),
        TxtEntry::new("protovers", "1.1"),
        TxtEntry::new("pi", device.pi.clone()),
        TxtEntry::new("pk", device.public_key_hex()),
        TxtEntry::new("srcvers", SOURCE_VERSION),
        TxtEntry::new("vv", "2"),
    ]
}

/// The `_airplay._tcp` advert: the bare name, on the AirPlay 1
/// receiver's port.
pub fn service(device: &Device, port: u16) -> Service {
    Service {
        service_type: AIRPLAY_SERVICE_TYPE.to_owned(),
        instance: fit_label(&device.name, 0).to_owned(),
        port,
        txt: txt(device),
    }
}

/// TXT entries in DNS-SD's wire form, as `txtAirPlay` wants them: each
/// `key=value` behind its one-byte length.
pub fn pack_txt(txt: &[TxtEntry]) -> Vec<u8> {
    let mut out = Vec::new();
    for entry in txt {
        let text = match &entry.value {
            Some(v) => format!("{}={v}", entry.key),
            None => entry.key.clone(),
        };
        let bytes = &text.as_bytes()[..text.len().min(255)];
        out.push(bytes.len() as u8);
        out.extend_from_slice(bytes);
    }
    out
}

/// What [`pack_txt`] packed, back as entries.
pub fn unpack_txt(mut packed: &[u8]) -> Option<Vec<TxtEntry>> {
    let mut txt = Vec::new();
    while let Some((&len, rest)) = packed.split_first() {
        let text = std::str::from_utf8(rest.get(..len as usize)?).ok()?;
        txt.push(match text.split_once('=') {
            Some((k, v)) => TxtEntry::new(k, v),
            None => TxtEntry::flag(text),
        });
        packed = &rest[len as usize..];
    }
    Some(txt)
}

/// The `GET /info` answer, a binary plist. The fixed fields are
/// shairport-sync's get_info_response.xml, as openairplay2 has them.
pub fn info_plist(device: &Device) -> Vec<u8> {
    let mut playback = Dictionary::new();
    playback.insert("supportsInterstitials".into(), false.into());
    playback.insert("supportsFPSSecureStop".into(), false.into());
    playback.insert("supportsUIForAudioOnlyContent".into(), false.into());

    let mut dict = Dictionary::new();
    dict.insert("vv".into(), 2u64.into());
    dict.insert("protocolVersion".into(), "1.1".into());
    dict.insert("volumeControlType".into(), 3u64.into());
    dict.insert("canRecordScreenStream".into(), false.into());
    dict.insert("keepAliveSendStatsAsBody".into(), false.into());
    dict.insert("screenDemoMode".into(), false.into());
    dict.insert("receiverHDRCapability".into(), "4k60".into());
    dict.insert("playbackCapabilities".into(), playback.into());

    dict.insert("deviceID".into(), device.device_id_text().into());
    dict.insert("name".into(), device.name.clone().into());
    dict.insert("model".into(), MODEL.into());
    dict.insert("sourceVersion".into(), SOURCE_VERSION.into());
    dict.insert("features".into(), FEATURES.into());
    dict.insert("statusFlags".into(), u64::from(STATUS_FLAGS).into());
    dict.insert("pi".into(), device.pi.clone().into());
    dict.insert("pk".into(), Value::Data(device.public_key().to_vec()));
    dict.insert("txtAirPlay".into(), Value::Data(pack_txt(&txt(device))));

    let mut out = Vec::new();
    Value::Dictionary(dict)
        .to_writer_binary(&mut out)
        .expect("writing a plist to memory doesn't fail");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ap2::Identity;

    fn device() -> Device {
        Identity::generate().device([0x02, 0xAB, 0, 1, 2, 0xFF], "kitchen", "Kitchen")
    }

    fn value<'a>(txt: &'a [TxtEntry], key: &str) -> Option<&'a str> {
        txt.iter()
            .find(|e| e.key == key)
            .and_then(|e| e.value.as_deref())
    }

    #[test]
    fn the_advert_carries_the_identity_and_features() {
        let d = device();
        let s = service(&d, 7123);
        assert_eq!(s.service_type, "_airplay._tcp.local.");
        assert_eq!((s.instance.as_str(), s.port), ("Kitchen", 7123));
        assert_eq!(value(&s.txt, "deviceid"), Some("02:AB:00:01:02:FF"));
        assert_eq!(value(&s.txt, "features"), Some("0x405FCA00,0x18340"));
        assert_eq!(value(&s.txt, "flags"), Some("0x4"));
        assert_eq!(value(&s.txt, "pk"), Some(d.public_key_hex().as_str()));
        assert_eq!(value(&s.txt, "pi"), Some(d.pi.as_str()));
        assert_eq!(value(&s.txt, "acl"), Some("0"));
        assert_eq!(value(&s.txt, "srcvers"), Some(SOURCE_VERSION));
    }

    #[test]
    fn info_parses_back_to_the_advertised_fields() {
        let d = device();
        let plist = plist::Value::from_reader(std::io::Cursor::new(info_plist(&d))).unwrap();
        let dict = plist.as_dictionary().unwrap();
        let s = |k: &str| dict.get(k).and_then(plist::Value::as_string);
        assert_eq!(s("deviceID"), Some("02:AB:00:01:02:FF"));
        assert_eq!(s("name"), Some("Kitchen"));
        assert_eq!(s("pi"), Some(d.pi.as_str()));
        assert_eq!(s("model"), Some(MODEL));
        assert_eq!(s("sourceVersion"), Some(SOURCE_VERSION));
        assert_eq!(
            dict.get("features").unwrap().as_unsigned_integer(),
            Some(FEATURES)
        );
        assert_eq!(
            dict.get("statusFlags").unwrap().as_unsigned_integer(),
            Some(u64::from(STATUS_FLAGS))
        );
        assert_eq!(dict.get("pk").unwrap().as_data(), Some(&d.public_key()[..]));
        // The TXT record inside is the one advertised.
        let packed = dict.get("txtAirPlay").unwrap().as_data().unwrap();
        assert_eq!(unpack_txt(packed).unwrap(), service(&d, 1).txt);
    }

    #[test]
    fn txt_packs_length_prefixed() {
        let txt = [TxtEntry::new("ab", "c"), TxtEntry::flag("d")];
        let packed = pack_txt(&txt);
        assert_eq!(packed, [4, b'a', b'b', b'=', b'c', 1, b'd']);
        assert_eq!(unpack_txt(&packed).unwrap(), txt);
        assert_eq!(unpack_txt(&[5, b'a']), None, "cut short");
    }
}
