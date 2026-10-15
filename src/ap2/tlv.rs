//! TLV8, the body of `/pair-setup` and `/pair-verify`: a run of
//! `[type][length][value]` items, a value longer than 255 bytes split into
//! items of the same type, which a reader joins back up.
//!
//! Ported from `openairplay2` 0.5's tlv.rs (MIT, Copyright (c) 2026
//! Stefan Arentz).

/// The item types pairing uses.
pub mod ty {
    pub const METHOD: u8 = 0x00;
    pub const IDENTIFIER: u8 = 0x01;
    pub const SALT: u8 = 0x02;
    pub const PUBLIC_KEY: u8 = 0x03;
    pub const PROOF: u8 = 0x04;
    pub const ENCRYPTED_DATA: u8 = 0x05;
    pub const STATE: u8 = 0x06;
    pub const ERROR: u8 = 0x07;
    pub const SIGNATURE: u8 = 0x0a;
    pub const FLAGS: u8 = 0x13;
}

/// The `ERROR` values we send.
pub mod error {
    /// A wrong proof or signature.
    pub const AUTHENTICATION: u8 = 0x02;
    /// Something we don't do, such as persistent pairing.
    pub const UNAVAILABLE: u8 = 0x06;
}

/// One TLV8 message, its items in order.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Tlv(Vec<(u8, Vec<u8>)>);

impl Tlv {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(mut self, ty: u8, value: impl Into<Vec<u8>>) -> Self {
        self.0.push((ty, value.into()));
        self
    }

    pub fn put_u8(self, ty: u8, value: u8) -> Self {
        self.put(ty, vec![value])
    }

    /// The first item of type `ty`.
    pub fn get(&self, ty: u8) -> Option<&[u8]> {
        self.0
            .iter()
            .find(|(t, _)| *t == ty)
            .map(|(_, v)| v.as_slice())
    }

    pub fn get_u8(&self, ty: u8) -> Option<u8> {
        self.get(ty)?.first().copied()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for (ty, value) in &self.0 {
            if value.is_empty() {
                out.extend([*ty, 0]);
            }
            for chunk in value.chunks(255) {
                out.extend([*ty, chunk.len() as u8]);
                out.extend_from_slice(chunk);
            }
        }
        out
    }

    /// `None` when an item runs past the end.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut items: Vec<(u8, Vec<u8>)> = Vec::new();
        let mut i = 0;
        while i < bytes.len() {
            let ty = bytes[i];
            let len = usize::from(*bytes.get(i + 1)?);
            let value = bytes.get(i + 2..i + 2 + len)?;
            i += 2 + len;
            // An item follows on from the one before when that one is the
            // same type and filled its 255 bytes.
            match items.last_mut() {
                Some((last, v)) if *last == ty && !v.is_empty() && v.len() % 255 == 0 => {
                    v.extend_from_slice(value);
                }
                _ => items.push((ty, value.to_vec())),
            }
        }
        Some(Self(items))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let tlv = Tlv::new()
            .put_u8(ty::STATE, 2)
            .put(ty::SALT, vec![0xaa; 16]);
        let back = Tlv::decode(&tlv.encode()).unwrap();
        assert_eq!(back.get_u8(ty::STATE), Some(2));
        assert_eq!(back.get(ty::SALT), Some(&[0xaa; 16][..]));
        assert_eq!(back.get(ty::PROOF), None);
    }

    #[test]
    fn a_long_value_is_split_and_joined() {
        // An SRP public key: 384 bytes, as 255 + 129.
        let value: Vec<u8> = (0..384).map(|i| i as u8).collect();
        let wire = Tlv::new().put(ty::PUBLIC_KEY, value.clone()).encode();
        assert_eq!(wire.len(), 384 + 4);
        assert_eq!(&wire[..2], [ty::PUBLIC_KEY, 255]);
        assert_eq!(&wire[257..259], [ty::PUBLIC_KEY, 129]);
        let back = Tlv::decode(&wire).unwrap();
        assert_eq!(back.get(ty::PUBLIC_KEY), Some(&value[..]));
    }

    #[test]
    fn a_cut_off_item_is_refused() {
        assert_eq!(Tlv::decode(&[ty::SALT, 5, 1, 2]), None);
    }
}
