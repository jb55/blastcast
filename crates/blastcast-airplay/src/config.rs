//! What identifies one AirPlay receiver: its target, advertised name and
//! port.

use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

/// AirPlay receiver ports are `7000 + hash % 1000`, as in MA.
pub const PORT_BASE: u16 = 7000;
pub const PORT_RANGE: u16 = 1000;

/// The advertised-name template's default: the player's own name.
pub const DEFAULT_NAME_TEMPLATE: &str = "{player}";

/// The scheme of every receiver's id. It is Music Assistant's provider
/// instance id, so a receiver's id is also its source's uri there.
pub const SOURCE_PROVIDER: &str = "airplay_receiver";

/// The id of `target_id`'s receiver, which its telemetry and status are
/// keyed by: MA's `AudioSource` uri for it.
pub fn source_id(target_id: &str) -> String {
    format!("{SOURCE_PROVIDER}://audio_source/{target_id}")
}

/// The receiver whose id is `source_id`.
pub fn target_of(source_id: &str) -> Option<&str> {
    source_id
        .strip_prefix(SOURCE_PROVIDER)?
        .strip_prefix("://audio_source/")
        .filter(|t| !t.is_empty())
}

/// Everything that identifies one running receiver. A change to any of it
/// restarts it; the starting volume is not part of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverInfo {
    /// The player or group the receiver plays to.
    pub target_id: String,
    /// The name the phone shows.
    pub airplay_name: String,
    pub port: u16,
}

impl ReceiverInfo {
    pub fn new(target_id: &str, airplay_name: String, port: u16) -> Self {
        Self {
            target_id: target_id.to_owned(),
            airplay_name,
            port,
        }
    }
}

/// The port of each target: `7000 + hash(server_id, target) % 1000`, so it
/// stays the same across restarts, with a collision probing upward (and
/// wrapping within the range). Targets are taken in sorted order, so the
/// result doesn't depend on the order they are given in.
pub fn receiver_ports<'a>(
    server_id: &str,
    targets: impl IntoIterator<Item = &'a str>,
) -> BTreeMap<String, u16> {
    let targets: BTreeSet<&str> = targets.into_iter().collect();
    let mut claimed = BTreeSet::new();
    let mut ports = BTreeMap::new();
    // More targets than ports: those after the first 1000 get none.
    for target in targets.into_iter().take(PORT_RANGE.into()) {
        let mut offset = port_hash(server_id, target);
        while !claimed.insert(offset) {
            offset = (offset + 1) % PORT_RANGE;
        }
        ports.insert(target.to_owned(), PORT_BASE + offset);
    }
    ports
}

fn port_hash(server_id: &str, target: &str) -> u16 {
    let digest = Sha256::digest(format!("{server_id}_{target}").as_bytes());
    let n = u64::from_be_bytes(digest[..8].try_into().expect("8 bytes"));
    (n % u64::from(PORT_RANGE)) as u16
}

/// The advertised name, from a template: `{player}` is the target's name
/// and `{server}` the server's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameTemplate(String);

impl Default for NameTemplate {
    fn default() -> Self {
        Self(DEFAULT_NAME_TEMPLATE.to_owned())
    }
}

impl NameTemplate {
    pub fn new(template: impl Into<String>) -> Self {
        Self(template.into())
    }

    /// Falls back to the player's name when the template comes out blank.
    pub fn render(&self, player: &str, server: &str) -> String {
        let name = self
            .0
            .replace("{player}", player)
            .replace("{server}", server);
        if name.trim().is_empty() {
            player.to_owned()
        } else {
            name.trim().to_owned()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ports_are_stable_in_range_and_unique() {
        let targets: Vec<String> = (0..300).map(|i| format!("player-{i}")).collect();
        let ports = receiver_ports("srv", targets.iter().map(String::as_str));
        assert_eq!(ports.len(), 300);
        let unique: BTreeSet<_> = ports.values().collect();
        assert_eq!(unique.len(), 300, "collisions were not probed");
        assert!(ports.values().all(|p| (7000..8000).contains(p)));
        // Same answer in any order, and from another call.
        let reversed = receiver_ports("srv", targets.iter().rev().map(String::as_str));
        assert_eq!(ports, reversed);
        // Another server spreads its ports differently.
        let other = receiver_ports("other", targets.iter().map(String::as_str));
        assert_ne!(ports, other);
    }

    #[test]
    fn a_collision_probes_upward_and_wraps() {
        // Find two targets that hash to the same port.
        let base = port_hash("srv", "t0");
        let twin = (1..)
            .map(|i| format!("t{i}"))
            .find(|t| port_hash("srv", t) == base)
            .unwrap();
        let ports = receiver_ports("srv", ["t0", twin.as_str()]);
        let (first, second) = if "t0" < twin.as_str() {
            (ports["t0"], ports[&twin])
        } else {
            (ports[&twin], ports["t0"])
        };
        assert_eq!(first, PORT_BASE + base);
        assert_eq!(second, PORT_BASE + (base + 1) % PORT_RANGE);
    }

    #[test]
    fn name_templates() {
        let t = NameTemplate::default();
        assert_eq!(t.render("Kitchen", "Tune Assistant"), "Kitchen");
        let t = NameTemplate::new("{player} | {server}");
        assert_eq!(t.render("Kitchen", "TA"), "Kitchen | TA");
        let t = NameTemplate::new("  ");
        assert_eq!(t.render("Kitchen", "TA"), "Kitchen");
    }
}
