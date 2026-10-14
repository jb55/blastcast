//! What one shairport-sync instance is told: its advertised name, port,
//! fifos and config file, and the config text itself.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// The config template, Music Assistant's with our markers.
pub const TEMPLATE: &str = include_str!("../shairport-sync.conf");

/// Written into the metadata fifo by shairport's session-control hooks
/// when a sender starts playing, and when it has been silent for
/// `active_state_timeout`. The same markers MA uses.
pub const PLAY_BEGIN: &str = "MA_PLAY_BEGIN";
pub const PLAY_END: &str = "MA_PLAY_END";

/// AirPlay receiver ports are `7000 + hash % 1000`, as in MA.
pub const PORT_BASE: u16 = 7000;
pub const PORT_RANGE: u16 = 1000;

/// The advertised-name template's default: the player's own name.
pub const DEFAULT_NAME_TEMPLATE: &str = "{player}";

/// Everything that identifies one running receiver. A change to any of it
/// restarts the daemon; the starting volume is not part of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverInfo {
    /// The player or group the receiver plays to.
    pub target_id: String,
    /// The name the phone shows.
    pub airplay_name: String,
    pub port: u16,
    /// shairport writes raw PCM, s16le 44.1 kHz stereo, here.
    pub audio_pipe: PathBuf,
    /// shairport writes its metadata items, and the hooks write
    /// [`PLAY_BEGIN`] and [`PLAY_END`], here.
    pub metadata_pipe: PathBuf,
    pub config_file: PathBuf,
    /// `general.interface`: the network interface to advertise on.
    pub interface: Option<String>,
}

impl ReceiverInfo {
    pub fn new(
        run_dir: &Path,
        target_id: &str,
        airplay_name: String,
        port: u16,
        interface: Option<String>,
    ) -> Self {
        let key = file_key(target_id);
        Self {
            target_id: target_id.to_owned(),
            airplay_name,
            port,
            audio_pipe: run_dir.join(format!("airplay_audio_{key}")),
            metadata_pipe: run_dir.join(format!("airplay_metadata_{key}")),
            config_file: run_dir.join(format!("shairport-sync_{key}.conf")),
            interface,
        }
    }

    /// The config file text. `volume` (0-100) is the volume shairport
    /// reports to the sender before it has heard one, mapped onto AirPlay's
    /// -30..0 dB; `None` is full volume.
    pub fn render(&self, volume: Option<u8>) -> String {
        let db = f64::from(volume.unwrap_or(100).min(100)) / 100.0 * 30.0 - 30.0;
        let interface = match &self.interface {
            Some(name) => format!("\tinterface = \"{}\";\n", escape(name)),
            None => String::new(),
        };
        fill(TEMPLATE, |key| {
            Some(match key {
                "AIRPLAY_NAME" => escape(&self.airplay_name),
                "DEFAULT_VOLUME" => format!("{db:.1}"),
                "PORT" => self.port.to_string(),
                "INTERFACE_LINE" => interface.clone(),
                "METADATA_PIPE" => escape(&self.metadata_pipe.to_string_lossy()),
                "AUDIO_PIPE" => escape(&self.audio_pipe.to_string_lossy()),
                "PLAY_BEGIN" => PLAY_BEGIN.to_owned(),
                "PLAY_END" => PLAY_END.to_owned(),
                _ => return None,
            })
        })
    }
}

/// Replaces each `{KEY}` in `template` with `value(KEY)`, in one pass, so a
/// value that itself contains `{KEY}` is left alone. Unknown keys stay.
fn fill(template: &str, value: impl Fn(&str) -> Option<String>) -> String {
    let mut out = String::with_capacity(template.len() + 256);
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let key_len = after
            .find(|c: char| !(c.is_ascii_uppercase() || c == '_'))
            .unwrap_or(after.len());
        match (after[key_len..].starts_with('}'), value(&after[..key_len])) {
            (true, Some(v)) if key_len > 0 => {
                out.push_str(&v);
                rest = &after[key_len + 1..];
            }
            _ => {
                out.push('{');
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

/// Escapes `s` for a double-quoted libconfig string, so a quote, backslash
/// or newline in a player's name can't break the config.
pub fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 || c == '\x7f' => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// A target id made safe for a file name, with a hash of the original so
/// two ids that sanitize alike still get their own files.
fn file_key(target_id: &str) -> String {
    let safe: String = target_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let digest = Sha256::digest(target_id.as_bytes());
    let hex: String = digest[..4].iter().map(|b| format!("{b:02x}")).collect();
    format!("{safe}-{hex}")
}

/// Whether a run dir can go into the config as is. The session-control
/// hooks name the metadata fifo inside a shell command, which shairport
/// splits like a shell would, so the path must need no quoting there.
pub fn is_plain_path(path: &Path) -> bool {
    path.to_str().is_some_and(|s| {
        !s.is_empty()
            && s.chars().all(|c| {
                c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-' | '+' | ',' | '@')
            })
    })
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
    // More targets than ports can only come from a hand-edited state file.
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

    fn info(name: &str) -> ReceiverInfo {
        ReceiverInfo::new(
            Path::new("/run/tas"),
            "ss-kitchen",
            name.to_owned(),
            7123,
            None,
        )
    }

    #[test]
    fn the_config_has_everything_ma_sets() {
        let text = info("Kitchen").render(Some(50));
        let meta = "/run/tas/airplay_metadata_ss-kitchen-";
        for want in [
            "name = \"Kitchen\";",
            "ignore_volume_control = \"yes\";",
            "output_backend = \"pipe\";",
            "default_airplay_volume = -15.0;",
            "port = 7123;",
            "include_cover_art = \"yes\";",
            "pipe_timeout = 0;",
            "name = \"/run/tas/airplay_audio_ss-kitchen-",
            "active_state_timeout = 10.0;",
            "/bin/sh -c 'echo MA_PLAY_BEGIN > /run/tas/airplay_metadata_ss-kitchen-",
            "/bin/sh -c 'echo MA_PLAY_END > /run/tas/airplay_metadata_ss-kitchen-",
        ] {
            assert!(text.contains(want), "missing {want:?} in\n{text}");
        }
        assert!(text.contains(&format!("pipe_name = \"{meta}")));
        assert!(!text.contains("interface"), "{text}");
        for key in [
            "{AIRPLAY_NAME}",
            "{PORT}",
            "{METADATA_PIPE}",
            "{INTERFACE_LINE}",
        ] {
            assert!(!text.contains(key), "{key} left in\n{text}");
        }
    }

    #[test]
    fn the_volume_maps_onto_airplay_decibels() {
        let line = |v| {
            info("x")
                .render(v)
                .lines()
                .find(|l| l.contains("default_airplay_volume"))
                .unwrap()
                .trim()
                .to_owned()
        };
        assert_eq!(line(None), "default_airplay_volume = 0.0;");
        assert_eq!(line(Some(100)), "default_airplay_volume = 0.0;");
        assert_eq!(line(Some(0)), "default_airplay_volume = -30.0;");
        assert_eq!(line(Some(200)), "default_airplay_volume = 0.0;");
    }

    #[test]
    fn an_interface_is_pinned_when_given() {
        let mut i = info("x");
        i.interface = Some("eth0".into());
        assert!(i.render(None).contains("\tinterface = \"eth0\";\n};"));
    }

    #[test]
    fn names_are_escaped_and_never_refilled() {
        let text = info("Bob's \"Den\" \\ {PORT}\n").render(None);
        assert!(
            text.contains(r#"name = "Bob's \"Den\" \\ {PORT}\n";"#),
            "{text}"
        );
        assert_eq!(escape("a\u{1}b"), "a\\x01b");
        assert_eq!(escape("Küche ♪"), "Küche ♪");
    }

    #[test]
    fn fill_leaves_unknown_and_unclosed_braces() {
        let out = fill("{A} {b} {A {} {NOPE} x{A}", |k| {
            (k == "A").then(|| "1".into())
        });
        assert_eq!(out, "1 {b} {A {} {NOPE} x1");
    }

    #[test]
    fn file_names_are_safe_and_distinct() {
        let a = ReceiverInfo::new(Path::new("/r"), "a:b/c", "n".into(), 7000, None);
        let b = ReceiverInfo::new(Path::new("/r"), "a_b_c", "n".into(), 7000, None);
        let name = a.audio_pipe.file_name().unwrap().to_str().unwrap();
        assert!(name.starts_with("airplay_audio_a_b_c-"), "{name}");
        assert_eq!(a.audio_pipe.parent(), Some(Path::new("/r")));
        assert_ne!(a.audio_pipe, b.audio_pipe);
        assert_ne!(a.metadata_pipe, a.audio_pipe);
        assert!(is_plain_path(&a.metadata_pipe));
    }

    #[test]
    fn plain_paths_need_no_shell_quoting() {
        assert!(is_plain_path(Path::new(
            "/home/u/.tune-assistant-server/run"
        )));
        assert!(is_plain_path(Path::new("/run/tas+1,a@b")));
        for bad in ["/home/my user/run", "/tmp/it's", "/tmp/$x", "/tmp/a;b", ""] {
            assert!(!is_plain_path(Path::new(bad)), "{bad}");
        }
    }

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
