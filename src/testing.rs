//! Fakes for the tests: shell scripts standing in for shairport-sync,
//! and what it writes to its fifos.

use std::path::{Path, PathBuf};

use base64::Engine;

/// Writes an executable `/bin/sh` script `name` in `dir` running `body`.
///
/// A `cp` child writes the executable, never this process: a test on
/// another thread that forks while we hold a write fd on it would make
/// exec fail with "Text file busy".
pub fn script(dir: &Path, name: &str, body: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join(name);
    let source = dir.join(format!(".{name}.src"));
    std::fs::write(&source, format!("#!/bin/sh\n{body}\n")).unwrap();
    let copied = std::process::Command::new("cp")
        .arg(&source)
        .arg(&path)
        .status()
        .unwrap();
    assert!(copied.success());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path
}

/// Writes one metadata item the way shairport's `metadata/pipe.c` does.
pub fn item(kind: &str, code: &str, data: &[u8]) -> Vec<u8> {
    let hex = |s: &str| u32::from_be_bytes(s.as_bytes().try_into().unwrap());
    let mut out = format!(
        "<item><type>{:x}</type><code>{:x}</code><length>{}</length>",
        hex(kind),
        hex(code),
        data.len()
    )
    .into_bytes();
    if !data.is_empty() {
        out.extend(b"\n<data encoding=\"base64\">\n");
        for group in data.chunks(57) {
            out.extend(
                base64::engine::general_purpose::STANDARD
                    .encode(group)
                    .bytes(),
            );
        }
        out.extend(b"</data>");
    }
    out.extend(b"</item>\n");
    out
}

/// Makes a fifo, as the supervisor does before a run.
pub fn mkfifo(path: &Path) {
    rustix::fs::mkfifoat(
        rustix::fs::CWD,
        path,
        rustix::fs::Mode::from_raw_mode(0o600),
    )
    .unwrap();
}
