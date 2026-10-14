//! Fakes for the tests: shell scripts standing in for shairport-sync.

use std::path::{Path, PathBuf};

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
