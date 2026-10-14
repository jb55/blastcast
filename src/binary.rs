//! Finding the shairport-sync binary: a configured path, then `PATH`, each
//! checked by running it with `--version`.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::process::Command;

pub const BINARY_NAME: &str = "shairport-sync";
/// How long `--version` may take before the binary counts as broken.
const VERSION_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum BinaryError {
    #[error("{path}: {reason}")]
    Unusable { path: PathBuf, reason: String },
    #[error("{BINARY_NAME} is not on PATH; install it or pass its path")]
    NotFound,
}

/// A shairport-sync that answered `--version`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shairport {
    pub path: PathBuf,
    /// Its `--version` line, e.g. `4.3.7-OpenSSL-tinysvcmdns-stdout-pipe-metadata`.
    pub version: String,
}

/// Finds a working shairport-sync. A `configured` path is the only one
/// tried, so a broken one is an error rather than quietly replaced by
/// another on `PATH`.
pub async fn find(configured: Option<&Path>) -> Result<Shairport, BinaryError> {
    if let Some(path) = configured {
        return check(path).await;
    }
    let path = std::env::var_os("PATH").ok_or(BinaryError::NotFound)?;
    find_in_path(&path).await
}

async fn find_in_path(path: &OsStr) -> Result<Shairport, BinaryError> {
    let mut last_err = BinaryError::NotFound;
    for dir in std::env::split_paths(path) {
        let candidate = dir.join(BINARY_NAME);
        if !candidate.is_file() {
            continue;
        }
        match check(&candidate).await {
            Ok(found) => return Ok(found),
            Err(e) => last_err = e,
        }
    }
    Err(last_err)
}

/// Runs `path --version` and keeps its first line of output.
pub async fn check(path: &Path) -> Result<Shairport, BinaryError> {
    let unusable = |reason: String| BinaryError::Unusable {
        path: path.to_owned(),
        reason,
    };
    let output = Command::new(path)
        .arg("--version")
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output();
    let output = tokio::time::timeout(VERSION_TIMEOUT, output)
        .await
        .map_err(|_| unusable(format!("--version took over {VERSION_TIMEOUT:?}")))?
        .map_err(|e| unusable(e.to_string()))?;
    if !output.status.success() {
        return Err(unusable(format!("--version exited with {}", output.status)));
    }
    // shairport-sync 3.x printed the version on stderr.
    let text = [&output.stdout, &output.stderr]
        .into_iter()
        .map(|b| String::from_utf8_lossy(b).trim().to_owned())
        .find(|s| !s.is_empty())
        .ok_or_else(|| unusable("--version printed nothing".into()))?;
    let version = text.lines().next().unwrap_or_default().trim().to_owned();
    Ok(Shairport {
        path: path.to_owned(),
        version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::script;

    #[tokio::test]
    async fn a_configured_path_is_checked() {
        let dir = tempfile::tempdir().unwrap();
        let bin = script(dir.path(), "good", "echo 4.3.7-fake-pipe-metadata");
        let found = find(Some(&bin)).await.unwrap();
        assert_eq!(found.path, bin);
        assert_eq!(found.version, "4.3.7-fake-pipe-metadata");
    }

    #[tokio::test]
    async fn a_failing_binary_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let bin = script(dir.path(), "bad", "echo nope >&2; exit 3");
        let err = find(Some(&bin)).await.unwrap_err();
        assert!(matches!(err, BinaryError::Unusable { .. }), "{err}");
        let missing = dir.path().join("missing");
        assert!(find(Some(&missing)).await.is_err());
    }

    #[tokio::test]
    async fn the_version_on_stderr_counts() {
        let dir = tempfile::tempdir().unwrap();
        let bin = script(dir.path(), "old", "echo 3.3.9 >&2");
        assert_eq!(check(&bin).await.unwrap().version, "3.3.9");
    }

    #[tokio::test]
    async fn path_is_searched_in_order_past_broken_ones() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b, c) = (
            dir.path().join("a"),
            dir.path().join("b"),
            dir.path().join("c"),
        );
        for d in [&a, &b, &c] {
            std::fs::create_dir(d).unwrap();
        }
        script(&b, BINARY_NAME, "exit 1");
        let good = script(&c, BINARY_NAME, "echo from-c");
        let path = std::env::join_paths([&a, &b, &c]).unwrap();
        assert_eq!(find_in_path(&path).await.unwrap().path, good);

        let path = std::env::join_paths([&a]).unwrap();
        assert!(matches!(
            find_in_path(&path).await,
            Err(BinaryError::NotFound)
        ));
    }
}
