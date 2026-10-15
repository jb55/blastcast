#!/usr/bin/env bash
# The gate every commit passes: formatting, clippy with warnings denied, tests.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test
