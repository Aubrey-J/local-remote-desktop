#!/usr/bin/env bash
set -euo pipefail

host="$(rustc -vV | sed -n 's/^host: //p')"
case "$host" in
  aarch64-apple-darwin)
    rustflags_variable=CARGO_TARGET_AARCH64_APPLE_DARWIN_RUSTFLAGS
    ;;
  x86_64-apple-darwin)
    rustflags_variable=CARGO_TARGET_X86_64_APPLE_DARWIN_RUSTFLAGS
    ;;
  *)
    echo "Unsupported macOS Rust host target: $host" >&2
    exit 1
    ;;
esac

if ! command -v xcrun >/dev/null 2>&1; then
  echo "xcrun is required; install the Xcode command-line tools." >&2
  exit 1
fi

runtime_archive="$(xcrun clang --print-resource-dir)/lib/darwin/libclang_rt.osx.a"
if [[ ! -f "$runtime_archive" ]]; then
  echo "Clang runtime archive not found: $runtime_archive" >&2
  exit 1
fi

env "$rustflags_variable=-C link-arg=$runtime_archive" npm run tauri -- build --bundles dmg
