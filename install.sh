#!/bin/sh
# backbeat CLI installer.
#
#   curl -fsSL https://raw.githubusercontent.com/camshaft/backbeat/main/install.sh | sh
#
# Detects your OS/arch, downloads the matching prebuilt `backbeat` binary from the latest GitHub
# release, verifies its SHA-256, and installs it. Override the install directory with
# BACKBEAT_INSTALL_DIR, or pin a version with BACKBEAT_VERSION (e.g. v0.1.0).
set -eu

REPO="camshaft/backbeat"
BIN="backbeat"
# A specific tag (v0.1.0) or "latest". GitHub's /releases/latest/download/ redirect always resolves
# to the newest non-prerelease release, so the default needs no version bump here.
VERSION="${BACKBEAT_VERSION:-latest}"
INSTALL_DIR="${BACKBEAT_INSTALL_DIR:-$HOME/.local/bin}"

err() {
	echo "install: $*" >&2
	exit 1
}

need() {
	command -v "$1" >/dev/null 2>&1 || err "required command not found: $1"
}

# Pick a downloader that's almost certainly present.
if command -v curl >/dev/null 2>&1; then
	dl() { curl -fsSL "$1" -o "$2"; }
elif command -v wget >/dev/null 2>&1; then
	dl() { wget -qO "$2" "$1"; }
else
	err "need either curl or wget"
fi
need tar

# Map uname output to one of the release's target triples. We ship static musl Linux builds for
# x86_64/aarch64 and a self-contained aarch64 macOS build.
os="$(uname -s)"
arch="$(uname -m)"
case "$os/$arch" in
	Linux/x86_64 | Linux/amd64) target="x86_64-unknown-linux-musl" ;;
	Linux/aarch64 | Linux/arm64) target="aarch64-unknown-linux-musl" ;;
	Darwin/arm64) target="aarch64-apple-darwin" ;;
	Darwin/x86_64)
		err "no prebuilt binary for Intel macOS; install from source with: cargo install backbeat-cli"
		;;
	*)
		err "unsupported platform $os/$arch; install from source with: cargo install backbeat-cli"
		;;
esac

stage="${BIN}-${target}"
archive="${stage}.tar.gz"
if [ "$VERSION" = "latest" ]; then
	base="https://github.com/${REPO}/releases/latest/download"
else
	base="https://github.com/${REPO}/releases/download/${VERSION}"
fi

# Stage the download in a temp dir we always clean up.
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "install: downloading $BIN ($target, $VERSION)..."
dl "${base}/${archive}" "${tmp}/${archive}" || err "download failed: ${base}/${archive}"

# Verify the checksum when a sha256 tool is available (the release ships ${archive}.sha256). The
# checksum file is "<hash>  <archive>", so compare against just the staged file's hash.
if dl "${base}/${archive}.sha256" "${tmp}/${archive}.sha256" 2>/dev/null; then
	expected="$(awk '{print $1}' "${tmp}/${archive}.sha256")"
	if command -v sha256sum >/dev/null 2>&1; then
		actual="$(sha256sum "${tmp}/${archive}" | awk '{print $1}')"
	elif command -v shasum >/dev/null 2>&1; then
		actual="$(shasum -a 256 "${tmp}/${archive}" | awk '{print $1}')"
	else
		actual=""
	fi
	if [ -n "$actual" ] && [ "$actual" != "$expected" ]; then
		err "checksum mismatch (expected $expected, got $actual)"
	fi
	[ -n "$actual" ] && echo "install: checksum ok"
else
	echo "install: warning: no checksum file found; skipping verification" >&2
fi

# Each tarball stages the binary under ${BIN}-${target}/. Extract and locate it.
tar -xzf "${tmp}/${archive}" -C "$tmp"
src="${tmp}/${stage}/${BIN}"
[ -f "$src" ] || src="$(find "$tmp" -name "$BIN" -type f | head -n1)"
[ -n "$src" ] && [ -f "$src" ] || err "binary not found in archive"

mkdir -p "$INSTALL_DIR"
chmod +x "$src"
mv -f "$src" "${INSTALL_DIR}/${BIN}"
echo "install: installed ${BIN} to ${INSTALL_DIR}/${BIN}"

# Nudge if the install dir isn't on PATH.
case ":${PATH}:" in
	*":${INSTALL_DIR}:"*) ;;
	*) echo "install: note: ${INSTALL_DIR} is not on your PATH; add it to use \`${BIN}\` directly" >&2 ;;
esac

"${INSTALL_DIR}/${BIN}" --version 2>/dev/null || true
