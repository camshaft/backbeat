# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crates in this workspace
(`backbeat`, `backbeat-cli`, `backbeat-macros`) share a single version, published together to
[crates.io](https://crates.io) on every `v*` tag (see `.github/workflows/release.yml`).

## [0.1.3] - 2026-09-28

### Added

- **Programmatic global configuration.** A `Config` type can now be built and installed in code,
  alongside the existing `BACKBEAT_*` environment variables, so an embedding application can
  configure the global recorder without relying on the process environment (#19).
- **`#[event(crate = <path>)]` on the derive macro.** Events can now name the path at which
  `backbeat` is reachable, so the derive works when `backbeat` is re-exported under a different
  path (e.g. from a crate that vendors or wraps it) (#13).

### Fixed

- **`__rseq_offset` resolution.** The per-CPU fast path now resolves `__rseq_offset` via `dlsym`
  instead of an `extern static`, fixing link/load failures on toolchains and libc versions where
  the symbol is not available as a static (#20).

### Performance

- **Record de-duplication by ring offset.** Records are de-duplicated by their ring-buffer offset
  rather than by hashing `event_id` plus the field bytes, cutting per-record work on the dump path
  (#11).
- **Streaming large dumps end-to-end.** Large dumps are streamed rather than materialized, and the
  worker-thread count is capped, bounding memory and CPU when converting multi-gigabyte dumps
  (#10).

## [0.1.2] - 2026-06-24

Earlier releases (`0.1.0`–`0.1.2`) predate this changelog. See the
[GitHub release notes](https://github.com/camshaft/backbeat/releases) and the git history for
details.

[0.1.3]: https://github.com/camshaft/backbeat/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/camshaft/backbeat/releases/tag/v0.1.2
