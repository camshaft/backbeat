# backbeat

A low-overhead flight recorder for Rust: capture structured events into
per-core ring buffers, dump them to disk, and read the dump back as Parquet or a
Chrome/Perfetto trace.

## Features

- **Cheap to capture.** The hot path is an atomic `fetch_add` plus a `memcpy`
  into a per-core ring — no locks, no cross-core contention, no allocation. When
  disabled it's a single relaxed load.
- **Self-describing dumps.** Add, remove, or reorder an event's fields freely;
  the embedded schema reflects it, and old tooling still reads new dumps.
- **Define events with a derive.** `#[derive(Event)]` on a `#[repr(C)]` struct is
  all it takes; events register themselves, so the dumper finds them automatically.
- **Spans, not just points.** Mark a pair of events as a span and the trace
  output renders real begin/end duration slices.
- **Useful output.** `convert` writes sparse-wide [Parquet](https://parquet.apache.org/)
  (query it with anything) or Chrome/Perfetto trace JSON (open it in
  [Perfetto](https://ui.perfetto.dev/) or `chrome://tracing`).
- **`no_std`-friendly.** Events can be defined in `no_std` crates; the default recorder
  runtime needs `std`.

## Getting started

Add the library to the crate that produces events:

```toml
[dependencies]
backbeat = "0.1"
zerocopy = { version = "0.8", features = ["derive"] }
```

Define events, record them, and dump:

```rust
use backbeat::{Event, EventEnum};
use backbeat::zerocopy::{Immutable, IntoBytes};
use backbeat::recorder::Recorder;

/// Which side is sending a packet.
#[derive(EventEnum, IntoBytes, Immutable, Clone, Copy)]
#[repr(u8)]
enum Role { Client = 0, Server = 1 }

/// A packet was sent.
#[derive(Event, IntoBytes, Immutable)]
#[event(namespace = "my_app::net")]
#[repr(C)]
struct PacketSent {
    /// The connection the packet belongs to.
    #[event(key)]
    conn_id: u64,

    /// The size of the packet in bytes.
    #[event(unit = "bytes")]
    len: u32,

    role: Role, // a strongly-typed enum field
    _pad: [u8; 3], // explicit padding: IntoBytes rejects implicit gaps
}

let rec = Recorder::new(/* shards */ 4, /* bytes/shard */ 1 << 20);
rec.set_enabled(true);
rec.record(&PacketSent { conn_id: 7, len: 1200, role: Role::Server, _pad: [0; 3] });

// Dump everything compiled in (the registry self-populates via the derive).
let dump = rec.dump(backbeat::registry::schemas(), std::iter::empty(), "my-host");
std::fs::write("trace.bb", &dump).unwrap();
```

Then read the dump with the CLI:

```console
$ cargo install backbeat-cli
$ backbeat inspect trace.bb              # envelope, schema registry, per-instance + per-shard counts
$ backbeat convert trace.bb -o out.parquet   # → sparse-wide Parquet
$ backbeat convert trace.bb -o out.json      # → Chrome / Perfetto trace
$ backbeat merge a.bb b.bb -o all.bb         # → one multi-instance .bb
```

`convert` accepts multiple dumps and merges them into one output, de-duplicating records that
overlapping dumps captured more than once (successive dumps of one process share a ring, so the
newer one re-contains the older's tail).

### Merging dumps

A `.bb` is inherently multi-instance: metadata, intern tables, and shard rings are each tagged with
the `instance_id` of the process that produced them, while the schema registry is unified
(content-addressed by event id). `backbeat merge` combines several dumps — from one process over
time, or many processes across a fleet — into a single file:

```console
$ backbeat merge run.*.bb -o combined.bb            # decode, de-duplicate, re-pack compact shards
$ backbeat merge host.*.bb -o upload.bb --no-dedup  # cheap raw splice (keeps duplicates)
```

By default `merge` trims the result to one record per distinct event. `--no-dedup` is a fast,
lossless byte-splice — useful for concatenating a host's dumps before upload, since `convert` dedups
on the way out regardless. Converting a merged file yields exactly what converting its inputs
together would.

## Global recorder

For a system-wide recorder you usually want a single process-wide instance you can reach from
anywhere, dumped asynchronously off the hot path. `backbeat::global` provides exactly that — a
lazily-built recorder behind a `OnceLock` plus a background dumper thread:

```rust
use backbeat::global;

global::enable();                  // arm capture (starts disabled)
global::record(&PacketSent { /* … */ });   // instrument anywhere — no handle to thread through

// When something looks wrong, ask the background thread to flush the rings to disk. This returns
// immediately; the (blocking) snapshot + write happen on the dumper thread, throttled so a trigger
// storm can't fill the disk. Each dump is a new timestamp-named file, so a dump's age is obvious and
// no earlier dump is overwritten.
global::trigger();
```

It is configured by environment variables, read once on first use, so a binary can be traced with no
code change:

| Variable | Meaning | Default |
| --- | --- | --- |
| `BACKBEAT_ENABLE` | Arm capture as soon as the recorder is built | off |
| `BACKBEAT_SHARDS` | Number of per-CPU rings | available parallelism (capped at 16) |
| `BACKBEAT_BYTES` | Bytes per shard | 16 MiB |
| `BACKBEAT_PATH` | Base dump path (each dump inserts a UTC timestamp before the extension) | `${TMPDIR}/backbeat.<pid>.bb` |
| `BACKBEAT_THROTTLE_MS` | Minimum interval between dumps | 1000 |
| `BACKBEAT_MAX_DUMPS` | Cap on dump files kept (`0` = unlimited) | 8 |
| `BACKBEAT_LIMIT_POLICY` | At the cap: `keep-oldest` (stop) or `keep-newest` (evict oldest) | `keep-oldest` |
| `BACKBEAT_HOST` | Host label embedded in each dump | system hostname |
| `BACKBEAT_SIGNAL` | Unix: `kill -<sig>` triggers a dump (`usr1`/`usr2`/number) | none |
| `BACKBEAT_DUMP_ON_PANIC` | Trigger a final dump from a panic hook | off |

## Prebuilt binaries

Install the latest `backbeat` CLI with one line — it detects your platform, verifies the checksum,
and drops the binary in `~/.local/bin`:

```console
$ curl -fsSL https://raw.githubusercontent.com/camshaft/backbeat/main/install.sh | sh
```

Override where it lands with `BACKBEAT_INSTALL_DIR`, or pin a version with `BACKBEAT_VERSION=v0.1.0`.

Tagged releases publish prebuilt `backbeat` CLI binaries you can drop onto a host to convert a dump:
statically-linked musl builds for `x86_64`/`aarch64` Linux and a self-contained `aarch64` macOS
build, attached to the [GitHub Release](https://github.com/camshaft/backbeat/releases) as
per-target `.tar.gz` archives (each with a `.sha256`). The `latest` release always resolves to the
newest tag, so the installer URL and
`https://github.com/camshaft/backbeat/releases/latest/download/backbeat-<target>.tar.gz` never need
updating. Prefer to build from source? `cargo install backbeat-cli`.

## License

This project is licensed under the [MIT license](./LICENSE).
