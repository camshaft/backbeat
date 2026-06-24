// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! End-to-end test for the process-wide global recorder + background dumper.
//!
//! This is its own test binary so the `OnceLock`-backed global recorder is built exactly once, with
//! the environment this test controls (it sets `BACKBEAT_PATH`/`BACKBEAT_BYTES` before first use).

use backbeat::{
    global,
    wire::DumpReader,
    zerocopy::{Immutable, IntoBytes},
    Event,
};
use std::time::Duration;

/// A simple point event recorded through the global recorder.
#[derive(Event, IntoBytes, Immutable)]
#[event(namespace = "test::global")]
#[repr(C)]
struct Tick {
    /// A monotonically increasing marker so we can find our records in the dump.
    #[event(key)]
    seq: u64,
}

#[test]
fn record_trigger_and_async_dump() {
    // Configure before first use of the global recorder. A small ring keeps the dump cheap; a zero
    // throttle so repeated triggers in this test are never dropped.
    let dir = std::env::temp_dir().join(format!("backbeat-global-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let base = dir.join("trace.bb");
    std::env::set_var("BACKBEAT_PATH", &base);
    std::env::set_var("BACKBEAT_BYTES", "65536");
    std::env::set_var("BACKBEAT_THROTTLE_MS", "0");
    std::env::set_var("BACKBEAT_HOST", "test-host");

    // Capture starts disabled until armed.
    assert!(!global::is_enabled() || cfg!(not(feature = "capture")));
    global::enable();
    assert_eq!(global::is_enabled(), cfg!(feature = "capture"));

    // The configured path is what we set.
    assert_eq!(global::dump_path(), base);
    assert_eq!(global::host(), "test-host");

    // Record a handful of events, then ask for an async dump.
    for seq in 0..16u64 {
        global::record(&Tick { seq });
    }
    global::trigger();

    // The dumper is a fire-and-forget background thread that names files by timestamp; poll the
    // directory until at least one non-empty `trace.*.bb` dump appears.
    let bytes = wait_for_dump(&dir, 1).expect("background dumper should write a dump");

    // The dump must be a valid `.bb` carrying our host label and at least one shard.
    let reader = DumpReader::new(bytes).expect("dump decodes");
    let metas = reader.metas().unwrap();
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].host, "test-host");
    let shards = reader.shards().unwrap();
    assert!(!shards.is_empty());

    // Under the `capture` feature our Tick events should be present; count records across shards by
    // walking each shard and matching Tick's content-addressed id.
    #[cfg(feature = "capture")]
    {
        use backbeat::ring;
        let mut ticks = 0usize;
        for shard in &shards {
            let region = shard.region.clone();
            ring::walk(&region, shard.head as usize, region.len(), |rec| {
                // Record payload is [event_id u64 LE][fields]; ts is stripped by the shard framing?
                // The recorder writes [ts][event_id][fields]; walk yields the whole record payload.
                if rec.len() >= 16 {
                    let id = u64::from_le_bytes(rec[8..16].try_into().unwrap());
                    if id == Tick::ID.get() {
                        ticks += 1;
                    }
                }
                true
            });
        }
        assert!(ticks > 0, "recorded Tick events should appear in the dump");
    }

    // A second trigger (throttle is 0) should produce a second dump file.
    global::record(&Tick { seq: 99 });
    global::trigger();
    assert!(
        wait_for_dump(&dir, 2).is_some(),
        "a second trigger should write a second dump file (throttle is 0)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Lists the dump files (`trace.*.bb`) in `dir`, sorted by name (= chronological, since names are
/// timestamps).
fn dump_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            name.starts_with("trace.") && name.ends_with(".bb")
        })
        .collect();
    files.sort();
    files
}

/// Polls until at least `want` non-empty dump files exist in `dir`, returning the newest one's bytes
/// (or `None` on timeout).
fn wait_for_dump(dir: &std::path::Path, want: usize) -> Option<Vec<u8>> {
    for _ in 0..300 {
        let files = dump_files(dir);
        if files.len() >= want {
            if let Some(last) = files.last() {
                if let Ok(b) = std::fs::read(last) {
                    if !b.is_empty() {
                        return Some(b);
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    None
}
