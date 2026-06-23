// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! Integration test for the global dumper's `BACKBEAT_MAX_DUMPS` + `keep-newest` limit policy.
//!
//! Its own test binary so the `OnceLock`-backed global recorder is built once with this test's
//! environment (a small limit and the `keep-newest` policy).

use backbeat::{
    global,
    zerocopy::{Immutable, IntoBytes},
};
use std::time::Duration;

#[derive(backbeat::Event, IntoBytes, Immutable)]
#[event(namespace = "test::global_limit")]
#[repr(C)]
struct Beat {
    #[event(key)]
    seq: u64,
}

/// Lists the dump files (`trace.*.bb`) in `dir`, sorted by name (= chronological).
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

/// Polls until `dir` holds at least `want` dump files, returning the full sorted list (or whatever
/// exists at timeout).
fn wait_for_count(dir: &std::path::Path, want: usize) -> Vec<std::path::PathBuf> {
    let mut files = dump_files(dir);
    for _ in 0..300 {
        files = dump_files(dir);
        if files.len() >= want {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    files
}

#[test]
fn keep_newest_bounds_file_count_to_limit() {
    let dir = std::env::temp_dir().join(format!("backbeat-limit-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let base = dir.join("trace.bb");
    std::env::set_var("BACKBEAT_PATH", &base);
    std::env::set_var("BACKBEAT_BYTES", "65536");
    std::env::set_var("BACKBEAT_THROTTLE_MS", "0");
    std::env::set_var("BACKBEAT_MAX_DUMPS", "2");
    std::env::set_var("BACKBEAT_LIMIT_POLICY", "keep-newest");

    global::enable();

    // Trigger three dumps; the dumper names files by timestamp and disambiguates same-millisecond
    // dumps with a `-N` suffix, so all three are distinct files momentarily — but keep-newest with
    // a limit of 2 evicts the oldest, so the count must settle at exactly 2.
    for seq in 0..3u64 {
        global::record(&Beat { seq });
        global::trigger();
        // Wait for this dump to land before the next, so eviction ordering is deterministic.
        wait_for_count(&dir, (seq as usize + 1).min(2));
        // A tiny gap so timestamps advance and ordering is stable.
        std::thread::sleep(Duration::from_millis(5));
    }

    // After the dust settles, keep-newest must hold the count at the limit (2), never more.
    let mut settled = Vec::new();
    for _ in 0..300 {
        settled = dump_files(&dir);
        if settled.len() == 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        settled.len(),
        2,
        "keep-newest with max_dumps=2 must keep exactly 2 files, found {settled:?}"
    );

    // The retained files must be the two newest (largest timestamps). Trigger one more and confirm
    // the oldest of the current pair is gone afterward.
    let before = dump_files(&dir);
    let oldest = before.first().cloned().unwrap();
    std::thread::sleep(Duration::from_millis(5));
    global::record(&Beat { seq: 99 });
    global::trigger();
    let mut evicted = false;
    for _ in 0..300 {
        if !oldest.exists() && dump_files(&dir).len() == 2 {
            evicted = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        evicted,
        "a further dump should evict the previously-oldest file and keep the count at 2"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
