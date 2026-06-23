// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! A process-wide global [`Recorder`] with an asynchronous, throttled dumper.
//!
//! Most programs want exactly one recorder for the whole process: instrument anywhere with a bare
//! [`record`] call, no handle to thread through. This module provides that — a single lazily
//! initialized [`Recorder`] behind a [`OnceLock`], plus a background thread that writes the rings to
//! disk when [`trigger`]ed, decoupling the (slow, blocking) dump from whatever hot path noticed the
//! anomaly worth dumping.
//!
//! ```ignore
//! use backbeat::global;
//!
//! global::enable();                  // arm capture (off by default)
//! global::record(&MyEvent { .. });   // instrument anywhere — no recorder handle needed
//! global::trigger();                 // ask the background thread to write a dump
//! ```
//!
//! ## Lifecycle
//!
//! The recorder is built on first use ([`recorder`]/[`record`]/[`trigger`]) from environment
//! overrides (read once):
//!
//! * `BACKBEAT_SHARDS` — number of per-CPU rings (default: available parallelism, capped).
//! * `BACKBEAT_BYTES` — bytes per shard (default 16 MiB), floored at one page.
//! * `BACKBEAT_PATH` — base dump path (default `${TMPDIR}/backbeat.<pid>.bb`).
//! * `BACKBEAT_THROTTLE_MS` — minimum interval between dumps (default 1000 ms).
//! * `BACKBEAT_MAX_DUMPS` — cap on the number of dump files kept (default 8; `0` = unlimited).
//! * `BACKBEAT_LIMIT_POLICY` — at the cap, `keep-oldest` (default: stop dumping, preserving the
//!   lead-up) or `keep-newest` (evict the oldest file so the latest `max_dumps` survive).
//! * `BACKBEAT_HOST` — host label embedded in each dump (default the system hostname, else empty).
//! * `BACKBEAT_SIGNAL` (Unix) — install a handler so `kill -<sig> <pid>` triggers a dump
//!   (`usr1`/`usr2` or a signal number; unset = no handler).
//! * `BACKBEAT_ENABLE` — if set to a truthy value, arm capture as soon as the recorder is built (so
//!   a binary can be traced without a code change to call [`enable`]).
//! * `BACKBEAT_DUMP_ON_PANIC` — if truthy, install a panic hook that triggers a final dump.
//!
//! Capture starts **disabled**; call [`enable`] (or set `BACKBEAT_ENABLE`). With the `capture`
//! feature compiled out, [`record`] folds to nothing — see [`crate::recorder`].
//!
//! ## Dumps
//!
//! Each [`trigger`] asks the background thread for one dump. Dumps are written to their own
//! timestamp-named files (`backbeat.<pid>.20260623T142530123Z.bb`, …) so a dump's age is obvious at
//! a glance, the files sort chronologically, and no earlier dump is ever overwritten. Back-to-back
//! triggers within the throttle window coalesce into a single dump to cap disk churn; the first dump
//! is never throttled.
//!
//! To bound disk use over a long run, set `BACKBEAT_MAX_DUMPS`. At the cap the [`LimitPolicy`]
//! decides what to keep: `keep-oldest` (the default) stops writing so the earliest dumps — the
//! history leading into the bad state — survive; `keep-newest` keeps a ring of the most recent
//! `max_dumps` files, evicting the oldest as new dumps arrive.

use crate::recorder::Recorder;
use std::{
    io::Write as _,
    path::{Path, PathBuf},
    sync::{Condvar, Mutex, OnceLock},
    time::{Duration, Instant},
};

/// Default bytes per shard when `BACKBEAT_BYTES` is unset. 16 MiB per shard.
const DEFAULT_BYTES_PER_SHARD: usize = 16 << 20;

/// Default minimum interval between dumps (`BACKBEAT_THROTTLE_MS`).
const DEFAULT_THROTTLE_MS: u64 = 1000;

/// Default cap on the number of dump files kept (`BACKBEAT_MAX_DUMPS`). A dump serializes the whole
/// ring capacity per shard, so an uncapped, repeatedly-triggered process could quietly write many
/// gigabytes; a small default keeps that bounded. Set `BACKBEAT_MAX_DUMPS=0` for unlimited.
const DEFAULT_MAX_DUMPS: u64 = 8;

/// What the background dumper does once it has already written [`Dumper::max_dumps`] dumps.
///
/// A bounded dump count keeps a long-running, repeatedly-triggered process from filling the disk.
/// Which dumps to keep depends on the failure: the lead-up is usually most useful, but sometimes the
/// latest state is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LimitPolicy {
    /// Keep the **oldest** dumps: once the limit is reached, drop further dump requests. The
    /// earliest dumps — capturing the history that led into the bad state — are preserved. This is
    /// the default, because the first dump (closest to when the problem began) is usually the most
    /// valuable.
    KeepOldest,
    /// Keep the **newest** dumps: once the limit is reached, each new dump evicts the oldest dump
    /// file, so the most recent `limit` dumps are retained (a ring of files).
    KeepNewest,
}

impl LimitPolicy {
    /// Parses `BACKBEAT_LIMIT_POLICY`: `keep-oldest`/`oldest` or `keep-newest`/`newest`
    /// (case-insensitive).
    fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "keep-oldest" | "oldest" => Some(Self::KeepOldest),
            "keep-newest" | "newest" => Some(Self::KeepNewest),
            _ => None,
        }
    }
}

/// Upper bound on the shard count derived from available parallelism, so a 256-core box does not
/// reserve gigabytes of rings by default. An explicit `BACKBEAT_SHARDS` is not capped.
const MAX_DEFAULT_SHARDS: usize = 16;

/// Process-wide recorder plus its background dumper, built once on first use.
struct Global {
    recorder: Recorder,
    dumper: Dumper,
    /// Host label embedded in every dump (`BACKBEAT_HOST`, else the system hostname, else empty).
    host: String,
}

/// The background-dump handoff: a single pending flag plus a condvar the dumper thread waits on.
struct Dumper {
    /// The coalescing "dump requested" flag set by [`trigger`] and cleared by the dumper thread when
    /// it picks the request up. The thread waits on `condvar` for this to become `true`.
    state: Mutex<bool>,
    condvar: Condvar,
    /// Base dump path; per-dump files insert a UTC timestamp before the extension.
    path: PathBuf,
    /// Minimum wall-clock interval between dumps. A request arriving within this window of the last
    /// completed dump is dropped (the first dump is never throttled), so a trigger storm cannot
    /// write a dump per trigger and fill the disk.
    throttle: Duration,
    /// Maximum number of dump files to keep on disk, or `None` for unlimited. Bounds disk use over a
    /// long-running, repeatedly-triggered process.
    max_dumps: Option<u64>,
    /// What to do once `max_dumps` is reached — see [`LimitPolicy`].
    limit_policy: LimitPolicy,
}

static GLOBAL: OnceLock<Global> = OnceLock::new();

/// Returns the process-wide [`Recorder`], building it (and spawning the dumper thread) on first use.
///
/// Use this when you need the recorder handle itself — e.g. to open a span with
/// [`Recorder::enter`]. For point events, [`record`] is more direct.
pub fn recorder() -> &'static Recorder {
    &global().recorder
}

fn global() -> &'static Global {
    GLOBAL.get_or_init(build)
}

/// Records one event into the global recorder. A no-op (after one relaxed load) when capture is
/// disabled, and folded away entirely when the `capture` feature is off.
///
/// This is the bare-call instrumentation entry point: no recorder handle to thread through.
#[inline]
pub fn record<E: crate::event::Event>(event: &E) {
    global().recorder.record(event);
}

/// Enables capture on the global recorder (it starts disabled). Idempotent.
pub fn enable() {
    global().recorder.set_enabled(true);
}

/// Disables capture on the global recorder. The rings retain what they hold; [`trigger`] still dumps.
pub fn disable() {
    global().recorder.set_enabled(false);
}

/// Whether capture is currently enabled. Always `false` with the `capture` feature compiled out.
pub fn is_enabled() -> bool {
    global().recorder.is_enabled()
}

/// Requests an asynchronous dump of the global recorder's rings to disk.
///
/// Sets the coalescing handoff flag and wakes the background dumper, which writes the next
/// sequence-numbered file. Returns immediately — the (blocking) snapshot + write happen on the
/// dumper thread. Triggers that arrive while a dump is in flight, or within the throttle window of
/// the last dump, coalesce so at most one extra dump follows.
///
/// Triggering does not require capture to be enabled (you can dump whatever the rings hold), but a
/// recorder that was never enabled has empty rings.
pub fn trigger() {
    let g = global();
    // Recover from a poisoned lock instead of `.unwrap()`-ing so `trigger()` can never panic. This
    // matters most from the panic hook: if a panic poisoned this mutex, `.lock().unwrap()` here
    // would panic *during* unwinding and abort the process — swallowing the very crash dump the
    // hook exists to write. The guarded data is a single bool we're about to overwrite anyway, so a
    // poisoned lock is safe to take. Blocking (not `try_lock`) is fine: the dumper holds this lock
    // only momentarily to check/reset the flag, never across the dump itself.
    let mut requested = g.dumper.state.lock().unwrap_or_else(|e| e.into_inner());
    *requested = true;
    g.dumper.condvar.notify_one();
}

/// Builds the global recorder from the environment and spawns the background dumper.
fn build() -> Global {
    let shards = env_usize("BACKBEAT_SHARDS")
        .unwrap_or_else(default_shards)
        .max(1);
    let bytes = env_usize("BACKBEAT_BYTES")
        .unwrap_or(DEFAULT_BYTES_PER_SHARD)
        // Floor at one page: a ring must hold at least a full record, and a sub-record ring is
        // useless anyway.
        .max(4096);

    let recorder = Recorder::new(shards, bytes);

    let path = std::env::var_os("BACKBEAT_PATH")
        .map(PathBuf::from)
        // PID-qualify the default so concurrent processes (or a test harness's per-test processes)
        // don't clobber each other's numbered dumps.
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("backbeat.{}.bb", std::process::id()))
        });

    let throttle_ms = env_u64("BACKBEAT_THROTTLE_MS").unwrap_or(DEFAULT_THROTTLE_MS);

    // Unset → a modest default cap; explicit `0` → unlimited; any positive value → that cap. A cap
    // is on by default because each dump can be large (the full ring capacity per shard), so an
    // uncapped process that triggers repeatedly could accidentally fill the disk.
    let max_dumps = match env_u64("BACKBEAT_MAX_DUMPS") {
        None => Some(DEFAULT_MAX_DUMPS),
        Some(0) => None,
        Some(n) => Some(n),
    };
    let limit_policy = std::env::var("BACKBEAT_LIMIT_POLICY")
        .ok()
        .and_then(|s| LimitPolicy::parse(&s))
        .unwrap_or(LimitPolicy::KeepOldest);

    let host = std::env::var("BACKBEAT_HOST")
        .ok()
        .filter(|h| !h.is_empty())
        .unwrap_or_else(hostname);

    let dumper = Dumper {
        state: Mutex::new(false),
        condvar: Condvar::new(),
        path,
        throttle: Duration::from_millis(throttle_ms),
        max_dumps,
        limit_policy,
    };

    let g = Global {
        recorder,
        dumper,
        host,
    };

    // Arm capture eagerly if asked, so a binary can be traced via env alone.
    if env_truthy("BACKBEAT_ENABLE") {
        g.recorder.set_enabled(true);
    }

    spawn_dumper();

    if env_truthy("BACKBEAT_DUMP_ON_PANIC") {
        install_panic_hook();
    }

    #[cfg(unix)]
    if let Some(sig) = std::env::var("BACKBEAT_SIGNAL")
        .ok()
        .and_then(|s| signal::parse(&s))
    {
        signal::install_full(sig);
    }

    g
}

/// Default shard count: the machine's available parallelism, capped at [`MAX_DEFAULT_SHARDS`].
fn default_shards() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .min(MAX_DEFAULT_SHARDS)
}

/// The system hostname, or an empty string if it can't be read. No external dependency: we read the
/// platform's `HOSTNAME`/`COMPUTERNAME` env var, falling back to the `hostname` command.
fn hostname() -> String {
    if let Ok(h) = std::env::var("HOSTNAME").or_else(|_| std::env::var("COMPUTERNAME")) {
        if !h.is_empty() {
            return h;
        }
    }
    std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok().and_then(|v| v.parse().ok())
}

/// Whether an env var is set to a truthy value (`1`/`true`/`yes`/`on`, case-insensitive).
fn env_truthy(key: &str) -> bool {
    std::env::var(key)
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

/// Spawns the single-consumer dumper thread. It blocks on the condvar and, on each request,
/// snapshots the rings via [`Recorder::dump`] and writes a fresh timestamp-named file, honoring the
/// configured dump-count limit and [`LimitPolicy`].
fn spawn_dumper() {
    let _ = std::thread::Builder::new()
        .name("backbeat::dumper".into())
        .spawn(move || {
            let g = global();
            let base = &g.dumper.path;
            // Dump files are named by wall-clock timestamp so their age is obvious and they sort
            // chronologically. We track the live files we've written (oldest first) so KeepNewest can
            // evict by real path — independent of the naming scheme — rather than recomputing a name.
            let mut live: std::collections::VecDeque<PathBuf> = std::collections::VecDeque::new();
            // The last stamp we used, to disambiguate two dumps that land in the same millisecond.
            let mut last_stamp = String::new();
            let mut last_dump: Option<Instant> = None;
            // One-shot guard so we warn (once) the first time the cap starts dropping dumps, rather
            // than spamming on every trigger or staying silent. Otherwise "why did dumps stop?" is a
            // mystery — the answer is the `KeepOldest` cap, so say so on stderr.
            let mut warned_cap = false;
            loop {
                {
                    let mut requested = g.dumper.state.lock().unwrap();
                    while !*requested {
                        requested = g.dumper.condvar.wait(requested).unwrap();
                    }
                    *requested = false;
                }

                // Apply the dump-count limit before doing any work. `KeepOldest` stops here once the
                // cap is hit (preserving the lead-up); `KeepNewest` proceeds and evicts the oldest.
                match limit_action(live.len(), g.dumper.max_dumps, g.dumper.limit_policy) {
                    LimitAction::Stop => {
                        if !warned_cap {
                            warned_cap = true;
                            let limit = g.dumper.max_dumps.unwrap_or(0);
                            eprintln!(
                                "backbeat: dump limit reached ({limit} files kept, policy \
                                 keep-oldest); further dumps are being dropped. Raise \
                                 BACKBEAT_MAX_DUMPS, set it to 0 for unlimited, or use \
                                 BACKBEAT_LIMIT_POLICY=keep-newest to keep the most recent instead."
                            );
                        }
                        continue;
                    }
                    LimitAction::Write { evict } => {
                        // Throttle: drop a request within `throttle` of the last completed dump (the
                        // first dump always proceeds). The rings already retain the history, so a
                        // dump this close would be near-identical; this caps disk churn under a
                        // trigger storm.
                        if throttled(last_dump.map(|l| l.elapsed()), g.dumper.throttle) {
                            continue;
                        }

                        let stamp = unique_stamp(now_unix_millis(), &last_stamp);
                        let path = stamped_path(base, &stamp);
                        last_stamp = stamp;
                        let bytes = g.recorder.dump(
                            crate::registry::schemas(),
                            core::iter::empty(),
                            &g.host,
                        );
                        // Best-effort: a failed dump must not take down the process.
                        if write_dump(&path, &bytes).is_ok() {
                            live.push_back(path);
                        }
                        // Under KeepNewest, remove the now-out-of-window oldest file so at most
                        // `max_dumps` files remain. Best-effort: a stale file is harmless.
                        if evict {
                            if let Some(old) = live.pop_front() {
                                let _ = std::fs::remove_file(old);
                            }
                        }
                        last_dump = Some(Instant::now());
                    }
                }
            }
        });
}

/// What the dumper should do for the next dump, given how many of its files are currently live and
/// the configured limit/policy.
#[derive(Debug, PartialEq, Eq)]
enum LimitAction {
    /// Write the dump; if `evict` is true, delete the oldest live dump file afterward (KeepNewest).
    Write { evict: bool },
    /// Drop this dump request (KeepOldest, limit reached).
    Stop,
}

/// Decides the [`LimitAction`] for the next dump, where `live` is the number of this run's dump files
/// currently on disk, under `max_dumps`/`policy`.
///
/// * No limit → always write, never evict.
/// * `KeepOldest` → write while fewer than `limit` files exist, then stop (the early history wins).
/// * `KeepNewest` → always write, evicting the oldest once the window is full, so the most recent
///   `limit` files remain.
fn limit_action(live: usize, max_dumps: Option<u64>, policy: LimitPolicy) -> LimitAction {
    let Some(limit) = max_dumps else {
        return LimitAction::Write { evict: false };
    };
    let full = live as u64 >= limit;
    match policy {
        LimitPolicy::KeepOldest => {
            if full {
                LimitAction::Stop
            } else {
                LimitAction::Write { evict: false }
            }
        }
        // Writing one more would exceed the window, so evict the oldest to stay at `limit`.
        LimitPolicy::KeepNewest => LimitAction::Write { evict: full },
    }
}

/// Whether a dump request should be dropped. `since_last` is the time elapsed since the previous
/// completed dump (`None` if there hasn't been one — the first dump is never throttled). A request
/// is throttled iff a previous dump happened less than `throttle` ago.
fn throttled(since_last: Option<Duration>, throttle: Duration) -> bool {
    matches!(since_last, Some(elapsed) if elapsed < throttle)
}

/// Builds the per-dump filename by inserting a UTC timestamp before the base path's extension:
/// `backbeat.bb` → `backbeat.20260623T142530123Z.bb`. Timestamps sort chronologically, make a
/// dump's age obvious at a glance, and disambiguate dumps across runs.
fn stamped_path(base: &Path, stamp: &str) -> PathBuf {
    let stem = base.file_stem().map(|s| s.to_string_lossy().into_owned());
    let ext = base.extension().map(|s| s.to_string_lossy().into_owned());
    let name = match (stem, ext) {
        (Some(stem), Some(ext)) => format!("{stem}.{stamp}.{ext}"),
        (Some(stem), None) => format!("{stem}.{stamp}"),
        // No file name component — fall back to a sensible default name.
        _ => format!("backbeat.{stamp}.bb"),
    };
    base.with_file_name(name)
}

/// Current wall-clock time as milliseconds since the Unix epoch (0 if the clock is before the epoch).
fn now_unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Formats `unix_millis` as a compact UTC stamp `YYYYMMDDThhmmssSSSZ` (e.g. `20260623T142530123Z`),
/// guaranteeing it differs from `prev`: if two dumps land in the same millisecond, a `-N` suffix is
/// appended so filenames never collide. Computed without a calendar dependency.
fn unique_stamp(unix_millis: u64, prev: &str) -> String {
    let base = format_utc_millis(unix_millis);
    if base != prev && !prev.starts_with(&format!("{base}-")) {
        return base;
    }
    // Same millisecond as the previous dump — bump a disambiguating suffix.
    let next = prev
        .rsplit_once('-')
        .and_then(|(stem, n)| (stem == base).then(|| n.parse::<u64>().ok()).flatten())
        .unwrap_or(0)
        + 1;
    format!("{base}-{next}")
}

/// Formats milliseconds-since-epoch as `YYYYMMDDThhmmssSSSZ` in UTC, using the civil-from-days
/// algorithm (Howard Hinnant's `days_from_civil` inverse) so we need no `chrono`/`time` dependency.
fn format_utc_millis(unix_millis: u64) -> String {
    let secs = unix_millis / 1000;
    let millis = unix_millis % 1000;
    let days = (secs / 86_400) as i64;
    let secs_of_day = secs % 86_400;
    let (hour, min, sec) = (
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60,
    );

    // Civil date from days since 1970-01-01 (proleptic Gregorian). See
    // https://howardhinnant.github.io/date_algorithms.html#civil_from_days
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let year = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = if month <= 2 { year + 1 } else { year };

    format!("{year:04}{month:02}{day:02}T{hour:02}{min:02}{sec:02}{millis:03}Z",)
}

/// Writes a serialized dump to `path`. The dump is already a complete `.bb` (envelope + sections),
/// so this is a single buffered write.
fn write_dump(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = std::io::BufWriter::new(std::fs::File::create(path)?);
    file.write_all(bytes)?;
    file.flush()
}

/// Installs a panic hook that triggers a final dump before delegating to the previous hook, so a
/// crash leaves the rings on disk.
fn install_panic_hook() {
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        trigger();
        // Give the background thread a moment to flush before the previous hook (which may abort).
        std::thread::sleep(Duration::from_millis(100));
        prev(info);
    }));
}

/// Test hook: the configured base dump path. Lets an end-to-end test find the files the background
/// thread writes.
#[doc(hidden)]
pub fn dump_path() -> PathBuf {
    global().dumper.path.clone()
}

/// Test hook: the embedded host label.
#[doc(hidden)]
pub fn host() -> &'static str {
    &global().host
}

#[cfg(unix)]
mod signal {
    //! Optional `kill -<sig>` dump trigger. We register a handler that does the one async-signal-safe
    //! thing we need — set the dumper's pending flag and wake its condvar via the libc primitives —
    //! so an operator can force a dump from outside the process.

    /// Parses `BACKBEAT_SIGNAL`: `usr1`/`usr2` (case-insensitive) or a raw signal number.
    pub fn parse(s: &str) -> Option<i32> {
        match s.trim().to_ascii_lowercase().as_str() {
            "usr1" | "sigusr1" => Some(libc::SIGUSR1),
            "usr2" | "sigusr2" => Some(libc::SIGUSR2),
            other => other.parse::<i32>().ok().filter(|n| *n > 0 && *n < 64),
        }
    }

    /// Installs the signal handler for `sig`.
    fn install(sig: i32) {
        // SAFETY: registering a handler with `sigaction` is sound; the handler itself
        // ([`handler`]) only calls async-signal-safe operations (see its docs).
        unsafe {
            let mut action: libc::sigaction = core::mem::zeroed();
            action.sa_sigaction = handler as usize;
            action.sa_flags = libc::SA_RESTART;
            libc::sigemptyset(&mut action.sa_mask);
            libc::sigaction(sig, &action, core::ptr::null_mut());
        }
    }

    /// The signal handler. It must use only async-signal-safe operations, so it cannot lock the
    /// dumper's mutex or allocate. Instead it sets an atomic flag and writes a byte to a self-pipe
    /// that a small forwarder thread reads, turning the signal into an ordinary [`super::trigger`]
    /// off the signal-handler context.
    extern "C" fn handler(_sig: i32) {
        // Async-signal-safe: a single relaxed store and a non-blocking 1-byte write to the pipe.
        PENDING.store(true, core::sync::atomic::Ordering::Relaxed);
        let fd = PIPE_WRITE.load(core::sync::atomic::Ordering::Relaxed);
        if fd >= 0 {
            let byte = [1u8];
            // SAFETY: `fd` is the live write end of our self-pipe; a 1-byte write is async-signal-safe.
            unsafe {
                libc::write(fd, byte.as_ptr() as *const libc::c_void, 1);
            }
        }
    }

    use core::sync::atomic::{AtomicBool, AtomicI32, Ordering};

    static PENDING: AtomicBool = AtomicBool::new(false);
    static PIPE_WRITE: AtomicI32 = AtomicI32::new(-1);

    /// Creates the self-pipe and the forwarder thread, then registers `handler`. The forwarder
    /// blocks reading the pipe and calls [`super::trigger`] for each signal, so the heavy lifting
    /// happens off the signal-handler context.
    fn setup_forwarder() {
        let mut fds = [0i32; 2];
        // SAFETY: `pipe` fills the two-element array with the read/write fds.
        if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
            return;
        }
        let (read_fd, write_fd) = (fds[0], fds[1]);
        PIPE_WRITE.store(write_fd, Ordering::Relaxed);
        let _ = std::thread::Builder::new()
            .name("backbeat::signal".into())
            .spawn(move || {
                let mut buf = [0u8; 64];
                loop {
                    // SAFETY: `read_fd` is the live read end; blocking until a signal writes a byte.
                    let n = unsafe {
                        libc::read(read_fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len())
                    };
                    if n <= 0 {
                        continue;
                    }
                    if PENDING.swap(false, Ordering::Relaxed) {
                        super::trigger();
                    }
                }
            });
    }

    /// Installs the forwarder + handler. Called once from `build`.
    pub fn install_full(sig: i32) {
        setup_forwarder();
        install(sig);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn throttle_drops_only_within_window() {
        let throttle = Duration::from_millis(1000);
        // First dump (no previous) is never throttled.
        assert!(!throttled(None, throttle));
        // A dump that just happened throttles the next request.
        assert!(throttled(Some(Duration::from_millis(10)), throttle));
        assert!(throttled(Some(Duration::from_millis(999)), throttle));
        // Past the window, the request proceeds.
        assert!(!throttled(Some(Duration::from_millis(1000)), throttle));
        assert!(!throttled(Some(Duration::from_millis(5000)), throttle));
        // A zero throttle never drops anything.
        assert!(!throttled(Some(Duration::ZERO), Duration::ZERO));
    }

    #[test]
    fn limit_action_unlimited_always_writes() {
        for live in [0, 1, 100, 10_000] {
            assert_eq!(
                limit_action(live, None, LimitPolicy::KeepOldest),
                LimitAction::Write { evict: false }
            );
            assert_eq!(
                limit_action(live, None, LimitPolicy::KeepNewest),
                LimitAction::Write { evict: false }
            );
        }
    }

    #[test]
    fn limit_action_keep_oldest_stops_when_full() {
        let limit = Some(3);
        // While fewer than `limit` files exist, write without evicting.
        assert_eq!(
            limit_action(0, limit, LimitPolicy::KeepOldest),
            LimitAction::Write { evict: false }
        );
        assert_eq!(
            limit_action(2, limit, LimitPolicy::KeepOldest),
            LimitAction::Write { evict: false }
        );
        // Once `limit` files exist, drop further requests — the early dumps are preserved.
        assert_eq!(
            limit_action(3, limit, LimitPolicy::KeepOldest),
            LimitAction::Stop
        );
        assert_eq!(
            limit_action(9, limit, LimitPolicy::KeepOldest),
            LimitAction::Stop
        );
    }

    #[test]
    fn limit_action_keep_newest_evicts_when_full() {
        let limit = Some(3);
        // While the window has room, write with nothing to evict.
        assert_eq!(
            limit_action(0, limit, LimitPolicy::KeepNewest),
            LimitAction::Write { evict: false }
        );
        assert_eq!(
            limit_action(2, limit, LimitPolicy::KeepNewest),
            LimitAction::Write { evict: false }
        );
        // With `limit` files already on disk, each new dump evicts the oldest — a 3-file ring.
        assert_eq!(
            limit_action(3, limit, LimitPolicy::KeepNewest),
            LimitAction::Write { evict: true }
        );
        assert_eq!(
            limit_action(4, limit, LimitPolicy::KeepNewest),
            LimitAction::Write { evict: true }
        );
    }

    #[test]
    fn limit_policy_parses() {
        assert_eq!(
            LimitPolicy::parse("keep-oldest"),
            Some(LimitPolicy::KeepOldest)
        );
        assert_eq!(LimitPolicy::parse("OLDEST"), Some(LimitPolicy::KeepOldest));
        assert_eq!(
            LimitPolicy::parse("keep-newest"),
            Some(LimitPolicy::KeepNewest)
        );
        assert_eq!(
            LimitPolicy::parse(" newest "),
            Some(LimitPolicy::KeepNewest)
        );
        assert_eq!(LimitPolicy::parse("sideways"), None);
    }

    #[test]
    fn stamped_path_inserts_timestamp_before_extension() {
        let base = PathBuf::from("/tmp/backbeat.bb");
        assert_eq!(
            stamped_path(&base, "20260623T142530123Z"),
            PathBuf::from("/tmp/backbeat.20260623T142530123Z.bb")
        );
        // No extension.
        let base = PathBuf::from("/tmp/trace");
        assert_eq!(
            stamped_path(&base, "20260623T142530123Z"),
            PathBuf::from("/tmp/trace.20260623T142530123Z")
        );
    }

    #[test]
    fn format_utc_millis_matches_known_instants() {
        // The Unix epoch.
        assert_eq!(format_utc_millis(0), "19700101T000000000Z");
        // 2026-06-23 14:25:30.123 UTC — 1_782_224_730_123 ms since epoch.
        assert_eq!(format_utc_millis(1_782_224_730_123), "20260623T142530123Z");
        // A leap-year date: 2024-02-29 00:00:00 UTC — 1_709_164_800_000 ms.
        assert_eq!(format_utc_millis(1_709_164_800_000), "20240229T000000000Z");
    }

    #[test]
    fn unique_stamp_disambiguates_same_millisecond() {
        let ms = 1_782_224_730_123;
        let base = format_utc_millis(ms); // 20260623T142530123Z
                                          // First use of a fresh millisecond: the bare stamp.
        let s0 = unique_stamp(ms, "");
        assert_eq!(s0, base);
        // Same millisecond again: a -1 suffix.
        let s1 = unique_stamp(ms, &s0);
        assert_eq!(s1, format!("{base}-1"));
        // And again: -2, parsed off the previous suffix.
        let s2 = unique_stamp(ms, &s1);
        assert_eq!(s2, format!("{base}-2"));
        // A new millisecond resets to the bare stamp.
        let later = format_utc_millis(ms + 1);
        assert_eq!(unique_stamp(ms + 1, &s2), later);
    }

    #[test]
    fn env_truthy_parses_common_forms() {
        for (val, want) in [
            ("1", true),
            ("true", true),
            ("TRUE", true),
            ("yes", true),
            ("on", true),
            ("0", false),
            ("false", false),
            ("", false),
            ("nope", false),
        ] {
            std::env::set_var("BACKBEAT_TEST_TRUTHY", val);
            assert_eq!(env_truthy("BACKBEAT_TEST_TRUTHY"), want, "value {val:?}");
        }
        std::env::remove_var("BACKBEAT_TEST_TRUTHY");
    }
}
