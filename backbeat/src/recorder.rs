// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! The recorder runtime: sharded capture rings and the dumper.
//!
//! A [`Recorder`] owns `N` per-CPU [`Ring`]s. The hot path, [`Recorder::record`], is:
//!
//! 1. cheap enable check ([`Recorder::set_enabled`]) — folds to a single relaxed load when off;
//! 2. read the current-CPU [hint](crate::cpu::current_hint) to pick a shard (no syscall);
//! 3. read the timestamp from the installed [`Clock`];
//! 4. `push_parts(&[ts, event_id, fields])` into that shard's ring — allocation-free.
//!
//! No global sequence counter and no cross-shard coordination: shards fill independently, and
//! global order is reconstructed at read time as `(ts_nanos, shard_id, local_seq)`. Count-skew
//! across shards is accepted as correct retention (a busy core keeps deeper history).
//!
//! [`Recorder::dump`] serializes a `.bb`: the schema registry (from [`crate::registry`], so it is
//! exactly the events compiled in), an optional intern table, a dump-level metadata section (the
//! process `instance_id`), and one shard section per ring.
//!
//! ## Spans
//!
//! A span is a pair of [`Event`] types — one `#[event(span = enter)]`, one `#[event(span = exit)]`,
//! both carrying a `#[event(span_id)]` field. [`Recorder::enter`] mints a cheap span id, records the
//! enter event, and returns a [`SpanGuard`] that records the exit event when it drops. Children read
//! the guard's [`id`](SpanGuard::id) and thread it into their own `#[event(parent_span_id)]` field,
//! so the converter can rebuild the tree — all without any global span registry.

use crate::{
    cpu,
    event::Event,
    record::{ID_OFFSET, TS_OFFSET},
    ring::Ring,
    wire::DumpWriter,
};
use alloc::{boxed::Box, vec, vec::Vec};
use core::{
    cell::Cell,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

/// splitmix64 finalizer — a fast, dependency-free bit mixer for deriving well-distributed ids from
/// a counter or timestamp.
fn mix64(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Generates a random `instance_id`. One `Recorder` is one process instance; the trace converter
/// keys spans by `(instance_id, span_id)`, so this only needs to be distinct across the processes
/// whose dumps are merged.
///
/// We borrow `std`'s OS-seeded entropy without pulling in an `rand` dependency: `RandomState` is
/// freshly seeded from the platform RNG on each construction (it is what makes `HashMap`
/// DoS-resistant), so hashing a fixed value through it yields a different `u64` per process. We mix
/// two independent draws so the result is well-distributed across the full 64 bits.
fn gen_instance_id() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let make = || {
        std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish()
    };
    mix64(make() ^ mix64(make()))
}

/// A unique-per-thread 16-bit seed for the low bits of a span id. Derived by mixing a global
/// per-thread counter, so the seeds are well-distributed and collision-free up to 2¹⁶ live threads
/// (beyond that, two threads can share a seed; widen if you expect more — see STATUS risks).
fn gen_thread_seed() -> u16 {
    static SRC: AtomicU64 = AtomicU64::new(0);
    let n = SRC.fetch_add(1, Ordering::Relaxed);
    (mix64(n.wrapping_add(0x9e37_79b9_7f4a_7c15)) & 0xFFFF) as u16
}

std::thread_local! {
    /// This thread's span-id seed (low 16 bits), assigned once on first span.
    static SPAN_SEED: u16 = gen_thread_seed();
    /// This thread's monotonic span counter (high 48 bits).
    static SPAN_COUNTER: Cell<u64> = const { Cell::new(0) };
}

/// A source of capture timestamps in nanoseconds.
///
/// The default ([`SystemClock`]) latches wall-clock at construction and adds a monotonic delta, so
/// timestamps are comparable across a run without re-reading the (slow) wall clock each event. A
/// simulation can install its own deterministic clock instead (e.g. driven by a virtual-time
/// scheduler) so recorded timestamps line up with simulated time.
pub trait Clock: Send + Sync + 'static {
    /// The current time in nanoseconds. Must be cheap; it is on the record hot path.
    fn now_nanos(&self) -> u64;
}

/// A shared clock is itself a clock — so a caller that needs to keep poking the clock (e.g. a
/// `ManualClock` in a test, or a clock shared across recorders) can pass `Arc<C>` and retain a
/// handle. The default `Recorder<SystemClock>` stores the clock by value, so it pays no `Arc`.
impl<C: Clock> Clock for alloc::sync::Arc<C> {
    #[inline]
    fn now_nanos(&self) -> u64 {
        (**self).now_nanos()
    }
}

/// Wall-clock based [`Clock`]: nanoseconds since the Unix epoch, read from a monotonic instant plus
/// a base latched at construction.
#[cfg(feature = "std")]
pub struct SystemClock {
    base_nanos: u64,
    start: std::time::Instant,
}

#[cfg(feature = "std")]
impl SystemClock {
    /// Latches the current wall-clock as the base for subsequent monotonic reads.
    pub fn new() -> Self {
        let base_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        Self {
            base_nanos,
            start: std::time::Instant::now(),
        }
    }
}

#[cfg(feature = "std")]
impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "std")]
impl Clock for SystemClock {
    #[inline]
    fn now_nanos(&self) -> u64 {
        // When built with `bach` and running inside a simulation, read the simulated clock so
        // recorded timestamps line up with simulated time. `try_now` returns `None` when no bach
        // scope is in scope (e.g. production), in which case we fall through to the wall clock.
        // Bach time starts at the Unix epoch (`Duration::ZERO`), so elapsed-since-start *is* the
        // nanos-since-epoch we store, deterministic and host-independent.
        #[cfg(feature = "bach")]
        if let Some(now) = bach::time::Instant::try_now() {
            return now.elapsed_since_start().as_nanos() as u64;
        }

        self.base_nanos
            .wrapping_add(self.start.elapsed().as_nanos() as u64)
    }
}

/// A deterministic [`Clock`] returning a manually advanced counter — for simulation and tests.
pub struct ManualClock(AtomicU64);

impl ManualClock {
    /// A clock starting at `start` nanoseconds.
    pub fn new(start: u64) -> Self {
        Self(AtomicU64::new(start))
    }
    /// Advances the clock by `delta` nanoseconds and returns the new value.
    pub fn advance(&self, delta: u64) -> u64 {
        self.0.fetch_add(delta, Ordering::Relaxed) + delta
    }
}

impl Clock for ManualClock {
    #[inline]
    fn now_nanos(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// A sharded, in-memory flight recorder.
///
/// Generic over the [`Clock`] so the timestamp read on the record hot path monomorphizes to a
/// direct, inlinable call — there is no `dyn` dispatch. Defaults to [`SystemClock`]; a simulation
/// installs a deterministic clock via [`Recorder::new`].
pub struct Recorder<C: Clock = SystemClock> {
    enabled: AtomicBool,
    shards: Box<[Ring]>,
    clock: C,
    instance_id: u64,
}

impl Recorder {
    /// Creates a recorder with `num_shards` rings of `bytes_per_shard` each (rounded up to a power
    /// of two), using the given clock. Starts **disabled**; call [`set_enabled(true)`].
    ///
    /// # Panics
    /// Panics if `num_shards` is zero.
    pub fn new(num_shards: usize, bytes_per_shard: usize) -> Self {
        assert!(num_shards > 0, "num_shards must be non-zero");
        let shards = (0..num_shards)
            .map(|_| Ring::new(bytes_per_shard))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let clock = SystemClock::new();
        Self {
            enabled: AtomicBool::new(false),
            shards,
            clock,
            instance_id: gen_instance_id(),
        }
    }
}

impl<C: Clock> Recorder<C> {
    pub fn with_clock<NewClock: Clock>(self, clock: NewClock) -> Recorder<NewClock> {
        Recorder {
            enabled: self.enabled,
            shards: self.shards,
            clock,
            instance_id: self.instance_id,
        }
    }

    /// Enables or disables capture at runtime. While disabled, [`record`](Self::record) returns
    /// immediately after one relaxed load, so an instrumented binary that never turns the recorder
    /// on pays essentially nothing. No-op when the `capture` feature is compiled out.
    pub fn set_enabled(&self, on: bool) {
        self.enabled.store(on, Ordering::Relaxed);
    }

    /// Whether capture is currently enabled.
    ///
    /// With the default `capture` feature this is one relaxed load of the enable flag. With
    /// `capture` compiled out it is `const false`, so the optimizer dead-code-eliminates the rest of
    /// every [`record`](Self::record)/[`enter`](Self::enter) call — a build that never wants capture
    /// pays nothing, not even the load. (`enabled` is still stored so the type is unchanged; it is
    /// simply never consulted.)
    #[inline(always)]
    pub fn is_enabled(&self) -> bool {
        #[cfg(feature = "capture")]
        {
            self.enabled.load(Ordering::Relaxed)
        }
        #[cfg(not(feature = "capture"))]
        {
            let _ = &self.enabled;
            false
        }
    }

    /// The number of shards.
    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    /// Records one event. No-op (after a single relaxed load) when disabled.
    ///
    /// Writes `[ts_nanos u64][event_id u64][fields]` into the shard for the current CPU, borrowing
    /// the event's bytes — nothing is allocated.
    #[inline]
    pub fn record<E: Event>(&self, event: &E) {
        if !self.is_enabled() {
            return;
        }
        let shard = cpu::current_hint(self.shards.len());
        let ts = self.clock.now_nanos().to_le_bytes();
        let id = E::ID.get().to_le_bytes();
        // Layout must match crate::record: [ts][id][fields].
        debug_assert_eq!(TS_OFFSET, 0);
        debug_assert_eq!(ID_OFFSET, ts.len());
        self.shards[shard].push_parts(&[&ts, &id, event.as_bytes()]);
    }

    /// The process `instance_id` written into the dump's metadata. Spans are keyed by
    /// `(instance_id, span_id)`, so merging dumps from different processes never confuses ids.
    pub fn instance_id(&self) -> u64 {
        self.instance_id
    }

    /// Mints a fresh span id for the current thread, cheaply and without coordination.
    ///
    /// The id is `(counter << 16) | seed`: a per-thread monotonic counter in the high 48 bits and a
    /// per-thread random seed in the low 16. Both are thread-local — a plain `Cell` increment and a
    /// once-per-thread seed — so this is just a couple of TLS reads, no atomics or syscalls. The
    /// seed keys to the *thread* (not the shard), because the id is generated once at enter and
    /// threaded by the app; a thread that migrates between enter and the guard's drop is fine.
    pub fn new_span_id(&self) -> u64 {
        let seed = SPAN_SEED.with(|s| *s) as u64;
        let counter = SPAN_COUNTER.with(|c| {
            let next = c.get().wrapping_add(1);
            c.set(next);
            next
        });
        (counter << 16) | seed
    }

    /// Opens a span: mints an id, records the enter event, and returns a [`SpanGuard`] that records
    /// the exit event when it drops.
    ///
    /// Both closures receive the freshly minted span id so they can stamp it into their
    /// `#[event(span_id)]` field (and an inner span threads an outer guard's [`id`](SpanGuard::id)
    /// into its own `#[event(parent_span_id)]`). `make_exit` runs at drop time, so it can capture
    /// end-of-span state through `Cell`/atomic handles (e.g. a byte count finalized during the
    /// span) — that is where the app chooses a minimal vs. full closing payload.
    ///
    /// ```ignore
    /// let bytes = std::cell::Cell::new(0u64);
    /// let span = rec.enter(
    ///     |id| WorkStart { span: id, parent: 0 },
    ///     |id| WorkEnd { span: id, bytes: bytes.get() },
    /// );
    /// bytes.set(4096);
    /// // span exit (with bytes = 4096) is recorded when `span` drops.
    /// ```
    pub fn enter<En, Ex, MkEx>(
        &self,
        make_enter: impl FnOnce(u64) -> En,
        make_exit: MkEx,
    ) -> SpanGuard<'_, C, Ex, MkEx>
    where
        En: Event,
        Ex: Event,
        MkEx: FnOnce(u64) -> Ex,
    {
        let id = self.new_span_id();
        if self.is_enabled() {
            self.record(&make_enter(id));
        }
        SpanGuard {
            recorder: self,
            id,
            make_exit: Some(make_exit),
        }
    }

    /// Serializes the current state to a `.bb` dump: the schema registry, the given intern-table
    /// entries, a metadata section (this recorder's `instance_id` plus `host`), and one shard
    /// section per ring. `schemas` is normally [`crate::registry::schemas()`]; it is a parameter so
    /// tests (and `no_std`-defined event sets) can supply an explicit registry. Pass `""` for `host`
    /// if no host label is wanted.
    pub fn dump<'a>(
        &self,
        schemas: impl IntoIterator<Item = &'a crate::schema::EventSchema>,
        intern: impl IntoIterator<Item = (u32, &'a [u8])>,
        host: &str,
    ) -> Vec<u8> {
        let mut w = DumpWriter::new();
        w.schema_registry(schemas);
        w.intern_table(self.instance_id, intern);
        let mut region = vec![0u8; self.shards.first().map_or(0, |r| r.capacity())];
        for (i, ring) in self.shards.iter().enumerate() {
            // Each ring may differ in capacity in principle; size the scratch to this one.
            if region.len() != ring.capacity() {
                region = vec![0u8; ring.capacity()];
            }
            let head = ring.snapshot_into(&mut region);
            w.shard(self.instance_id, i as u32, head as u64, &region);
        }
        w.meta(self.instance_id, host);
        w.finish()
    }
}

/// An RAII span: records the exit event when it drops.
///
/// Returned by [`Recorder::enter`]. Hold it for the duration of the span; read [`id`](Self::id) to
/// stamp child events' `#[event(parent_span_id)]`. The exit event is built by the closure given to
/// `enter` at the moment the guard drops, so it can observe end-of-span state.
#[must_use = "the span ends when the guard is dropped; bind it to a variable"]
pub struct SpanGuard<'a, C: Clock, Ex: Event, MkEx: FnOnce(u64) -> Ex> {
    recorder: &'a Recorder<C>,
    id: u64,
    /// `Some` until drop; taken and called to build the exit event. `Option` because `Drop` only
    /// gives `&mut self` and `FnOnce` must be moved out to call it.
    make_exit: Option<MkEx>,
}

impl<C: Clock, Ex: Event, MkEx: FnOnce(u64) -> Ex> SpanGuard<'_, C, Ex, MkEx> {
    /// This span's id — thread it into child events' `#[event(parent_span_id)]` field.
    pub fn id(&self) -> u64 {
        self.id
    }
}

impl<C: Clock, Ex: Event, MkEx: FnOnce(u64) -> Ex> Drop for SpanGuard<'_, C, Ex, MkEx> {
    fn drop(&mut self) {
        if let Some(make) = self.make_exit.take() {
            // Build + record the exit event with the span id. Gated like any record() so a disabled
            // recorder (or one disabled mid-span) writes nothing.
            self.recorder.record(&make(self.id));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::DumpReader;

    #[test]
    fn disabled_records_nothing() {
        let rec = Recorder::new(1, 4096).with_clock(ManualClock::new(0));
        // No event type needed: a disabled recorder returns before touching the event.
        assert!(!rec.is_enabled());
        let bytes = rec.dump(core::iter::empty(), core::iter::empty(), "");
        let r = DumpReader::new(bytes).unwrap();
        let shards = r.shards().unwrap();
        assert_eq!(shards.len(), 1);
        // Head is 0: nothing was written.
        assert_eq!(shards[0].head, 0);
    }

    #[test]
    fn span_id_is_unique_and_monotonic_per_thread() {
        let rec = Recorder::new(1, 4096).with_clock(ManualClock::new(0));
        let a = rec.new_span_id();
        let b = rec.new_span_id();
        // Same thread → same low-16 seed, strictly increasing high-48 counter.
        assert_eq!(a & 0xFFFF, b & 0xFFFF);
        assert!(b >> 16 > a >> 16);
    }

    #[test]
    fn dump_carries_instance_id() {
        let rec = Recorder::new(1, 4096).with_clock(ManualClock::new(0));
        let bytes = rec.dump(core::iter::empty(), core::iter::empty(), "host-x");
        let metas = DumpReader::new(bytes).unwrap().metas().unwrap();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].instance_id, rec.instance_id());
        assert_eq!(metas[0].host, "host-x");
    }

    #[test]
    fn capture_feature_gates_enable() {
        let rec = Recorder::new(1, 4096).with_clock(ManualClock::new(0));
        rec.set_enabled(true);
        // With `capture` on, the runtime toggle takes effect; with it off, `is_enabled` is a
        // hard `false` no matter what `set_enabled` was told — record() compiles to nothing.
        assert_eq!(rec.is_enabled(), cfg!(feature = "capture"));
    }
}
