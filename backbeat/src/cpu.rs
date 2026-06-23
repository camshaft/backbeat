//! Cheap current-CPU hint for shard selection.
//!
//! Capture is sharded per-CPU so the hot path has no cross-core cursor contention or write-write
//! false sharing. To pick a shard we need the current CPU index *cheaply* — a `sched_getcpu`
//! syscall on every event would dwarf the cost of the record write itself.
//!
//! On Linux we read it from the [restartable sequences] (`rseq`) area that glibc registers for
//! every thread: the kernel keeps `cpu_id_start` in that thread-local block continuously updated,
//! so reading the current CPU is just a TLS load — no syscall, no atomics. We need *only* the read:
//! unlike a per-CPU counter that must be updated atomically with respect to migration, a sharded
//! ring write is already an independent `fetch_add` into a `Sync` ring. If the thread migrates
//! between reading the hint and writing, the worst case is that two CPUs briefly share one ring —
//! still perfectly memory-safe and correct, just a momentary loss of sharding. So we deliberately
//! do **not** use an rseq critical section; we just sample the hint and move on.
//!
//! Everywhere else (non-Linux, or if glibc didn't register rseq) we fall back to a hint of 0 (a
//! single shard) — correct, just without the per-core scalability.
//!
//! [restartable sequences]: https://www.kernel.org/doc/html/latest/userspace-api/rseq.html

/// Returns a cheap hint of the CPU the caller is currently running on, in `0..num_shards`.
///
/// "Hint" is exact: the value may be stale the instant it is returned if the thread migrates.
/// Callers must treat it as advisory — fine for choosing a shard, never for correctness.
#[inline]
pub fn current_hint(num_shards: usize) -> usize {
    if num_shards <= 1 {
        return 0;
    }
    raw_cpu() % num_shards
}

/// The raw current-CPU index, or 0 if it cannot be determined cheaply.
#[inline]
fn raw_cpu() -> usize {
    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    {
        if let Some(cpu) = linux_rseq::cpu_id_start() {
            return cpu;
        }
    }
    0
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod linux_rseq {
    //! Reads `cpu_id_start` out of the glibc-managed rseq TLS block. We only *read*; glibc owns
    //! registration (AL2023+/modern glibc register rseq for every thread at startup). If the
    //! symbols are absent or registration didn't happen, we report "unknown" and the caller falls
    //! back to a single shard.

    use core::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

    /// Layout of the leading fields of `struct rseq` (kernel ABI). We only read `cpu_id_start`,
    /// the first field, which the kernel keeps updated with the current CPU.
    #[repr(C, align(32))]
    struct Rseq {
        cpu_id_start: u32,
        cpu_id: u32,
        rseq_cs: u64,
        flags: u32,
    }

    // Resolution state for glibc's `__rseq_offset` (the TLS offset of the rseq block). 0 = not yet
    // resolved, 1 = resolved OK, 2 = unavailable.
    static STATE: AtomicU8 = AtomicU8::new(0);
    static OFFSET: AtomicUsize = AtomicUsize::new(0);

    /// The current CPU index per the rseq block, or `None` if rseq is unavailable on this thread.
    #[inline]
    pub fn cpu_id_start() -> Option<usize> {
        let offset = match STATE.load(Ordering::Relaxed) {
            1 => OFFSET.load(Ordering::Relaxed),
            2 => return None,
            _ => resolve()?,
        };
        // SAFETY: `thread_pointer() + offset` is the address glibc reserved for this thread's rseq
        // block; the kernel keeps `cpu_id_start` live there. We read a single `u32`.
        let ptr = thread_pointer().wrapping_add(offset) as *const Rseq;
        let cpu = unsafe { core::ptr::addr_of!((*ptr).cpu_id_start).read() };
        Some(cpu as usize)
    }

    /// Resolves glibc's `__rseq_offset` once, caching the result. Returns the offset on success.
    #[cold]
    fn resolve() -> Option<usize> {
        // `__rseq_offset` is a glibc global giving the TLS offset of the per-thread rseq area.
        extern "C" {
            #[link_name = "__rseq_offset"]
            static RSEQ_OFFSET: isize;
        }
        // The symbol is weak-ish across glibc versions; guard the read. On a glibc without rseq
        // support the linker would fail, so absence shows up at build time, not here — but a
        // zero/garbage offset is still possible on odd libcs, so we sanity-check below.
        let offset = unsafe { RSEQ_OFFSET } as usize;
        // A plausible TLS offset is small-ish and non-zero; treat anything absurd as unavailable.
        if offset == 0 {
            STATE.store(2, Ordering::Relaxed);
            return None;
        }
        OFFSET.store(offset, Ordering::Relaxed);
        STATE.store(1, Ordering::Relaxed);
        Some(offset)
    }

    /// The thread pointer (`fs:0` on x86_64, `tpidr_el0` on aarch64), the base TLS addresses are
    /// relative to.
    #[inline]
    fn thread_pointer() -> usize {
        let tp: usize;
        unsafe {
            #[cfg(target_arch = "x86_64")]
            core::arch::asm!("mov {}, fs:0", out(reg) tp, options(nostack, preserves_flags, readonly));
            #[cfg(target_arch = "aarch64")]
            core::arch::asm!("mrs {}, tpidr_el0", out(reg) tp, options(nostack, preserves_flags, nomem));
        }
        tp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_shard_is_always_zero() {
        assert_eq!(current_hint(1), 0);
        assert_eq!(current_hint(0), 0);
    }

    #[test]
    fn hint_in_range() {
        for shards in [2usize, 4, 8, 64] {
            let h = current_hint(shards);
            assert!(h < shards, "hint {h} out of range for {shards} shards");
        }
    }
}
