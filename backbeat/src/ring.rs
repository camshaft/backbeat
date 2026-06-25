//! A lock-free, bump-allocating ring buffer — the per-shard capture store.
//!
//! Writers reserve space with a single `fetch_add` on a monotonic cursor and then memcpy their
//! record into the backing buffer (wrapping at the power-of-two boundary). There is no
//! coordination between writers beyond the atomic reservation, so the hot path is just an atomic
//! add plus a copy, with the contended cursor isolated on its own cache line so it cannot
//! false-share with neighbouring data.
//!
//! Records are self-delimiting: each is stored as its payload bytes followed by a little-endian
//! [`RecLen`] suffix holding the *payload* length. A reader recovers records newest-first by
//! reading the suffix at the head, stepping back over the payload, and repeating ([`walk`]).
//! Because writers race ahead and wrap around, the oldest records are continuously overwritten;
//! the walk is therefore *best effort* — it stops as soon as it would step into a region that has
//! been (or is being) overwritten.
//!
//! In backbeat a record payload is `[event_id: u64 LE][event fields…]`. The reader discriminates
//! a record by looking its `event_id` up in the dump's schema registry and validating the
//! declared `record_size` against the ring's length suffix — that mismatch check *is* the
//! torn-record guard, so individual records carry no per-record magic byte.

use alloc::{boxed::Box, vec::Vec};
use bytes::Bytes;
use core::{
    cell::UnsafeCell,
    sync::atomic::{AtomicUsize, Ordering},
};

/// The integer type stored in each record's trailing length suffix.
type RecLen = u16;

/// Number of bytes occupied by the trailing length suffix.
pub const LEN_SUFFIX: usize = core::mem::size_of::<RecLen>();

/// The largest payload a single record may hold (bounded by [`RecLen`]).
pub const MAX_RECORD: usize = RecLen::MAX as usize;

/// Cache-line padding target. 128 bytes (not 64) dodges x86 adjacent-line prefetch, which can pull
/// a neighbouring line into the same coherency unit and reintroduce false sharing.
#[repr(align(128))]
struct CacheAligned(AtomicUsize);

/// A fixed-capacity, lock-free bump-allocating ring buffer.
pub struct Ring {
    /// Backing storage of length `capacity()` (a power of two). `UnsafeCell` because writers
    /// mutate disjoint (best-effort) byte ranges without holding a lock.
    buf: Box<[UnsafeCell<u8>]>,
    /// `capacity() - 1`; masks an absolute offset down to a physical index.
    mask: usize,
    /// Monotonically increasing count of bytes ever reserved, isolated on its own cache line so
    /// the one contended word never shares a line with `buf`/`mask`. Never masked — the physical
    /// index for absolute offset `abs` is `abs & mask`.
    offset: CacheAligned,
}

// SAFETY: writes go to disjoint byte ranges reserved by the atomic `offset`, and the `UnsafeCell`
// bytes are plain `u8` with no destructor or interior pointers. Readers observe best-effort
// snapshots and tolerate torn data (see module docs), so sharing across threads is sound.
unsafe impl Send for Ring {}
unsafe impl Sync for Ring {}

impl Ring {
    /// Creates a ring with capacity `requested` rounded up to the next power of two.
    ///
    /// # Panics
    /// Panics if `requested` is zero.
    pub fn new(requested: usize) -> Self {
        assert!(requested > 0, "Ring capacity must be non-zero");
        let capacity = requested.next_power_of_two();
        let mut buf = Vec::with_capacity(capacity);
        buf.resize_with(capacity, || UnsafeCell::new(0u8));
        Self {
            buf: buf.into_boxed_slice(),
            mask: capacity - 1,
            offset: CacheAligned(AtomicUsize::new(0)),
        }
    }

    /// The physical capacity in bytes (a power of two).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }

    /// The current write head: the total number of bytes ever reserved.
    #[inline]
    pub fn head(&self) -> usize {
        self.offset.0.load(Ordering::Acquire)
    }

    /// Appends `payload` as a single record and returns its starting absolute offset.
    ///
    /// The record is `payload` followed by a little-endian [`RecLen`] suffix. This reserves space
    /// with one `fetch_add` and then copies the bytes in; it never blocks.
    ///
    /// # Panics
    /// Panics if `payload.len() > MAX_RECORD`, or if the whole record does not fit in
    /// [`capacity`](Self::capacity) — a record larger than the ring could never be recovered and
    /// would wrap more than once, corrupting unrelated records.
    pub fn push(&self, payload: &[u8]) -> usize {
        assert!(
            payload.len() <= MAX_RECORD,
            "record payload exceeds MAX_RECORD"
        );
        let total = payload.len() + LEN_SUFFIX;
        assert!(
            total <= self.capacity(),
            "record size exceeds ring capacity"
        );

        // Reserve our slice of the ring. Relaxed is sufficient: writers never read each other's
        // bytes, and a reader establishes ordering via its own `head()` (Acquire) load plus the
        // synchronization that delivered the dump request.
        let start = self.offset.0.fetch_add(total, Ordering::Relaxed);
        self.write_wrapping(start, payload);
        self.write_wrapping(
            start + payload.len(),
            &(payload.len() as RecLen).to_le_bytes(),
        );
        start
    }

    /// Records an [`Event`] as a single `[event_id: u64 LE][fields…]` record, allocation-free.
    ///
    /// This is the hot-path entry point. The field bytes are *borrowed* straight from `event` via
    /// [`IntoBytes::as_bytes`] (no serialization), and the id, fields, and length suffix are copied
    /// directly into the slice reserved by one `fetch_add` — nothing touches the heap. Returns the
    /// record's starting absolute offset.
    ///
    /// # Panics
    /// Panics under the same conditions as [`push`](Self::push): a record larger than
    /// [`MAX_RECORD`] or than the ring [`capacity`](Self::capacity).
    pub fn push_event<E: crate::event::Event>(&self, event: &E) -> usize {
        let id = E::ID.get().to_le_bytes();
        self.push_parts(&[&id, event.as_bytes()])
    }

    /// Appends one record formed by concatenating `parts`, followed by the length suffix —
    /// allocation-free. The whole record is reserved with a single `fetch_add` and the parts are
    /// memcpy'd into place in order, so the caller can assemble a record from borrowed pieces
    /// (e.g. `[timestamp][event_id][fields]`) without a temporary buffer. Returns the record's
    /// starting absolute offset.
    ///
    /// # Panics
    /// Panics if the parts total more than [`MAX_RECORD`], or if the whole record (parts + suffix)
    /// does not fit in [`capacity`](Self::capacity).
    pub fn push_parts(&self, parts: &[&[u8]]) -> usize {
        let payload_len: usize = parts.iter().map(|p| p.len()).sum();
        assert!(
            payload_len <= MAX_RECORD,
            "record payload exceeds MAX_RECORD"
        );
        let total = payload_len + LEN_SUFFIX;
        assert!(
            total <= self.capacity(),
            "record size exceeds ring capacity"
        );

        let start = self.offset.0.fetch_add(total, Ordering::Relaxed);
        let mut at = start;
        for part in parts {
            self.write_wrapping(at, part);
            at += part.len();
        }
        self.write_wrapping(at, &(payload_len as RecLen).to_le_bytes());
        start
    }

    /// Copies `src` into the ring starting at absolute offset `abs`, wrapping once at the boundary.
    #[inline]
    fn write_wrapping(&self, abs: usize, src: &[u8]) {
        let begin = abs & self.mask;
        let first = (self.capacity() - begin).min(src.len());
        // SAFETY: the range was reserved by our `fetch_add`; bytes are plain `u8`.
        unsafe {
            let base = self.buf.as_ptr() as *mut u8;
            core::ptr::copy_nonoverlapping(src.as_ptr(), base.add(begin), first);
            if first < src.len() {
                core::ptr::copy_nonoverlapping(src.as_ptr().add(first), base, src.len() - first);
            }
        }
    }

    /// Copies the entire ring region into `out` (which must be `capacity()` long) and returns the
    /// current head. The snapshot is best-effort: writers may be racing, so torn records near the
    /// head are expected and tolerated by [`walk`].
    pub fn snapshot_into(&self, out: &mut [u8]) -> usize {
        assert_eq!(
            out.len(),
            self.capacity(),
            "snapshot buffer must be capacity()"
        );
        let head = self.head();
        // SAFETY: reading plain `u8`s; torn data is tolerated by the walk.
        unsafe {
            core::ptr::copy_nonoverlapping(
                self.buf.as_ptr() as *const u8,
                out.as_mut_ptr(),
                self.capacity(),
            );
        }
        head
    }
}

/// Walks a ring snapshot newest-first, yielding each record's payload (without the length suffix)
/// to a *validating* callback, resynchronizing byte-by-byte when a candidate fails validation.
///
/// `region` must be exactly `capacity` bytes and `capacity` a power of two; `head` is the value
/// returned by [`Ring::snapshot_into`].
///
/// # Why a validating callback
///
/// A snapshot is taken while writers race, so the bytes just behind `head` may be a record that is
/// still mid-copy: its length suffix can be garbage, or its payload only half-written. A naive walk
/// that trusted the first suffix it read would compute a wrong record boundary and **lose every
/// older record behind the torn one** — one in-flight write would invalidate the whole trace.
///
/// Instead, at each candidate end-position the walk decodes the length suffix, reads the would-be
/// payload, and hands it to `f` to validate. The backbeat validator is the schema check already
/// settled on (look the record's `event_id` up in the dump's registry and confirm the declared
/// `record_size` matches the suffix length): a random or half-written suffix essentially never
/// names a registered 64-bit event id with a matching size, so it is rejected. On rejection the
/// walk steps back a single byte and tries again, resynchronizing onto the end of the last
/// fully-written record. This needs **no per-record magic byte** — validation alone is the
/// torn-record guard, so the hot `push` path stays a bare memcpy. The cost is read-time only: at
/// worst `O(capacity)` validator calls when nothing parses.
///
/// `f` receives each record's payload as a [`Bytes`]; it returns `true` to accept (the walk
/// continues from the byte before the record) or `false` to reject (the walk steps back one byte
/// and retries). The walk stops once it can no longer form a candidate within the still-valid
/// region.
///
/// The payload is produced **zero-copy** for the common case: a record that lies contiguously in
/// `region` is just a refcounted [`Bytes::slice`] of it, so accepting millions of records costs
/// only refcount bumps, no allocation. The single exception is the (at most one) record that wraps
/// the physical ring boundary — its bytes aren't contiguous, so it is reconstructed into a fresh
/// `Bytes`. Callers may cheaply clone the payload to retain it.
pub fn walk(region: &Bytes, head: usize, capacity: usize, mut f: impl FnMut(Bytes) -> bool) {
    walk_indexed(region, head, capacity, |payload, _| f(payload));
}

/// Like [`walk`], but the callback also learns *where* each record's payload lives within `region`:
/// `Some(physical_offset)` when the record is a contiguous slice of `region` beginning at that byte
/// offset (the common case), or `None` for the at-most-one record per ring that wraps the physical
/// boundary and was therefore reconstructed into a fresh buffer.
///
/// This lets a reader keep a compact `(offset, len)` locator per record instead of retaining a
/// 32-byte [`Bytes`] handle each — the difference between a few hundred MiB and several GiB of
/// per-record bookkeeping on a dump with hundreds of millions of records. The yielded `Bytes` is
/// still the payload (so the callback can validate it); the offset is purely additional.
pub fn walk_indexed(
    region: &Bytes,
    head: usize,
    capacity: usize,
    mut f: impl FnMut(Bytes, Option<usize>) -> bool,
) {
    if region.len() != capacity || !capacity.is_power_of_two() {
        return;
    }
    let mask = capacity - 1;
    let valid_low = head.saturating_sub(capacity);

    let read_wrapping = |abs: usize, len: usize, out: &mut [u8]| {
        let begin = abs & mask;
        let first = (capacity - begin).min(len);
        out[..first].copy_from_slice(&region[begin..begin + first]);
        if first < len {
            out[first..len].copy_from_slice(&region[..len - first]);
        }
    };

    let mut abs = head;
    while abs >= valid_low + LEN_SUFFIX {
        let suffix_start = abs - LEN_SUFFIX;
        let mut len_bytes = [0u8; LEN_SUFFIX];
        read_wrapping(suffix_start, LEN_SUFFIX, &mut len_bytes);
        let payload_len = u16::from_le_bytes(len_bytes) as usize;
        let rec_total = payload_len + LEN_SUFFIX;

        // Can a record of this length even end at `abs` within the valid region? If not, this is
        // not a record boundary — resync one byte back rather than giving up.
        if rec_total > abs || abs - rec_total < valid_low {
            abs -= 1;
            continue;
        }

        let payload_start = abs - rec_total;
        let begin = payload_start & mask;

        // Contiguous record → zero-copy slice of `region`, and we can report its physical offset.
        // Only the one record that wraps the physical boundary needs reconstruction into a fresh
        // `Bytes`, and has no single contiguous offset (so the callback gets `None`).
        let (payload, phys) = if begin + payload_len <= capacity {
            (region.slice(begin..begin + payload_len), Some(begin))
        } else {
            let mut buf = alloc::vec![0u8; payload_len];
            read_wrapping(payload_start, payload_len, &mut buf);
            (Bytes::from(buf), None)
        };

        if f(payload, phys) {
            // Accepted: the record occupies `[payload_start, abs)`; continue behind it.
            abs = payload_start;
        } else {
            // Rejected (torn / spurious): step back a byte and try the next end-position.
            abs -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_newest_first() {
        let ring = Ring::new(4096);
        for i in 0u32..5 {
            ring.push(&i.to_le_bytes());
        }
        let mut region = alloc::vec![0u8; ring.capacity()];
        let head = ring.snapshot_into(&mut region);
        let region = Bytes::from(region);
        let mut got = Vec::new();
        walk(&region, head, ring.capacity(), |p| {
            // Validator: a real record here is exactly 4 bytes. Accept it.
            if p.len() != 4 {
                return false;
            }
            got.push(u32::from_le_bytes(p[..].try_into().unwrap()));
            true
        });
        // newest-first
        assert_eq!(got, [4, 3, 2, 1, 0]);
    }

    #[test]
    fn capacity_rounds_up_to_pow2() {
        assert_eq!(Ring::new(1000).capacity(), 1024);
        assert_eq!(Ring::new(4096).capacity(), 4096);
    }

    #[test]
    fn wrapping_keeps_newest_records() {
        // Tiny ring so older records get overwritten; the walk must still recover the newest run.
        let ring = Ring::new(64);
        for i in 0u64..100 {
            ring.push(&i.to_le_bytes());
        }
        let mut region = alloc::vec![0u8; ring.capacity()];
        let head = ring.snapshot_into(&mut region);
        let region = Bytes::from(region);
        let mut got = Vec::new();
        walk(&region, head, ring.capacity(), |p| {
            if p.len() != 8 {
                return false;
            }
            got.push(u64::from_le_bytes(p[..].try_into().unwrap()));
            true
        });
        assert!(!got.is_empty());
        // Newest first, contiguous, and the very newest is 99.
        assert_eq!(got[0], 99);
        for w in got.windows(2) {
            assert_eq!(w[0], w[1] + 1);
        }
    }

    #[test]
    fn resyncs_past_a_torn_head_record() {
        // Records carry a 4-byte tag so the validator can recognize a real record and reject
        // garbage. We corrupt the bytes just behind the head (an in-flight write) and assert the
        // walk still recovers every older record rather than bailing at the first anomaly.
        let ring = Ring::new(4096);
        for i in 0u32..6 {
            // payload = [0xED, 0xED, 0xED, 0xED][i as le u32]
            let mut p = [0xEDu8; 8];
            p[4..].copy_from_slice(&i.to_le_bytes());
            ring.push(&p);
        }
        let mut region = alloc::vec![0u8; ring.capacity()];
        let head = ring.snapshot_into(&mut region);

        // Simulate a torn in-flight record: scribble over exactly the head record (8-byte payload
        // + 2-byte suffix = 10 bytes) so only the newest record is corrupted.
        let begin = head.saturating_sub(8 + LEN_SUFFIX);
        for b in &mut region[begin..head] {
            *b = 0x77;
        }
        let region = Bytes::from(region);

        let mut got = Vec::new();
        walk(&region, head, ring.capacity(), |p| {
            // Validator: a real record is 8 bytes tagged with four 0xED bytes.
            if p.len() != 8 || p[..4] != [0xED; 4] {
                return false;
            }
            got.push(u32::from_le_bytes(p[4..].try_into().unwrap()));
            true
        });

        // The newest (record 5) was torn, but every record behind it is recovered, newest-first.
        assert_eq!(got, [4, 3, 2, 1, 0]);
    }

    #[test]
    fn recovers_a_record_that_wraps_the_boundary() {
        // Push records until at least one straddles the physical ring boundary, then assert the
        // walk reconstructs it correctly (the one allocating path) alongside the zero-copy ones.
        let ring = Ring::new(64);
        for i in 0u64..20 {
            ring.push(&i.to_le_bytes());
        }
        let mut region = alloc::vec![0u8; ring.capacity()];
        let head = ring.snapshot_into(&mut region);
        let region = Bytes::from(region);

        let mut got = Vec::new();
        walk(&region, head, ring.capacity(), |p| {
            if p.len() != 8 {
                return false;
            }
            got.push(u64::from_le_bytes(p[..].try_into().unwrap()));
            true
        });
        // Newest is 19, contiguous and decreasing — proving the wrapped record decoded right.
        assert_eq!(got[0], 19);
        for w in got.windows(2) {
            assert_eq!(w[0], w[1] + 1);
        }
    }
}
