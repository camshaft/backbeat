// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! Shared dump-loading model behind both output formats.
//!
//! A dump is decoded into one [`Loaded`] **per instance** it contains — its `instance_id`, the
//! shared schema registry, that instance's intern table, and the records recovered from its shards.
//! Records are not flattened into one big per-instance list: each shard keeps its own [`ShardRecs`]
//! (its raw ring region plus a compact, sorted list of [`Locator`]s into it). A single-process dump
//! yields one `Loaded`; a merged dump yields one per process it bundles.
//!
//! ## Memory model — why locators, not records
//!
//! A multi-gigabyte dump holds hundreds of millions of records. Retaining a 32-byte [`Bytes`] handle
//! (plus a timestamp, ids, and a seq) per record is several GiB of bookkeeping *on top of* the
//! mmap'd dump itself. Instead each record is a 16-byte [`Locator`] — `(ts_nanos, offset,
//! schema_idx)` — and its field bytes are recovered on demand by slicing the shard's `region`
//! (which is a zero-copy slice of the demand-paged mmap) at `offset`, for the length the schema's
//! `record_size` gives. So per-record overhead drops 4× and the field bytes never leave the mapping.
//!
//! ## Streaming merge — why no global record vector
//!
//! The ring lays records down in time order, so each `(dump, instance, shard)` is already a sorted
//! stream once its locators are sorted by the merge key. [`merge_records`] does a lazy k-way merge
//! across all those streams, yielding records in the global order `(ts_nanos, instance_id, shard_id,
//! event_id, fields)` with duplicates (overlapping dumps re-capture shared ring content) dropped on
//! the fly. The Parquet ([`crate::convert`]) and trace ([`crate::trace`]) writers consume this
//! iterator and flush bounded chunks, so converting an N-record dump never holds N records in memory
//! at once — only the merge's per-stream cursors (one per shard) plus the current output chunk.

use anyhow::{Context, Result};
use backbeat::{
    record::{RecordView, FIELDS_OFFSET},
    ring::walk_indexed,
    wire::{DumpReader, OwnedSchema},
};
use bytes::Bytes;
use rayon::prelude::*;
use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

/// Sentinel [`Locator::offset`] for the at-most-one record per shard that wraps the physical ring
/// boundary: it has no single contiguous offset in `region`, so its reconstructed payload is held
/// out-of-line in [`ShardRecs::wrapped`] instead.
const WRAPPED: u32 = u32::MAX;

/// A compact pointer to one record within a [`ShardRecs::region`]. Sixteen bytes: the timestamp
/// (needed for the merge order and the output), the physical byte offset of the record *payload*
/// within the region (`[ts][event_id][fields]` lies contiguously there — see [`backbeat::record`]),
/// and the index of its event in the owning [`Loaded::schemas`] (which yields the `record_size` that
/// bounds the field bytes, so no length need be stored).
#[derive(Clone, Copy)]
pub struct Locator {
    pub ts_nanos: u64,
    /// Physical offset of the record payload in `region`, or [`WRAPPED`] for the boundary record.
    offset: u32,
    /// Index into the owning [`Loaded::schemas`].
    schema_idx: u32,
}

/// One shard's recovered records: its id, the raw ring region (a zero-copy slice of the dump mmap),
/// the reconstructed payload of the single boundary-wrapping record (if any), and a list of
/// [`Locator`]s **sorted by the merge key** `(ts_nanos, event_id, fields)`. Because `instance_id`
/// and `shard_id` are constant within a shard, that per-shard order is exactly the projection of the
/// global merge key onto this stream, so a k-way merge of all shards is globally ordered.
pub struct ShardRecs {
    pub shard_id: u32,
    region: Bytes,
    /// Reconstructed payload of the one record that wraps the ring boundary, if present.
    wrapped: Option<Bytes>,
    locs: Vec<Locator>,
}

impl ShardRecs {
    /// The field bytes of the record at `loc`, recovered zero-copy from `region` (or from the
    /// out-of-line `wrapped` payload for the boundary record). `schemas` supplies the `record_size`
    /// that bounds the fields. Cloning the returned [`Bytes`] is a refcount bump, not a copy.
    fn fields(&self, loc: &Locator, schemas: &[OwnedSchema]) -> Bytes {
        let rec_size = schemas[loc.schema_idx as usize].record_size as usize;
        if loc.offset == WRAPPED {
            // The boundary record's payload was reconstructed at load time; slice the fields out.
            let payload = self
                .wrapped
                .as_ref()
                .expect("WRAPPED locator without a reconstructed payload");
            payload.slice(FIELDS_OFFSET..FIELDS_OFFSET + rec_size)
        } else {
            let start = loc.offset as usize + FIELDS_OFFSET;
            self.region.slice(start..start + rec_size)
        }
    }
}

/// A single decoded instance: its identity, the (shared) registry, its intern table, and its
/// recovered records grouped per shard. A dump file decodes to one of these per instance it holds.
pub struct Loaded {
    /// Where it was read from (for error messages).
    pub path: PathBuf,
    /// The producing process's id; `(instance_id, span_id)` keys spans across merged dumps.
    pub instance_id: u64,
    /// Host label from this instance's metadata (empty if unset).
    pub host: String,
    /// The dump's schema registry, sorted by `qualified_name` for deterministic output. Shared by
    /// every instance in the same file (the registry is unified, not per-instance).
    pub schemas: Vec<OwnedSchema>,
    /// The dump's registered query-DDL view sets (verbatim text), in file order. Dump-level like the
    /// registry — every instance decoded from one file carries the same list (convert dedups by
    /// content across files).
    pub views: Vec<String>,
    /// This instance's interned `id → string` for `Interned` fields.
    pub intern: HashMap<u32, String>,
    /// This instance's shards, each with its own sorted locator list (ascending `shard_id`).
    pub shards: Vec<ShardRecs>,
}

impl Loaded {
    /// The total number of recovered records across all of this instance's shards.
    pub fn record_count(&self) -> usize {
        self.shards.iter().map(|s| s.locs.len()).sum()
    }

    /// Iterates this instance's records in per-shard order (shards ascending, then each shard's
    /// merge-key order). Cheap — yields a [`RecRef`] whose field bytes are a zero-copy slice of the
    /// mmap. Useful for code that just needs every record of one instance without a cross-instance
    /// merge.
    pub fn records(&self) -> impl Iterator<Item = RecRef> + '_ {
        self.shards.iter().flat_map(move |sh| {
            sh.locs.iter().map(move |loc| RecRef {
                ts_nanos: loc.ts_nanos,
                shard_id: sh.shard_id,
                schema_idx: loc.schema_idx as usize,
                fields: sh.fields(loc, &self.schemas),
            })
        })
    }
}

/// A decoded record from a [`Loaded`]: its timestamp, owning shard, event (via `schema_idx`), and
/// field bytes (a zero-copy refcounted slice of the dump mmap).
pub struct RecRef {
    pub ts_nanos: u64,
    pub shard_id: u32,
    pub schema_idx: usize,
    pub fields: Bytes,
}

/// One record yielded by the streaming [`merge_records`]: the owning [`Loaded`] (for its schemas,
/// intern table, and `instance_id`) plus the record's timestamp, shard, event, and field bytes.
pub struct MergedRec<'a> {
    pub loaded: &'a Loaded,
    pub ts_nanos: u64,
    pub shard_id: u32,
    pub schema_idx: usize,
    /// Zero-copy field bytes sliced from the dump mmap (length equals the schema's `record_size`).
    pub fields: Bytes,
}

/// The full ordering/identity key of a record: `(ts_nanos, instance_id, shard_id, offset)`. These
/// four values *are* a record's true identity — the ring is a bump allocator over a monotonic
/// cursor (see [`backbeat::ring`]), so a record lands at one fixed offset in its `(instance_id,
/// shard_id)` ring for the recorder's whole life. Two overlapping dumps that re-capture the same
/// logged event therefore reproduce the identical offset, making their keys equal; [`merge_records`]
/// drops the duplicate with an equality check against the previously emitted key — no hash set.
/// Sorting by this key also yields the global converter order.
///
/// `event_id` and `fields` are deliberately absent — comparing the variable-length field bytes was
/// the dominant cost of both the per-shard sort and the merge, and the offset already pins identity:
/// within one dump's snapshot a physical offset belongs to at most one walked record, and across
/// dumps a shared physical slot implies a full ring-cycle (`capacity` bytes) gap in absolute offset,
/// hence a different `ts_nanos`. So `(ts_nanos, offset)` separates distinct records within a shard
/// without ever touching their bytes. `local_seq` is likewise absent: it is assigned per-walk, so
/// the same event gets different seqs in two dumps and could never match.
///
/// (The at-most-one boundary-wrapping record per shard carries the [`WRAPPED`] sentinel offset; it
/// is consistent across dumps — whether a record straddles the physical boundary depends only on its
/// fixed absolute range mod `capacity` — so it dedups on `(ts_nanos, WRAPPED)` like any other.)
#[derive(Clone, Copy, PartialEq, Eq)]
struct Key {
    ts_nanos: u64,
    instance_id: u64,
    shard_id: u32,
    offset: u32,
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ts_nanos
            .cmp(&other.ts_nanos)
            .then(self.instance_id.cmp(&other.instance_id))
            .then(self.shard_id.cmp(&other.shard_id))
            .then(self.offset.cmp(&other.offset))
    }
}
impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Loads and decodes a dump from `bytes`, returning one [`Loaded`] per instance it contains.
/// `path` is carried for diagnostics. A single-process dump yields a one-element vec; a merged dump
/// yields one entry per process it bundles, each with its own intern table, host, and shards.
pub fn load(path: &Path, bytes: Bytes) -> Result<Vec<Loaded>> {
    let reader = DumpReader::new(bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut schemas = reader.schemas().map_err(|e| anyhow::anyhow!("{e}"))?;
    let intern_tables = reader.intern_tables().map_err(|e| anyhow::anyhow!("{e}"))?;
    let metas = reader.metas().map_err(|e| anyhow::anyhow!("{e}"))?;
    let views = reader.views().map_err(|e| anyhow::anyhow!("{e}"))?;
    let shards = reader.shards().map_err(|e| anyhow::anyhow!("{e}"))?;

    // Deterministic registry order regardless of how the producer's inventory was linked. Shared by
    // every instance in this file.
    schemas.sort_by(|a, b| a.qualified_name.cmp(&b.qualified_name));
    let by_id: HashMap<u64, usize> = schemas
        .iter()
        .enumerate()
        .map(|(i, s)| (s.id.get(), i))
        .collect();

    // Per-instance intern table: id → string, keyed by the owning instance.
    let mut intern_by_instance: HashMap<u64, HashMap<u32, String>> = HashMap::new();
    for table in intern_tables {
        let map = intern_by_instance.entry(table.instance_id).or_default();
        for (id, bytes) in table.entries {
            map.insert(id, String::from_utf8_lossy(&bytes).into_owned());
        }
    }

    // Walk every shard, attributing each record to a schema and recording a compact locator into the
    // shard's region. Shards are independent (no shared state in the walk), so we parse — and sort —
    // them in parallel with rayon. The closure is walk's validator (see backbeat::ring::walk_indexed):
    // accept a candidate only if its event_id is registered and its declared record_size matches the
    // field bytes, so walk resynchronizes past torn data.
    let walked: Vec<(u64, ShardRecs)> = shards
        .par_iter()
        .map(|shard| {
            // The locator offset is a u32 physical position within the region; a single shard larger
            // than 4 GiB can't be addressed that way. Ring capacities are far smaller in practice
            // (default 16 MiB), so this is a clear error rather than silent truncation.
            if shard.capacity > u32::MAX as u64 {
                return Err(anyhow::anyhow!(
                    "shard region of {} bytes exceeds the 4 GiB this reader supports",
                    shard.capacity
                ));
            }
            let mut locs: Vec<Locator> = Vec::new();
            let mut wrapped: Option<Bytes> = None;
            walk_indexed(
                &shard.region,
                shard.head as usize,
                shard.capacity as usize,
                |payload, phys| {
                    let Some(rec) = RecordView::parse(&payload[..]) else {
                        return false;
                    };
                    match by_id.get(&rec.event_id.get()) {
                        Some(&idx) if rec.fields.len() == schemas[idx].record_size as usize => {
                            let offset = match phys {
                                Some(p) => p as u32,
                                None => {
                                    // The one boundary-wrapping record: keep its reconstructed
                                    // payload out-of-line (there is at most one per shard).
                                    wrapped = Some(payload.clone());
                                    WRAPPED
                                }
                            };
                            locs.push(Locator {
                                ts_nanos: rec.ts_nanos,
                                offset,
                                schema_idx: idx as u32,
                            });
                            true
                        }
                        _ => false,
                    }
                },
            );

            // Sort this shard's locators by the merge key's per-stream projection
            // `(ts_nanos, offset)` (`instance_id`/`shard_id` are constant within a shard), so a
            // k-way merge across shards is globally ordered and duplicates land adjacent for dedup.
            // This is a pure integer compare — no field-byte slicing — because the offset already
            // identifies the record (see [`Key`]).
            locs.sort_by(|a, b| a.ts_nanos.cmp(&b.ts_nanos).then(a.offset.cmp(&b.offset)));

            let sh = ShardRecs {
                shard_id: shard.shard_id,
                region: shard.region.clone(),
                wrapped,
                locs,
            };
            Ok((shard.instance_id, sh))
        })
        .collect::<Result<_>>()?;

    // Group shards by instance. The set of instances is the union of those that have metadata, an
    // intern table, or any shards.
    let mut shards_by_instance: HashMap<u64, Vec<ShardRecs>> = HashMap::new();
    for (instance_id, sh) in walked {
        shards_by_instance.entry(instance_id).or_default().push(sh);
    }

    let mut instance_ids: Vec<u64> = Vec::new();
    let mut seen = HashSet::new();
    for m in &metas {
        if seen.insert(m.instance_id) {
            instance_ids.push(m.instance_id);
        }
    }
    for &id in shards_by_instance.keys() {
        if seen.insert(id) {
            instance_ids.push(id);
        }
    }
    // A dump with neither metadata nor shards still loads as one empty instance.
    if instance_ids.is_empty() {
        instance_ids.push(0);
    }
    // Deterministic instance order regardless of HashMap iteration.
    instance_ids.sort_unstable();

    let host_of: HashMap<u64, String> =
        metas.into_iter().map(|m| (m.instance_id, m.host)).collect();

    let loaded = instance_ids
        .into_iter()
        .map(|instance_id| {
            let mut shards = shards_by_instance.remove(&instance_id).unwrap_or_default();
            // Deterministic shard order within the instance.
            shards.sort_by_key(|s| s.shard_id);
            Loaded {
                path: path.to_path_buf(),
                instance_id,
                host: host_of.get(&instance_id).cloned().unwrap_or_default(),
                schemas: schemas.clone(),
                views: views.clone(),
                intern: intern_by_instance.remove(&instance_id).unwrap_or_default(),
                shards,
            }
        })
        .collect();
    Ok(loaded)
}

/// A cursor into one shard's sorted locator stream, for the k-way merge.
struct Cursor<'a> {
    loaded: &'a Loaded,
    shard: &'a ShardRecs,
    pos: usize,
}

impl<'a> Cursor<'a> {
    /// The merge key of the locator at the cursor's current position (`None` if exhausted). Built
    /// from the locator's `(ts_nanos, offset)` alone — no schema lookup, no field slice — since the
    /// offset already identifies the record within its shard (see [`Key`]).
    fn key(&self) -> Option<Key> {
        let loc = self.shard.locs.get(self.pos)?;
        Some(Key {
            ts_nanos: loc.ts_nanos,
            instance_id: self.loaded.instance_id,
            shard_id: self.shard.shard_id,
            offset: loc.offset,
        })
    }
}

/// A heap entry: the current key of a stream paired with the stream's index. Ordered by key (a
/// `Reverse` wrapper at the call site turns the max-heap into a min-heap), with the stream index as
/// a final tiebreaker so equal keys pop in a deterministic order.
struct HeapEntry {
    key: Key,
    stream: usize,
}
impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.stream == other.stream
    }
}
impl Eq for HeapEntry {}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then(self.stream.cmp(&other.stream))
    }
}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A lazy k-way merge over every `(dump, instance, shard)` stream, yielding [`MergedRec`]s in the
/// global order `(ts_nanos, instance_id, shard_id, event_id, fields)` with duplicates removed.
///
/// Each shard's locators are already sorted by this key's per-stream projection (see [`load`]), so
/// the merge only maintains one cursor per shard in a binary heap. A duplicate — any record sharing
/// another's full [`Key`], which happens when overlapping dumps re-capture shared ring content — is
/// dropped by comparing each popped key against the previously emitted one. Peak extra memory is the
/// heap (one entry per shard), not the records, so converting an arbitrarily large dump stays
/// bounded. The yielded order and dedup are identical to sorting every record and removing adjacent
/// equals, so output is byte-for-byte what a materialize-everything approach produced.
pub struct MergeIter<'a> {
    cursors: Vec<Cursor<'a>>,
    heap: BinaryHeap<Reverse<HeapEntry>>,
    last: Option<Key>,
}

impl<'a> Iterator for MergeIter<'a> {
    type Item = MergedRec<'a>;

    fn next(&mut self) -> Option<MergedRec<'a>> {
        loop {
            let Reverse(entry) = self.heap.pop()?;
            let stream = entry.stream;

            // Advance that stream and re-push its next key, if any.
            let cursor = &mut self.cursors[stream];
            let loc = cursor.shard.locs[cursor.pos];
            cursor.pos += 1;
            if let Some(key) = cursor.key() {
                self.heap.push(Reverse(HeapEntry { key, stream }));
            }

            // Drop duplicates: a record whose full key equals the one we just emitted.
            if self.last.as_ref() == Some(&entry.key) {
                continue;
            }
            self.last = Some(entry.key);

            let cursor = &self.cursors[stream];
            return Some(MergedRec {
                loaded: cursor.loaded,
                ts_nanos: loc.ts_nanos,
                shard_id: cursor.shard.shard_id,
                schema_idx: loc.schema_idx as usize,
                fields: cursor.shard.fields(&loc, &cursor.loaded.schemas),
            });
        }
    }
}

/// Builds the streaming [`MergeIter`] over all `dumps`. See [`MergeIter`] for the ordering and dedup
/// guarantees. This is the primitive the converters consume; [`unique_records`] is the eager
/// `Vec`-collecting convenience over the same sequence.
pub fn merge_records(dumps: &[Loaded]) -> MergeIter<'_> {
    let mut cursors: Vec<Cursor> = Vec::new();
    for d in dumps {
        for sh in &d.shards {
            if !sh.locs.is_empty() {
                cursors.push(Cursor {
                    loaded: d,
                    shard: sh,
                    pos: 0,
                });
            }
        }
    }
    let mut heap = BinaryHeap::with_capacity(cursors.len());
    for (stream, cursor) in cursors.iter().enumerate() {
        if let Some(key) = cursor.key() {
            heap.push(Reverse(HeapEntry { key, stream }));
        }
    }
    MergeIter {
        cursors,
        heap,
        last: None,
    }
}

/// Eagerly collects [`merge_records`] into a `Vec` — the global, de-duplicated record sequence. A
/// convenience for callers that genuinely need every record at once (e.g. `merge`'s re-pack, which
/// groups by shard, and tests asserting counts). The streaming converters use [`merge_records`]
/// directly so they never materialize all records.
pub fn unique_records(dumps: &[Loaded]) -> Vec<MergedRec<'_>> {
    merge_records(dumps).collect()
}

/// Loads several dumps in parallel (rayon), flattening each file's instances. Files are returned in
/// input order; instances within a file in ascending `instance_id` order. Each dump is memory-mapped
/// (see [`map_dump`]), so the bytes are demand-paged rather than read into heap.
pub fn load_many(paths: &[PathBuf]) -> Result<Vec<Loaded>> {
    let per_file: Vec<Vec<Loaded>> = paths
        .par_iter()
        .map(|p| {
            let bytes = map_dump(p)?;
            load(p, bytes).with_context(|| format!("decoding dump {}", p.display()))
        })
        .collect::<Result<_>>()?;
    Ok(per_file.into_iter().flatten().collect())
}

/// Memory-maps a dump file into a [`Bytes`], so a multi-gigabyte dump is demand-paged by the OS
/// rather than read into the heap up front. The records the converters hold are zero-copy slices of
/// this mapping (see [`load`]), so only the pages actually touched are resident, and the kernel can
/// evict clean pages under memory pressure.
///
/// A zero-length file maps to empty bytes (some platforms reject `mmap` of an empty file, so we
/// skip the syscall). That is not a valid dump — it has no envelope — so the caller's
/// [`DumpReader::new`] then rejects it with a clear "bad magic"/"unexpected EOF" error, exactly as
/// a truncated dump would.
pub fn map_dump(path: &Path) -> Result<Bytes> {
    let file = fs::File::open(path).with_context(|| format!("opening dump {}", path.display()))?;
    let len = file
        .metadata()
        .with_context(|| format!("stat dump {}", path.display()))?
        .len();
    if len == 0 {
        return Ok(Bytes::new());
    }
    // SAFETY: a dump is read-only input. We never mutate the mapping, and if another process
    // truncates the file concurrently a SIGBUS is possible — the same exposure as any mmap-based
    // reader (and as reading a file being rewritten under us); acceptable for a CLI over its inputs.
    let mmap = unsafe {
        memmap2::Mmap::map(&file).with_context(|| format!("mmapping dump {}", path.display()))?
    };
    // Advise the kernel we'll stream the mapping roughly sequentially (the shard walks scan their
    // regions); best-effort, ignored where unsupported. `advise` is Unix-only in memmap2.
    #[cfg(unix)]
    let _ = mmap.advise(memmap2::Advice::Sequential);
    Ok(Bytes::from_owner(mmap))
}
