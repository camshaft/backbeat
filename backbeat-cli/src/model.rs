// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! Shared dump-loading model behind both output formats.
//!
//! A dump is decoded into one [`Loaded`] **per instance** it contains — its `instance_id`, the
//! shared schema registry, that instance's intern table, and the flat list of records recovered from
//! its shards (sorted into the global `(ts_nanos, shard_id, local_seq)` order). A single-process
//! dump yields one `Loaded`; a merged dump yields one per process it bundles. The Parquet
//! ([`crate::convert`]) and Chrome-trace ([`crate::trace`]) writers both build on `&[Loaded]`, so a
//! merged file decodes to exactly the same slice that loading its source dumps separately would —
//! `convert merged.bb` is byte-identical to `convert a.bb b.bb`.

use anyhow::{Context, Result};
use backbeat::{
    record::RecordView,
    ring::walk,
    wire::{DumpReader, OwnedSchema},
};
use bytes::Bytes;
use rayon::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

/// One decoded record, attributed to a schema within its [`Loaded`] dump.
pub struct Rec {
    pub ts_nanos: u64,
    pub shard_id: u32,
    /// Position within the shard, oldest-first — the per-shard `local_seq`.
    pub local_seq: u64,
    /// Index into the owning [`Loaded::schemas`].
    pub schema_idx: usize,
    /// The event's raw field bytes (length equals the schema's `record_size`). A cheap refcounted
    /// slice into the dump's reconstructed-payload arena — cloning it bumps a count, not the heap.
    pub fields: Bytes,
}

/// A single decoded instance: its identity, the (shared) registry, its intern table, and its
/// recovered records. A dump file decodes to one of these per instance it contains.
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
    /// Every valid record from this instance's shards, in global `(ts_nanos, shard_id, local_seq)`
    /// order.
    pub records: Vec<Rec>,
}

/// The identity of a logged record, used both to order records globally and to drop duplicates when
/// dumps overlap. The tuple is `(ts_nanos, instance_id, shard_id, event_id, fields)`.
///
/// Successive dumps of one process share a ring, so the newer dump re-contains records the older one
/// already captured; merging or converting them together would otherwise double-count. Two records
/// that match on this whole key are the *same* logged event — the recorder writes monotonic
/// timestamps, so a genuine pair of distinct events on one shard differs in at least one component —
/// so collapsing them to one is correct.
///
/// Time leads the tuple so sorting by it yields the global order the converters want *and* makes
/// byte-identical records adjacent, letting [`unique_records`] dedup with a single sort + scan
/// rather than a hash set. `local_seq` is deliberately absent: it is assigned per-walk, so the same
/// event gets different seqs in two dumps and could never match — it is only a sort tiebreaker, and
/// `(event_id, fields)` already breaks ties deterministically. The fields are borrowed (a zero-copy
/// slice of the dump buffer), so a key costs no allocation.
pub type RecordKey<'a> = (u64, u64, u32, u64, &'a [u8]);

/// Computes the [`RecordKey`] for a record within its owning [`Loaded`].
pub fn record_key<'a>(d: &Loaded, r: &'a Rec) -> RecordKey<'a> {
    (
        r.ts_nanos,
        d.instance_id,
        r.shard_id,
        d.schemas[r.schema_idx].id.get(),
        &r.fields,
    )
}

/// Loads and decodes a dump from `bytes`, returning one [`Loaded`] per instance it contains.
/// `path` is carried for diagnostics. A single-process dump yields a one-element vec; a merged dump
/// yields one entry per process it bundles, each with its own intern table, host, and records.
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

    // Walk every shard, attributing each record to a schema and bucketing by the shard's instance.
    // Shards are independent (no shared state in the walk), so we parse them in parallel with rayon.
    // The closure is walk's validator (see backbeat::ring::walk): accept a candidate only if its
    // event_id is registered and its declared record_size matches the field bytes, so walk
    // resynchronizes past torn data.
    //
    // `walk` hands us each payload as a `Bytes` — a zero-copy refcounted slice of the shard region
    // (itself a slice of the file buffer), except for the one record per shard that wraps the ring
    // boundary. So storing a record's fields is a refcount bump, not a per-record copy.
    let walked: Vec<(u64, Vec<Rec>)> = shards
        .par_iter()
        .map(|shard| {
            let mut shard_recs: Vec<Rec> = Vec::new();
            // walk yields newest-first; assign `local_seq` descending from `u64::MAX` so the global
            // ascending sort by `(ts, shard, local_seq)` orders this shard oldest-first — no second
            // pass. (Only the relative order matters; the absolute value is just a sort tiebreaker.)
            let mut seq = u64::MAX;
            walk(
                &shard.region,
                shard.head as usize,
                shard.capacity as usize,
                |payload| {
                    let Some(rec) = RecordView::parse(&payload[..]) else {
                        return false;
                    };
                    match by_id.get(&rec.event_id.get()) {
                        Some(&idx) if rec.fields.len() == schemas[idx].record_size as usize => {
                            // The fields follow the fixed `[ts][event_id]` prefix; slice them out of
                            // the owned `Bytes` zero-copy (RecordView only borrowed it to validate).
                            let fields_off = payload.len() - rec.fields.len();
                            shard_recs.push(Rec {
                                ts_nanos: rec.ts_nanos,
                                shard_id: shard.shard_id,
                                local_seq: seq,
                                schema_idx: idx,
                                fields: payload.slice(fields_off..),
                            });
                            seq -= 1;
                            true
                        }
                        _ => false,
                    }
                },
            );
            (shard.instance_id, shard_recs)
        })
        .collect();

    // Group records by instance. The set of instances is the union of those that have metadata,
    // an intern table, or any shards — so an instance with shards but no Meta still surfaces (with
    // id 0 / empty host, matching a metadata-less single-process dump's old behavior).
    let mut records_by_instance: HashMap<u64, Vec<Rec>> = HashMap::new();
    for (instance_id, recs) in walked {
        records_by_instance
            .entry(instance_id)
            .or_default()
            .extend(recs);
    }

    let mut instance_ids: Vec<u64> = Vec::new();
    let mut seen = HashSet::new();
    for m in &metas {
        if seen.insert(m.instance_id) {
            instance_ids.push(m.instance_id);
        }
    }
    for &id in records_by_instance.keys() {
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
            let mut records = records_by_instance.remove(&instance_id).unwrap_or_default();
            records.sort_by_key(|r| (r.ts_nanos, r.shard_id, r.local_seq));
            Loaded {
                path: path.to_path_buf(),
                instance_id,
                host: host_of.get(&instance_id).cloned().unwrap_or_default(),
                schemas: schemas.clone(),
                views: views.clone(),
                intern: intern_by_instance.remove(&instance_id).unwrap_or_default(),
                records,
            }
        })
        .collect();
    Ok(loaded)
}

/// Returns a reference to every record across `dumps`, sorted into the global order
/// `(ts_nanos, instance_id, shard_id, event_id, fields)`, with duplicates removed.
///
/// A duplicate is any record sharing another's full [`RecordKey`] — which happens whenever
/// overlapping dumps are combined (successive dumps of one process re-capture shared ring contents).
/// Because the key leads with the fields the converters sort by and ends with the bytes that
/// distinguish records, sorting on it both produces the output order *and* lands byte-identical
/// records side by side, so a single linear `dedup` scan removes them. No hash set: the only extra
/// memory is the `Vec` of borrowed `(&Loaded, &Rec)` pairs (the field bytes stay in the dump
/// buffer), which the caller needs anyway. Callers therefore receive already-sorted, de-duplicated
/// records and need no further sort.
pub fn unique_records(dumps: &[Loaded]) -> Vec<(&Loaded, &Rec)> {
    let total: usize = dumps.iter().map(|d| d.records.len()).sum();
    let mut out: Vec<(&Loaded, &Rec)> = Vec::with_capacity(total);
    for d in dumps {
        for r in &d.records {
            out.push((d, r));
        }
    }
    out.sort_by(|a, b| record_key(a.0, a.1).cmp(&record_key(b.0, b.1)));
    out.dedup_by(|a, b| record_key(a.0, a.1) == record_key(b.0, b.1));
    out
}

/// Loads several dumps in parallel (rayon), flattening each file's instances. Files are returned in
/// input order; instances within a file in ascending `instance_id` order.
pub fn load_many(paths: &[PathBuf]) -> Result<Vec<Loaded>> {
    let per_file: Vec<Vec<Loaded>> = paths
        .par_iter()
        .map(|p| {
            let bytes = fs::read(p).with_context(|| format!("reading dump {}", p.display()))?;
            load(p, bytes.into()).with_context(|| format!("decoding dump {}", p.display()))
        })
        .collect::<Result<_>>()?;
    Ok(per_file.into_iter().flatten().collect())
}
