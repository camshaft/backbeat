// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! Shared dump-loading model behind both output formats.
//!
//! A dump is decoded once into a [`Loaded`] — its `instance_id`, schema registry, intern table, and
//! the flat list of records recovered from every shard (sorted into the global
//! `(ts_nanos, shard_id, local_seq)` order). The Parquet ([`crate::convert`]) and Chrome-trace
//! ([`crate::trace`]) writers both build on `&[Loaded]`, so multi-dump merge is uniform: load each
//! input (in parallel), then hand the slice to the writer.

use anyhow::{Context, Result};
use backbeat::{
    record::RecordView,
    ring::walk,
    wire::{DumpReader, OwnedSchema},
};
use bytes::Bytes;
use rayon::prelude::*;
use std::{
    collections::HashMap,
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

/// A single decoded dump: its identity, registry, intern table, and recovered records.
pub struct Loaded {
    /// Where it was read from (for error messages).
    pub path: PathBuf,
    /// The producing process's id; `(instance_id, span_id)` keys spans across merged dumps.
    pub instance_id: u64,
    /// Host label from the dump's metadata (empty if unset).
    pub host: String,
    /// The schema registry, sorted by `qualified_name` for deterministic output.
    pub schemas: Vec<OwnedSchema>,
    /// Interned `id → string` for `Interned` fields.
    pub intern: HashMap<u32, String>,
    /// Every valid record, in global `(ts_nanos, shard_id, local_seq)` order.
    pub records: Vec<Rec>,
}

/// Loads and decodes a single dump from `bytes`. `path` is carried for diagnostics.
pub fn load(path: &Path, bytes: Bytes) -> Result<Loaded> {
    let reader = DumpReader::new(bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut schemas = reader.schemas().map_err(|e| anyhow::anyhow!("{e}"))?;
    let intern_pairs = reader.intern_table().map_err(|e| anyhow::anyhow!("{e}"))?;
    let meta = reader.meta().map_err(|e| anyhow::anyhow!("{e}"))?;
    let shards = reader.shards().map_err(|e| anyhow::anyhow!("{e}"))?;

    // Deterministic registry order regardless of how the producer's inventory was linked.
    schemas.sort_by(|a, b| a.qualified_name.cmp(&b.qualified_name));

    let intern: HashMap<u32, String> = intern_pairs
        .into_iter()
        .map(|(id, bytes)| (id, String::from_utf8_lossy(&bytes).into_owned()))
        .collect();
    let by_id: HashMap<u64, usize> = schemas
        .iter()
        .enumerate()
        .map(|(i, s)| (s.id.get(), i))
        .collect();

    // Walk every shard, attributing each record to a schema. Shards are independent (no shared
    // state in the walk), so we parse them in parallel with rayon and concatenate. The closure is
    // walk's validator (see backbeat::ring::walk): accept a candidate only if its event_id is
    // registered and its declared record_size matches the field bytes, so walk resynchronizes past
    // torn data.
    //
    // `walk` hands us each payload as a `Bytes` — a zero-copy refcounted slice of the shard region
    // (itself a slice of the file buffer), except for the one record per shard that wraps the ring
    // boundary. So storing a record's fields is a refcount bump, not a per-record copy.
    let mut records: Vec<Rec> = shards
        .par_iter()
        .flat_map(|shard| {
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
            shard_recs
        })
        .collect();
    records.sort_by_key(|r| (r.ts_nanos, r.shard_id, r.local_seq));

    let (instance_id, host) = meta
        .map(|m| (m.instance_id, m.host))
        .unwrap_or((0, String::new()));
    Ok(Loaded {
        path: path.to_path_buf(),
        instance_id,
        host,
        schemas,
        intern,
        records,
    })
}

/// Loads several dumps in parallel (rayon). Returns them in input order.
pub fn load_many(paths: &[PathBuf]) -> Result<Vec<Loaded>> {
    paths
        .par_iter()
        .map(|p| {
            let bytes = fs::read(p).with_context(|| format!("reading dump {}", p.display()))?;
            load(p, bytes.into()).with_context(|| format!("decoding dump {}", p.display()))
        })
        .collect()
}
