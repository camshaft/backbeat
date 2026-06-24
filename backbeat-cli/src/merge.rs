// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! `backbeat merge`: combine several `.bb` dumps into one multi-instance `.bb`.
//!
//! The dump format is inherently multi-instance — Meta, Intern, and Shard sections are each tagged
//! with the `instance_id` of the process that produced them, and the schema registry is content
//! addressed by a stable [`EventId`](backbeat::id::EventId). Merging therefore has two modes:
//!
//! * **Default (dedup + trim).** Decode each input's records, drop duplicates — the same logged
//!   event re-captured by overlapping dumps (e.g. one process's successive ring snapshots) — and
//!   re-pack the survivors into fresh, compact shards. The output is the smallest faithful dump:
//!   one row per distinct event. This is what you want before analysis, and it avoids paying to
//!   store/transfer redundant ring data.
//!
//! * **`--no-dedup` (raw splice).** Copy every input's Meta/Intern/Shard bodies through verbatim and
//!   union the registries. Nothing is decoded, so it is cheap and lossless, but overlapping dumps
//!   keep their duplicates. Use this to quickly concatenate a host's dumps for upload; `convert`
//!   always dedups on the way out, so the duplicates never reach the final table.
//!
//! Either way the registry is unioned by id (identical event types collapse to one entry) and
//! `instance_id`s are preserved, so converting the merged file yields exactly what converting the
//! inputs together would.

use crate::model::{self, Loaded, Rec};
use anyhow::{Context, Result};
use backbeat::{
    format::SectionKind,
    record::{FIELDS_OFFSET, ID_OFFSET, TS_OFFSET},
    ring::{LEN_SUFFIX, MAX_RECORD},
    wire::{DumpReader, DumpWriter, OwnedSchema},
};
use rayon::prelude::*;
use std::{
    collections::{BTreeMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

/// Merges `inputs` into one multi-instance dump written to `output`.
///
/// When `dedup` is true (the default), records are decoded, de-duplicated, and re-packed into
/// compact shards. When false, the inputs' sections are spliced through verbatim (a cheap raw
/// concat). Returns the number of distinct event schemas in the unified registry.
pub fn merge(inputs: &[PathBuf], output: &Path, dedup: bool) -> Result<usize> {
    if dedup {
        merge_dedup(inputs, output)
    } else {
        merge_splice(inputs, output)
    }
}

// ---------------------------------------------------------------------------
// Raw splice (`--no-dedup`): copy section bodies through verbatim.
// ---------------------------------------------------------------------------

/// One input dump's spliceable contents: its raw, undecoded section bodies plus the schemas it
/// contributes (decoded only so the registry can be unioned by id).
struct Spliceable {
    schemas: Vec<OwnedSchema>,
    metas: Vec<Vec<u8>>,
    interns: Vec<Vec<u8>>,
    shards: Vec<Vec<u8>>,
}

/// Reads one dump into its [`Spliceable`] form. The Meta/Intern/Shard bodies are taken verbatim
/// (never re-decoded); only the registry is parsed, so it can be unioned.
fn read_spliceable(path: &Path, bytes: bytes::Bytes) -> Result<Spliceable> {
    let reader = DumpReader::new(bytes)
        .map_err(|e| anyhow::anyhow!("{e}"))
        .with_context(|| format!("reading dump {}", path.display()))?;
    let schemas = reader.schemas().map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(Spliceable {
        schemas,
        metas: reader.raw_bodies(SectionKind::Meta),
        interns: reader.raw_bodies(SectionKind::Intern),
        shards: reader.raw_bodies(SectionKind::Shard),
    })
}

/// Raw concat: union registries, copy every instance-tagged section through verbatim.
fn merge_splice(inputs: &[PathBuf], output: &Path) -> Result<usize> {
    let spliceables: Vec<Spliceable> = inputs
        .par_iter()
        .map(|p| {
            let bytes = fs::read(p).with_context(|| format!("reading dump {}", p.display()))?;
            read_spliceable(p, bytes.into())
        })
        .collect::<Result<_>>()?;

    let mut writer = DumpWriter::new();

    let mut seen = HashSet::new();
    let mut registry: Vec<OwnedSchema> = Vec::new();
    for s in &spliceables {
        for schema in &s.schemas {
            if seen.insert(schema.id.get()) {
                registry.push(schema.clone());
            }
        }
    }
    writer.schema_registry_owned(&registry);

    for s in spliceables {
        for body in s.metas {
            writer.raw_section(SectionKind::Meta, body);
        }
        for body in s.interns {
            writer.raw_section(SectionKind::Intern, body);
        }
        for body in s.shards {
            writer.raw_section(SectionKind::Shard, body);
        }
    }

    let bytes = writer.finish();
    fs::write(output, &bytes).with_context(|| format!("writing {}", output.display()))?;
    Ok(registry.len())
}

// ---------------------------------------------------------------------------
// Dedup + trim (default): decode, drop duplicates, re-pack compact shards.
// ---------------------------------------------------------------------------

/// Decode every input, drop duplicate records, and re-encode compact per-(instance, shard) rings.
fn merge_dedup(inputs: &[PathBuf], output: &Path) -> Result<usize> {
    // Decode all inputs to their per-instance `Loaded` form (records are zero-copy slices of the
    // file buffers). `load_many` already flattens each file's instances.
    let dumps = model::load_many(inputs)?;

    let mut writer = DumpWriter::new();

    // Union the registries by content-addressed id, preserving first-seen order.
    let mut seen = HashSet::new();
    let mut registry: Vec<OwnedSchema> = Vec::new();
    for d in &dumps {
        for schema in &d.schemas {
            if seen.insert(schema.id.get()) {
                registry.push(schema.clone());
            }
        }
    }
    writer.schema_registry_owned(&registry);

    // Group the de-duplicated records by their owning (instance_id, shard_id). `unique_records`
    // drops any record re-captured by an overlapping dump, so each survivor is emitted once.
    // BTreeMap keeps a deterministic instance/shard order in the output. We keep the `(Loaded, Rec)`
    // pair so re-packing can resolve each record's event id via its schema.
    let mut by_shard: BTreeMap<(u64, u32), Vec<(&Loaded, &Rec)>> = BTreeMap::new();
    let mut hosts: BTreeMap<u64, String> = BTreeMap::new();
    let mut interns: BTreeMap<u64, Vec<(u32, Vec<u8>)>> = BTreeMap::new();
    let mut instance_ids: HashSet<u64> = HashSet::new();
    for (d, r) in model::unique_records(&dumps) {
        by_shard
            .entry((d.instance_id, r.shard_id))
            .or_default()
            .push((d, r));
        instance_ids.insert(d.instance_id);
    }
    // Carry each instance's host label and intern table through. Every instance that appeared in
    // any input keeps its metadata even if dedup left it with no records, matching what loading the
    // sources separately would surface. Intern entries dedupe by id (an id maps to one string within
    // an instance, so any input's copy is fine).
    for d in &dumps {
        instance_ids.insert(d.instance_id);
        hosts.entry(d.instance_id).or_insert_with(|| d.host.clone());
        let table = interns.entry(d.instance_id).or_default();
        let present: HashSet<u32> = table.iter().map(|(id, _)| *id).collect();
        for (id, s) in &d.intern {
            if !present.contains(id) {
                table.push((*id, s.clone().into_bytes()));
            }
        }
    }

    // Emit one Meta + Intern per instance (deterministic order), then the compact shards. Each
    // rebuilt ring is sized to exactly hold its surviving records (rounded to a power of two, as
    // `Ring` requires), so the output carries no dead ring space.
    let mut ordered: Vec<u64> = instance_ids.into_iter().collect();
    ordered.sort_unstable();
    for instance_id in ordered {
        writer.meta(
            instance_id,
            hosts.get(&instance_id).map_or("", |h| h.as_str()),
        );
        if let Some(table) = interns.get(&instance_id) {
            if !table.is_empty() {
                let entries = table.iter().map(|(id, b)| (*id, b.as_slice()));
                writer.intern_table(instance_id, entries);
            }
        }
    }
    for ((instance_id, shard_id), recs) in &by_shard {
        let (region, head) = repack_shard(recs);
        writer.shard(*instance_id, *shard_id, head, &region);
    }

    let bytes = writer.finish();
    fs::write(output, &bytes).with_context(|| format!("writing {}", output.display()))?;
    Ok(registry.len())
}

/// Re-packs a shard's surviving records into a fresh, compactly-sized ring region.
///
/// Records are written oldest-first as `[ts][event_id][fields]` payloads each followed by the
/// little-endian length suffix — exactly the layout [`crate::model::load`] walks back out. The
/// region is the next power of two large enough to hold them all (a `Ring` capacity must be a power
/// of two), and `head` is the total bytes written, so the walk recovers every record and nothing
/// else. Each record's event id comes from its owning [`Loaded`]'s schema. Returns `(region, head)`.
///
/// `recs` are passed oldest-first (`Loaded::records` is sorted ascending by `(ts, shard, seq)`), so
/// we write them in order — oldest at offset 0, newest nearest `head` — and a re-walk from `head`
/// recovers them newest-first, exactly as the original ring did.
fn repack_shard(recs: &[(&Loaded, &Rec)]) -> (Vec<u8>, u64) {
    // Each record occupies prefix(16) + fields + suffix(2). Sum to size the ring exactly.
    let needed: usize = recs
        .iter()
        .map(|(_, r)| FIELDS_OFFSET + r.fields.len() + LEN_SUFFIX)
        .sum();
    let capacity = needed.max(1).next_power_of_two();
    let mut region = vec![0u8; capacity];

    let mut at = 0usize;
    for (d, r) in recs {
        let event_id = d.schemas[r.schema_idx].id.get();
        at += write_record(&mut region[at..], r.ts_nanos, event_id, &r.fields);
    }
    (region, at as u64)
}

/// Writes one record payload + length suffix at the front of `out`, returning the bytes consumed.
/// Layout matches [`backbeat::record`]: `[ts][event_id][fields]` then the LE length suffix.
fn write_record(out: &mut [u8], ts_nanos: u64, event_id: u64, fields: &[u8]) -> usize {
    let payload_len = FIELDS_OFFSET + fields.len();
    debug_assert!(
        payload_len <= MAX_RECORD,
        "re-packed record exceeds MAX_RECORD"
    );
    out[TS_OFFSET..ID_OFFSET].copy_from_slice(&ts_nanos.to_le_bytes());
    out[ID_OFFSET..FIELDS_OFFSET].copy_from_slice(&event_id.to_le_bytes());
    out[FIELDS_OFFSET..payload_len].copy_from_slice(fields);
    out[payload_len..payload_len + LEN_SUFFIX].copy_from_slice(&(payload_len as u16).to_le_bytes());
    payload_len + LEN_SUFFIX
}
