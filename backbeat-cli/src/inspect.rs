// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! `backbeat inspect`: print a dump's envelope, schema registry, and per-shard record counts.
//!
//! Everything here is driven by the dump's embedded registry — there is no compiled-in knowledge of
//! any event type. A record's `event_id` is matched against the registry to attribute it to an
//! event and validate its size; unknown ids and size mismatches are reported rather than guessed.

use anyhow::Result;
use backbeat::{
    record::RecordView,
    ring::walk,
    schema::{FieldRole, Phase},
    wire::{DumpReader, OwnedSchema},
};
use bytes::Bytes;
use std::{collections::BTreeMap, io::Write};

/// Reads `bytes` as a dump and writes a human-readable summary to `out`.
pub fn inspect(bytes: impl Into<Bytes>, out: &mut impl Write) -> Result<()> {
    let bytes = bytes.into();
    let total_len = bytes.len();
    let reader = DumpReader::new(bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
    let schemas = reader.schemas().map_err(|e| anyhow::anyhow!("{e}"))?;
    let intern = reader.intern_table().map_err(|e| anyhow::anyhow!("{e}"))?;
    let shards = reader.shards().map_err(|e| anyhow::anyhow!("{e}"))?;

    // Index schemas by id for record attribution.
    let by_id: BTreeMap<u64, &OwnedSchema> = schemas.iter().map(|s| (s.id.get(), s)).collect();

    writeln!(out, "envelope")?;
    writeln!(out, "  size:      {total_len} bytes")?;
    writeln!(out, "  flags:     {:#06x}", reader.flags())?;
    writeln!(out, "  sections:  {}", reader.section_count())?;

    writeln!(out, "\nschema registry ({} events)", schemas.len())?;
    for s in &schemas {
        let phase = match s.phase {
            Phase::Enter => "  [span enter]",
            Phase::Exit => "  [span exit]",
            _ => "",
        };
        writeln!(
            out,
            "  {:#018x}  {}  ({} bytes, {} fields){}{}",
            s.id.get(),
            s.qualified_name,
            s.record_size,
            s.fields.len(),
            phase,
            s.description
                .as_deref()
                .map(|d| format!("  — {d}"))
                .unwrap_or_default()
        )?;
        for f in &s.fields {
            // Role marker: * key, $ span id, ^ parent span id.
            let marker = match f.role {
                FieldRole::Key => "*",
                FieldRole::SpanId => "$",
                FieldRole::ParentSpanId => "^",
                _ => " ",
            };
            writeln!(
                out,
                "      {}{}: {:?}{}",
                marker,
                f.name,
                f.ty,
                f.unit
                    .as_deref()
                    .map(|u| format!(" [{u}]"))
                    .unwrap_or_default()
            )?;
        }
    }
    writeln!(out, "  (* = key, $ = span id, ^ = parent span id)")?;

    writeln!(out, "\nintern table: {} entries", intern.len())?;

    // Walk each shard, attributing records to events and counting them. The closure is walk's
    // validator: it accepts (and counts) only records whose event_id is in the registry with a
    // matching record_size, and rejects anything else so walk resynchronizes past torn data.
    writeln!(out, "\nshards ({})", shards.len())?;
    let mut total = 0usize;
    let mut per_event: BTreeMap<String, usize> = BTreeMap::new();

    for shard in &shards {
        let mut count = 0usize;
        walk(
            &shard.region,
            shard.head as usize,
            shard.capacity as usize,
            |payload| match RecordView::parse(&payload[..]) {
                Some(rec) => match by_id.get(&rec.event_id.get()) {
                    Some(schema) if rec.fields.len() == schema.record_size as usize => {
                        *per_event.entry(schema.qualified_name.clone()).or_default() += 1;
                        count += 1;
                        true
                    }
                    _ => false,
                },
                None => false,
            },
        );
        total += count;
        writeln!(
            out,
            "  shard {:>3}: head {:>10}  capacity {:>10}  {} records",
            shard.shard_id, shard.head, shard.capacity, count
        )?;
    }

    writeln!(out, "\nrecords: {total} total")?;
    for (name, n) in &per_event {
        writeln!(out, "  {name}: {n}")?;
    }

    Ok(())
}
