// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! `backbeat` → Chrome / Perfetto trace JSON.
//!
//! Spans become **async** events (`ph:"b"`/`"e"`, paired by `id`), not duration events (`B`/`E`):
//! backbeat spans carry explicit ids, can overlap, may be emitted across different shards (thread
//! migration), and may be orphaned by ring eviction — all of which violate the per-thread LIFO
//! stack contract that `B`/`E` assume. Async events pair by explicit `id`, tolerate overlap, and
//! need no nesting. Plain (non-span) events become instants (`ph:"i"`).
//!
//! Pairing is by `(instance_id, span_id)` so merged multi-process dumps never collide. Orphans (the
//! other half evicted from the ring) are surfaced rather than dropped: an enter with no exit gets a
//! synthetic close at the trace's max timestamp; an exit with no enter becomes a zero-width instant.
//!
//! The output is read by `chrome://tracing`, [Perfetto](https://ui.perfetto.dev/), and any
//! Trace-Event-Format consumer.

use crate::model::{self, Loaded};
use anyhow::{Context, Result};
use backbeat::schema::{FieldType, Phase};
use serde_json::{json, Value as Json};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

/// Writes the loaded dumps to `output` as Chrome Trace Event Format JSON. Returns the event count.
///
/// The output is **streamed**: events are written one at a time straight to a buffered file, never
/// collected into an in-memory array. A trace can have hundreds of millions of events, and a
/// `serde_json::Value` per event (a heap map of boxed values) is far larger than its serialized
/// form — materializing them all is what made large dumps blow up memory. We instead make one cheap
/// pass to find orphaned spans (which needs only each record's `(span_id, phase)`, not its decoded
/// fields), then a second pass that decodes and emits each event's JSON and immediately drops it.
/// Peak extra memory is therefore one event, not all of them.
pub fn to_trace(dumps: &[Loaded], output: &Path) -> Result<usize> {
    // Deduplicate up front: overlapping dumps re-capture shared ring contents, and a duplicated
    // span enter/exit would corrupt the orphan accounting as well as double the output. The set is
    // references into the dump buffers (no record copy) — see `model::unique_records`.
    let records = model::unique_records(dumps);

    // Pass 1: the trace's max timestamp (for synthetic orphan closes) and which (instance, span)
    // keys have an enter / an exit. This touches only span ids and phases — no field decoding, no
    // per-event allocation — so it is cheap even for a huge dump.
    let mut max_ts = 0u64;
    let mut has_enter: HashSet<(u64, u64)> = HashSet::new();
    let mut has_exit: HashSet<(u64, u64)> = HashSet::new();
    for (d, r) in &records {
        let s = &d.schemas[r.schema_idx];
        max_ts = max_ts.max(r.ts_nanos);
        if let Some(sid) = s
            .span_id()
            .and_then(|f| read_u64(&r.fields, f.offset as usize))
        {
            match s.phase {
                Phase::Enter => {
                    has_enter.insert((d.instance_id, sid));
                }
                Phase::Exit => {
                    has_exit.insert((d.instance_id, sid));
                }
                _ => {}
            }
        }
    }

    // Pass 2: stream the document. We hand-write the envelope around the `traceEvents` array and
    // serialize each event into the array as we go, separated by commas, so the full array never
    // exists in memory at once.
    let file = File::create(output).with_context(|| format!("creating {}", output.display()))?;
    let mut w = BufWriter::new(file);
    write!(w, "{{\"displayTimeUnit\":\"ns\",\"traceEvents\":[").context("writing trace JSON")?;

    let mut count = 0usize;
    for (d, r) in &records {
        let s = &d.schemas[r.schema_idx];
        let span_id = s
            .span_id()
            .and_then(|f| read_u64(&r.fields, f.offset as usize));
        // Decode this one record's fields. The args map lives only for this iteration.
        let args: serde_json::Map<String, Json> = s
            .fields
            .iter()
            .map(|f| (f.name.to_string(), decode_json(f, &r.fields, &d.intern)))
            .collect();
        let f = Flat {
            instance_id: d.instance_id,
            shard_id: r.shard_id,
            ts_nanos: r.ts_nanos,
            phase: s.phase,
            span_id,
            name: &s.qualified_name,
        };
        emit_record(&mut w, &f, args, max_ts, &has_enter, &has_exit, &mut count)?;
    }

    write!(w, "]}}").context("writing trace JSON")?;
    w.flush().context("flushing trace JSON")?;
    Ok(count)
}

/// The render-time context for one record, independent of which dump it came from. Args are passed
/// alongside rather than stored, so a `Flat` is cheap and short-lived (one per emitted record).
struct Flat<'a> {
    instance_id: u64,
    shard_id: u32,
    ts_nanos: u64,
    phase: Phase,
    span_id: Option<u64>,
    name: &'a str,
}

/// Writes a single serialized event into the `traceEvents` array, prefixing a comma before all but
/// the first (tracked via `count`). Used by the streaming emitter so events never accumulate.
fn write_event(w: &mut impl Write, event: &Json, count: &mut usize) -> Result<()> {
    if *count > 0 {
        write!(w, ",").context("writing trace JSON")?;
    }
    serde_json::to_writer(&mut *w, event).context("writing trace JSON")?;
    *count += 1;
    Ok(())
}

/// Emits the one or two trace events a record produces (a span enter/exit, plus a synthetic close
/// for an orphan; or a plain instant), streaming each straight to `w`.
fn emit_record(
    w: &mut impl Write,
    f: &Flat,
    args: serde_json::Map<String, Json>,
    max_ts: u64,
    has_enter: &HashSet<(u64, u64)>,
    has_exit: &HashSet<(u64, u64)>,
    count: &mut usize,
) -> Result<()> {
    let cat = f.name.split("::").next().unwrap_or("");
    // pid = process (instance), tid = shard/core. Pairing is by `id`, so tid is free to show
    // per-core activity (a span's begin and end may even land on different tids after migration).
    let pid = f.instance_id;
    let tid = f.shard_id;

    match (f.phase, f.span_id) {
        (Phase::Enter, Some(sid)) => {
            // Orphaned enter (exit evicted): synthesize a close at the trace's end.
            if !has_exit.contains(&(f.instance_id, sid)) {
                let mut a = args.clone();
                a.insert("backbeat_open".to_string(), json!(true));
                let close = async_event_at("e", f, sid, cat, pid, tid, max_ts, a);
                write_event(w, &async_event("b", f, sid, cat, pid, tid, args), count)?;
                write_event(w, &close, count)?;
            } else {
                write_event(w, &async_event("b", f, sid, cat, pid, tid, args), count)?;
            }
        }
        (Phase::Exit, Some(sid)) => {
            // Orphaned exit (enter evicted): a zero-width instant marks where it closed.
            if !has_enter.contains(&(f.instance_id, sid)) {
                let mut a = args;
                a.insert("backbeat_orphan_exit".to_string(), json!(true));
                write_event(w, &instant_event(f, cat, pid, tid, a), count)?;
            } else {
                write_event(w, &async_event("e", f, sid, cat, pid, tid, args), count)?;
            }
        }
        // A plain event: an instant. If it carries a parent_span_id it still renders as an
        // instant (Chrome has no "point attached to a span"); the parent link is in args.
        _ => write_event(w, &instant_event(f, cat, pid, tid, args), count)?,
    }
    Ok(())
}

/// An async-event (`b`/`e`) at the record's own timestamp.
fn async_event(
    ph: &str,
    f: &Flat,
    sid: u64,
    cat: &str,
    pid: u64,
    tid: u32,
    args: serde_json::Map<String, Json>,
) -> Json {
    async_event_at(ph, f, sid, cat, pid, tid, f.ts_nanos, args)
}

/// An async-event at an explicit timestamp (used for synthetic orphan closes).
#[allow(clippy::too_many_arguments)]
fn async_event_at(
    ph: &str,
    f: &Flat,
    sid: u64,
    cat: &str,
    pid: u64,
    tid: u32,
    ts_nanos: u64,
    args: serde_json::Map<String, Json>,
) -> Json {
    json!({
        "ph": ph,
        "name": f.name,
        "cat": cat,
        "id": format!("{sid:#018x}"),
        "pid": pid,
        "tid": tid,
        "ts": ts_micros(ts_nanos),
        "args": args,
    })
}

/// An instant event (`ph:"i"`) at the record's timestamp.
fn instant_event(
    f: &Flat,
    cat: &str,
    pid: u64,
    tid: u32,
    args: serde_json::Map<String, Json>,
) -> Json {
    json!({
        "ph": "i",
        "name": f.name,
        "cat": cat,
        "pid": pid,
        "tid": tid,
        "ts": ts_micros(f.ts_nanos),
        "s": "t", // thread-scoped instant
        "args": args,
    })
}

/// Trace Event Format `ts` is microseconds (fractional ok). We keep nanosecond precision via the
/// fraction and set `displayTimeUnit: "ns"` on the document.
fn ts_micros(ts_nanos: u64) -> f64 {
    ts_nanos as f64 / 1000.0
}

/// Reads a little-endian `u64` at `offset`, or `None` if out of range.
fn read_u64(bytes: &[u8], offset: usize) -> Option<u64> {
    bytes
        .get(offset..offset + 8)
        .map(|s| u64::from_le_bytes(s.try_into().unwrap()))
}

/// Decodes a field to a JSON value using only its schema descriptor (mirrors the Parquet decoder).
fn decode_json(
    field: &backbeat::wire::OwnedField,
    bytes: &[u8],
    intern: &HashMap<u32, String>,
) -> Json {
    let start = field.offset as usize;
    let Some(slice) = bytes.get(start..start + field.width as usize) else {
        return Json::Null;
    };
    // `field.width` is read from the (possibly corrupt or foreign) dump and is not guaranteed to
    // match the natural width of `field.ty`, so every fixed-width read goes through a fallible
    // conversion: a mismatch renders as `null` rather than panicking the tool. This mirrors the
    // Parquet decoder, which is already fallible throughout.
    match field.ty {
        FieldType::U8 => le::<1>(slice).map_or(Json::Null, |b| json!(b[0])),
        FieldType::U16 => le::<2>(slice).map_or(Json::Null, |b| json!(u16::from_le_bytes(b))),
        FieldType::U32 => le::<4>(slice).map_or(Json::Null, |b| json!(u32::from_le_bytes(b))),
        FieldType::U64 => le::<8>(slice).map_or(Json::Null, |b| json!(u64::from_le_bytes(b))),
        FieldType::I8 => le::<1>(slice).map_or(Json::Null, |b| json!(b[0] as i8)),
        FieldType::I16 => le::<2>(slice).map_or(Json::Null, |b| json!(i16::from_le_bytes(b))),
        FieldType::I32 => le::<4>(slice).map_or(Json::Null, |b| json!(i32::from_le_bytes(b))),
        FieldType::I64 => le::<8>(slice).map_or(Json::Null, |b| json!(i64::from_le_bytes(b))),
        FieldType::Bool => slice.first().map_or(Json::Null, |b| json!(*b != 0)),
        FieldType::Bytes => json!(hex(slice)),
        FieldType::Enum { repr } => {
            let Some(raw_bytes) = slice.get(..repr as usize) else {
                return Json::Null;
            };
            let mut buf = [0u8; 8];
            buf[..repr as usize].copy_from_slice(raw_bytes);
            let raw = u64::from_le_bytes(buf);
            match field.enum_labels.iter().find(|l| l.value == raw) {
                Some(l) => json!(l.label),
                None => json!(raw),
            }
        }
        FieldType::Interned { .. } => match le::<4>(slice) {
            Some(b) => {
                let id = u32::from_le_bytes(b);
                match intern.get(&id) {
                    Some(s) => json!(s),
                    None => json!(format!("#{id}")),
                }
            }
            None => Json::Null,
        },
        _ => json!(hex(slice)),
    }
}

/// Reads exactly `N` little-endian bytes from the front of `slice`, or `None` if the field's
/// declared width is too small to hold the type. Guards against a corrupt/foreign dump whose
/// `field.width` disagrees with `field.ty`.
fn le<const N: usize>(slice: &[u8]) -> Option<[u8; N]> {
    slice.get(..N)?.try_into().ok()
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}
