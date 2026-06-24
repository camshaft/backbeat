// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! `backbeat convert`: decode a dump to sparse-wide Parquet, driven entirely by its embedded schema.
//!
//! The output is one table (the layout settled in the project design):
//!
//! * **Dense common columns** present on every row: `seq`, `ts_nanos`, `event`, `event_id`,
//!   `instance_id`, plus every `#[event(key)]` field promoted to a top-level (nullable) column.
//! * **Per-event struct columns**: one nullable `Struct` column per event type holding that event's
//!   remaining (non-key) fields. A row carries a value only in its own event's struct; all other
//!   struct columns are null. Parquet's columnar encoding makes that sparsity essentially free.
//!
//! Every field is decoded from its raw bytes using only the registry's `FieldType`/offset/width, so
//! the converter has no compiled-in knowledge of any event. Enums render as their label, interned
//! ids resolve against the intern table, byte arrays render as hex. Each column carries its
//! backbeat semantics (role, unit, span phase, description) as Arrow *field* metadata, and the
//! dump's `instance_id`/`host` go in the footer key-value metadata — so the Parquet is
//! self-describing without copying the (potentially large) raw dump into it.

use crate::model::{self, Loaded};
use anyhow::{Context, Result};
use arrow::{
    array::{ArrayRef, BooleanBuilder, Int64Builder, StringBuilder, StructArray, UInt64Builder},
    buffer::NullBuffer,
    datatypes::{DataType, Field, Fields, Schema},
    record_batch::RecordBatch,
};
use backbeat::{
    schema::{FieldRole, FieldType, Phase},
    wire::{OwnedField, OwnedSchema},
};
use parquet::{
    arrow::ArrowWriter,
    file::{metadata::KeyValue, properties::WriterProperties},
};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    path::Path,
    sync::Arc,
};

/// Writes the records of one or more loaded dumps to `output` as one sparse-wide Parquet table.
/// Returns the total row count. `host` overrides the dumps' host label in the footer when non-empty.
/// `zstd_level` is the zstd compression level (1–22).
///
/// Two side effects beyond the Parquet rows, both carrying the assembled query DDL (Tier-1 views
/// generated from the registry + the dumps' registered Tier-2 view sets, deduped by content):
/// the path-independent DDL is embedded in the Parquet footer under key `backbeat.views`, and the
/// same DDL — prefixed with a bootstrap that binds the base `events` view to this Parquet — is
/// written to a `<output>.views.sql` sidecar (see [`sidecar_path`]), **overwriting** any existing
/// file at that path. See [`crate::views`].
pub fn to_parquet(dumps: &[Loaded], output: &Path, host: &str, zstd_level: i32) -> Result<usize> {
    // Union the registries across all dumps by event id (schemas with the same id are identical
    // since the id is the fnv1a64 of the layout's qualified name). Sorted for stable column order.
    let mut schemas: Vec<OwnedSchema> = Vec::new();
    let mut seen = HashSet::new();
    for d in dumps {
        for s in &d.schemas {
            if seen.insert(s.id.get()) {
                schemas.push(s.clone());
            }
        }
    }
    schemas.sort_by(|a, b| a.qualified_name.cmp(&b.qualified_name));
    let by_id: HashMap<u64, usize> = schemas
        .iter()
        .enumerate()
        .map(|(i, s)| (s.id.get(), i))
        .collect();

    // Flatten every dump's records into merged rows, tagged with their source instance_id.
    // `unique_records` already returns them in the global order `(ts_nanos, instance_id, shard_id,
    // …)` with duplicates (the same logged event re-captured by overlapping dumps) dropped, so a
    // merged or multi-input conversion never double-counts and needs no further sort.
    let rows: Vec<Row> = model::unique_records(dumps)
        .into_iter()
        .map(|(d, r)| {
            let id = d.schemas[r.schema_idx].id.get();
            Row {
                ts_nanos: r.ts_nanos,
                instance_id: d.instance_id,
                schema_idx: by_id[&id],
                fields: &r.fields,
                intern: &d.intern,
            }
        })
        .collect();

    let batch = build_batch(&schemas, &rows)?;

    // Footer metadata: a host override wins; else the first dump's host. instance_id is per-row, so
    // it is a column, not footer metadata, once dumps are merged.
    let footer_host = if !host.is_empty() {
        host
    } else {
        dumps.first().map(|d| d.host.as_str()).unwrap_or("")
    };

    // Assemble the query DDL: Tier-1 generated from the (unioned) registry + each dump's registered
    // Tier-2 view set, deduped by content across input files. The path-independent body goes in the
    // Parquet footer (`backbeat.views`); the sidecar is that prefixed with a bootstrap that binds
    // the base `events` view to this Parquet, so it is ready to `.read` as-is.
    let mut seen_views = HashSet::new();
    let mut tier2: Vec<String> = Vec::new();
    for d in dumps {
        for sql in &d.views {
            if seen_views.insert(sql.clone()) {
                tier2.push(sql.clone());
            }
        }
    }
    let ddl = crate::views::assemble(&schemas, &tier2);

    write_parquet(output, &batch, footer_host, zstd_level, &ddl)?;

    // Sidecar `.sql` next to the Parquet: `<output>.views.sql`. Bootstrap + the same DDL. The
    // bootstrap references the Parquet by file name only (sidecar and Parquet are written together),
    // so the recommended `duckdb out.parquet -init out.views.sql` works from their directory
    // regardless of whether `output` was given as a relative or absolute path.
    let sidecar = sidecar_path(output);
    let parquet_name = output
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| output.to_string_lossy().into_owned());
    let sidecar_body = format!("{}{ddl}", crate::views::bootstrap(&parquet_name));
    std::fs::write(&sidecar, sidecar_body)
        .with_context(|| format!("writing views sidecar {}", sidecar.display()))?;

    Ok(rows.len())
}

/// The `.views.sql` sidecar path for a Parquet output: `out.parquet` → `out.views.sql`.
fn sidecar_path(output: &Path) -> std::path::PathBuf {
    output.with_extension("views.sql")
}

/// One merged row across all input dumps, borrowing its field bytes and intern table from the
/// owning [`Loaded`].
struct Row<'a> {
    ts_nanos: u64,
    instance_id: u64,
    /// Index into the unioned `schemas`.
    schema_idx: usize,
    fields: &'a [u8],
    intern: &'a HashMap<u32, String>,
}

/// A decoded field value, mapped onto one of Parquet's four column kinds.
enum Value {
    U64(u64),
    I64(i64),
    Bool(bool),
    Str(String),
}

/// Decodes a field from a record's field bytes, or `None` if the bytes are too short *or* the field
/// equals its declared sentinel — the producer's in-band "absent" marker — so the column lands as
/// SQL NULL (see [`OwnedField::sentinel`]). The sentinel compares against the raw little-endian
/// integer image, matching how it was declared and serialized.
fn decode_field(field: &OwnedField, bytes: &[u8], intern: &HashMap<u32, String>) -> Option<Value> {
    let start = field.offset as usize;
    let end = start + field.width as usize;
    let slice = bytes.get(start..end)?;
    // Sentinel → NULL: only meaningful for inline integers ≤ 8 bytes, which is what `read_uint`
    // reads. A sentinel on a wider/other type simply never matches, so it is a harmless no-op.
    if let Some(sentinel) = field.sentinel {
        if field.width as usize <= 8 {
            if let Some(raw) = read_uint(slice, field.width as usize) {
                if raw == sentinel {
                    return None;
                }
            }
        }
    }
    Some(match field.ty {
        FieldType::U8 => Value::U64(slice[0] as u64),
        FieldType::U16 => Value::U64(u16::from_le_bytes(slice.try_into().ok()?) as u64),
        FieldType::U32 => Value::U64(u32::from_le_bytes(slice.try_into().ok()?) as u64),
        FieldType::U64 => Value::U64(u64::from_le_bytes(slice.try_into().ok()?)),
        FieldType::I8 => Value::I64(slice[0] as i8 as i64),
        FieldType::I16 => Value::I64(i16::from_le_bytes(slice.try_into().ok()?) as i64),
        FieldType::I32 => Value::I64(i32::from_le_bytes(slice.try_into().ok()?) as i64),
        FieldType::I64 => Value::I64(i64::from_le_bytes(slice.try_into().ok()?)),
        FieldType::Bool => Value::Bool(slice[0] != 0),
        FieldType::Bytes => Value::Str(hex(slice)),
        FieldType::Enum { repr } => {
            let raw = read_uint(slice, repr as usize)?;
            let label = field
                .enum_labels
                .iter()
                .find(|l| l.value == raw)
                .map(|l| l.label.clone())
                .unwrap_or_else(|| raw.to_string());
            Value::Str(label)
        }
        FieldType::Interned { .. } => {
            let id = u32::from_le_bytes(slice.get(..4)?.try_into().ok()?);
            Value::Str(intern.get(&id).cloned().unwrap_or_else(|| format!("#{id}")))
        }
        // FieldType is #[non_exhaustive]; a future kind we don't understand renders as hex bytes.
        _ => Value::Str(hex(slice)),
    })
}

/// Reads a little-endian unsigned integer of `width` (1, 2, 4, or 8) bytes.
fn read_uint(slice: &[u8], width: usize) -> Option<u64> {
    let mut buf = [0u8; 8];
    buf.get_mut(..width)?.copy_from_slice(slice.get(..width)?);
    Some(u64::from_le_bytes(buf))
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// The Parquet column kind a field decodes to.
fn arrow_type(ty: &FieldType) -> DataType {
    match ty {
        FieldType::U8 | FieldType::U16 | FieldType::U32 | FieldType::U64 => DataType::UInt64,
        FieldType::I8 | FieldType::I16 | FieldType::I32 | FieldType::I64 => DataType::Int64,
        FieldType::Bool => DataType::Boolean,
        FieldType::Bytes | FieldType::Enum { .. } | FieldType::Interned { .. } => DataType::Utf8,
        // FieldType is #[non_exhaustive]; unknown future kinds render as text (see decode_field).
        _ => DataType::Utf8,
    }
}

/// A display name per schema index: the `qualified_name`, suffixed with `#<id-hex>` only when more
/// than one schema shares that name (distinct content-addressed ids → genuinely distinct event
/// types). Used for both the `event` column value and the per-event struct column name, so a merged
/// dump with two versions of an event produces two unambiguous, separately-queryable columns.
fn display_names(schemas: &[OwnedSchema]) -> Vec<String> {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for s in schemas {
        *counts.entry(s.qualified_name.as_str()).or_default() += 1;
    }
    schemas
        .iter()
        .map(|s| {
            if counts[s.qualified_name.as_str()] > 1 {
                format!("{}#{:016x}", s.qualified_name, s.id.get())
            } else {
                s.qualified_name.clone()
            }
        })
        .collect()
}

/// Whether a field's role makes it a top-level promoted column (keys and span ids) versus a field
/// nested under its per-event struct.
fn is_promoted(role: FieldRole) -> bool {
    matches!(
        role,
        FieldRole::Key | FieldRole::SpanId | FieldRole::ParentSpanId
    )
}

/// Arrow field-level metadata mirroring a field's backbeat semantics (role, unit, description), so
/// the Parquet schema is self-describing without the original dump.
fn field_metadata(f: &OwnedField) -> HashMap<String, String> {
    let mut m = HashMap::new();
    let role = match f.role {
        FieldRole::None => None,
        FieldRole::Key => Some("key"),
        FieldRole::SpanId => Some("span_id"),
        FieldRole::ParentSpanId => Some("parent_span_id"),
        _ => None,
    };
    if let Some(role) = role {
        m.insert("backbeat.role".to_string(), role.to_string());
    }
    if let Some(unit) = &f.unit {
        m.insert("backbeat.unit".to_string(), unit.clone());
    }
    if let Some(sentinel) = f.sentinel {
        // Record the raw "absent" marker so the column stays self-describing even though convert
        // has already mapped matching values to NULL in the data.
        m.insert("backbeat.sentinel".to_string(), sentinel.to_string());
    }
    if let Some(desc) = &f.description {
        m.insert("backbeat.description".to_string(), desc.clone());
    }
    m
}

/// Arrow metadata for a per-event struct column: the span phase and the event's description.
fn event_metadata(s: &OwnedSchema) -> HashMap<String, String> {
    let mut m = HashMap::new();
    let phase = match s.phase {
        Phase::None => None,
        Phase::Enter => Some("enter"),
        Phase::Exit => Some("exit"),
        _ => None,
    };
    if let Some(phase) = phase {
        m.insert("backbeat.span".to_string(), phase.to_string());
    }
    if let Some(desc) = &s.description {
        m.insert("backbeat.description".to_string(), desc.clone());
    }
    m
}

/// A typed column builder that can append a decoded [`Value`] or a null.
enum Col {
    U64(UInt64Builder),
    I64(Int64Builder),
    Bool(BooleanBuilder),
    Str(StringBuilder),
}

impl Col {
    fn new(dt: &DataType) -> Self {
        match dt {
            DataType::UInt64 => Col::U64(UInt64Builder::new()),
            DataType::Int64 => Col::I64(Int64Builder::new()),
            DataType::Boolean => Col::Bool(BooleanBuilder::new()),
            DataType::Utf8 => Col::Str(StringBuilder::new()),
            other => unreachable!("unexpected column type {other:?}"),
        }
    }

    fn append(&mut self, v: Value) {
        match (self, v) {
            (Col::U64(b), Value::U64(x)) => b.append_value(x),
            (Col::I64(b), Value::I64(x)) => b.append_value(x),
            (Col::Bool(b), Value::Bool(x)) => b.append_value(x),
            (Col::Str(b), Value::Str(x)) => b.append_value(x),
            // Type drift between schema and value: record a null rather than panicking.
            (c, _) => c.append_null(),
        }
    }

    fn append_null(&mut self) {
        match self {
            Col::U64(b) => b.append_null(),
            Col::I64(b) => b.append_null(),
            Col::Bool(b) => b.append_null(),
            Col::Str(b) => b.append_null(),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Col::U64(b) => Arc::new(b.finish()),
            Col::I64(b) => Arc::new(b.finish()),
            Col::Bool(b) => Arc::new(b.finish()),
            Col::Str(b) => Arc::new(b.finish()),
        }
    }
}

/// Builds the sparse-wide [`RecordBatch`] from the merged rows.
fn build_batch(schemas: &[OwnedSchema], rows: &[Row]) -> Result<RecordBatch> {
    // Per-schema display name. Two schemas can share a `qualified_name` (same event, different
    // builds → different content-addressed ids); they are genuinely distinct event types, so we
    // disambiguate the name with a `#<id>` suffix *only* when it collides. Unique names stay clean.
    let names = display_names(schemas);

    // --- Plan the columns. ---
    // Promoted columns, unioned by name across all events (first declaration wins the type). Keys
    // and span ids are all promoted to the top level so they are queryable/join-able directly; the
    // rest of each event's fields nest under its per-event struct.
    let mut key_names: Vec<String> = Vec::new();
    let mut key_type: HashMap<String, DataType> = HashMap::new();
    for s in schemas {
        for f in s.fields.iter().filter(|f| is_promoted(f.role)) {
            if !key_type.contains_key(&f.name) {
                key_names.push(f.name.clone());
                key_type.insert(f.name.clone(), arrow_type(&f.ty));
            }
        }
    }

    // Dense common builders.
    let mut seq = UInt64Builder::new();
    let mut ts = UInt64Builder::new();
    let mut event = StringBuilder::new();
    let mut event_id = UInt64Builder::new();
    let mut instance_id = UInt64Builder::new();
    let mut key_cols: Vec<Col> = key_names.iter().map(|n| Col::new(&key_type[n])).collect();

    // Per-event struct child builders + the struct's own row validity.
    struct EventCols {
        /// (field, child builder) for each non-promoted field of this event.
        children: Vec<(OwnedField, Col)>,
        /// Whether each row belongs to this event (the struct's null mask).
        valid: Vec<bool>,
    }
    let mut event_cols: Vec<EventCols> = schemas
        .iter()
        .map(|s| EventCols {
            children: s
                .fields
                .iter()
                .filter(|f| !is_promoted(f.role))
                .map(|f| (f.clone(), Col::new(&arrow_type(&f.ty))))
                .collect(),
            valid: Vec::with_capacity(rows.len()),
        })
        .collect();

    // --- Fill row by row. ---
    for (i, row) in rows.iter().enumerate() {
        let s = &schemas[row.schema_idx];
        seq.append_value(i as u64);
        ts.append_value(row.ts_nanos);
        event.append_value(&names[row.schema_idx]);
        event_id.append_value(s.id.get());
        instance_id.append_value(row.instance_id);

        // Top-level promoted columns: value if this event declares the column, else null.
        for (name, col) in key_names.iter().zip(key_cols.iter_mut()) {
            match s
                .fields
                .iter()
                .find(|f| is_promoted(f.role) && &f.name == name)
            {
                Some(f) => match decode_field(f, row.fields, row.intern) {
                    Some(v) => col.append(v),
                    None => col.append_null(),
                },
                None => col.append_null(),
            }
        }

        // Per-event structs: fill this event's struct, null the rest.
        for (idx, ec) in event_cols.iter_mut().enumerate() {
            let mine = idx == row.schema_idx;
            ec.valid.push(mine);
            for (f, col) in ec.children.iter_mut() {
                if mine {
                    match decode_field(f, row.fields, row.intern) {
                        Some(v) => col.append(v),
                        None => col.append_null(),
                    }
                } else {
                    col.append_null();
                }
            }
        }
    }

    // --- Assemble arrays + arrow schema fields. ---
    let mut fields: Vec<Field> = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();

    fields.push(Field::new("seq", DataType::UInt64, false));
    arrays.push(Arc::new(seq.finish()));
    fields.push(Field::new("ts_nanos", DataType::UInt64, false));
    arrays.push(Arc::new(ts.finish()));
    fields.push(Field::new("event", DataType::Utf8, false));
    arrays.push(Arc::new(event.finish()));
    fields.push(Field::new("event_id", DataType::UInt64, false));
    arrays.push(Arc::new(event_id.finish()));
    fields.push(Field::new("instance_id", DataType::UInt64, false));
    arrays.push(Arc::new(instance_id.finish()));

    for (name, mut col) in key_names.iter().zip(key_cols) {
        // A promoted column may be declared by several events; describe it from the first.
        let decl = schemas
            .iter()
            .find_map(|s| s.fields.iter().find(|f| &f.name == name));
        let mut field = Field::new(name, key_type[name].clone(), true);
        if let Some(f) = decl {
            field = field.with_metadata(field_metadata(f));
        }
        fields.push(field);
        arrays.push(col.finish());
    }

    for (idx, mut ec) in event_cols.into_iter().enumerate() {
        let s = &schemas[idx];
        // An event whose fields are all promoted (e.g. a span enter with only span/parent ids) or a
        // marker event with no fields has nothing left to nest. Parquet can't write an empty struct,
        // and there is nothing to put in one — the dense `event` column already marks which rows are
        // this event — so we simply omit its struct column.
        if ec.children.is_empty() {
            continue;
        }
        let child_fields: Fields = ec
            .children
            .iter()
            .map(|(f, _)| {
                Field::new(&f.name, arrow_type(&f.ty), true).with_metadata(field_metadata(f))
            })
            .collect::<Vec<_>>()
            .into();
        let child_arrays: Vec<ArrayRef> = ec.children.iter_mut().map(|(_, c)| c.finish()).collect();
        let nulls = NullBuffer::from(ec.valid);
        let struct_array = StructArray::new(child_fields.clone(), child_arrays, Some(nulls));
        let dt = DataType::Struct(child_fields);
        // Carry the event's span phase + description as struct-column metadata, so the Parquet is
        // self-describing without the original dump. Use the (collision-disambiguated) display name.
        let mut field = Field::new(&names[idx], dt, true);
        field = field.with_metadata(event_metadata(s));
        fields.push(field);
        arrays.push(Arc::new(struct_array));
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays).context("assembling record batch")
}

/// Writes `batch` to `output` as Parquet, recording `host` in the footer key-value metadata.
///
/// The per-row `instance_id` is a column (dumps may be merged), not footer metadata. We deliberately
/// do *not* embed the original dump: the records already are the Parquet rows, and the per-column
/// semantics (role, unit, span phase, description) ride along as Arrow *field* metadata on the
/// schema (see [`build_batch`]). Copying the raw shard rings into the footer would roughly double
/// the file for no gain.
fn write_parquet(
    output: &Path,
    batch: &RecordBatch,
    host: &str,
    zstd_level: i32,
    ddl: &str,
) -> Result<()> {
    let mut kv = vec![KeyValue::new(
        "backbeat.format".to_string(),
        "1".to_string(),
    )];
    if !host.is_empty() {
        kv.push(KeyValue::new("backbeat.host".to_string(), host.to_string()));
    }
    // The query DDL travels in the footer so the Parquet is self-describing: extract it with
    // `parquet_kv_metadata()` (DuckDB can't execute footer SQL directly, so it lands in a `.sql`
    // first — see the `.views.sql` sidecar convert also writes). Path-independent: it references a
    // base `events` view the reader binds to whatever source.
    if !ddl.is_empty() {
        kv.push(KeyValue::new("backbeat.views".to_string(), ddl.to_string()));
    }
    // Trace data is highly repetitive (dictionary-friendly event names, low-cardinality ids,
    // sorted timestamps), so zstd compresses it dramatically.
    let level = parquet::basic::ZstdLevel::try_new(zstd_level)
        .with_context(|| format!("invalid zstd level {zstd_level} (valid range is 1–22)"))?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(level))
        .set_key_value_metadata(Some(kv))
        .build();
    let file = File::create(output).with_context(|| format!("creating {}", output.display()))?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(())
}
