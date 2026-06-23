// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! End-to-end: define events with the derive, record them into a `Recorder`, dump to `.bb`, then
//! read it back via the schema-driven reader and convert to Parquet — asserting on both. This is
//! the full vertical slice the project is built around.

use backbeat::{
    recorder::{ManualClock, Recorder},
    wire::DumpReader,
    zerocopy::{Immutable, IntoBytes},
    Event,
};
use std::sync::Arc;

/// Which side of a connection.
#[derive(backbeat::EventEnum, IntoBytes, Immutable, Clone, Copy)]
#[repr(u8)]
#[allow(dead_code)] // `Client` is referenced only via its label in assertions
enum Role {
    Client = 0,
    Server = 1,
}

/// A connection was opened.
#[derive(Event, IntoBytes, Immutable)]
#[event(namespace = "test::net")]
#[repr(C)]
struct Connect {
    /// The connection id.
    #[event(key)]
    conn_id: u64,
    #[event(unit = "bytes")]
    window: u32,
    role: Role,
    _pad: [u8; 3],
}

/// A packet was sent.
#[derive(Event, IntoBytes, Immutable)]
#[event(namespace = "test::net")]
#[repr(C)]
struct PacketSent {
    #[event(key)]
    conn_id: u64,
    #[event(key)]
    packet_number: u64,
    len: u32,
    is_fin: bool,
    _pad: [u8; 3],
}

/// A request began.
#[derive(Event, IntoBytes, Immutable)]
#[event(namespace = "test::work", span = enter)]
#[repr(C)]
struct RequestStart {
    #[event(span_id)]
    span: u64,
    #[event(parent_span_id)]
    parent: u64,
}

/// A request finished.
#[derive(Event, IntoBytes, Immutable)]
#[event(namespace = "test::work", span = exit)]
#[repr(C)]
struct RequestEnd {
    #[event(span_id)]
    span: u64,
    /// Bytes written during the request — captured at drop time.
    bytes: u64,
}

/// Drives the recorder with a manual clock so timestamps are deterministic and ordered.
fn dump_with_three_records() -> Vec<u8> {
    let clock = Arc::new(ManualClock::new(1000));
    let rec = Recorder::new(1, 64 * 1024).with_clock(clock.clone());
    rec.set_enabled(true);
    // Advance the clock between records so global order is well-defined.
    rec.record(&Connect {
        conn_id: 7,
        window: 65535,
        role: Role::Server,
        _pad: [0; 3],
    });
    clock.advance(10);
    rec.record(&PacketSent {
        conn_id: 7,
        packet_number: 0,
        len: 1200,
        is_fin: false,
        _pad: [0; 3],
    });
    clock.advance(10);
    rec.record(&PacketSent {
        conn_id: 7,
        packet_number: 1,
        len: 40,
        is_fin: true,
        _pad: [0; 3],
    });

    // The inventory registry auto-collected both event types since they derive Event in a std bin.
    let schemas: Vec<_> = backbeat::registry::schemas().collect();
    rec.dump(schemas, std::iter::empty(), "")
}

#[test]
fn registry_autopopulates_from_derive() {
    let names: Vec<_> = backbeat::registry::schemas()
        .map(|s| s.qualified_name)
        .collect();
    assert!(names.contains(&"test::net::Connect"));
    assert!(names.contains(&"test::net::PacketSent"));
}

#[test]
fn dump_round_trips_through_reader() {
    let bytes = dump_with_three_records();
    let reader = DumpReader::new(bytes).unwrap();

    // Registry carries both events with descriptions/keys intact.
    let schemas = reader.schemas().unwrap();
    let connect = schemas
        .iter()
        .find(|s| s.qualified_name == "test::net::Connect")
        .unwrap();
    assert_eq!(
        connect.description.as_deref(),
        Some("A connection was opened.")
    );
    assert_eq!(
        connect
            .fields
            .iter()
            .find(|f| f.name == "conn_id")
            .unwrap()
            .role,
        backbeat::FieldRole::Key
    );

    // Walk the single shard and count records via the shared record framing.
    use backbeat::{record::RecordView, ring::walk};
    let shards = reader.shards().unwrap();
    assert_eq!(shards.len(), 1);
    let known: std::collections::HashSet<u64> = schemas.iter().map(|s| s.id.get()).collect();
    let sizes: std::collections::HashMap<u64, usize> = schemas
        .iter()
        .map(|s| (s.id.get(), s.record_size as usize))
        .collect();
    let mut tss = Vec::new();
    walk(
        &shards[0].region,
        shards[0].head as usize,
        shards[0].capacity as usize,
        |p| match RecordView::parse(&p[..]) {
            Some(rec)
                if known.contains(&rec.event_id.get())
                    && sizes.get(&rec.event_id.get()) == Some(&rec.fields.len()) =>
            {
                tss.push(rec.ts_nanos);
                true
            }
            _ => false,
        },
    );
    // Three records, newest-first: ts 1020, 1010, 1000.
    assert_eq!(tss, [1020, 1010, 1000]);
}

#[test]
fn span_guard_records_enter_and_exit_with_paired_id() {
    use backbeat::{record::RecordView, ring::walk, Phase};
    use std::cell::Cell;

    let clock = Arc::new(ManualClock::new(100));
    let rec = Recorder::new(1, 64 * 1024).with_clock(clock.clone());
    rec.set_enabled(true);

    let bytes_written = Cell::new(0u64);
    {
        let span = rec.enter(
            |id| RequestStart {
                span: id,
                parent: 0,
            },
            |id| RequestEnd {
                span: id,
                bytes: bytes_written.get(),
            },
        );
        // A child event would thread `span.id()` into its parent_span_id; assert the id is exposed.
        assert_ne!(span.id(), 0);
        clock.advance(50);
        bytes_written.set(4096);
        // span drops here → exit recorded at ts 150 with bytes = 4096.
    }

    let schemas: Vec<_> = backbeat::registry::schemas().collect();
    let start_id = RequestStart::ID.get();
    let end_id = RequestEnd::ID.get();
    let dump = rec.dump(schemas.iter().copied(), std::iter::empty(), "");

    let reader = DumpReader::new(dump).unwrap();
    let owned_schemas = reader.schemas().unwrap();
    let by_id: std::collections::HashMap<u64, &backbeat::wire::OwnedSchema> =
        owned_schemas.iter().map(|s| (s.id.get(), s)).collect();
    let shards = reader.shards().unwrap();

    // Collect (event_id, ts, span_id) for each record by reading the span_id field via the schema.
    let mut records: Vec<(u64, u64, u64)> = Vec::new();
    walk(
        &shards[0].region,
        shards[0].head as usize,
        shards[0].capacity as usize,
        |p| match RecordView::parse(&p[..]) {
            Some(r) if by_id.contains_key(&r.event_id.get()) => {
                let s = by_id[&r.event_id.get()];
                if r.fields.len() != s.record_size as usize {
                    return false;
                }
                let span_field = s.span_id().expect("span event has a span_id");
                let off = span_field.offset as usize;
                let span_id = u64::from_le_bytes(r.fields[off..off + 8].try_into().unwrap());
                records.push((r.event_id.get(), r.ts_nanos, span_id));
                true
            }
            _ => false,
        },
    );

    // Newest-first: exit then enter. Both carry the same span id; timestamps bracket the work.
    assert_eq!(records.len(), 2);
    let (exit, enter) = (&records[0], &records[1]);
    assert_eq!(exit.0, end_id);
    assert_eq!(enter.0, start_id);
    assert_eq!(enter.2, exit.2, "enter and exit share the span id");
    assert_eq!(enter.1, 100, "enter recorded at start time");
    assert_eq!(exit.1, 150, "exit recorded at drop time");

    // The two schemas carry their phases.
    assert_eq!(by_id[&start_id].phase, Phase::Enter);
    assert_eq!(by_id[&end_id].phase, Phase::Exit);
}

#[test]
fn inspect_reports_counts() {
    let bytes = dump_with_three_records();
    let mut out = Vec::new();
    backbeat_cli::inspect::inspect(bytes, &mut out).unwrap();
    let text = String::from_utf8(out).unwrap();
    assert!(text.contains("test::net::Connect: 1"), "{text}");
    assert!(text.contains("test::net::PacketSent: 2"), "{text}");
    assert!(text.contains("3 total"), "{text}");
}

#[test]
fn convert_writes_readable_parquet() {
    use arrow::array::{Array, StringArray, StructArray, UInt64Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let bytes = dump_with_three_records();
    let dir = std::env::temp_dir().join("backbeat_e2e_test");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("out.parquet");

    let loaded = backbeat_cli::model::load(std::path::Path::new("in.bb"), bytes.into()).unwrap();
    let rows = backbeat_cli::convert::to_parquet(&[loaded], &path, "", 3).unwrap();
    assert_eq!(rows, 3);

    // Read it back with the parquet reader.
    let file = std::fs::File::open(&path).unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 3);

    let schema = batch.schema();
    // Dense common columns exist.
    for col in ["seq", "ts_nanos", "event", "event_id", "instance_id"] {
        assert!(schema.column_with_name(col).is_some(), "missing {col}");
    }
    // Promoted key columns exist (unioned across both events).
    assert!(schema.column_with_name("conn_id").is_some());
    assert!(schema.column_with_name("packet_number").is_some());
    // Per-event struct columns exist.
    assert!(schema.column_with_name("test::net::Connect").is_some());
    assert!(schema.column_with_name("test::net::PacketSent").is_some());

    // Rows are ordered by ts_nanos.
    let (idx, _) = schema.column_with_name("ts_nanos").unwrap();
    let ts = batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!((ts.value(0), ts.value(1), ts.value(2)), (1000, 1010, 1020));

    // The first row is the Connect; its event column says so and conn_id key is promoted.
    let (eidx, _) = schema.column_with_name("event").unwrap();
    let event = batch
        .column(eidx)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(event.value(0), "test::net::Connect");
    assert_eq!(event.value(1), "test::net::PacketSent");

    let (cidx, _) = schema.column_with_name("conn_id").unwrap();
    let conn = batch
        .column(cidx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(conn.value(0), 7);

    // The Connect struct column carries the non-key fields for row 0 and is null for the packets.
    let (sidx, _) = schema.column_with_name("test::net::Connect").unwrap();
    let connect_struct = batch
        .column(sidx)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(connect_struct.is_valid(0));
    assert!(!connect_struct.is_valid(1));
    // The enum field rendered as its label.
    let role = connect_struct
        .column_by_name("role")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(role.value(0), "Server");

    let _ = std::fs::remove_file(&path);
}

/// Records a span, converts to Chrome-trace JSON, and asserts the paired `b`/`e` async events.
fn dump_one_span() -> Vec<u8> {
    let clock = Arc::new(ManualClock::new(100));
    let rec = Recorder::new(1, 64 * 1024).with_clock(clock.clone());
    rec.set_enabled(true);
    {
        let _span = rec.enter(
            |id| RequestStart {
                span: id,
                parent: 0,
            },
            |id| RequestEnd {
                span: id,
                bytes: 4096,
            },
        );
        clock.advance(50);
    }
    let schemas: Vec<_> = backbeat::registry::schemas().collect();
    rec.dump(schemas, std::iter::empty(), "")
}

#[test]
fn convert_writes_chrome_trace_json() {
    let bytes = dump_one_span();
    let dir = std::env::temp_dir().join("backbeat_e2e_test");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("trace.json");

    let loaded = backbeat_cli::model::load(std::path::Path::new("in.bb"), bytes.into()).unwrap();
    let n = backbeat_cli::trace::to_trace(&[loaded], &path).unwrap();
    assert!(n >= 2, "expected at least the enter/exit pair, got {n}");

    let doc: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let events = doc["traceEvents"].as_array().unwrap();

    // Find the begin and end async events for the span; they must share one id.
    let begin = events
        .iter()
        .find(|e| e["ph"] == "b")
        .expect("a begin event");
    let end = events
        .iter()
        .find(|e| e["ph"] == "e")
        .expect("an end event");
    assert_eq!(begin["name"], "test::work::RequestStart");
    assert_eq!(end["name"], "test::work::RequestEnd");
    assert_eq!(begin["id"], end["id"], "enter/exit pair by the same id");
    // ts is microseconds: enter at 100ns = 0.1µs, exit at 150ns = 0.15µs.
    assert_eq!(begin["ts"], 0.1);
    assert_eq!(end["ts"], 0.15);
    // The exit carried its drop-time payload.
    assert_eq!(end["args"]["bytes"], 4096);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn convert_merges_multiple_dumps() {
    // Two independent dumps (distinct instance_ids) merge into one Parquet, rows keyed per-dump.
    let d1 = dump_with_three_records();
    let d2 = dump_with_three_records();
    let l1 = backbeat_cli::model::load(std::path::Path::new("a.bb"), d1.into()).unwrap();
    let l2 = backbeat_cli::model::load(std::path::Path::new("b.bb"), d2.into()).unwrap();
    assert_ne!(
        l1.instance_id, l2.instance_id,
        "each Recorder gets its own instance_id"
    );

    let dir = std::env::temp_dir().join("backbeat_e2e_test");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("merged.parquet");
    let rows = backbeat_cli::convert::to_parquet(&[l1, l2], &path, "", 3).unwrap();
    assert_eq!(rows, 6, "both dumps' records present");
    let _ = std::fs::remove_file(&path);
}
