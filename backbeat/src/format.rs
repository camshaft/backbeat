//! The on-disk dump format.
//!
//! A backbeat dump is self-describing: it carries the schema needed to decode its own records, so
//! the reader needs no compiled-in knowledge of the producing crate. The file is a small envelope
//! header pointing at a sequence of sections:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │ Envelope header                                                       │
//! │   magic[8]      = b"BACKBEAT"                                          │
//! │   format u16    = FORMAT_VERSION   (governs the *envelope* only)      │
//! │   flags  u16    = endianness, etc. (see HeaderFlags)                  │
//! │   section_count u16                                                   │
//! │   reserved u16                                                        │
//! │   then `section_count` × SectionEntry { kind u16, _pad u16,           │
//! │                                          offset u64, len u64 }         │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │ SCHEMA registry section                                               │
//! │   for each event type compiled into the producer:                     │
//! │     { id u64, record_size u16, name, fields[] }                       │
//! │   (the serialized form of crate::schema::EventSchema)                 │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │ INTERN table section(s) (optional) — one per instance                 │
//! │   { instance_id u64, (id u32 → bytes)* }  — resolves Interned fields  │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │ SHARD section(s) — one per capture ring                               │
//! │   { instance_id u64, shard_id u32, head u64, capacity u64,            │
//! │     region[capacity] }                                                │
//! │   the raw ring snapshot; records are `[event_id u64][payload]` walked  │
//! │   newest-first via the trailing length suffix (see crate::ring)       │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │ META section(s) (optional) — one per instance                         │
//! │   { instance_id u64, host label }  — spans key on (instance_id, …)    │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! The format is inherently **multi-instance**: Meta, Intern, and Shard sections are each tagged
//! with the `instance_id` of the process that produced them, while the schema registry is unified
//! (content-addressed by [`EventId`](crate::id::EventId)). A single-process dump carries one Meta,
//! one Intern table, and its shards; `backbeat merge` splices several dumps into one by unioning the
//! registries and copying every instance's tagged sections through verbatim — so a merged dump
//! reconstructs each process's records and spans exactly as the source dumps would.
//!
//! Crucially, `FORMAT_VERSION` governs only the *envelope and section framing* — event layouts
//! evolve freely because each is described by its own schema in the registry. There is no
//! "append, never renumber" discipline to maintain by hand: add, remove, or reorder an event's
//! fields and the embedded schema simply reflects it.

/// File magic at the very front of every dump.
pub const MAGIC: [u8; 8] = *b"BACKBEAT";

/// Version of the envelope/section framing (NOT of any event layout — those are self-described).
/// Bump only when the envelope, section table, or a section's own framing changes.
pub const FORMAT_VERSION: u16 = 1;

/// Flags in the envelope header.
pub mod header_flags {
    /// Records and integer sections are little-endian. (Big-endian dumps are not produced today;
    /// the flag reserves space to describe them.)
    pub const LITTLE_ENDIAN: u16 = 1 << 0;
}

/// Identifies what a section contains. Stored as the `kind` of a `SectionEntry`.
///
/// Stable on-disk values — append, never renumber (this is the *one* place that discipline
/// applies, and it governs section kinds, not event fields).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum SectionKind {
    /// The schema registry: how to decode every event id present in the dump.
    Schema = 0,
    /// The intern table: `u32` id → bytes, for [`crate::schema::FieldType::Interned`] fields.
    Intern = 1,
    /// A single capture ring's raw snapshot, prefixed with its shard id, head, and capacity.
    Shard = 2,
    /// Dump-level metadata: the `instance_id` (one per `Recorder`/process) plus an optional host
    /// label. One process is one instance, so this lives at the dump level rather than on every
    /// record; the trace converter keys spans by `(instance_id, span_id)`.
    Meta = 3,
    /// Consumer-supplied query DDL: opaque SQL text (typically DuckDB `CREATE VIEW`/`CREATE MACRO`
    /// statements) a producer registers via `register_views!`, carried verbatim so a dump describes
    /// not just how to decode its events but how to query them. backbeat never parses this text; the
    /// CLI's `convert` appends it after the schema-derived views it generates.
    Views = 4,
}

impl SectionKind {
    /// Maps a raw on-disk value back to a `SectionKind`, or `None` if unknown (a reader skips
    /// section kinds it does not recognize, so forward-compatibility is free).
    pub fn from_u16(v: u16) -> Option<Self> {
        Some(match v {
            0 => SectionKind::Schema,
            1 => SectionKind::Intern,
            2 => SectionKind::Shard,
            3 => SectionKind::Meta,
            4 => SectionKind::Views,
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn section_kind_round_trips() {
        for k in [
            SectionKind::Schema,
            SectionKind::Intern,
            SectionKind::Shard,
            SectionKind::Meta,
            SectionKind::Views,
        ] {
            assert_eq!(SectionKind::from_u16(k as u16), Some(k));
        }
        assert_eq!(SectionKind::from_u16(999), None);
    }
}
