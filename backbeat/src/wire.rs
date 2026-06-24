// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! Byte-level serialization of the dump format.
//!
//! [`format`](crate::format) documents the on-disk layout; this module implements it. A dump is
//! built with [`DumpWriter`] (append a schema registry, an optional intern table, and one section
//! per shard, then `finish()` into a byte buffer) and parsed with [`DumpReader`] (validate the
//! envelope, then pull sections out by kind).
//!
//! The schema descriptors ([`crate::schema`]) borrow `&'static str`, which a reader cannot
//! reconstruct, so reading produces the owned mirrors [`OwnedSchema`] / [`OwnedField`]. Everything
//! is little-endian; the [`header_flags::LITTLE_ENDIAN`](crate::format::header_flags) bit records
//! that in the envelope.
//!
//! All integers are fixed-width LE. Strings are `u16` length + UTF-8 bytes; an optional string is a
//! presence byte (`0`/`1`) followed by the string when present. These primitives keep the framing
//! trivial to validate — every read is bounds-checked and returns [`Error`] rather than panicking,
//! so a truncated or corrupt dump is a clean error, never UB.

use crate::{
    format::{header_flags, SectionKind, FORMAT_VERSION, MAGIC},
    id::EventId,
    schema::{EventSchema, FieldRole, FieldType, Phase},
};
use alloc::{string::String, vec::Vec};
use bytes::Bytes;

/// Size of the fixed envelope header: magic[8] + format u16 + flags u16 + section_count u16 +
/// reserved u16.
const HEADER_LEN: usize = 8 + 2 + 2 + 2 + 2;

/// Size of one section-table entry: kind u16 + _pad u16 + offset u64 + len u64.
const SECTION_ENTRY_LEN: usize = 2 + 2 + 8 + 8;

/// FieldType on-disk tags. Append, never renumber (these live in the schema section framing, which
/// `FORMAT_VERSION` governs).
mod ty_tag {
    pub const U8: u8 = 0;
    pub const U16: u8 = 1;
    pub const U32: u8 = 2;
    pub const U64: u8 = 3;
    pub const I8: u8 = 4;
    pub const I16: u8 = 5;
    pub const I32: u8 = 6;
    pub const I64: u8 = 7;
    pub const BOOL: u8 = 8;
    pub const BYTES: u8 = 9;
    pub const ENUM: u8 = 10;
    pub const INTERNED: u8 = 11;
}

/// FieldRole on-disk tags. `0`/`1` are deliberately `None`/`Key`, matching the `key as u8` that the
/// pre-span format wrote, so a non-span schema serializes identically. Append, never renumber.
mod role_tag {
    pub const NONE: u8 = 0;
    pub const KEY: u8 = 1;
    pub const SPAN_ID: u8 = 2;
    pub const PARENT_SPAN_ID: u8 = 3;
}

/// Phase on-disk tags. Append, never renumber.
mod phase_tag {
    pub const NONE: u8 = 0;
    pub const ENTER: u8 = 1;
    pub const EXIT: u8 = 2;
}

/// An error decoding a dump.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// The file is shorter than a structure being read requires.
    UnexpectedEof,
    /// The leading magic bytes are not [`MAGIC`].
    BadMagic,
    /// The envelope `format` version is one this build does not understand.
    UnsupportedVersion(u16),
    /// A section's declared `offset`/`len` falls outside the file.
    SectionOutOfBounds,
    /// A string field was not valid UTF-8.
    InvalidUtf8,
    /// A `FieldType` tag (or other tagged value) was not a known value.
    BadTag(u8),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Error::UnexpectedEof => write!(f, "unexpected end of dump"),
            Error::BadMagic => write!(f, "bad magic: not a backbeat dump"),
            Error::UnsupportedVersion(v) => write!(f, "unsupported dump format version {v}"),
            Error::SectionOutOfBounds => write!(f, "section offset/len out of bounds"),
            Error::InvalidUtf8 => write!(f, "string field is not valid UTF-8"),
            Error::BadTag(t) => write!(f, "unknown tag byte {t}"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for Error {}

type Result<T> = core::result::Result<T, Error>;

// ---------------------------------------------------------------------------
// Owned read-side mirrors of the schema descriptors.
// ---------------------------------------------------------------------------

/// A value→label pair for an enum field, owned (read side).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedEnumLabel {
    pub value: u64,
    pub label: String,
}

/// Owned mirror of [`crate::schema::FieldSchema`] produced when decoding a dump.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedField {
    pub name: String,
    pub description: Option<String>,
    pub ty: FieldType,
    pub offset: u16,
    pub width: u16,
    pub role: FieldRole,
    pub unit: Option<String>,
    /// In-band "absent" marker the converter maps to SQL NULL; see [`crate::schema::FieldSchema`].
    pub sentinel: Option<u64>,
    pub enum_labels: Vec<OwnedEnumLabel>,
}

/// Owned mirror of [`crate::schema::EventSchema`] produced when decoding a dump's registry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedSchema {
    pub id: EventId,
    pub qualified_name: String,
    pub description: Option<String>,
    pub record_size: u16,
    pub phase: Phase,
    pub fields: Vec<OwnedField>,
}

impl OwnedSchema {
    /// The span-id field, if this event declares one. Mirrors
    /// [`EventSchema::span_id`](crate::schema::EventSchema::span_id).
    pub fn span_id(&self) -> Option<&OwnedField> {
        self.fields.iter().find(|f| f.role == FieldRole::SpanId)
    }

    /// The parent-span-id field, if this event declares one. Mirrors
    /// [`EventSchema::parent_span`](crate::schema::EventSchema::parent_span).
    pub fn parent_span(&self) -> Option<&OwnedField> {
        self.fields
            .iter()
            .find(|f| f.role == FieldRole::ParentSpanId)
    }
}

/// Dump-level metadata from a [`Meta`](SectionKind::Meta) section. A dump carries one per instance
/// it contains — a single-process dump has one; a merged dump has one per merged process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedMeta {
    /// Random id identifying the process/`Recorder` that produced this instance's records; the
    /// converter keys spans by `(instance_id, span_id)`.
    pub instance_id: u64,
    /// Optional human label for the host/process (empty string if unset).
    pub host: String,
}

/// One decoded [`Intern`](SectionKind::Intern) section: the instance it belongs to plus its
/// `id → bytes` entries. Interned ids are per-process, so each is namespaced by `instance_id` to
/// keep two merged processes' tables from colliding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedIntern {
    /// The instance whose `Interned` fields these entries resolve.
    pub instance_id: u64,
    /// `(id, bytes)` pairs, in file order.
    pub entries: Vec<(u32, Vec<u8>)>,
}

/// One decoded shard section: the instance it belongs to, its id, write head, and raw ring region
/// (see [`crate::ring`]).
#[derive(Clone, Debug)]
pub struct ShardData {
    /// The instance (process) that produced this ring; `0` for a dump with no metadata.
    pub instance_id: u64,
    pub shard_id: u32,
    pub head: u64,
    pub capacity: u64,
    /// The raw ring snapshot, `capacity` bytes; a zero-copy [`Bytes`] slice of the dump buffer.
    /// Walk it with [`crate::ring::walk`], slicing this `region` for each record's payload.
    pub region: Bytes,
}

// ---------------------------------------------------------------------------
// Writing.
// ---------------------------------------------------------------------------

/// A pending section: its kind and its already-serialized body.
struct PendingSection {
    kind: SectionKind,
    body: Vec<u8>,
}

/// Builds a dump byte buffer: add the registry, an optional intern table, and shard sections, then
/// [`finish`](DumpWriter::finish).
#[derive(Default)]
pub struct DumpWriter {
    sections: Vec<PendingSection>,
}

impl DumpWriter {
    /// A new, empty writer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds the schema registry section: every event type that may appear in the shards.
    pub fn schema_registry<'a>(&mut self, schemas: impl IntoIterator<Item = &'a EventSchema>) {
        let mut body = Vec::new();
        let schemas: Vec<&EventSchema> = schemas.into_iter().collect();
        put_u32(&mut body, schemas.len() as u32);
        for s in schemas {
            put_schema(&mut body, s);
        }
        self.sections.push(PendingSection {
            kind: SectionKind::Schema,
            body,
        });
    }

    /// Adds the schema registry section from already-decoded [`OwnedSchema`]s. Used by `merge` to
    /// re-emit a registry unioned across input dumps; the serialized form is identical to
    /// [`schema_registry`](Self::schema_registry), so a re-read round-trips exactly.
    pub fn schema_registry_owned<'a>(
        &mut self,
        schemas: impl IntoIterator<Item = &'a OwnedSchema>,
    ) {
        let mut body = Vec::new();
        let schemas: Vec<&OwnedSchema> = schemas.into_iter().collect();
        put_u32(&mut body, schemas.len() as u32);
        for s in schemas {
            put_owned_schema(&mut body, s);
        }
        self.sections.push(PendingSection {
            kind: SectionKind::Schema,
            body,
        });
    }

    /// Adds an intern table section for one instance: `id → bytes`, resolving that instance's
    /// `Interned` fields at read time. Interned ids are per-process, so the section is namespaced by
    /// `instance_id` — a merged dump carries one such section per instance, never colliding.
    pub fn intern_table<'a>(
        &mut self,
        instance_id: u64,
        entries: impl IntoIterator<Item = (u32, &'a [u8])>,
    ) {
        let mut body = Vec::new();
        put_u64(&mut body, instance_id);
        let entries: Vec<(u32, &[u8])> = entries.into_iter().collect();
        put_u32(&mut body, entries.len() as u32);
        for (id, bytes) in entries {
            put_u32(&mut body, id);
            put_u32(&mut body, bytes.len() as u32);
            body.extend_from_slice(bytes);
        }
        self.sections.push(PendingSection {
            kind: SectionKind::Intern,
            body,
        });
    }

    /// Adds a views section: opaque query DDL (typically DuckDB `CREATE VIEW`/`CREATE MACRO` text)
    /// a producer registered via `register_views!`. The text is stored verbatim — the section length
    /// delimits it, so there is no inner framing — and backbeat never parses it. A producer that
    /// registers several view sets yields several sections, in registration order; the CLI
    /// concatenates them after the views it derives from the schema registry.
    pub fn views(&mut self, sql: &str) {
        self.sections.push(PendingSection {
            kind: SectionKind::Views,
            body: sql.as_bytes().to_vec(),
        });
    }

    /// Adds a metadata section for one instance: its `instance_id` and an optional host label (pass
    /// `""` for none). A merged dump carries one per merged process.
    pub fn meta(&mut self, instance_id: u64, host: &str) {
        let mut body = Vec::new();
        put_u64(&mut body, instance_id);
        put_str(&mut body, host);
        self.sections.push(PendingSection {
            kind: SectionKind::Meta,
            body,
        });
    }

    /// Adds one shard section: the instance it belongs to, its id, write head, and raw ring region
    /// (length is its capacity). The `instance_id` ties the ring to its [`meta`](Self::meta) and
    /// intern table, so merged dumps keep each process's records and spans separate.
    pub fn shard(&mut self, instance_id: u64, shard_id: u32, head: u64, region: &[u8]) {
        let mut body = Vec::with_capacity(28 + region.len());
        put_u64(&mut body, instance_id);
        put_u32(&mut body, shard_id);
        put_u64(&mut body, head);
        put_u64(&mut body, region.len() as u64);
        body.extend_from_slice(region);
        self.sections.push(PendingSection {
            kind: SectionKind::Shard,
            body,
        });
    }

    /// Appends an already-serialized section body verbatim under `kind`. Used by `merge` to splice
    /// instance-tagged Meta/Intern/Shard bodies from input dumps into a combined dump without
    /// decoding their contents.
    pub fn raw_section(&mut self, kind: SectionKind, body: Vec<u8>) {
        self.sections.push(PendingSection { kind, body });
    }

    /// Serializes the whole dump: envelope header, section table, then each section body.
    pub fn finish(self) -> Vec<u8> {
        let table_len = self.sections.len() * SECTION_ENTRY_LEN;
        // Section bodies begin right after the header and the (fixed-size) section table.
        let mut body_offset = HEADER_LEN + table_len;

        let mut out = Vec::new();
        out.extend_from_slice(&MAGIC);
        put_u16(&mut out, FORMAT_VERSION);
        put_u16(&mut out, header_flags::LITTLE_ENDIAN);
        put_u16(&mut out, self.sections.len() as u16);
        put_u16(&mut out, 0); // reserved

        // Section table: compute each body's absolute offset as we go.
        for s in &self.sections {
            put_u16(&mut out, s.kind as u16);
            put_u16(&mut out, 0); // _pad
            put_u64(&mut out, body_offset as u64);
            put_u64(&mut out, s.body.len() as u64);
            body_offset += s.body.len();
        }

        // Section bodies, in table order.
        for s in &self.sections {
            out.extend_from_slice(&s.body);
        }
        out
    }
}

/// Serializes one [`EventSchema`] into `out` (registry entry framing).
fn put_schema(out: &mut Vec<u8>, s: &EventSchema) {
    put_u64(out, s.id.get());
    put_str(out, s.qualified_name);
    put_opt_str(out, s.description);
    put_u16(out, s.record_size);
    out.push(encode_phase(s.phase));
    put_u16(out, s.fields.len() as u16);
    for f in s.fields {
        put_str(out, f.name);
        put_opt_str(out, f.description);
        put_field_type(out, f.ty);
        put_u16(out, f.offset);
        put_u16(out, f.width);
        out.push(encode_role(f.role));
        put_opt_str(out, f.unit);
        put_opt_u64(out, f.sentinel);
        put_u16(out, f.enum_labels.len() as u16);
        for l in f.enum_labels {
            put_u64(out, l.value);
            put_str(out, l.label);
        }
    }
}

/// Serializes one [`OwnedSchema`] into `out`, byte-for-byte identically to [`put_schema`] on the
/// equivalent borrowed [`EventSchema`]. The inverse of [`get_schema`], used by `merge` to re-emit a
/// unioned registry.
fn put_owned_schema(out: &mut Vec<u8>, s: &OwnedSchema) {
    put_u64(out, s.id.get());
    put_str(out, &s.qualified_name);
    put_opt_str(out, s.description.as_deref());
    put_u16(out, s.record_size);
    out.push(encode_phase(s.phase));
    put_u16(out, s.fields.len() as u16);
    for f in &s.fields {
        put_str(out, &f.name);
        put_opt_str(out, f.description.as_deref());
        put_field_type(out, f.ty);
        put_u16(out, f.offset);
        put_u16(out, f.width);
        out.push(encode_role(f.role));
        put_opt_str(out, f.unit.as_deref());
        put_opt_u64(out, f.sentinel);
        put_u16(out, f.enum_labels.len() as u16);
        for l in &f.enum_labels {
            put_u64(out, l.value);
            put_str(out, &l.label);
        }
    }
}

/// The on-disk tag byte for a [`FieldRole`].
fn encode_role(role: FieldRole) -> u8 {
    match role {
        FieldRole::None => role_tag::NONE,
        FieldRole::Key => role_tag::KEY,
        FieldRole::SpanId => role_tag::SPAN_ID,
        FieldRole::ParentSpanId => role_tag::PARENT_SPAN_ID,
    }
}

/// The on-disk tag byte for a [`Phase`].
fn encode_phase(phase: Phase) -> u8 {
    match phase {
        Phase::None => phase_tag::NONE,
        Phase::Enter => phase_tag::ENTER,
        Phase::Exit => phase_tag::EXIT,
    }
}

/// Encodes a [`FieldType`] as a tag byte plus any inline payload.
fn put_field_type(out: &mut Vec<u8>, ty: FieldType) {
    match ty {
        FieldType::U8 => out.push(ty_tag::U8),
        FieldType::U16 => out.push(ty_tag::U16),
        FieldType::U32 => out.push(ty_tag::U32),
        FieldType::U64 => out.push(ty_tag::U64),
        FieldType::I8 => out.push(ty_tag::I8),
        FieldType::I16 => out.push(ty_tag::I16),
        FieldType::I32 => out.push(ty_tag::I32),
        FieldType::I64 => out.push(ty_tag::I64),
        FieldType::Bool => out.push(ty_tag::BOOL),
        FieldType::Bytes => out.push(ty_tag::BYTES),
        FieldType::Enum { repr } => {
            out.push(ty_tag::ENUM);
            out.push(repr);
        }
        FieldType::Interned { dynamic } => {
            out.push(ty_tag::INTERNED);
            out.push(dynamic as u8);
        }
    }
}

fn put_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn put_str(out: &mut Vec<u8>, s: &str) {
    put_u16(out, s.len() as u16);
    out.extend_from_slice(s.as_bytes());
}
fn put_opt_str(out: &mut Vec<u8>, s: Option<&str>) {
    match s {
        Some(s) => {
            out.push(1);
            put_str(out, s);
        }
        None => out.push(0),
    }
}
fn put_opt_u64(out: &mut Vec<u8>, v: Option<u64>) {
    match v {
        Some(v) => {
            out.push(1);
            put_u64(out, v);
        }
        None => out.push(0),
    }
}

// ---------------------------------------------------------------------------
// Reading.
// ---------------------------------------------------------------------------

/// A parsed dump: the validated envelope plus access to its sections.
///
/// Owns the dump bytes as a refcounted [`Bytes`], so shard regions are handed out as zero-copy
/// slices of the same buffer — reading a multi-gigabyte dump never copies the ring data.
pub struct DumpReader {
    bytes: Bytes,
    flags: u16,
    /// `(kind_raw, offset, len)` for each section, in file order.
    sections: Vec<(u16, usize, usize)>,
}

impl DumpReader {
    /// Parses and validates a dump's envelope and section table. Takes anything convertible into
    /// [`Bytes`] (a `Vec<u8>`, a `Bytes`, a `&'static [u8]`); the reader then owns the buffer and
    /// slices records out of it without copying.
    pub fn new(bytes: impl Into<Bytes>) -> Result<Self> {
        let bytes = bytes.into();
        let mut cur = Cursor::new(&bytes[..]);
        let magic = cur.take(8)?;
        if magic != MAGIC {
            return Err(Error::BadMagic);
        }
        let format = cur.u16()?;
        if format != FORMAT_VERSION {
            return Err(Error::UnsupportedVersion(format));
        }
        let flags = cur.u16()?;
        let section_count = cur.u16()? as usize;
        let _reserved = cur.u16()?;

        let mut sections = Vec::with_capacity(section_count);
        for _ in 0..section_count {
            let kind = cur.u16()?;
            let _pad = cur.u16()?;
            let offset = cur.u64()? as usize;
            let len = cur.u64()? as usize;
            // Validate the section lies within the file.
            let end = offset.checked_add(len).ok_or(Error::SectionOutOfBounds)?;
            if end > bytes.len() {
                return Err(Error::SectionOutOfBounds);
            }
            sections.push((kind, offset, len));
        }

        Ok(Self {
            bytes,
            flags,
            sections,
        })
    }

    /// The envelope flag bits (see [`header_flags`](crate::format::header_flags)).
    pub fn flags(&self) -> u16 {
        self.flags
    }

    /// The number of sections in the dump.
    pub fn section_count(&self) -> usize {
        self.sections.len()
    }

    /// The raw bodies of every section of `kind`, in file order. Unknown section kinds are simply
    /// never matched, so forward-compatibility is free.
    fn bodies_of(&self, kind: SectionKind) -> impl Iterator<Item = &[u8]> + '_ {
        let bytes = &self.bytes;
        self.sections
            .iter()
            .filter(move |(k, _, _)| *k == kind as u16)
            .map(move |&(_, off, len)| &bytes[off..off + len])
    }

    /// Decodes the schema registry into owned schemas. Empty if the dump has no registry section.
    pub fn schemas(&self) -> Result<Vec<OwnedSchema>> {
        let Some(body) = self.bodies_of(SectionKind::Schema).next() else {
            return Ok(Vec::new());
        };
        let mut cur = Cursor::new(body);
        let count = cur.u32()? as usize;
        // Clamp the pre-allocation to what the section could actually hold (≥1 byte/schema). A
        // truncated/forged count still errors in the loop via `take`, but never OOMs up front.
        let mut out = Vec::with_capacity(count.min(cur.remaining()));
        for _ in 0..count {
            out.push(get_schema(&mut cur)?);
        }
        Ok(out)
    }

    /// Decodes every intern table, one [`OwnedIntern`] per [`Intern`](SectionKind::Intern) section,
    /// in file order. Empty if the dump has no intern section. A single-process dump has one entry;
    /// a merged dump has one per instance, each namespaced by its `instance_id`.
    pub fn intern_tables(&self) -> Result<Vec<OwnedIntern>> {
        let mut tables = Vec::new();
        for body in self.bodies_of(SectionKind::Intern) {
            let mut cur = Cursor::new(body);
            let instance_id = cur.u64()?;
            let count = cur.u32()? as usize;
            // Clamp the pre-allocation: each entry is ≥8 bytes (id + len), so `remaining()` is a
            // safe over-estimate that still bounds a forged count to the file size.
            let mut entries = Vec::with_capacity(count.min(cur.remaining()));
            for _ in 0..count {
                let id = cur.u32()?;
                let len = cur.u32()? as usize;
                entries.push((id, cur.take(len)?.to_vec()));
            }
            tables.push(OwnedIntern {
                instance_id,
                entries,
            });
        }
        Ok(tables)
    }

    /// Decodes every instance's metadata, one [`OwnedMeta`] per [`Meta`](SectionKind::Meta) section,
    /// in file order. Empty if the dump has no metadata. A single-process dump has one entry; a
    /// merged dump has one per merged process.
    pub fn metas(&self) -> Result<Vec<OwnedMeta>> {
        let mut out = Vec::new();
        for body in self.bodies_of(SectionKind::Meta) {
            let mut cur = Cursor::new(body);
            let instance_id = cur.u64()?;
            let host = cur.str()?;
            out.push(OwnedMeta { instance_id, host });
        }
        Ok(out)
    }

    /// Decodes every views section, one [`String`] per [`Views`](SectionKind::Views) section, in
    /// file order. Empty if the dump has none. Each is opaque query DDL the producer registered via
    /// `register_views!`; backbeat does not parse it. A merged dump preserves every input's sets.
    pub fn views(&self) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for body in self.bodies_of(SectionKind::Views) {
            out.push(
                core::str::from_utf8(body)
                    .map(String::from)
                    .map_err(|_| Error::InvalidUtf8)?,
            );
        }
        Ok(out)
    }

    /// Decodes every shard section, in file order. Each `region` is a zero-copy [`Bytes`] slice of
    /// the dump buffer — no ring data is copied, however large the dump.
    pub fn shards(&self) -> Result<Vec<ShardData>> {
        // Section body layout: instance_id u64, shard_id u32, head u64, capacity u64, then region.
        const PREFIX: usize = 8 + 4 + 8 + 8;
        let mut out = Vec::new();
        for &(kind, off, len) in &self.sections {
            if kind != SectionKind::Shard as u16 {
                continue;
            }
            let body = &self.bytes[off..off + len];
            let mut cur = Cursor::new(body);
            let instance_id = cur.u64()?;
            let shard_id = cur.u32()?;
            let head = cur.u64()?;
            let capacity = cur.u64()? as usize;
            // Bounds-check the region before slicing it out of the owned buffer.
            if PREFIX.checked_add(capacity).is_none_or(|end| end > len) {
                return Err(Error::SectionOutOfBounds);
            }
            let region = self.bytes.slice(off + PREFIX..off + PREFIX + capacity);
            out.push(ShardData {
                instance_id,
                shard_id,
                head,
                capacity: capacity as u64,
                region,
            });
        }
        Ok(out)
    }

    /// The raw, undecoded body of every section of `kind`, in file order. `merge` uses this to
    /// splice instance-tagged Meta/Intern/Shard bodies through verbatim.
    pub fn raw_bodies(&self, kind: SectionKind) -> Vec<Vec<u8>> {
        self.bodies_of(kind).map(|b| b.to_vec()).collect()
    }
}

/// Decodes one [`OwnedSchema`] from `cur` (inverse of [`put_schema`]).
fn get_schema(cur: &mut Cursor) -> Result<OwnedSchema> {
    let id = EventId(cur.u64()?);
    let qualified_name = cur.str()?;
    let description = cur.opt_str()?;
    let record_size = cur.u16()?;
    let phase = cur.phase()?;
    let field_count = cur.u16()? as usize;
    let mut fields = Vec::with_capacity(field_count.min(cur.remaining()));
    for _ in 0..field_count {
        let name = cur.str()?;
        let description = cur.opt_str()?;
        let ty = cur.field_type()?;
        let offset = cur.u16()?;
        let width = cur.u16()?;
        let role = cur.role()?;
        let unit = cur.opt_str()?;
        let sentinel = cur.opt_u64()?;
        let label_count = cur.u16()? as usize;
        let mut enum_labels = Vec::with_capacity(label_count.min(cur.remaining()));
        for _ in 0..label_count {
            let value = cur.u64()?;
            let label = cur.str()?;
            enum_labels.push(OwnedEnumLabel { value, label });
        }
        fields.push(OwnedField {
            name,
            description,
            ty,
            offset,
            width,
            role,
            unit,
            sentinel,
            enum_labels,
        });
    }
    Ok(OwnedSchema {
        id,
        qualified_name,
        description,
        record_size,
        phase,
        fields,
    })
}

/// A bounds-checked forward cursor over a byte slice.
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or(Error::UnexpectedEof)?;
        let slice = self.bytes.get(self.pos..end).ok_or(Error::UnexpectedEof)?;
        self.pos = end;
        Ok(slice)
    }

    /// Bytes left to read. Used to cap speculative `Vec::with_capacity` against an attacker-supplied
    /// element count: every element consumes at least one byte, so a count larger than this cannot
    /// possibly be honest and must not drive a multi-gigabyte pre-allocation off a tiny file.
    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn str(&mut self) -> Result<String> {
        let len = self.u16()? as usize;
        let bytes = self.take(len)?;
        core::str::from_utf8(bytes)
            .map(String::from)
            .map_err(|_| Error::InvalidUtf8)
    }

    fn opt_str(&mut self) -> Result<Option<String>> {
        if self.u8()? == 0 {
            Ok(None)
        } else {
            Ok(Some(self.str()?))
        }
    }

    fn opt_u64(&mut self) -> Result<Option<u64>> {
        if self.u8()? == 0 {
            Ok(None)
        } else {
            Ok(Some(self.u64()?))
        }
    }

    /// Reads a [`FieldRole`] tag. Unknown tags decode to [`FieldRole::None`] so a dump written by a
    /// newer producer (with roles this build doesn't know) degrades rather than failing.
    fn role(&mut self) -> Result<FieldRole> {
        Ok(match self.u8()? {
            role_tag::NONE => FieldRole::None,
            role_tag::KEY => FieldRole::Key,
            role_tag::SPAN_ID => FieldRole::SpanId,
            role_tag::PARENT_SPAN_ID => FieldRole::ParentSpanId,
            _ => FieldRole::None,
        })
    }

    /// Reads a [`Phase`] tag. Unknown tags decode to [`Phase::None`] (forward-compatible).
    fn phase(&mut self) -> Result<Phase> {
        Ok(match self.u8()? {
            phase_tag::NONE => Phase::None,
            phase_tag::ENTER => Phase::Enter,
            phase_tag::EXIT => Phase::Exit,
            _ => Phase::None,
        })
    }

    fn field_type(&mut self) -> Result<FieldType> {
        Ok(match self.u8()? {
            ty_tag::U8 => FieldType::U8,
            ty_tag::U16 => FieldType::U16,
            ty_tag::U32 => FieldType::U32,
            ty_tag::U64 => FieldType::U64,
            ty_tag::I8 => FieldType::I8,
            ty_tag::I16 => FieldType::I16,
            ty_tag::I32 => FieldType::I32,
            ty_tag::I64 => FieldType::I64,
            ty_tag::BOOL => FieldType::Bool,
            ty_tag::BYTES => FieldType::Bytes,
            ty_tag::ENUM => FieldType::Enum { repr: self.u8()? },
            ty_tag::INTERNED => FieldType::Interned {
                dynamic: self.u8()? != 0,
            },
            other => return Err(Error::BadTag(other)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{EnumLabel, FieldSchema};

    const FIELDS: &[FieldSchema] = &[
        FieldSchema {
            name: "packet_number",
            description: Some("the packet number"),
            ty: FieldType::U64,
            offset: 0,
            width: 8,
            role: FieldRole::Key,
            unit: None,
            sentinel: Some(u64::MAX),
            enum_labels: &[],
        },
        FieldSchema {
            name: "direction",
            description: None,
            ty: FieldType::Enum { repr: 1 },
            offset: 8,
            width: 1,
            role: FieldRole::None,
            unit: Some("dir"),
            sentinel: None,
            enum_labels: &[
                EnumLabel {
                    value: 0,
                    label: "in",
                },
                EnumLabel {
                    value: 1,
                    label: "out",
                },
            ],
        },
    ];

    const SCHEMA: EventSchema = EventSchema {
        id: EventId::of("test::Demo"),
        qualified_name: "test::Demo",
        description: Some("a demo event"),
        record_size: 9,
        phase: Phase::None,
        fields: FIELDS,
    };

    // A span-enter event: a `span_id` field plus a `parent_span_id`.
    const SPAN_FIELDS: &[FieldSchema] = &[
        FieldSchema {
            name: "span",
            description: None,
            ty: FieldType::U64,
            offset: 0,
            width: 8,
            role: FieldRole::SpanId,
            unit: None,
            sentinel: None,
            enum_labels: &[],
        },
        FieldSchema {
            name: "parent",
            description: None,
            ty: FieldType::U64,
            offset: 8,
            width: 8,
            role: FieldRole::ParentSpanId,
            unit: None,
            sentinel: None,
            enum_labels: &[],
        },
    ];

    const SPAN_SCHEMA: EventSchema = EventSchema {
        id: EventId::of("test::Work"),
        qualified_name: "test::Work",
        description: None,
        record_size: 16,
        phase: Phase::Enter,
        fields: SPAN_FIELDS,
    };

    #[test]
    fn round_trips_a_full_dump() {
        let mut w = DumpWriter::new();
        w.schema_registry([&SCHEMA]);
        w.intern_table(0xDEADBEEF, [(7u32, b"hello".as_slice()), (9, b"world")]);
        w.meta(0xDEADBEEF, "host-7");
        w.views("CREATE VIEW packets AS SELECT * FROM events;");
        w.shard(0xDEADBEEF, 0, 32, &[0xAA; 64]);
        w.shard(0xDEADBEEF, 3, 8, &[0xBB; 16]);
        let bytes = w.finish();

        let r = DumpReader::new(bytes).unwrap();
        assert_eq!(r.flags(), header_flags::LITTLE_ENDIAN);
        assert_eq!(r.section_count(), 6);

        // Registry round-trips, descriptions and enum labels included.
        let schemas = r.schemas().unwrap();
        assert_eq!(schemas.len(), 1);
        let s = &schemas[0];
        assert_eq!(s.id, SCHEMA.id);
        assert_eq!(s.qualified_name, "test::Demo");
        assert_eq!(s.description.as_deref(), Some("a demo event"));
        assert_eq!(s.record_size, 9);
        assert_eq!(s.phase, Phase::None);
        assert_eq!(s.fields.len(), 2);
        assert_eq!(s.fields[0].name, "packet_number");
        assert_eq!(
            s.fields[0].description.as_deref(),
            Some("the packet number")
        );
        assert_eq!(s.fields[0].role, FieldRole::Key);
        assert_eq!(s.fields[0].sentinel, Some(u64::MAX));
        assert_eq!(s.fields[1].sentinel, None);
        assert_eq!(s.fields[1].ty, FieldType::Enum { repr: 1 });
        assert_eq!(s.fields[1].unit.as_deref(), Some("dir"));
        assert_eq!(s.fields[1].enum_labels[1].label, "out");

        // Intern table round-trips, namespaced by instance.
        let intern = r.intern_tables().unwrap();
        assert_eq!(intern.len(), 1);
        assert_eq!(intern[0].instance_id, 0xDEADBEEF);
        assert_eq!(
            intern[0].entries,
            [(7, b"hello".to_vec()), (9, b"world".to_vec())]
        );

        // Meta round-trips.
        let metas = r.metas().unwrap();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].instance_id, 0xDEADBEEF);
        assert_eq!(metas[0].host, "host-7");

        // Views round-trip verbatim.
        let views = r.views().unwrap();
        assert_eq!(views, ["CREATE VIEW packets AS SELECT * FROM events;"]);

        // Shards round-trip in order, each tagged with its instance.
        let shards = r.shards().unwrap();
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].instance_id, 0xDEADBEEF);
        assert_eq!((shards[0].shard_id, shards[0].head), (0, 32));
        assert_eq!(&shards[0].region[..], &[0xAA; 64]);
        assert_eq!((shards[1].shard_id, shards[1].head), (3, 8));
        assert_eq!(shards[1].region.len(), 16);
    }

    #[test]
    fn round_trips_multiple_instances() {
        // A merged dump: one unified registry, but per-instance meta/intern/shard sections. Two
        // instances reuse intern id 5 for different strings — the namespacing must keep them apart.
        let mut w = DumpWriter::new();
        w.schema_registry([&SCHEMA]);
        w.meta(0xA, "host-a");
        w.intern_table(0xA, [(5u32, b"alpha".as_slice())]);
        w.shard(0xA, 0, 16, &[0xAA; 32]);
        w.meta(0xB, "host-b");
        w.intern_table(0xB, [(5u32, b"bravo".as_slice())]);
        w.shard(0xB, 0, 16, &[0xBB; 32]);
        let bytes = w.finish();

        let r = DumpReader::new(bytes).unwrap();
        let metas = r.metas().unwrap();
        assert_eq!(metas.len(), 2);
        assert_eq!(
            (metas[0].instance_id, metas[0].host.as_str()),
            (0xA, "host-a")
        );
        assert_eq!(
            (metas[1].instance_id, metas[1].host.as_str()),
            (0xB, "host-b")
        );

        // Same intern id, different instance, different string.
        let intern = r.intern_tables().unwrap();
        assert_eq!(intern.len(), 2);
        assert_eq!(intern[0].instance_id, 0xA);
        assert_eq!(intern[0].entries, [(5, b"alpha".to_vec())]);
        assert_eq!(intern[1].instance_id, 0xB);
        assert_eq!(intern[1].entries, [(5, b"bravo".to_vec())]);

        // Each shard carries its owning instance.
        let shards = r.shards().unwrap();
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].instance_id, 0xA);
        assert_eq!(&shards[0].region[..], &[0xAA; 32]);
        assert_eq!(shards[1].instance_id, 0xB);
        assert_eq!(&shards[1].region[..], &[0xBB; 32]);
    }

    #[test]
    fn round_trips_span_roles_and_phase() {
        let mut w = DumpWriter::new();
        w.schema_registry([&SPAN_SCHEMA]);
        let bytes = w.finish();

        let r = DumpReader::new(bytes).unwrap();
        let s = &r.schemas().unwrap()[0];
        assert_eq!(s.phase, Phase::Enter);
        assert_eq!(s.fields[0].role, FieldRole::SpanId);
        assert_eq!(s.fields[1].role, FieldRole::ParentSpanId);
    }

    #[test]
    fn meta_absent_decodes_to_empty() {
        let mut w = DumpWriter::new();
        w.schema_registry([&SCHEMA]);
        let bytes = w.finish();
        assert!(DumpReader::new(bytes).unwrap().metas().unwrap().is_empty());
    }

    #[test]
    fn rejects_bad_magic_and_truncation() {
        assert_eq!(
            DumpReader::new(b"nope".as_slice()).err(),
            Some(Error::UnexpectedEof)
        );
        assert_eq!(
            DumpReader::new(b"XXXXXXXX\x01\x00\x01\x00\x00\x00\x00\x00".as_slice()).err(),
            Some(Error::BadMagic)
        );

        let mut w = DumpWriter::new();
        w.schema_registry([&SCHEMA]);
        let bytes = w.finish();
        // Truncating the body makes the section run past EOF.
        let truncated = bytes[..bytes.len() - 4].to_vec();
        assert_eq!(
            DumpReader::new(truncated).err(),
            Some(Error::SectionOutOfBounds)
        );
    }

    #[test]
    fn empty_sections_decode_to_empty() {
        let bytes = DumpWriter::new().finish();
        let r = DumpReader::new(bytes).unwrap();
        assert_eq!(r.section_count(), 0);
        assert!(r.schemas().unwrap().is_empty());
        assert!(r.intern_tables().unwrap().is_empty());
        assert!(r.metas().unwrap().is_empty());
        assert!(r.views().unwrap().is_empty());
        assert!(r.shards().unwrap().is_empty());
    }

    #[test]
    fn round_trips_multiple_view_sets() {
        // Several registered view sets become several sections, preserved in order.
        let mut w = DumpWriter::new();
        w.schema_registry([&SCHEMA]);
        w.views("CREATE VIEW a AS SELECT 1;");
        w.views("CREATE MACRO m(x) AS TABLE SELECT x;");
        let r = DumpReader::new(w.finish()).unwrap();
        assert_eq!(
            r.views().unwrap(),
            [
                "CREATE VIEW a AS SELECT 1;",
                "CREATE MACRO m(x) AS TABLE SELECT x;"
            ]
        );
    }
}
