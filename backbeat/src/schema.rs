//! Self-describing event schemas.
//!
//! The defining idea of backbeat: a dump carries, alongside the raw records, a registry of
//! [`EventSchema`]s describing how to decode them. The reader is generic over this registry — it
//! knows how to turn *any* event into columns purely from the descriptor, with no compiled-in
//! knowledge of the producing crate's types. This is what replaces the hand-maintained,
//! byte-compatible decoder that an out-of-band schema forces you to keep in sync.
//!
//! A schema is a flat list of [`FieldSchema`]s. Each field names a byte range within the
//! fixed-size record payload and a [`FieldType`] saying how to interpret it. The derive macro
//! ([`backbeat_macros`]) generates a `const EventSchema` for each annotated type, so the schema
//! is as compile-time as the layout it describes.
//!
//! These types are the *in-memory* description. The on-disk encoding lives in [`crate::format`];
//! keeping them separate lets the descriptor stay `no_std` and lets the wire format evolve
//! independently of the Rust types.

use crate::id::EventId;

/// How to interpret the bytes of a field.
///
/// The taxonomy is deliberately small. Most fields are [`scalar`](FieldType::U64) integers stored
/// inline. [`Enum`](FieldType::Enum) is an inline small integer plus a value→label map, so the
/// reader can render `3` as `"ack-lost"` without the producer hand-writing a `match` in two
/// places. [`Interned`](FieldType::Interned) is the escape hatch for variable-length data: the
/// record stores a small `u32` id inline and the value is resolved from the dump's intern table,
/// so the hot record stays fixed-width and all-scalar events never pay for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FieldType {
    /// Unsigned integers, stored little-endian inline.
    U8,
    U16,
    U32,
    U64,
    /// Signed integers, stored little-endian inline.
    I8,
    I16,
    I32,
    I64,
    /// A single byte, `0` = false, non-zero = true.
    Bool,
    /// A fixed-width opaque byte array (e.g. a 16-byte credential id), rendered as hex.
    Bytes,
    /// A small inline integer (width given by `repr`) whose values map to labels via
    /// [`FieldSchema::enum_labels`].
    Enum {
        /// Width of the inline discriminant (1, 2, 4, or 8 bytes).
        repr: u8,
    },
    /// A `u32` id stored inline, resolved against the dump's intern table to a string. Set
    /// `dynamic` when the value is built at runtime (a real hashmap lookup on the hot path);
    /// leave it clear for `&'static str` interning, which is near-free.
    Interned {
        /// Whether the interned value is computed at runtime (vs. a stable `&'static str`).
        dynamic: bool,
    },
}

/// The semantic role a field plays beyond its raw type.
///
/// Roles are mutually exclusive — a field is at most one of these — so a single enum makes the
/// illegal combinations unrepresentable instead of forcing consumers to reconcile parallel
/// booleans. [`Key`](FieldRole::Key) is the original `#[event(key)]` promotion; the two span roles
/// let the trace converter pair begin/end records and build parent/child links purely from the
/// schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum FieldRole {
    /// An ordinary field with no special role.
    #[default]
    None,
    /// A high-value join/index column the reader promotes to the top level (`#[event(key)]`).
    Key,
    /// The span's own id (`#[event(span_id)]`): a `u64` the converter pairs enter/exit records by.
    SpanId,
    /// The id of the enclosing span (`#[event(parent_span_id)]`): a `u64` linking this record to
    /// its parent, for nesting in the trace view.
    ParentSpanId,
}

/// Which phase of a span an event type represents.
///
/// A span has three relevant states, so this is an enum rather than a bool: most events are
/// [`None`](Phase::None) (plain point-in-time events), while a span is expressed as a paired
/// [`Enter`](Phase::Enter) / [`Exit`](Phase::Exit) event type sharing a `span_id`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Phase {
    /// Not part of a span: an instantaneous event.
    #[default]
    None,
    /// The opening of a span (`#[event(span = enter)]`).
    Enter,
    /// The closing of a span (`#[event(span = exit)]`).
    Exit,
}

/// A single value→label mapping for an [`FieldType::Enum`] field.
#[derive(Clone, Copy, Debug)]
pub struct EnumLabel {
    /// The discriminant value as stored in the record.
    pub value: u64,
    /// The human-readable label for that value.
    pub label: &'static str,
}

/// Describes one field within an event record's fixed-size payload.
#[derive(Clone, Copy, Debug)]
pub struct FieldSchema {
    /// Field name; becomes the (namespaced) column name in the output table.
    pub name: &'static str,
    /// The field's doc comment, if any, lifted verbatim from the source `///` lines by the derive
    /// macro. Carried into the dump so the registry documents itself — a reader can show what a
    /// column *means*, not just its name and type.
    pub description: Option<&'static str>,
    /// How to interpret the bytes.
    pub ty: FieldType,
    /// Byte offset of the field within the record payload.
    pub offset: u16,
    /// Width of the field in bytes. Redundant with `ty` for fixed types, but explicit so the
    /// reader can validate the record length and skip fields it does not understand.
    pub width: u16,
    /// The field's semantic role: a key column, a span id, a parent span id, or nothing special.
    /// Set from `#[event(key)]` / `#[event(span_id)]` / `#[event(parent_span_id)]`.
    pub role: FieldRole,
    /// Optional unit hint (`"bytes"`, `"ns"`, …) carried through to the output for tooling.
    pub unit: Option<&'static str>,
    /// Value→label map for [`FieldType::Enum`] fields; empty otherwise.
    pub enum_labels: &'static [EnumLabel],
}

/// The complete description of one event type.
///
/// The derive macro emits one of these as an associated `const` per annotated struct, and the
/// recorder serializes every registered schema into the dump's [registry](crate::format).
#[derive(Clone, Copy, Debug)]
pub struct EventSchema {
    /// Stable id (`fnv1a64` of `qualified_name`); the join key to records in the ring.
    pub id: EventId,
    /// Fully-qualified event name, `"namespace::EventName"`.
    pub qualified_name: &'static str,
    /// The event's doc comment, if any, lifted from the source struct's `///` lines by the derive
    /// macro, so the embedded registry documents what each event *is*.
    pub description: Option<&'static str>,
    /// Total size in bytes of the fixed record payload (must equal the sum of field widths plus
    /// any explicit padding the layout includes).
    pub record_size: u16,
    /// Which span phase this event represents: [`None`](Phase::None) for a plain event, or
    /// [`Enter`](Phase::Enter)/[`Exit`](Phase::Exit) for the two halves of a span. Set from
    /// `#[event(span = enter|exit)]`.
    pub phase: Phase,
    /// The fields, in declaration order.
    pub fields: &'static [FieldSchema],
}

/// Narrows a `usize` layout quantity (a field offset/width or a whole record's size) to the `u16`
/// the schema stores, panicking if it does not fit.
///
/// The derive macro wraps every `offset_of!` / `size_of` in this, so it is evaluated in `const`
/// context: an event whose layout exceeds `u16::MAX` (≈64 KiB) becomes a clear compile-time error
/// instead of a silently truncated offset/width that would corrupt the registry and make every
/// reader misinterpret the record.
#[doc(hidden)]
pub const fn layout_u16(value: usize) -> u16 {
    assert!(
        value <= u16::MAX as usize,
        "event layout exceeds 65535 bytes (u16::MAX): backbeat's schema stores field offsets, \
         widths, and record sizes as u16, so an event this large cannot be described"
    );
    value as u16
}

impl EventSchema {
    /// Looks up a field by name.
    pub fn field(&self, name: &str) -> Option<&FieldSchema> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// The key fields, in declaration order — the columns the reader promotes to the top level.
    pub fn keys(&self) -> impl Iterator<Item = &FieldSchema> {
        self.fields.iter().filter(|f| f.role == FieldRole::Key)
    }

    /// The span-id field, if this event declares one (`#[event(span_id)]`).
    pub fn span_id(&self) -> Option<&FieldSchema> {
        self.fields.iter().find(|f| f.role == FieldRole::SpanId)
    }

    /// The parent-span-id field, if this event declares one (`#[event(parent_span_id)]`).
    pub fn parent_span(&self) -> Option<&FieldSchema> {
        self.fields
            .iter()
            .find(|f| f.role == FieldRole::ParentSpanId)
    }

    /// Computes the content-addressed [`EventId`] for a schema's `(qualified_name, phase, fields)`
    /// — *everything that defines the event*, so two builds whose layout or metadata differs get
    /// distinct ids and are treated as separate event types that merely share a name.
    ///
    /// `record_size` and the schema's own `id` are deliberately excluded: `id` would be circular,
    /// and `record_size` is fully determined by the field offsets/widths already folded in. This is
    /// a `const fn` so the derive evaluates it at compile time; it folds the same FNV-1a stream the
    /// rest of the format uses.
    pub const fn compute_id(qualified_name: &str, phase: Phase, fields: &[FieldSchema]) -> EventId {
        let mut h = crate::id::Fnv::new();
        h = h.str(qualified_name);
        h = h.byte(phase as u8);
        let mut i = 0;
        while i < fields.len() {
            let f = &fields[i];
            h = h.str(f.name);
            h = h.opt_str(f.description);
            h = h.field_type(f.ty);
            h = h.u16(f.offset);
            h = h.u16(f.width);
            h = h.byte(f.role as u8);
            h = h.opt_str(f.unit);
            let mut j = 0;
            while j < f.enum_labels.len() {
                let l = &f.enum_labels[j];
                h = h.u64(l.value);
                h = h.str(l.label);
                j += 1;
            }
            i += 1;
        }
        EventId(h.finish())
    }
}

impl FieldType {
    /// The natural byte width of a fixed-width type, if it has one. Variable-rendered types
    /// ([`Bytes`](FieldType::Bytes)) carry their width on the [`FieldSchema`] instead.
    pub const fn fixed_width(self) -> Option<u16> {
        Some(match self {
            FieldType::U8 | FieldType::I8 | FieldType::Bool => 1,
            FieldType::U16 | FieldType::I16 => 2,
            FieldType::U32 | FieldType::I32 | FieldType::Interned { .. } => 4,
            FieldType::U64 | FieldType::I64 => 8,
            FieldType::Enum { repr } => repr as u16,
            FieldType::Bytes => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: EventSchema = EventSchema {
        id: EventId::of("test::Demo"),
        qualified_name: "test::Demo",
        description: Some("A demo event."),
        record_size: 16,
        phase: Phase::None,
        fields: &[
            FieldSchema {
                name: "packet_number",
                description: Some("The packet number."),
                ty: FieldType::U64,
                offset: 0,
                width: 8,
                role: FieldRole::Key,
                unit: None,
                enum_labels: &[],
            },
            FieldSchema {
                name: "direction",
                description: None,
                ty: FieldType::Enum { repr: 1 },
                offset: 8,
                width: 1,
                role: FieldRole::None,
                unit: None,
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
        ],
    };

    #[test]
    fn field_lookup_and_keys() {
        assert!(SCHEMA.field("packet_number").is_some());
        assert!(SCHEMA.field("nope").is_none());
        let keys: Vec<_> = SCHEMA.keys().map(|f| f.name).collect();
        assert_eq!(keys, ["packet_number"]);
    }

    #[test]
    fn fixed_widths() {
        assert_eq!(FieldType::U64.fixed_width(), Some(8));
        assert_eq!(
            FieldType::Interned { dynamic: false }.fixed_width(),
            Some(4)
        );
        assert_eq!(FieldType::Enum { repr: 2 }.fixed_width(), Some(2));
        assert_eq!(FieldType::Bytes.fixed_width(), None);
    }
}
