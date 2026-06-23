//! The [`Event`] trait tying a struct to its schema and its bytes.
//!
//! `#[derive(Event)]` implements this for a `#[repr(C)]` struct. Two things make a type an event:
//!
//! * It is described by a compile-time [`EventSchema`] (so the dump is self-describing), exposed as
//!   the associated [`Event::SCHEMA`] and its [`EventId`] as [`Event::ID`].
//! * It is plain-old-data the recorder can capture with a single memcpy. We express that by
//!   requiring [`zerocopy::IntoBytes`] + [`zerocopy::Immutable`]: the field bytes are *borrowed*
//!   straight from the struct via [`as_bytes`](zerocopy::IntoBytes::as_bytes) with no
//!   serialization or allocation, and [`Ring::push_event`](crate::ring::Ring::push_event) copies
//!   `[event_id u64][fields]` directly into its preallocated, reserved slice. The `IntoBytes`
//!   derive *also* rejects any layout with implicit padding — precisely the layouts the schema
//!   reader could not describe — so the bound doubles as the padding check the derive would
//!   otherwise have to do by hand.

use crate::{
    id::EventId,
    schema::{EnumLabel, EventSchema},
};
use zerocopy::{Immutable, IntoBytes};

/// A type that can be recorded by backbeat.
///
/// Implemented by `#[derive(Event)]`; see the [module docs](self) for the meaning of the bounds.
/// The recorder captures one with [`Ring::push_event`](crate::ring::Ring::push_event), which writes
/// the record without touching the heap.
pub trait Event: IntoBytes + Immutable {
    /// The self-describing layout of this event, emitted as a `const` by the derive.
    const SCHEMA: EventSchema;

    /// The event's stable id — a hash over its full schema (name + layout + field metadata), so
    /// two builds with differing layouts get distinct ids and never alias in a dump's registry.
    /// Equal to `SCHEMA.id` and to the `event_id` prefix the ring writes ahead of the field bytes.
    const ID: EventId;

    /// The fully-qualified event name, `"namespace::TypeName"`.
    const QUALIFIED_NAME: &'static str;
}

/// A Rust enum usable as an event field.
///
/// Implemented by `#[derive(EventEnum)]` on a fieldless `#[repr(u8|u16|u32|u64)]` enum. Instead of
/// hand-writing `#[event(enum_labels(...))]` on every field, you define the enum once with its
/// strong type and pass it around; a field of that type is automatically reflected as a
/// [`FieldType::Enum`](crate::schema::FieldType::Enum) whose value→label map comes from
/// [`LABELS`](EventEnum::LABELS). The [`IntoBytes`] + [`Immutable`] bounds let the enum sit inline
/// in a `#[repr(C)]` event with no serialization.
///
/// ```ignore
/// use backbeat::EventEnum;
/// use backbeat::zerocopy::{Immutable, IntoBytes};
///
/// #[derive(EventEnum, IntoBytes, Immutable, Clone, Copy)]
/// #[repr(u8)]
/// enum Direction { Inbound = 0, Outbound = 1 }
/// ```
pub trait EventEnum: IntoBytes + Immutable + Copy {
    /// Width of the discriminant in bytes (1, 2, 4, or 8) — the enum's `#[repr]` width.
    const REPR: u8;
    /// The value→label map for every variant, in declaration order.
    const LABELS: &'static [EnumLabel];
}

/// How a Rust type appears as an event field, resolved at compile time.
///
/// This is the seam that lets `#[derive(Event)]` reflect a field *without knowing the field's type*:
/// it emits `<FieldTypeOfField as FieldTy>::FIELD_TYPE` / `::LABELS`, and the right impl is selected
/// at const-eval. The primitives ([`u8`]…[`i64`], [`bool`]) and `[u8; N]` have built-in impls here;
/// every `#[derive(EventEnum)]` type gets one via the [blanket impl](#impl-FieldTy-for-T). Fields
/// that are interned strings opt in with `#[event(interned)]` instead of going through `FieldTy`.
///
/// Implementors must be plain inline bytes ([`IntoBytes`] + [`Immutable`]) since the field sits
/// directly in the `#[repr(C)]` event.
pub trait FieldTy: IntoBytes + Immutable {
    /// How the reader should interpret this field's bytes.
    const FIELD_TYPE: crate::schema::FieldType;
    /// The enum value→label map, or empty for non-enum types.
    const LABELS: &'static [EnumLabel] = &[];
}

/// Every `EventEnum` is usable as a field: an [`Enum`](crate::schema::FieldType::Enum) of its repr
/// width carrying its labels.
impl<T: EventEnum> FieldTy for T {
    const FIELD_TYPE: crate::schema::FieldType = crate::schema::FieldType::Enum { repr: T::REPR };
    const LABELS: &'static [EnumLabel] = T::LABELS;
}

/// Implements [`FieldTy`] for a built-in scalar mapping to a [`FieldType`] variant.
macro_rules! impl_field_ty {
    ($($t:ty => $variant:ident),* $(,)?) => {
        $(
            impl FieldTy for $t {
                const FIELD_TYPE: crate::schema::FieldType = crate::schema::FieldType::$variant;
            }
        )*
    };
}
impl_field_ty! {
    u8 => U8, u16 => U16, u32 => U32, u64 => U64,
    i8 => I8, i16 => I16, i32 => I32, i64 => I64,
    bool => Bool,
}

/// A fixed-width byte array field, rendered as hex.
impl<const N: usize> FieldTy for [u8; N] {
    const FIELD_TYPE: crate::schema::FieldType = crate::schema::FieldType::Bytes;
}
