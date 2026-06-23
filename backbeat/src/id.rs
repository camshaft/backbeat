//! Stable, compile-time event identifiers.
//!
//! Every event type is identified by a 64-bit [`EventId`] derived from its fully-qualified name
//! (`"namespace::EventName"`). The id is computed with [FNV-1a], a hash chosen deliberately:
//!
//! * It is trivially expressible in a `const fn`, so the derive macro can emit
//!   `const ID: EventId = EventId::of("ns::Name")` and the id costs nothing at runtime.
//! * It is decentralized — anyone's crate can mint an id for its own events without coordinating
//!   with a central registry, and 64 bits makes accidental collision between distinct names
//!   astronomically unlikely.
//! * It is stable across builds, architectures, and hosts, so a dump written on one machine is
//!   readable anywhere and ids can be compared directly across dumps.
//!
//! The id is what links a record in the ring to its [`crate::schema::EventSchema`] in the dump's
//! embedded registry. The record carries the full 64-bit id (not a compacted local index): the
//! id is a compile-time constant so storing it is free on the hot path, and a low-cardinality id
//! column dictionary-compresses to almost nothing in the eventual columnar output — there is no
//! benefit to a runtime-assigned dense index, only the cost of a hot-path registration race.
//!
//! [FNV-1a]: https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// A chainable `const`-evaluable FNV-1a accumulator.
///
/// Used by [`crate::schema::EventSchema::compute_id`] to fold an event's whole schema into its id at
/// compile time. The methods take and return `self` by value so they chain in a `const fn` without
/// `&mut` (not yet allowed across all const contexts on the pinned toolchain).
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct Fnv {
    hash: u64,
}

impl Fnv {
    /// A fresh accumulator seeded with the FNV offset basis.
    pub const fn new() -> Self {
        Self { hash: FNV_OFFSET }
    }

    /// Folds in one byte.
    pub const fn byte(mut self, b: u8) -> Self {
        self.hash ^= b as u64;
        self.hash = self.hash.wrapping_mul(FNV_PRIME);
        self
    }

    /// Folds in a `u16` (little-endian).
    pub const fn u16(self, v: u16) -> Self {
        self.byte(v as u8).byte((v >> 8) as u8)
    }

    /// Folds in a `u64` (little-endian).
    pub const fn u64(mut self, v: u64) -> Self {
        let mut i = 0;
        while i < 8 {
            self = self.byte((v >> (i * 8)) as u8);
            i += 1;
        }
        self
    }

    /// Folds in a length-prefixed string (length disambiguates `"ab"+"c"` from `"a"+"bc"`).
    pub const fn str(mut self, s: &str) -> Self {
        let bytes = s.as_bytes();
        self = self.u16(bytes.len() as u16);
        let mut i = 0;
        while i < bytes.len() {
            self = self.byte(bytes[i]);
            i += 1;
        }
        self
    }

    /// Folds in an optional string: a presence byte, then the string when present.
    pub const fn opt_str(self, s: Option<&str>) -> Self {
        match s {
            Some(s) => self.byte(1).str(s),
            None => self.byte(0),
        }
    }

    /// Folds in a [`FieldType`](crate::schema::FieldType) — its discriminant plus any inline payload
    /// (enum repr, interned `dynamic`), so a type change forks the id.
    pub const fn field_type(self, ty: crate::schema::FieldType) -> Self {
        use crate::schema::FieldType::*;
        match ty {
            U8 => self.byte(0),
            U16 => self.byte(1),
            U32 => self.byte(2),
            U64 => self.byte(3),
            I8 => self.byte(4),
            I16 => self.byte(5),
            I32 => self.byte(6),
            I64 => self.byte(7),
            Bool => self.byte(8),
            Bytes => self.byte(9),
            Enum { repr } => self.byte(10).byte(repr),
            Interned { dynamic } => self.byte(11).byte(dynamic as u8),
        }
    }

    /// The accumulated 64-bit hash.
    pub const fn finish(self) -> u64 {
        self.hash
    }
}

impl Default for Fnv {
    fn default() -> Self {
        Self::new()
    }
}

/// A stable, 64-bit identifier for an event type.
///
/// Construct one at compile time from the event's fully-qualified name with [`EventId::of`]. The
/// id is the join key between a record in the ring and its schema in the dump registry.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct EventId(pub u64);

impl EventId {
    /// Computes the id for a fully-qualified event name (`"namespace::EventName"`).
    ///
    /// This is a `const fn`, so the derive macro evaluates it at compile time:
    ///
    /// ```
    /// use backbeat::id::EventId;
    /// const ID: EventId = EventId::of("my_app::net::PacketSent");
    /// ```
    pub const fn of(name: &str) -> Self {
        // Raw FNV-1a of the bytes (no length prefix) — keeps the canonical test vectors and the
        // simple "hash of a string" meaning. Schema-content ids use `EventSchema::compute_id`.
        let bytes = name.as_bytes();
        let mut hash = FNV_OFFSET;
        let mut i = 0;
        while i < bytes.len() {
            hash ^= bytes[i] as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
            i += 1;
        }
        EventId(hash)
    }

    /// The raw 64-bit value, as stored in a record and the schema registry.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl core::fmt::Debug for EventId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // Hex is the natural representation for a hash and matches how ids appear in dumps.
        write!(f, "EventId({:#018x})", self.0)
    }
}

impl core::fmt::Display for EventId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:#018x}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_known_fnv1a_vectors() {
        // Canonical FNV-1a 64 test vectors (the empty string is the offset basis).
        assert_eq!(EventId::of("").get(), FNV_OFFSET);
        assert_eq!(EventId::of("a").get(), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(EventId::of("foobar").get(), 0x85944171f73967e8);
    }

    #[test]
    fn distinct_names_distinct_ids() {
        assert_ne!(EventId::of("ns::A"), EventId::of("ns::B"));
        assert_ne!(EventId::of("ns1::A"), EventId::of("ns2::A"));
    }

    #[test]
    fn is_const_evaluable() {
        const ID: EventId = EventId::of("ns::Event");
        // If this compiles as a `const`, the macro can emit it with zero runtime cost.
        assert_eq!(ID, EventId::of("ns::Event"));
    }
}
