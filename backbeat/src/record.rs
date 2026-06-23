// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! The record framing shared by the recorder (writer) and the reader.
//!
//! Each record stored in a shard ring has the payload layout
//!
//! ```text
//! [ ts_nanos: u64 LE ][ event_id: u64 LE ][ fields … ]
//! ```
//!
//! the trailing length suffix that delimits records belongs to the ring ([`crate::ring`]) and is
//! not part of this payload. Defining the layout here — rather than in the recorder — keeps the one
//! definition both sides agree on: [`crate::recorder`] writes it with
//! [`Ring::push_parts`](crate::ring::Ring::push_parts) and the CLI reader parses it with
//! [`RecordView::parse`].
//!
//! The timestamp leads the record so records sort by time within a shard with no extra index, and
//! the global order is `(ts_nanos, shard_id, local_seq)`. The `event_id` is the join key into the
//! dump's schema registry; the remaining bytes are exactly the event struct's `as_bytes()`, which
//! the registry's [`EventSchema`](crate::schema::EventSchema) describes field-by-field.

use crate::id::EventId;

/// Byte offset of the timestamp within a record payload.
pub const TS_OFFSET: usize = 0;
/// Byte offset of the event id within a record payload.
pub const ID_OFFSET: usize = 8;
/// Byte offset of the first field within a record payload (end of the fixed prefix).
pub const FIELDS_OFFSET: usize = 16;

/// A parsed view over one record payload's fixed prefix and field bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordView<'a> {
    /// Capture timestamp in nanoseconds (source-defined epoch; see [`crate::recorder`]).
    pub ts_nanos: u64,
    /// The event id; look it up in the dump's schema registry to decode `fields`.
    pub event_id: EventId,
    /// The event struct's raw bytes, described field-by-field by its schema.
    pub fields: &'a [u8],
}

impl<'a> RecordView<'a> {
    /// Parses a record payload, or returns `None` if it is too short to hold the fixed prefix
    /// (a torn record near the ring head).
    pub fn parse(payload: &'a [u8]) -> Option<Self> {
        if payload.len() < FIELDS_OFFSET {
            return None;
        }
        let ts_nanos = u64::from_le_bytes(payload[TS_OFFSET..ID_OFFSET].try_into().unwrap());
        let event_id = EventId(u64::from_le_bytes(
            payload[ID_OFFSET..FIELDS_OFFSET].try_into().unwrap(),
        ));
        Some(Self {
            ts_nanos,
            event_id,
            fields: &payload[FIELDS_OFFSET..],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_prefix_and_fields() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&123u64.to_le_bytes());
        payload.extend_from_slice(&EventId::of("a::B").get().to_le_bytes());
        payload.extend_from_slice(&[1, 2, 3, 4]);

        let v = RecordView::parse(&payload).unwrap();
        assert_eq!(v.ts_nanos, 123);
        assert_eq!(v.event_id, EventId::of("a::B"));
        assert_eq!(v.fields, &[1, 2, 3, 4]);
    }

    #[test]
    fn rejects_short_payload() {
        assert!(RecordView::parse(&[0u8; 8]).is_none());
        assert!(RecordView::parse(&[0u8; 16]).is_some());
    }
}
