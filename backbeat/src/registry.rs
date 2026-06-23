// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! Self-populating registry of every event type compiled into the binary.
//!
//! The dumper must embed a schema for every event id that can appear in the rings, but the
//! producer should not have to enumerate its event types by hand — that list would drift the moment
//! someone adds an event. Instead `#[derive(Event)]` emits an [`inventory::submit!`] of a
//! [`Registration`] for each type, and `inventory` gathers them into a distributed slice the linker
//! assembles. [`schemas`] walks that slice, so the registry is always exactly the set of events the
//! binary can actually produce.
//!
//! This is `std`-only: `inventory`'s collection relies on life-before-main constructors that a bare
//! `no_std` target does not provide. Events can still be *defined* in `no_std` crates; they are
//! only auto-registered once linked into a `std` binary that records them.

use crate::schema::EventSchema;

/// One event type's registration in the global inventory. The derive submits one of these per
/// `#[derive(Event)]` type; [`schemas`] reads them back.
pub struct Registration {
    /// The event's compile-time schema.
    pub schema: EventSchema,
}

impl Registration {
    /// Creates a registration for an event's schema. Called by the derive's `inventory::submit!`.
    pub const fn new(schema: EventSchema) -> Self {
        Self { schema }
    }
}

inventory::collect!(Registration);

/// Every registered event schema, in unspecified order. This is the set the dumper embeds.
pub fn schemas() -> impl Iterator<Item = &'static EventSchema> {
    inventory::iter::<Registration>
        .into_iter()
        .map(|r| &r.schema)
}

/// The number of registered event types.
pub fn len() -> usize {
    inventory::iter::<Registration>.into_iter().count()
}
