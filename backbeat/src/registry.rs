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

/// One registered set of query DDL — opaque SQL text (typically DuckDB `CREATE VIEW`/`CREATE MACRO`
/// statements) describing how to query this binary's events. A consumer submits one via
/// [`register_views!`](crate::register_views), usually `include_str!`-ing a `.sql` file next to its
/// event definitions; [`views`] reads them back so the dumper can embed them. backbeat never parses
/// the text — it travels with the dump so a reader knows not just how to decode the events but how
/// to query them.
pub struct ViewSet {
    /// The DDL text, stored and embedded verbatim.
    pub sql: &'static str,
}

impl ViewSet {
    /// Creates a view-set registration. Called by [`register_views!`](crate::register_views).
    pub const fn new(sql: &'static str) -> Self {
        Self { sql }
    }
}

inventory::collect!(ViewSet);

/// Every registered [`ViewSet`]'s DDL text, in unspecified order. This is the set the dumper embeds
/// as [`Views`](crate::format::SectionKind::Views) sections.
pub fn views() -> impl Iterator<Item = &'static str> {
    inventory::iter::<ViewSet>.into_iter().map(|v| v.sql)
}
