// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! [backbeat](https://github.com/camshaft/backbeat): a system-wide flight recorder with a
//! self-describing on-disk format and a schema-driven query CLI.
//!
//! backbeat is the steady rhythm running underneath your application: every instrumented event is
//! captured into a low-overhead, lock-free ring and dumped to disk *along with a schema describing
//! how to read it*, so a reader needs no compiled-in knowledge of the event types.
//!
//! ```no_run
//! use backbeat::Event;
//! use backbeat::zerocopy::{Immutable, IntoBytes};
//! use backbeat::recorder::Recorder;
//!
//! /// A packet was sent.
//! #[derive(Event, IntoBytes, Immutable)]
//! #[event(namespace = "my_app::net")]
//! #[repr(C)]
//! struct PacketSent {
//!     #[event(key)] connection_id: u64,
//!     #[event(unit = "bytes")] len: u32,
//!     _pad: [u8; 4], // explicit padding: IntoBytes rejects implicit gaps
//! }
//!
//! let rec = Recorder::new(/* shards */ 4, /* bytes/shard */ 1 << 20);
//! rec.set_enabled(true);
//! rec.record(&PacketSent { connection_id: 7, len: 1200, _pad: [0; 4] });
//!
//! // Dump every event compiled in (the registry self-populates via the derive).
//! let dump = rec.dump(
//!     backbeat::registry::schemas(),
//!     std::iter::empty(),
//!     backbeat::registry::views(),
//!     "my-host",
//! );
//! # let _ = dump;
//! ```
//!
//! Feature gates keep the dependency footprint matched to the use case:
//!
//! * default (`std`) — the full recorder runtime, dumper, and self-populating registry.
//! * `default-features = false` — the format and descriptors only, `no_std`, for crates that just
//!   *define* events.
//!
//! The dump → Parquet/summary tooling lives in the separate `backbeat-cli` crate
//! (`cargo install backbeat-cli`), so a library consumer never pulls in arrow/parquet/clap.
//!
//! ## Modules
//!
//! * [`id`] — the stable, compile-time 64-bit [`EventId`](id::EventId) derived from an event's
//!   fully-qualified name; the join key between a record and its schema.
//! * [`schema`] — [`EventSchema`](schema::EventSchema) and friends: the self-describing field
//!   layout the derive emits and the dump embeds.
//! * [`event`] — the [`Event`](event::Event) trait the derive implements.
//! * [`format`] / [`wire`] — the on-disk dump envelope and its byte-level serialization.
//! * [`record`] — the shared `[ts][event_id][fields]` record framing.
//! * [`ring`] — the lock-free, bump-allocating [`Ring`](ring::Ring) each shard captures into.
//! * [`cpu`] — the cheap rseq-based current-CPU hint used to pick a shard without a syscall.
//! * [`recorder`] / [`registry`] — the runtime (`std` only): sharded capture and the dumper, plus
//!   the `inventory`-populated schema registry.

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

pub mod cpu;
pub mod event;
pub mod format;
pub mod id;
pub mod record;
pub mod ring;
pub mod schema;
pub mod wire;

// The recorder runtime and the inventory-based registry rely on allocation and life-before-main
// collection, so they are `std`-only. The format/descriptor modules above stay `no_std`.
#[cfg(feature = "std")]
pub mod global;
#[cfg(feature = "std")]
pub mod recorder;
#[cfg(feature = "std")]
pub mod registry;

pub use event::{Event, EventEnum, FieldTy};
pub use id::EventId;
pub use schema::{EnumLabel, EventSchema, FieldRole, FieldSchema, FieldType, Phase};

/// The `Event` derive macro: annotate a `#[repr(C)]` struct to make it a traceable event.
pub use backbeat_macros::Event;

/// The `EventEnum` derive macro: annotate a fieldless `#[repr(u8|u16|u32|u64)]` enum to use it as
/// a strongly-typed event field whose variants become value→label pairs in the schema.
pub use backbeat_macros::EventEnum;

/// Re-export of [`zerocopy`] so event authors can derive the `IntoBytes`/`Immutable` bounds that
/// `#[derive(Event)]` requires without depending on `zerocopy` directly:
///
/// ```ignore
/// use backbeat::zerocopy::{Immutable, IntoBytes};
/// ```
pub use zerocopy;

/// Re-export of [`bytes`] — the reader hands out records as [`bytes::Bytes`], so consumers can name
/// the type (and build their own `Bytes` regions for [`ring::walk`]) without a direct dependency.
pub use bytes;

/// Re-export of [`inventory`] so the derive macro's generated `submit!` can reference it through
/// `backbeat` without the producing crate depending on `inventory` directly.
#[cfg(feature = "std")]
pub use inventory;

/// Registers an event type into the global [`registry`] so the dumper can enumerate it.
///
/// `#[derive(Event)]` emits a call to this macro for every event type. It branches on the `std`
/// feature: with `std` it submits a [`registry::Registration`] into `inventory`'s distributed
/// slice; without `std` (where `inventory`'s life-before-main collection is unavailable) it expands
/// to nothing, so events can still be *defined* in `no_std` crates — they are simply not
/// auto-registered until linked into a `std` binary. Not called directly.
#[cfg(feature = "std")]
#[macro_export]
macro_rules! register_event {
    ($ty:ty) => {
        $crate::inventory::submit! {
            $crate::registry::Registration::new(<$ty as $crate::Event>::SCHEMA)
        }
    };
}

/// `no_std` build: auto-registration is unavailable, so this expands to nothing. See the `std`
/// variant for the real behavior.
#[cfg(not(feature = "std"))]
#[macro_export]
macro_rules! register_event {
    ($ty:ty) => {};
}

/// Registers a set of query DDL — opaque SQL (typically DuckDB `CREATE VIEW`/`CREATE MACRO`
/// statements) describing how to query this binary's events — so the dumper embeds it in every dump.
///
/// A consumer calls this once (usually `include_str!`-ing a `.sql` file kept next to its event
/// definitions) so a dump carries not just how to *decode* its events but how to *query* them. The
/// text is stored and embedded verbatim; backbeat never parses it. The CLI's `convert` appends these
/// registered sets after the views it derives from the schema registry, then writes the combined DDL
/// to a `.sql` sidecar and the Parquet footer.
///
/// ```ignore
/// backbeat::register_views!(include_str!("frame_trace.views.sql"));
/// ```
///
/// Like [`register_event!`], this submits into `inventory`'s distributed slice on `std` and expands
/// to nothing under `no_std` (where life-before-main collection is unavailable).
#[cfg(feature = "std")]
#[macro_export]
macro_rules! register_views {
    ($sql:expr) => {
        $crate::inventory::submit! {
            $crate::registry::ViewSet::new($sql)
        }
    };
}

/// `no_std` build: auto-registration is unavailable, so this expands to nothing. See the `std`
/// variant for the real behavior.
#[cfg(not(feature = "std"))]
#[macro_export]
macro_rules! register_views {
    ($sql:expr) => {};
}
