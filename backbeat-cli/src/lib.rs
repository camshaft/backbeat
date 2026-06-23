// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! Library half of the `backbeat` CLI: the schema-driven dump tooling.
//!
//! Everything is driven by each dump's embedded schema registry, with no compiled-in knowledge of
//! the producer's event types. Living in a library (not just the binary) lets integration tests —
//! and other tools — call these directly.
//!
//! * [`model`] — load + decode one or more `.bb` dumps into a common in-memory form.
//! * [`convert`] — write the decoded records to sparse-wide Parquet.
//! * [`trace`] — write them to Chrome / Perfetto trace JSON, pairing spans into duration slices.
//! * [`inspect`] — summarize a dump (envelope, registry, per-shard counts).

pub mod convert;
pub mod inspect;
pub mod model;
pub mod trace;
