// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! Generates DuckDB query DDL from a dump's schema registry, and assembles it with the
//! consumer-registered view sets carried in the dump.
//!
//! `convert` writes one sparse-wide Parquet table (see [`crate::convert`]). The DDL here turns that
//! flat table into an ergonomic query surface so an agent need not know the column layout:
//!
//! * a base view **`events`** every other statement builds on — the only thing bound to the Parquet
//!   path (so the rest is path-independent and can live in the dump/footer verbatim);
//! * one **per-event-type view** (named by the event's qualified name) selecting just that event's
//!   rows, so `SELECT * FROM "my::Event"` beats filtering the wide table by hand;
//! * a **`backbeat_keys`** manifest listing every promoted key/span column and which event declares
//!   it, so the joins are *discoverable* — an agent reads the manifest instead of guessing keys.
//!
//! These **Tier 1** views are generated purely from the registry — no domain knowledge. A consumer
//! adds **Tier 2** domain joins (e.g. dc's `stream_by_dump`) by `register_views!`-ing a `.sql` file;
//! those ride in the dump and are appended after Tier 1 by [`assemble`]. The whole DDL references
//! `events`, so binding that one view to a Parquet (or any source) activates everything.

use backbeat::{schema::FieldRole, wire::OwnedSchema};
use std::collections::HashMap;

/// Whether a field is promoted to a top-level Parquet column (mirrors [`crate::convert`]'s rule):
/// keys and span ids become dense, queryable/join-able columns; everything else nests in the
/// per-event struct column.
fn is_promoted(role: FieldRole) -> bool {
    matches!(
        role,
        FieldRole::Key | FieldRole::SpanId | FieldRole::ParentSpanId
    )
}

/// A short role label for the `backbeat_keys` manifest.
fn role_label(role: FieldRole) -> &'static str {
    match role {
        FieldRole::Key => "key",
        FieldRole::SpanId => "span_id",
        FieldRole::ParentSpanId => "parent_span_id",
        _ => "",
    }
}

/// Quotes a SQL identifier (event names contain `::`, so they always need quoting). Embedded double
/// quotes are doubled per SQL rules.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Escapes a single-quoted SQL string literal.
fn quote_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The per-schema display name used for the Parquet `event` column and the per-event view name: the
/// `qualified_name`, suffixed with `#<id>` only when two schemas collide on it (distinct
/// content-addressed ids — genuinely different event types sharing a name). Mirrors
/// `convert::display_names` so the generated views line up with the columns convert writes.
fn display_names(schemas: &[OwnedSchema]) -> Vec<String> {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for s in schemas {
        *counts.entry(s.qualified_name.as_str()).or_default() += 1;
    }
    schemas
        .iter()
        .map(|s| {
            if counts[s.qualified_name.as_str()] > 1 {
                format!("{}#{:016x}", s.qualified_name, s.id.get())
            } else {
                s.qualified_name.clone()
            }
        })
        .collect()
}

/// Generates the Tier-1 DDL from the unioned schema registry. References a base view `events`
/// (see [`bootstrap`]) but does not define it, so this text is path-independent and embeds verbatim
/// in the dump and the Parquet footer.
pub fn generate_tier1(schemas: &[OwnedSchema]) -> String {
    let names = display_names(schemas);
    let mut out = String::new();

    out.push_str(
        "-- backbeat Tier-1 views (generated from the dump's schema registry).\n\
         -- Everything below builds on a base view named `events`; create it over your Parquet\n\
         -- before running these (the `.sql` sidecar convert writes does this for you).\n\n",
    );

    // Per-event-type views: filter the wide table to one event by its stable content-addressed id
    // (robust across a merged dump that holds two versions of a name). `SELECT *` keeps the row
    // whole — the event's own struct column is populated; other events' columns are simply null.
    out.push_str("-- One view per event type.\n");
    for (s, name) in schemas.iter().zip(&names) {
        out.push_str(&format!(
            "CREATE OR REPLACE VIEW {} AS SELECT * FROM events WHERE event_id = {};\n",
            quote_ident(name),
            s.id.get()
        ));
    }

    // The `backbeat_keys` manifest: which promoted columns exist and which event declares each, so
    // an agent can discover the join/filter keys rather than guess them.
    out.push_str("\n-- Discoverable key/span columns: (event, column, role).\n");
    let mut rows: Vec<String> = Vec::new();
    for (s, name) in schemas.iter().zip(&names) {
        for f in s.fields.iter().filter(|f| is_promoted(f.role)) {
            rows.push(format!(
                "  ({}, {}, {})",
                quote_str(name),
                quote_str(&f.name),
                quote_str(role_label(f.role))
            ));
        }
    }
    if rows.is_empty() {
        // VALUES cannot be empty; emit a typed, zero-row manifest so the view always exists.
        // (`column` is a DuckDB reserved word, so the manifest names the column `field`.)
        out.push_str(
            "CREATE OR REPLACE VIEW backbeat_keys AS\n  \
             SELECT NULL::VARCHAR AS event, NULL::VARCHAR AS field, NULL::VARCHAR AS role WHERE false;\n",
        );
    } else {
        out.push_str("CREATE OR REPLACE VIEW backbeat_keys AS\n  SELECT * FROM (VALUES\n");
        out.push_str(&rows.join(",\n"));
        out.push_str("\n  ) AS t(event, field, role);\n");
    }

    out
}

/// A one-line bootstrap that binds the base `events` view to a Parquet file, prepended to the
/// `.sql` sidecar (where `convert` knows the output path). The footer-embedded copy omits this so it
/// stays path-independent.
pub fn bootstrap(parquet_path: &str) -> String {
    format!(
        "-- Bind the base table to this conversion's Parquet output.\n\
         CREATE OR REPLACE VIEW events AS SELECT * FROM read_parquet({});\n\n",
        quote_str(parquet_path)
    )
}

/// Assembles the full DDL body: Tier-1 (generated) followed by each registered Tier-2 view set
/// (verbatim, in dump order). This is the path-independent text written to the Parquet footer; the
/// sidecar is this prefixed with [`bootstrap`].
pub fn assemble(schemas: &[OwnedSchema], tier2: &[String]) -> String {
    let mut out = generate_tier1(schemas);
    for (i, sql) in tier2.iter().enumerate() {
        out.push_str(&format!("\n-- Registered view set #{}.\n", i + 1));
        out.push_str(sql);
        if !sql.ends_with('\n') {
            out.push('\n');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use backbeat::{
        id::EventId,
        schema::FieldType,
        wire::{OwnedField, OwnedSchema},
    };

    fn field(name: &str, role: FieldRole) -> OwnedField {
        OwnedField {
            name: name.to_string(),
            description: None,
            ty: FieldType::U64,
            offset: 0,
            width: 8,
            role,
            unit: None,
            sentinel: None,
            enum_labels: Vec::new(),
        }
    }

    fn schema(id: u64, name: &str, fields: Vec<OwnedField>) -> OwnedSchema {
        OwnedSchema {
            id: EventId(id),
            qualified_name: name.to_string(),
            description: None,
            record_size: 8,
            phase: backbeat::schema::Phase::None,
            fields,
        }
    }

    #[test]
    fn quotes_identifiers_and_strings() {
        assert_eq!(quote_ident("my::Event"), "\"my::Event\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_str("it's"), "'it''s'");
    }

    #[test]
    fn per_event_view_filters_by_id() {
        let s = vec![schema(7, "ns::Pkt", vec![field("conn_id", FieldRole::Key)])];
        let ddl = generate_tier1(&s);
        assert!(ddl.contains(
            "CREATE OR REPLACE VIEW \"ns::Pkt\" AS SELECT * FROM events WHERE event_id = 7;"
        ));
        // The promoted key is surfaced in the discovery manifest.
        assert!(ddl.contains("CREATE OR REPLACE VIEW backbeat_keys"));
        assert!(ddl.contains("('ns::Pkt', 'conn_id', 'key')"));
    }

    #[test]
    fn empty_key_manifest_is_a_typed_zero_row_view() {
        // An event with no promoted columns: backbeat_keys must still exist (VALUES can't be empty).
        let s = vec![schema(1, "ns::Marker", vec![field("x", FieldRole::None)])];
        let ddl = generate_tier1(&s);
        assert!(ddl.contains("CREATE OR REPLACE VIEW backbeat_keys"));
        assert!(ddl.contains("WHERE false"));
        assert!(!ddl.contains("VALUES"));
    }

    #[test]
    fn collision_suffixes_display_name_with_id() {
        // Two schemas sharing a qualified_name (distinct ids) get `#<id>`-suffixed view names.
        let s = vec![
            schema(0xAA, "ns::E", vec![field("k", FieldRole::Key)]),
            schema(0xBB, "ns::E", vec![field("k", FieldRole::Key)]),
        ];
        let ddl = generate_tier1(&s);
        assert!(ddl.contains("\"ns::E#00000000000000aa\""));
        assert!(ddl.contains("\"ns::E#00000000000000bb\""));
    }

    #[test]
    fn assemble_appends_tier2_with_trailing_newline() {
        let s = vec![schema(1, "ns::E", vec![field("k", FieldRole::Key)])];
        let ddl = assemble(&s, &["CREATE MACRO m() AS TABLE SELECT 1;".to_string()]);
        assert!(ddl.contains("-- Registered view set #1."));
        assert!(ddl.contains("CREATE MACRO m() AS TABLE SELECT 1;"));
        assert!(
            ddl.ends_with('\n'),
            "tier-2 without trailing newline gets one"
        );
    }

    #[test]
    fn bootstrap_binds_events_to_path() {
        assert!(bootstrap("out.parquet").contains(
            "CREATE OR REPLACE VIEW events AS SELECT * FROM read_parquet('out.parquet')"
        ));
    }
}
