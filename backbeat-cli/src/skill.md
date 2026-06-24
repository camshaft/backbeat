# Using the `backbeat` CLI

`backbeat` reads **self-describing flight-recorder dumps** (`.bb` files) and turns them into a
queryable table. A dump embeds its own schema, so the CLI needs no knowledge of the producing
program — and so do you: everything about a dump's events is discoverable from the dump itself.

This guide is what `backbeat skill` prints. It is meant for an agent (or human) ramping up fast.

## The 30-second version

```console
$ backbeat inspect dump.bb                 # what's in it: events, fields, counts, view sets
$ backbeat convert dump.bb -o out.parquet  # → Parquet + an out.views.sql query helper
$ duckdb out.parquet -init out.views.sql   # query it with the generated views loaded
```

Then, in DuckDB:

```sql
SELECT * FROM backbeat_keys;          -- discover the join/filter keys
SELECT * FROM "my::Event" LIMIT 20;   -- one view per event type
```

## Subcommands

- **`inspect <dump.bb>`** — print the envelope, the schema registry (every event type, its fields,
  units, key/span roles, descriptions), the instances, any registered query views, and per-shard
  record counts. Start here to learn what a dump contains.
- **`convert <dumps…> -o <out>`** — decode one or more dumps into a single output, merging them.
  Format is inferred from the extension:
  - `.parquet` → sparse-wide Parquet (query with DuckDB). Also writes a `<out>.views.sql` sidecar.
  - `.json` → Chrome/Perfetto trace JSON (open in <https://ui.perfetto.dev/> or `chrome://tracing`).
- **`merge <dumps…> -o <out.bb>`** — combine several `.bb` dumps into one multi-instance `.bb`
  (dedups overlapping records by default; `--no-dedup` for a fast raw splice).
- **`skill`** — print this guide.

## The Parquet table shape

`convert` writes **one wide table**, one row per recorded event:

- **Dense columns on every row:** `seq`, `ts_nanos`, `event` (the event's qualified name),
  `event_id` (its stable content-addressed id), `instance_id` (which process produced it).
- **Promoted key columns:** every field marked as a key or span id becomes a top-level column
  (e.g. `conn_id`, `packet_number`), unioned across all event types — so you can filter and join on
  them directly. A row has a value only for the keys its own event declares; others are null.
- **Per-event struct columns:** each event type gets one nullable `STRUCT` column, named by its
  qualified name, holding that event's remaining (non-key) fields. Reach a nested field with
  `"my::Event".field_name`. Only the row's own struct is populated; the rest are null.

This sparsity is free in Parquet's columnar encoding. Enums render as their label, byte arrays as
hex, interned strings resolve to text.

**Nulls mean absent.** Some integer fields declare a sentinel value the producer uses for "not
present" (the wire format has no nulls). `convert` maps those to SQL `NULL`, so `WHERE x IS NULL`
finds the rows where the field doesn't apply — you don't need to know the magic value. The raw
sentinel is preserved in the column's `backbeat.sentinel` field metadata if you need it.

## Querying with the generated views (recommended)

`convert` generates DuckDB DDL so you don't hand-write the column gymnastics. It lives in **two
places** — use whichever you have.

### You have the `.bb` or the `.parquet` + its sidecar

The sidecar `<out>.views.sql` is ready to load. It binds a base view `events` to the Parquet and
then defines the helpers:

```console
$ duckdb out.parquet -init out.views.sql
```

or from inside DuckDB:

```sql
.read out.views.sql
```

This gives you:

- **`events`** — the whole wide table.
- **One view per event type**, named by qualified name, e.g. `SELECT * FROM "my::Event"`.
- **`backbeat_keys`** — a manifest of `(event, field, role)` rows listing every promoted
  key/span column and which event declares it. **Query this first to discover how to join.**
- Any **domain views/macros** the producing program registered (see below).

### You have ONLY the `.parquet` (no sidecar)

The same DDL is embedded in the Parquet footer under the key `backbeat.views`. Extract it with
DuckDB's `decode()` (which faithfully turns the stored BLOB back into text — do **not** cast with
`::VARCHAR`, that escapes newlines), then load it:

```console
$ duckdb -noheader -list \
    -c "SELECT decode(value) FROM parquet_kv_metadata('out.parquet') WHERE key='backbeat.views';" \
    > footer.sql
$ { echo "CREATE OR REPLACE VIEW events AS SELECT * FROM read_parquet('out.parquet');"; \
    cat footer.sql; } > bound.sql
$ duckdb out.parquet -init bound.sql
```

The footer copy is **path-independent** (it references the base `events` view but doesn't bind it),
so you prepend the binding yourself, as shown — its very first statement builds on `events`, so the
binding must come first in the same file. (Don't pass the binding via `-c` and the views via
`-init`: DuckDB runs the `-init` file *before* any `-c`, so `events` wouldn't exist yet.) DuckDB
cannot execute SQL straight from the footer — it must land in a `.sql` file first.

## Domain views (Tier 2)

A program can register its own SQL — table macros that encode its real join keys — via backbeat's
`register_views!`. These ride inside the dump and are appended to the generated views. For example a
networking program might register a `stream_by_id(...)` macro that stitches together every event
for one connection across both endpoints. Run `backbeat inspect` to see whether a dump carries view
sets; `SELECT * FROM duckdb_functions() WHERE function_name LIKE '%...'` lists loaded macros.

## Common queries

```sql
-- What event types are present, and how many of each?
SELECT event, count(*) FROM events GROUP BY event ORDER BY 2 DESC;

-- Discover the keys before joining.
SELECT * FROM backbeat_keys;

-- A single event type, with a nested (non-key) field pulled out of its struct.
SELECT ts_nanos, conn_id, "my::net::PacketSent".len AS len
FROM "my::net::PacketSent"
ORDER BY ts_nanos;

-- Timeline for one key across all event types (events share promoted key columns).
SELECT ts_nanos, event, * EXCLUDE (seq) FROM events
WHERE conn_id = 7 ORDER BY ts_nanos;

-- Cross-process: rows from different instances merge into one table; filter or group by instance_id.
SELECT instance_id, event, count(*) FROM events GROUP BY 1, 2;
```

## Tips

- Order events globally by `ts_nanos` (then `instance_id`, `seq` to break ties). There is no global
  sequence number across shards/processes — time is the ordering key.
- A busy core keeps deeper history than a quiet one (per-CPU rings evict independently); a dump is a
  recent window, not a complete log. Absence of an old event may just mean it aged out.
- `event_id` is stable across builds for an unchanged layout; if a field changes, the id changes and
  the two versions appear as separate columns/views (the name gets a `#<id>` suffix on collision).
