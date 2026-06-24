// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! The `backbeat` CLI.
//!
//! Reads a self-describing dump and turns it into a queryable table. Because the dump embeds its
//! own schema registry, the CLI is generic over the event types — it needs no knowledge of the
//! producing crate. This is the headline difference from a converter with a hand-maintained,
//! byte-compatible decoder baked in.
//!
//! Subcommands:
//!   * `inspect <dump>` — print the envelope, schema registry, and per-shard record counts.
//!   * `convert <dump> [-o out.parquet]` — decode a dump to sparse-wide Parquet using its embedded
//!     schema, with the registry mirrored into the Parquet footer metadata.
//!   * `merge <dumps…> -o out.bb` — combine several dumps into one multi-instance `.bb`.
//!   * `skill` — print a Markdown guide to the CLI and its DuckDB query views.

use anyhow::{bail, Context, Result};
use backbeat_cli::{convert, inspect, merge, model, trace};
use clap::{Parser, Subcommand, ValueEnum};
use std::path::{Path, PathBuf};

// mimalloc handles the decode/convert path's many small allocations (and their cross-thread frees
// under rayon) noticeably faster than the system allocator. We use it on every target: unlike
// jemalloc — whose C build can't detect atomics under the musl cross-toolchain — mimalloc builds
// cleanly on musl, macOS, and Windows alike, so the static portable binaries get the speedup too.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(
    name = "backbeat",
    version,
    about = "Query self-describing backbeat trace dumps"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// The output format for `convert`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Format {
    /// Sparse-wide Apache Parquet.
    Parquet,
    /// Chrome / Perfetto trace-event JSON.
    Trace,
}

#[derive(Subcommand)]
enum Command {
    /// Decode one or more dumps to Parquet or Chrome-trace JSON, merging them into one output.
    ///
    /// The format is inferred from the output extension (`.parquet` → Parquet, `.json` → trace);
    /// pass `--format` to override, which is required when writing to stdout (`-o -`).
    Convert {
        /// The `.bb` dumps to convert. Multiple are merged into one output (decoded in parallel).
        #[arg(required = true)]
        dumps: Vec<PathBuf>,
        /// Output path (defaults to the first dump's path with the format's extension).
        #[arg(long, short = 'o')]
        output: Option<PathBuf>,
        /// Output format. Inferred from the output extension when omitted.
        #[arg(long, value_enum)]
        format: Option<Format>,
        /// Host label to stamp into the output (overrides the dumps' own host). Parquet only.
        #[arg(long)]
        host: Option<String>,
        /// zstd compression level for Parquet output (1–22).
        #[arg(long, default_value_t = 3)]
        compression_level: i32,
    },
    /// Combine several `.bb` dumps into one multi-instance `.bb`.
    ///
    /// By default the records are decoded, de-duplicated (overlapping dumps re-capture shared ring
    /// contents), and re-packed into compact shards — the smallest faithful dump. Pass `--no-dedup`
    /// for a cheap raw splice that copies every input's sections through verbatim (keeping
    /// duplicates) — handy for concatenating a host's dumps for upload, since `convert` dedups on
    /// the way out regardless. Either way schemas are unioned by id and instance ids are preserved,
    /// so converting the merged file yields exactly what converting the inputs together would.
    Merge {
        /// The `.bb` dumps to merge. Two or more are required.
        #[arg(required = true, num_args = 2..)]
        dumps: Vec<PathBuf>,
        /// Output path for the merged `.bb`.
        #[arg(long, short = 'o')]
        output: PathBuf,
        /// Skip de-duplication: splice the inputs' sections through verbatim (faster, but keeps
        /// duplicate records from overlapping dumps).
        #[arg(long)]
        no_dedup: bool,
    },
    /// Print the envelope, schema registry, and per-shard record counts.
    Inspect {
        /// The `.bb` dump to inspect.
        dump: PathBuf,
    },
    /// Print a Markdown guide to using this CLI (subcommands, the Parquet table shape, and how to
    /// load the generated DuckDB views) — a fast ramp-up for an agent or a new user.
    Skill,
}

/// The embedded agent/user guide, printed by `backbeat skill`.
const SKILL: &str = include_str!("skill.md");

/// Infers the output [`Format`] from an explicit flag or the output path's extension.
fn resolve_format(format: Option<Format>, output: Option<&Path>) -> Result<Format> {
    if let Some(f) = format {
        return Ok(f);
    }
    match output.and_then(|p| p.extension()).and_then(|e| e.to_str()) {
        Some("parquet") => Ok(Format::Parquet),
        Some("json") => Ok(Format::Trace),
        _ => bail!("cannot infer output format; pass --format parquet|trace (or use a .parquet / .json output path)"),
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Convert {
            dumps,
            output,
            format,
            host,
            compression_level,
        } => {
            let format = resolve_format(format, output.as_deref())?;
            // Default output: first dump's stem with the format's extension.
            let output = output.unwrap_or_else(|| {
                let ext = match format {
                    Format::Parquet => "parquet",
                    Format::Trace => "json",
                };
                dumps[0].with_extension(ext)
            });

            let loaded = model::load_many(&dumps)?;
            let count = match format {
                Format::Parquet => convert::to_parquet(
                    &loaded,
                    &output,
                    host.as_deref().unwrap_or(""),
                    compression_level,
                )?,
                Format::Trace => trace::to_trace(&loaded, &output)?,
            };
            let what = match format {
                Format::Parquet => "rows",
                Format::Trace => "events",
            };
            println!(
                "wrote {count} {what} from {} dump(s) to {}",
                dumps.len(),
                output.display()
            );
            Ok(())
        }
        Command::Merge {
            dumps,
            output,
            no_dedup,
        } => {
            let schemas = merge::merge(&dumps, &output, !no_dedup)?;
            let how = if no_dedup { "spliced" } else { "merged" };
            println!(
                "{how} {} dump(s) into {} ({schemas} event schema(s))",
                dumps.len(),
                output.display()
            );
            Ok(())
        }
        Command::Inspect { dump } => {
            let bytes =
                std::fs::read(&dump).with_context(|| format!("reading dump {}", dump.display()))?;
            inspect::inspect(bytes, &mut std::io::stdout())
                .with_context(|| format!("inspecting {}", dump.display()))
        }
        Command::Skill => {
            print!("{SKILL}");
            Ok(())
        }
    }
}
