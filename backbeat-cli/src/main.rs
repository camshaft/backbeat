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

use anyhow::{bail, Context, Result};
use backbeat_cli::{convert, inspect, model, trace};
use clap::{Parser, Subcommand, ValueEnum};
use std::path::{Path, PathBuf};

// jemalloc handles the decode/convert path's many small allocations (and their cross-thread frees
// under rayon) noticeably faster than the system allocator. Not available under MSVC.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

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
    /// Print the envelope, schema registry, and per-shard record counts.
    Inspect {
        /// The `.bb` dump to inspect.
        dump: PathBuf,
    },
}

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
        Command::Inspect { dump } => {
            let bytes =
                std::fs::read(&dump).with_context(|| format!("reading dump {}", dump.display()))?;
            inspect::inspect(bytes, &mut std::io::stdout())
                .with_context(|| format!("inspecting {}", dump.display()))
        }
    }
}
