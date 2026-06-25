// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! Generates a large synthetic `.bb` dump for performance/memory testing of the CLI.
//!
//! Records a configurable number of events across several shards (one writer thread per shard, so
//! the per-CPU rings fill in parallel) and dumps to a file. The shard rings are sized large enough
//! to retain the whole run, so the dump's record count is deterministic — nothing is overwritten.
//!
//! Usage:
//!   cargo run --release --example gen_dump -- <out.bb> [shards] [MiB-per-shard]
//!
//! Example (≈4 GiB dump, 16 shards × 256 MiB):
//!   cargo run --release --example gen_dump -- /tmp/big.bb 16 256

use backbeat::{
    recorder::Recorder,
    zerocopy::{Immutable, IntoBytes},
};
use std::{sync::Arc, thread};

/// Which side of a connection.
#[derive(backbeat::EventEnum, IntoBytes, Immutable, Clone, Copy)]
#[repr(u8)]
#[allow(dead_code)] // `Client` is referenced only via its label in the schema
enum Role {
    Client = 0,
    Server = 1,
}

/// A connection was opened.
#[derive(Event, IntoBytes, Immutable)]
#[event(namespace = "bench::net")]
#[repr(C)]
struct Connect {
    #[event(key)]
    conn_id: u64,
    #[event(unit = "bytes")]
    window: u32,
    role: Role,
    _pad: [u8; 3],
}

/// A packet was sent — the bulk of the records.
#[derive(Event, IntoBytes, Immutable)]
#[event(namespace = "bench::net")]
#[repr(C)]
struct PacketSent {
    #[event(key)]
    conn_id: u64,
    #[event(key)]
    packet_number: u64,
    #[event(unit = "bytes")]
    len: u32,
    is_fin: bool,
    _pad: [u8; 3],
}

use backbeat::Event;

fn main() {
    let mut args = std::env::args().skip(1);
    let out = args
        .next()
        .expect("usage: gen_dump <out.bb> [shards] [MiB-per-shard]");
    let shards: usize = args.next().map_or(16, |s| s.parse().expect("shards"));
    let mib_per_shard: usize = args
        .next()
        .map_or(256, |s| s.parse().expect("MiB-per-shard"));
    let bytes_per_shard = mib_per_shard * 1024 * 1024;

    let rec = Arc::new(Recorder::new(shards, bytes_per_shard));
    rec.set_enabled(true);

    // PacketSent record on the wire: ts(8)+id(8)+fields(24)+suffix(2) = 42 bytes. Push enough per
    // shard to fill it ~1.05x so the ring is densely packed (a little overshoot is fine — the walk
    // just recovers the newest capacity-worth). Spread the writers across cores so the rseq CPU
    // hint lands them on distinct shards.
    let per_writer = (bytes_per_shard / 42) + 1;
    let start = std::time::Instant::now();
    let handles: Vec<_> = (0..shards)
        .map(|w| {
            let rec = rec.clone();
            thread::spawn(move || {
                // One Connect per writer, then a long run of PacketSent.
                rec.record(&Connect {
                    conn_id: w as u64,
                    window: 65535,
                    role: Role::Server,
                    _pad: [0; 3],
                });
                for n in 0..per_writer as u64 {
                    rec.record(&PacketSent {
                        conn_id: w as u64,
                        packet_number: n,
                        len: 1200,
                        is_fin: n % 17 == 0,
                        _pad: [0; 3],
                    });
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let recorded = start.elapsed();

    let schemas: Vec<_> = backbeat::registry::schemas().collect();
    let dump = rec.dump(
        schemas,
        std::iter::empty(),
        std::iter::empty(),
        "bench-host",
    );
    std::fs::write(&out, &dump).expect("write dump");

    eprintln!(
        "recorded {shards} shards x {per_writer} events in {recorded:?}; wrote {} ({:.2} GiB) to {out}",
        dump.len(),
        dump.len() as f64 / (1024.0 * 1024.0 * 1024.0),
    );
}
