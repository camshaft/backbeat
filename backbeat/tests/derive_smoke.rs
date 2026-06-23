use backbeat::{
    ring::{walk, Ring},
    schema::{FieldRole, FieldType},
    zerocopy::{Immutable, IntoBytes},
    Event, EventEnum,
};

/// Which way a packet was going.
#[derive(EventEnum, IntoBytes, Immutable, Clone, Copy)]
#[repr(u8)]
#[allow(dead_code)] // `Incoming` is only referenced through its label in assertions
enum Direction {
    Incoming = 0,
    Outgoing = 1,
}

/// A frame was queued for sending.
#[derive(Event, IntoBytes, Immutable)]
#[event(namespace = "my_app::net")]
#[repr(C)]
struct PacketSent {
    /// The connection this packet belongs to.
    #[event(key)]
    connection_id: u64,
    #[event(unit = "bytes")]
    bytes: u32,
    // A strongly-typed enum field — no per-field attribute needed.
    direction: Direction,
    is_fin: bool,
    // explicit padding so the layout has no implicit gaps (zerocopy's IntoBytes requires this)
    _pad: [u8; 2],
}

#[test]
fn derive_emits_stable_id_and_name() {
    assert_eq!(PacketSent::QUALIFIED_NAME, "my_app::net::PacketSent");
    // The id is content-addressed: stable across builds, but a hash of the whole schema (so it is
    // *not* the bare name hash). Just assert it's non-zero and equals the schema's id.
    assert_eq!(PacketSent::ID, PacketSent::SCHEMA.id);
    assert_ne!(PacketSent::ID.get(), 0);
}

#[test]
fn schema_reflects_fields() {
    let s = PacketSent::SCHEMA;
    assert_eq!(s.id, PacketSent::ID);
    assert_eq!(s.qualified_name, "my_app::net::PacketSent");
    assert_eq!(s.description, Some("A frame was queued for sending."));
    assert_eq!(s.record_size as usize, core::mem::size_of::<PacketSent>());
    assert_eq!(s.phase, backbeat::Phase::None);

    // Offsets are the real struct offsets; widths the real sizes.
    let cid = s.field("connection_id").unwrap();
    assert_eq!(cid.offset, 0);
    assert_eq!(cid.width, 8);
    assert_eq!(cid.ty, FieldType::U64);
    assert_eq!(cid.role, FieldRole::Key);
    assert_eq!(
        cid.description,
        Some("The connection this packet belongs to.")
    );

    let bytes = s.field("bytes").unwrap();
    assert_eq!(bytes.ty, FieldType::U32);
    assert_eq!(bytes.unit, Some("bytes"));
    assert_eq!(bytes.role, FieldRole::None);

    // The enum field carries its discriminant width and value→label map, resolved from the
    // `#[derive(EventEnum)]` on `Direction` — no per-field attribute.
    let dir = s.field("direction").unwrap();
    assert_eq!(dir.ty, FieldType::Enum { repr: 1 });
    let labels: Vec<_> = dir.enum_labels.iter().map(|l| (l.value, l.label)).collect();
    assert_eq!(labels, [(0, "Incoming"), (1, "Outgoing")]);

    assert_eq!(s.field("is_fin").unwrap().ty, FieldType::Bool);

    // Keys, in declaration order.
    let keys: Vec<_> = s.keys().map(|f| f.name).collect();
    assert_eq!(keys, ["connection_id"]);
}

/// A unit of work — a span.
#[derive(Event, IntoBytes, Immutable)]
#[event(namespace = "my_app::work", span = enter)]
#[repr(C)]
struct WorkStart {
    #[event(span_id)]
    span: u64,
    #[event(parent_span_id)]
    parent: u64,
    #[event(key)]
    job_id: u64,
}

#[test]
fn span_roles_and_phase_reflect() {
    let s = WorkStart::SCHEMA;
    assert_eq!(s.phase, backbeat::Phase::Enter);
    assert_eq!(s.span_id().unwrap().name, "span");
    assert_eq!(s.span_id().unwrap().role, FieldRole::SpanId);
    assert_eq!(s.parent_span().unwrap().name, "parent");
    assert_eq!(s.parent_span().unwrap().role, FieldRole::ParentSpanId);
    // The plain key field is unaffected.
    let keys: Vec<_> = s.keys().map(|f| f.name).collect();
    assert_eq!(keys, ["job_id"]);
}

#[test]
fn ring_records_event_without_allocating() {
    let ring = Ring::new(4096);
    let ev = PacketSent {
        connection_id: 0xABCD,
        bytes: 1500,
        direction: Direction::Outgoing,
        is_fin: true,
        _pad: [0; 2],
    };
    ring.push_event(&ev);

    let mut region = vec![0u8; ring.capacity()];
    let head = ring.snapshot_into(&mut region);
    let region = backbeat::bytes::Bytes::from(region);

    let mut records = Vec::new();
    walk(&region, head, ring.capacity(), |payload| {
        // Validator: a real record is the id prefix plus the event's bytes.
        if payload.len() != 8 + core::mem::size_of::<PacketSent>() {
            return false;
        }
        records.push(payload.to_vec());
        true
    });
    assert_eq!(records.len(), 1);

    // The record is `[event_id u64 LE][fields…]`.
    let rec = &records[0];
    let (id_bytes, fields) = rec.split_at(8);
    assert_eq!(
        u64::from_le_bytes(id_bytes.try_into().unwrap()),
        PacketSent::ID.get()
    );
    assert_eq!(fields, ev.as_bytes());
    assert_eq!(fields.len(), core::mem::size_of::<PacketSent>());
}

// Two events that share a qualified name but differ in layout must get *distinct* ids — that's the
// whole point of content-addressing: deployed versions don't alias in a dump's registry. These live
// in separate modules so they share the name `versioned::Evt`.
mod v1 {
    use super::*;
    #[derive(Event, IntoBytes, Immutable)]
    #[event(namespace = "versioned")]
    #[repr(C)]
    pub struct Evt {
        pub a: u64,
    }
}
mod v2 {
    use super::*;
    #[derive(Event, IntoBytes, Immutable)]
    #[event(namespace = "versioned")]
    #[repr(C)]
    pub struct Evt {
        pub a: u64,
        pub b: u32,
        pub _pad: [u8; 4],
    }
}

#[test]
fn content_addressed_id_forks_on_layout_change() {
    // Same qualified name…
    assert_eq!(v1::Evt::QUALIFIED_NAME, v2::Evt::QUALIFIED_NAME);
    // …but different layouts → different ids, so they're distinct event types in a registry.
    assert_ne!(v1::Evt::ID, v2::Evt::ID);
}
