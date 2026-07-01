// Copyright (c) 2026 Cameron Bytheway
// SPDX-License-Identifier: MIT

//! `#[derive(Event)]` for backbeat.
//!
//! Annotate a `#[repr(C)]` struct to make it a traceable event. The derive reflects the struct's
//! fields into a `const` [`EventSchema`] (offsets via [`core::mem::offset_of!`], widths via
//! [`core::mem::size_of`]), computes a content-addressed [`EventId`] by hashing that whole schema,
//! and implements [`backbeat::Event`] — whose `zerocopy::IntoBytes` bound makes recording a single
//! memcpy and rejects padded layouts the reader can't describe.
//!
//! ```ignore
//! use backbeat::{Event, EventEnum};
//! use backbeat::zerocopy::{Immutable, IntoBytes};
//!
//! /// Which way a frame is going.
//! #[derive(EventEnum, IntoBytes, Immutable, Clone, Copy)]
//! #[repr(u8)]
//! enum Direction { Incoming = 0, Outgoing = 1 }
//!
//! /// A frame was queued for sending.
//! #[derive(Event, IntoBytes, Immutable)]
//! #[event(namespace = "my_crate::frame")]
//! #[repr(C)]
//! struct QueueData {
//!     #[event(key)]            packet_number: u64,
//!     /// Offset into the stream.
//!     #[event(unit = "bytes")] offset: u64,
//!     direction: Direction,    // a strongly-typed enum field
//!     is_fin: bool,
//! }
//! ```
//!
//! The generated code exposes `QueueData::SCHEMA` (an `EventSchema`), `QueueData::ID` (its
//! `EventId`), and `QueueData::QUALIFIED_NAME`, and implements `Event`. Because the id hashes the
//! whole schema, two builds whose layout or field metadata differ get distinct ids and never alias
//! in a dump's registry.
//!
//! Container attributes (on the struct):
//!
//! * `#[event(namespace = "…")]` — required; the event's namespace prefix.
//! * `#[event(span = enter)]` / `#[event(span = exit)]` — mark this event as one half of a span, so
//!   the trace converter can pair begin/end records into a duration slice. A spanned event must
//!   carry exactly one `#[event(span_id)]` field.
//! * `#[event(crate = <path>)]` — reroot the paths the generated code references (default
//!   `::backbeat`) at `<path>`. Use this when backbeat is re-exported from a wrapper crate — e.g.
//!   `#[event(crate = my_crate::backbeat)]` — so the derive resolves the traits and helpers through
//!   that re-export instead of assuming a direct `backbeat` dependency. `#[derive(EventEnum)]`
//!   accepts the same attribute.
//!
//! Field attributes (mutually-exclusive *roles* — at most one per field):
//!
//! * `#[event(key)]` — promote this field to a top-level join/index column in the output table.
//! * `#[event(span_id)]` — this `u64` is the span's own id (required on a `span = enter|exit`
//!   event; the enter and exit halves carry the same value so the converter pairs them).
//! * `#[event(parent_span_id)]` — this `u64` is the enclosing span's id, linking this event under
//!   its parent. Allowed on any event, including plain (non-span) ones.
//!
//! Other field attributes (combine with a role):
//!
//! * `#[event(unit = "…")]` — attach a unit hint (`"bytes"`, `"ns"`, …) carried into the output.
//! * `#[event(sentinel = <const>)]` — declare an in-band "absent" marker for an integer field (e.g.
//!   `sentinel = u64::MAX` for an unassigned packet number, or `sentinel = 0` for a `dump_id` only
//!   some events carry). The converter maps a field equal to its sentinel to SQL NULL, so the wire
//!   format needs no nulls yet `WHERE x IS NULL` works.
//! * `#[event(interned)]` / `#[event(interned(dynamic))]` — the (`u32`) field is an intern id
//!   resolved against the dump's intern table; `dynamic` marks runtime-built values.
//!
//! Enum-typed fields use a separate `#[derive(EventEnum)]` on the (`#[repr(uN)]`, fieldless) enum;
//! the field then carries the strong type and the schema records its value→label map automatically.
//!
//! Field and struct doc comments (`///`) are lifted verbatim into the schema's `description`
//! fields, so the embedded registry documents itself.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    parse_macro_input, parse_quote, Data, DeriveInput, Expr, ExprLit, Fields, Lit, LitStr, Path,
    Type,
};

/// A field's role, mirroring `backbeat::schema::FieldRole` on the macro side.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Key,
    SpanId,
    ParentSpanId,
}

impl Role {
    /// The `backbeat::schema::FieldRole` variant tokens this role emits, rooted at `krate`.
    fn tokens(self, krate: &Path) -> TokenStream2 {
        match self {
            Role::Key => quote! { #krate::schema::FieldRole::Key },
            Role::SpanId => quote! { #krate::schema::FieldRole::SpanId },
            Role::ParentSpanId => quote! { #krate::schema::FieldRole::ParentSpanId },
        }
    }
}

/// A struct's span phase, mirroring `backbeat::schema::Phase`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    None,
    Enter,
    Exit,
}

impl Phase {
    fn tokens(self, krate: &Path) -> TokenStream2 {
        match self {
            Phase::None => quote! { #krate::schema::Phase::None },
            Phase::Enter => quote! { #krate::schema::Phase::Enter },
            Phase::Exit => quote! { #krate::schema::Phase::Exit },
        }
    }
}

/// Derives `Event` for a struct: its compile-time `EventId`, a reflected `EventSchema`, and an
/// `impl Event`.
#[proc_macro_derive(Event, attributes(event))]
pub fn derive_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Derives `EventEnum` for a fieldless `#[repr(u8|u16|u32|u64)]` enum, so it can be a strongly-typed
/// event field. Emits the variant→label map and the repr width.
#[proc_macro_derive(EventEnum, attributes(event))]
pub fn derive_event_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_event_enum(input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand_event_enum(input: DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;

    // `#[event(crate = <path>)]` reroots the emitted paths; defaults to `::backbeat`.
    let krate = crate_path(&input.attrs)?;

    let data = match &input.data {
        Data::Enum(e) => e,
        _ => {
            return Err(syn::Error::new_spanned(
                &input,
                "`#[derive(EventEnum)]` can only be applied to an enum",
            ))
        }
    };

    // The discriminant repr width must be an explicit `#[repr(u8|u16|u32|u64)]` — the recorder
    // stores the enum inline at that width, and the reader needs it to read the discriminant.
    let repr = enum_repr_width(&input)?;

    // Each variant must be fieldless (it sits inline as a bare discriminant) and contributes a
    // value→label pair. An explicit discriminant sets the value; otherwise it follows C rules
    // (previous + 1, starting at 0). We require explicit discriminants so the on-disk value is
    // never silently shifted by reordering — the value is part of the event's identity.
    let mut labels = Vec::with_capacity(data.variants.len());
    for variant in &data.variants {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(syn::Error::new_spanned(
                variant,
                "`#[derive(EventEnum)]` requires fieldless variants",
            ));
        }
        let label = variant.ident.to_string();
        let value =
            match &variant.discriminant {
                Some((_, expr)) => expr.clone(),
                None => return Err(syn::Error::new_spanned(
                    variant,
                    "`#[derive(EventEnum)]` requires an explicit discriminant, e.g. `Variant = 0` \
                     (the value is part of the event's on-disk identity)",
                )),
            };
        labels.push(quote! {
            #krate::schema::EnumLabel { value: (#value) as u64, label: #label }
        });
    }

    Ok(quote! {
        impl #krate::EventEnum for #name {
            const REPR: u8 = #repr;
            const LABELS: &'static [#krate::schema::EnumLabel] = &[ #(#labels),* ];
        }
    })
}

/// Reads the discriminant byte-width from a `#[repr(uN)]` attribute on an enum.
fn enum_repr_width(input: &DeriveInput) -> syn::Result<u8> {
    for attr in &input.attrs {
        if !attr.path().is_ident("repr") {
            continue;
        }
        let mut width = None;
        attr.parse_nested_meta(|meta| {
            width = meta
                .path
                .get_ident()
                .and_then(|i| match i.to_string().as_str() {
                    "u8" => Some(1),
                    "u16" => Some(2),
                    "u32" => Some(4),
                    "u64" => Some(8),
                    _ => None,
                });
            Ok(())
        })?;
        if let Some(w) = width {
            return Ok(w);
        }
    }
    Err(syn::Error::new_spanned(
        input,
        "`#[derive(EventEnum)]` requires an explicit `#[repr(u8|u16|u32|u64)]`",
    ))
}

fn expand(input: DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;

    // Container attributes: namespace (required), span phase, and crate reroot (both optional).
    let container = ContainerAttrs::parse(&input)?;
    let krate = &container.krate;

    // `#[derive(Event)]` describes a fixed C layout; anything else has no stable offsets to reflect.
    let fields =
        match &input.data {
            Data::Struct(s) => match &s.fields {
                Fields::Named(named) => named.named.iter().collect::<Vec<_>>(),
                // No fields is legal only for a plain marker event — a span needs a `span_id` field.
                Fields::Unit => Vec::new(),
                Fields::Unnamed(_) => return Err(syn::Error::new_spanned(
                    &s.fields,
                    "`#[derive(Event)]` requires named fields (tuple structs have no field names \
                     to use as column names)",
                )),
            },
            _ => {
                return Err(syn::Error::new_spanned(
                    &input,
                    "`#[derive(Event)]` can only be applied to a struct",
                ))
            }
        };

    let description = doc_of(&input.attrs);

    let mut field_defs = Vec::with_capacity(fields.len());
    // Track span-role fields for cross-field validation after the loop.
    let mut span_id_fields = 0usize;
    let mut parent_span_fields = 0usize;
    for field in &fields {
        let ident = field.ident.as_ref().expect("named field");
        let fname = ident.to_string();
        let fdesc = doc_of(&field.attrs);
        let attrs = FieldAttrs::parse(&field.attrs)?;

        // A span-id / parent-span-id field must be a bare `u64`: the converter compares ids for
        // equality across the enter and exit structs, so a uniform width avoids cross-struct
        // mismatch, and these roles are meaningless on non-integer/narrower types.
        if matches!(attrs.role, Some(Role::SpanId | Role::ParentSpanId)) {
            require_u64(&field.ty, attrs.role.unwrap())?;
        }

        // `sentinel` is an integer "absent" marker, mapped to NULL by comparing the raw integer
        // image. It is meaningless on a `bool`, a byte array, or an interned (string) field — reject
        // the cases the macro can see syntactically so a mistake is a clear compile error rather
        // than silently nulling valid rows (e.g. a `sentinel = 1` on a `bool` nulling every `true`).
        if attrs.sentinel.is_some() {
            if attrs.interned.is_some() {
                return Err(syn::Error::new_spanned(
                    field,
                    "`#[event(sentinel = …)]` is not valid on an interned field (it marks an \
                     absent *integer*, compared against the raw value)",
                ));
            }
            if is_bool(&field.ty) || is_byte_array(&field.ty) {
                return Err(syn::Error::new_spanned(
                    &field.ty,
                    "`#[event(sentinel = …)]` is only valid on an integer field (the sentinel is \
                     an in-band absent marker compared against the raw integer value)",
                ));
            }
        }
        match attrs.role {
            Some(Role::SpanId) => span_id_fields += 1,
            Some(Role::ParentSpanId) => parent_span_fields += 1,
            _ => {}
        }

        let fty = &field.ty;

        // Resolve the field's `FieldType` and labels. An `#[event(interned)]` field is a `u32`
        // intern id (attribute-driven). Everything else goes through the `FieldTy` trait, resolved
        // at const-eval: primitives, `[u8; N]`, and any `#[derive(EventEnum)]` type all implement
        // it, so the macro doesn't need to know the field's concrete type — including enums, whose
        // variants it cannot see.
        let (ty_expr, labels_expr) = if let Some(dynamic) = attrs.interned {
            (
                quote! { #krate::schema::FieldType::Interned { dynamic: #dynamic } },
                quote! { &[] },
            )
        } else {
            (
                quote! { <#fty as #krate::FieldTy>::FIELD_TYPE },
                quote! { <#fty as #krate::FieldTy>::LABELS },
            )
        };

        let desc_expr = opt_str(fdesc.as_deref());
        let unit_expr = opt_str(attrs.unit.as_deref());
        let role_expr = match attrs.role {
            Some(r) => r.tokens(krate),
            None => quote! { #krate::schema::FieldRole::None },
        };
        // Emit the sentinel as the zero-extended image of the field's stored bytes, which is what
        // the converter reconstructs with a width-bounded `read_uint`. We (a) cast through the
        // field's own type so the literal is range-checked and signed values are well-defined, then
        // (b) widen to `u64` and mask to the field width to STRIP the sign extension that an
        // `iN as u64` performs. So `#[event(sentinel = -1)] code: i32` yields
        // `((-1i32) as u64) & 0xFFFF_FFFF` = `0x0000_0000_FFFF_FFFF` — the zero-extension of the 4
        // on-disk bytes. A full-width `u64::MAX`/`0` masks with `u64::MAX` (the `>= 8` guard avoids
        // a `1 << 64` overflow). All in `const` context.
        let sentinel_expr = match &attrs.sentinel {
            Some(expr) => quote! {
                ::core::option::Option::Some({
                    let w = ::core::mem::size_of::<#fty>();
                    let mask = if w >= 8 { u64::MAX } else { (1u64 << (8 * w)) - 1 };
                    (((#expr) as #fty) as u64) & mask
                })
            },
            None => quote! { ::core::option::Option::None },
        };

        field_defs.push(quote! {
            #krate::schema::FieldSchema {
                name: #fname,
                description: #desc_expr,
                ty: #ty_expr,
                offset: #krate::schema::layout_u16(::core::mem::offset_of!(#name, #ident)),
                width: #krate::schema::layout_u16(::core::mem::size_of::<#fty>()),
                role: #role_expr,
                unit: #unit_expr,
                sentinel: #sentinel_expr,
                enum_labels: #labels_expr,
            }
        });
    }

    // Cross-field span validation.
    if span_id_fields > 1 {
        return Err(syn::Error::new_spanned(
            &input,
            "an event may declare at most one `#[event(span_id)]` field",
        ));
    }
    if parent_span_fields > 1 {
        return Err(syn::Error::new_spanned(
            &input,
            "an event may declare at most one `#[event(parent_span_id)]` field",
        ));
    }
    match container.phase {
        // A spanned event must carry exactly one span id (the value enter/exit are paired by).
        Phase::Enter | Phase::Exit if span_id_fields == 0 => {
            return Err(syn::Error::new_spanned(
                &input,
                "`#[event(span = enter|exit)]` requires exactly one `#[event(span_id)]` field",
            ));
        }
        // A span id only has meaning on an enter/exit event; a plain event associates with a span
        // via `parent_span_id` instead.
        Phase::None if span_id_fields > 0 => {
            return Err(syn::Error::new_spanned(
                &input,
                "`#[event(span_id)]` requires the event to be a span \
                 (`#[event(span = enter)]` or `#[event(span = exit)]`)",
            ));
        }
        _ => {}
    }

    Ok(emit(name, &container, description, field_defs))
}

/// Emits the inherent consts (`ID`, `QUALIFIED_NAME`, `SCHEMA`) and the `Event` impl.
fn emit(
    name: &syn::Ident,
    container: &ContainerAttrs,
    description: Option<String>,
    field_defs: Vec<TokenStream2>,
) -> TokenStream2 {
    let krate = &container.krate;
    let qualified = format!("{}::{name}", container.namespace);
    let desc_expr = opt_str(description.as_deref());
    let phase_expr = container.phase.tokens(krate);

    quote! {
        impl #name {
            /// Fully-qualified event name, `"namespace::TypeName"`.
            pub const QUALIFIED_NAME: &'static str = #qualified;

            /// The reflected field layout (also held by [`Self::SCHEMA`]). Named so [`Self::ID`] can
            /// hash it without referencing `SCHEMA` (which embeds the id — that would be circular).
            const FIELDS: &'static [#krate::schema::FieldSchema] = &[ #(#field_defs),* ];

            /// Content-addressed event id: a hash of the whole schema (name, phase, every field's
            /// name/type/offset/width/role/unit and any enum labels). Two builds with differing
            /// layouts get distinct ids and are treated as separate event types sharing a name.
            pub const ID: #krate::id::EventId =
                #krate::schema::EventSchema::compute_id(
                    Self::QUALIFIED_NAME,
                    #phase_expr,
                    Self::FIELDS,
                );

            /// Self-describing layout of this event, reflected from its fields at compile time.
            pub const SCHEMA: #krate::schema::EventSchema =
                #krate::schema::EventSchema {
                    id: Self::ID,
                    qualified_name: Self::QUALIFIED_NAME,
                    description: #desc_expr,
                    record_size: #krate::schema::layout_u16(::core::mem::size_of::<#name>()),
                    phase: #phase_expr,
                    fields: Self::FIELDS,
                };
        }

        impl #krate::Event for #name {
            const SCHEMA: #krate::schema::EventSchema = Self::SCHEMA;
            const ID: #krate::id::EventId = Self::ID;
            const QUALIFIED_NAME: &'static str = Self::QUALIFIED_NAME;
        }

        // Register the type so the dumper can self-populate its schema registry. Expands to a
        // `submit!` under `std` and to nothing on `no_std` (see `backbeat::register_event!`).
        #krate::register_event!(#name);
    }
}

/// Field-level `#[event(...)]` attributes.
#[derive(Default)]
struct FieldAttrs {
    /// The field's role, if any (`key`/`span_id`/`parent_span_id`). At most one — a second is an
    /// error, so the illegal combinations are unrepresentable.
    role: Option<Role>,
    unit: Option<String>,
    /// `Some(dynamic)` if the field is interned.
    interned: Option<bool>,
    /// `Some(expr)` if the field declares a `#[event(sentinel = …)]` "absent" marker. The expression
    /// is emitted as `(expr) as <field type> as u64` so a typed/signed/narrow constant (`u64::MAX`,
    /// `0`, `-1` on an `i32`, a named const) widens to the same image the converter reads back.
    sentinel: Option<Expr>,
}

impl FieldAttrs {
    fn parse(attrs: &[syn::Attribute]) -> syn::Result<Self> {
        let mut out = FieldAttrs::default();
        for attr in attrs {
            if !attr.path().is_ident("event") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("key") {
                    out.set_role(Role::Key, &meta)?;
                    Ok(())
                } else if meta.path.is_ident("span_id") {
                    out.set_role(Role::SpanId, &meta)?;
                    Ok(())
                } else if meta.path.is_ident("parent_span_id") {
                    out.set_role(Role::ParentSpanId, &meta)?;
                    Ok(())
                } else if meta.path.is_ident("unit") {
                    let lit: LitStr = meta.value()?.parse()?;
                    out.unit = Some(lit.value());
                    Ok(())
                } else if meta.path.is_ident("sentinel") {
                    // `sentinel = <expr>`: an integer constant the producer uses to mean "absent".
                    let expr: Expr = meta.value()?.parse()?;
                    out.sentinel = Some(expr);
                    Ok(())
                } else if meta.path.is_ident("interned") {
                    // `interned` or `interned(dynamic)`.
                    let mut dynamic = false;
                    if meta.input.peek(syn::token::Paren) {
                        meta.parse_nested_meta(|inner| {
                            if inner.path.is_ident("dynamic") {
                                dynamic = true;
                                Ok(())
                            } else {
                                Err(inner.error("expected `dynamic`"))
                            }
                        })?;
                    }
                    out.interned = Some(dynamic);
                    Ok(())
                } else {
                    Err(meta.error("unknown `#[event(...)]` field attribute"))
                }
            })?;
        }
        Ok(out)
    }

    /// Sets the field's role, erroring if one was already set (roles are mutually exclusive).
    fn set_role(&mut self, role: Role, meta: &syn::meta::ParseNestedMeta<'_>) -> syn::Result<()> {
        if self.role.is_some() {
            return Err(meta.error(
                "a field may have at most one role \
                 (`key` / `span_id` / `parent_span_id` are mutually exclusive)",
            ));
        }
        self.role = Some(role);
        Ok(())
    }
}

/// Container-level (`#[event(...)]` on the struct) attributes: the required namespace and the
/// optional span phase.
struct ContainerAttrs {
    namespace: String,
    phase: Phase,
    /// The path the generated code roots its `backbeat` references at, from
    /// `#[event(crate = <path>)]`; defaults to `::backbeat`. Lets a wrapper crate re-export
    /// backbeat under its own name and still derive events.
    krate: Path,
}

impl ContainerAttrs {
    fn parse(input: &DeriveInput) -> syn::Result<Self> {
        let mut namespace = None;
        let mut phase = Phase::None;
        let mut krate = None;
        for attr in &input.attrs {
            if !attr.path().is_ident("event") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("namespace") {
                    let lit: LitStr = meta.value()?.parse()?;
                    namespace = Some(lit.value());
                    Ok(())
                } else if meta.path.is_ident("crate") {
                    // `crate = <path>`: reroot emitted `::backbeat::…` references at `<path>`.
                    if krate.is_some() {
                        return Err(meta.error("duplicate `crate` in `#[event(...)]`"));
                    }
                    krate = Some(parse_crate_path(&meta)?);
                    Ok(())
                } else if meta.path.is_ident("span") {
                    // `span = enter` or `span = exit`.
                    let ident: syn::Ident = meta.value()?.parse()?;
                    phase = match ident.to_string().as_str() {
                        "enter" => Phase::Enter,
                        "exit" => Phase::Exit,
                        _ => {
                            return Err(meta.error("`span` must be `enter` or `exit`"));
                        }
                    };
                    Ok(())
                } else {
                    Err(meta.error("unknown container-level `#[event(...)]` attribute"))
                }
            })?;
        }
        let namespace = namespace.ok_or_else(|| {
            syn::Error::new_spanned(
                input,
                "`#[derive(Event)]` requires `#[event(namespace = \"...\")]`",
            )
        })?;
        let krate = krate.unwrap_or_else(default_crate_path);
        Ok(Self {
            namespace,
            phase,
            krate,
        })
    }
}

/// The default crate path (`::backbeat`) the generated code roots at when no
/// `#[event(crate = <path>)]` is given.
fn default_crate_path() -> Path {
    parse_quote! { ::backbeat }
}

/// Parses the `<path>` in `#[event(crate = <path>)]`. Accepts any path so a wrapper crate can point
/// at its own re-export (`crate`, `my_crate::backbeat`, `::some::backbeat`).
fn parse_crate_path(meta: &syn::meta::ParseNestedMeta<'_>) -> syn::Result<Path> {
    meta.value()?.parse()
}

/// Scans container attributes for a single `#[event(crate = <path>)]`, defaulting to `::backbeat`.
/// Used by `#[derive(EventEnum)]`, which has no other container attributes.
fn crate_path(attrs: &[syn::Attribute]) -> syn::Result<Path> {
    let mut krate = None;
    for attr in attrs {
        if !attr.path().is_ident("event") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("crate") {
                if krate.is_some() {
                    return Err(meta.error("duplicate `crate` in `#[event(...)]`"));
                }
                krate = Some(parse_crate_path(&meta)?);
                Ok(())
            } else {
                Err(meta.error(
                    "unknown `#[event(...)]` attribute (`#[derive(EventEnum)]` only \
                                 accepts `crate = <path>`)",
                ))
            }
        })?;
    }
    Ok(krate.unwrap_or_else(default_crate_path))
}

/// Whether a field type is the bare `bool` primitive.
fn is_bool(ty: &Type) -> bool {
    matches!(ty, Type::Path(p) if p.path.is_ident("bool"))
}

/// Whether a field type is an array `[_; N]` (e.g. `[u8; 16]` — a `Bytes` field).
fn is_byte_array(ty: &Type) -> bool {
    matches!(ty, Type::Array(_))
}

/// Enforces that a `span_id` / `parent_span_id` field is a bare `u64`.
fn require_u64(ty: &Type, role: Role) -> syn::Result<()> {
    let is_u64 = matches!(ty, Type::Path(p) if p.path.is_ident("u64"));
    if is_u64 {
        Ok(())
    } else {
        let attr = match role {
            Role::SpanId => "span_id",
            Role::ParentSpanId => "parent_span_id",
            Role::Key => unreachable!("require_u64 is only called for span roles"),
        };
        Err(syn::Error::new_spanned(
            ty,
            format!(
                "`#[event({attr})]` fields must be `u64` (span ids are compared for equality \
                 across the enter/exit events, so they need a uniform width)"
            ),
        ))
    }
}

/// Joins the `///` doc-comment lines on `attrs` into a single trimmed string, or `None` if there
/// are none. Each `///` line is a `#[doc = "..."]` attribute with a leading space.
fn doc_of(attrs: &[syn::Attribute]) -> Option<String> {
    let mut lines = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("doc") {
            continue;
        }
        if let syn::Meta::NameValue(nv) = &attr.meta {
            if let Expr::Lit(ExprLit {
                lit: Lit::Str(s), ..
            }) = &nv.value
            {
                lines.push(s.value().trim().to_string());
            }
        }
    }
    if lines.is_empty() {
        None
    } else {
        Some(lines.join("\n"))
    }
}

/// `Some("…")` → `::core::option::Option::Some("…")`, `None` → `::core::option::Option::None`.
fn opt_str(s: Option<&str>) -> TokenStream2 {
    match s {
        Some(s) => quote! { ::core::option::Option::Some(#s) },
        None => quote! { ::core::option::Option::None },
    }
}
