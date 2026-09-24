# Clustering Providers

Clustering describes a physical layout, not different logical table contents.
This contract identifies the implementation that organized each fragment and
the declaration it used. It does not prescribe a clustering algorithm, sampling
procedure, coordinate system, file boundary, or maintenance schedule.

The definitions are in `ClusteringState`, `ClusteringDeclaration`, and
`ClusteringReference` in `table.proto`. This feature is reserved but unsupported
until its writer preservation and invalidation rules are implemented.

## Table Declarations

`Manifest.clustering` contains an optional `current` reference and a list of
`declarations`. Absence of the state means no clustering declaration or fragment
reference. Absence of `current` disables new clustering work but permits retained
declarations and previously organized fragments. Ordinary appends need not be
clustered even when `current` is present.

Each declaration contains:

- `reference`: a non-empty, case-sensitive provider name and a non-zero `uint64`
  version. Provider names identify implementations, not users or engines in
  general; namespaced names are recommended to avoid collisions.
- `columns`: an ordered, non-empty list of distinct, non-negative schema field
  IDs. These identify all columns whose values or types affect the layout.
- `provider_metadata`: optional `google.protobuf.Any` configuration. Absence
  means no configuration; when present, its type URL must be non-empty. The
  named provider defines the message, its versions, and their compatibility.

The pair `(provider, version)` identifies one immutable declaration within a
table. Versions are provider-assigned identifiers, not dataset versions or
software releases. Different providers may use the same numeric version. No
ordering or compatibility is inferred from comparing versions. A pair must not
be reassigned to different columns, a different type URL, or different payload
bytes, including after its declaration is removed. A provider must allocate
identifiers accordingly, including when writing on divergent branches.

Declarations must have unique references. `current` and each live fragment's
reference must resolve to exactly one declaration in the same manifest; opening
an older dataset snapshot must not be necessary to resolve them. A current or
fragment-referenced declaration's column IDs must exist in that manifest's
schema. Unreferenced historical declarations may mention dropped fields.

Changing columns, provider configuration, or provider selects a new declaration;
it does not relabel old fragments or implicitly rewrite their data. Declarations
must remain while referenced by `current` or a live fragment. Unreferenced
declarations may be removed without changing earlier snapshots. An old provider
need not be installed to retain its declaration and fragments.

This contract stores provider configuration inline. It grants no file-retention
or clone semantics to paths embedded in `Any`; provider-specific external
artifacts require a separate lifecycle contract. Fragments carry no opaque
payloads or arbitrary key-value maps.

### Existing Schema Hints

When `Manifest.clustering` is present, declarations are the source of clustering
columns. Writers must clear `Field.unenforced_clustering_key_position` and the
older `Field.unenforced_clustering_key` hints rather than maintain a second copy
of the current declaration in schema metadata. When the state is absent, those
existing hints retain their prior meaning and do not imply fragment provenance.

## Fragment References and Maintenance Ownership

`DataFragment.clustering` contains only a provider and version. Absence makes no
claim about whether the fragment is organized. A present reference certifies
that the provider produced or verified the layout under the referenced
declaration; it is not proof of sort order, disjoint ranges, or query coverage.

Only an implementation that understands a declaration may assign its reference
to a fragment. Providers may represent generations, rewrite groups, or other
layout properties in their own declarations; the table format does not mandate
those concepts.

By default, provider-driven clustering maintenance may select unstamped
fragments and fragments belonging to the same provider. It must not select
another provider's fragments unless the user explicitly requests a takeover or
the new provider documents that it supersedes that implementation. Sharing a
provider name alone does not make different declaration versions compatible.

This ownership rule governs clustering selection, not permission to modify
table rows. It must not prohibit ordinary append, update, delete, or compaction
solely because the writer lacks a particular clustering provider.

## Writer Requirements

All writers supporting this feature must preserve retained declarations,
including unknown provider names, type URLs, and payload bytes. An unrelated
commit must not replace provider metadata with defaults or discard it. Invalid
references must not be emitted.

The following conservative rules apply without invoking a provider:

- Retain the reference of an unchanged fragment. Deletion-only changes may
  retain it: a provider's layout claim must remain valid for a subset of its
  rows at unchanged physical offsets. The claim does not promise current size
  or maintenance quality.
- Clear the reference on a new fragment produced by an ordinary rewrite,
  including split, merge, compaction, and row reordering. A provider-aware
  rewrite may assign a reference after verifying its own layout requirements.
- Clear a retained fragment's reference when an in-place column replacement or
  overlay changes a declared column. Changes limited to other columns may
  retain it only when row membership and physical row order are unchanged.
- Preserve references through renames that retain field IDs and types. Removing
  a declared field, changing its type, or changing a containing nested type
  clears the affected fragment references and `current`, if affected. An
  unknown provider must not prevent that schema operation.
- For a full overwrite, discard references on replaced fragments. Preserve the
  current declaration only if its field IDs and types remain compatible;
  otherwise clear `current`. Never resolve historical columns by name to new
  field IDs. Newly written fragments are unstamped unless verified by a provider.

A layout declaration, fragment replacements, and their references must become
visible in one atomic snapshot. A writer must revalidate its selected
declaration and source fragments against concurrent commits. It must not
overwrite a concurrent declaration change or drop references installed by
another writer. Publishing new fragments and assigning their references in
separate commits is permitted only if the intermediate fragments are unstamped
and the later assignment validates that they have not changed.

Snapshot restore restores that snapshot's declarations and references together.
Clone and import may preserve references only if they also preserve their
declarations and layout validity. Conflicting declarations with the same pair
must not be silently merged; the operation must reject the conflict or clear the
imported references. Fragment IDs alone are not clustering identifiers.

## Reader and Statistics Contract

Readers may ignore clustering metadata and scan the table normally. They must
not prune data or assume ordering based only on a provider/version reference.
Index coverage, row-address translation, and deletion handling retain their own
contracts and feature requirements.

Statistics producers and readers must agree on the value-comparison semantics
of the statistics they exchange, including strings, nulls, NaNs, and typed
bounds. A provider's normalization or internal ordering cannot silently change
those semantics. Provider-specific statistics require a reader or index plugin
that understands their contract; otherwise the optimization must not be used.

This feature does not introduce a statistics format or require a particular
index. Existing statistics and indices may be used independently of the
clustering provider. Missing or incomplete statistics cannot justify skipping
data. The provider owns layout quality; statistics describe actual data.

## Compatibility

A manifest containing clustering state or a fragment reference must set
`FLAG_CLUSTERING_METADATA` (`1 << 12`) in `writer_feature_flags`, not in
`reader_feature_flags`. A writer without this capability must refuse to modify
the table. A writer with the capability may carry unknown providers and perform
ordinary operations using the preservation and invalidation rules above; it
must reject requests to execute an unsupported provider.

The bit may be cleared only when both table state and all fragment references
are absent. A provider/configuration upgrade does not itself require a new
table feature. New semantics that cannot be safely preserved or invalidated by
these rules require separate feature negotiation; they cannot be smuggled into
an opaque payload and treated as ordinary clustering configuration.

## Relationship to Delta

Delta's [clustered-table protocol](https://github.com/delta-io/delta/blob/ef3e91b509365cdc2b23c1afdbbab3b4d3828d63/PROTOCOL.md#clustered-table)
separates clustering columns, file-level provider identity, and provider-owned
metadata. Its [domain metadata contract](https://github.com/delta-io/delta/blob/ef3e91b509365cdc2b23c1afdbbab3b4d3828d63/PROTOCOL.md#domain-metadata)
requires preservation of unknown domains. This proposal adopts that separation
and maintenance-ownership principle, not Delta's JSON actions or ZCube tags.
Lance uses versioned declarations and small fragment references instead of
opening a general-purpose fragment metadata map.
