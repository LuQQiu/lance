// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! What a commit attempt settles about the index list before the manifest
//! is built.
//!
//! The commit layer decides index validity; the manifest builder assembles.
//! Once per attempt, against the manifest current at that attempt,
//! [`Transaction::prepare_indices`](crate::transaction::Transaction::prepare_indices)
//! reads the index list as it is, keeps that original list untouched, and
//! derives a prepared list on which an in-place column rewrite has already
//! withdrawn or pruned the coverage it invalidates (on a table with a tagged
//! fragment reuse history along the lineage, on any other table by fragment
//! id). The builder then applies the operation to the prepared list and
//! publishes only the final list; the original list is what MemWAL coverage
//! is compared against. Both lists are in-memory metadata of one attempt:
//! nothing is copied on disk and a retry prepares afresh from the list it
//! reads then.

use crate::format::IndexMetadata;
use crate::transaction::TaggedRewriteAssembly;
use std::sync::Arc;
use uuid::Uuid;

/// Index-segment replacements derived by a registered maintenance hook for
/// one commit attempt (see the `lance` crate's commit index maintenance).
///
/// Best-effort by contract: the manifest build validates the update against
/// what the attempt actually commits and drops it with a warning on any
/// mismatch; it never fails a data commit. Hooks are re-invoked per attempt,
/// so values never carry across retries.
#[derive(Debug, Clone, Default)]
pub struct DerivedIndexUpdate {
    /// Segments this update supersedes; they leave the index list when the
    /// update applies.
    pub replaced_uuids: Vec<Uuid>,
    /// Replacement segments. Each must have a fresh UUID and claim, via its
    /// fragment bitmap, only fragments this commit creates or coverage the
    /// replaced segments already claimed.
    pub new_segments: Vec<IndexMetadata>,
}

impl DerivedIndexUpdate {
    pub fn is_empty(&self) -> bool {
        self.replaced_uuids.is_empty() && self.new_segments.is_empty()
    }

    pub fn merge(&mut self, other: Self) {
        self.replaced_uuids.extend(other.replaced_uuids);
        self.new_segments.extend(other.new_segments);
    }
}

/// What the commit path settled about the fragment reuse entry for one
/// attempt. In-memory only, like the rewrite intent it carries.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub enum FragReuseUpdate {
    /// Nothing was prepared for the entry. A tagged entry arriving on a
    /// `Rewrite`, or a `CreateIndex` touching a tagged entry, is refused:
    /// without the commit path's derivation such a shape is a pre-assembled
    /// snapshot that may splice away records a concurrent writer appended.
    #[default]
    None,
    /// A `Rewrite` on a tagged history: the entry the commit path assembled
    /// for THIS manifest, merging the records the operation's entry adds
    /// onto the current entry and validating them.
    Rewrite(Arc<TaggedRewriteAssembly>),
    /// A trim of the tagged entry derived by the reuse index cleanup against
    /// the current entry at commit time.
    Trim,
}

/// The index lists of one commit attempt: the list as read from the
/// manifest current at the attempt, the prepared list the builder starts
/// from, and what was settled about the fragment reuse entry.
///
/// Only [`Transaction::prepare_indices`](crate::transaction::Transaction::prepare_indices)
/// builds one, so a value always went through the tagged-table write gate
/// and the unified withdrawal step; the manifest it was prepared against is
/// recorded so the builder refuses a result carried over to another attempt.
#[derive(Debug, Clone)]
pub struct PreparedIndices {
    manifest_version: Option<u64>,
    original: Vec<IndexMetadata>,
    prepared: Vec<IndexMetadata>,
    frag_reuse: FragReuseUpdate,
    derived: DerivedIndexUpdate,
}

impl PreparedIndices {
    pub(crate) fn new(
        manifest_version: Option<u64>,
        original: Vec<IndexMetadata>,
        prepared: Vec<IndexMetadata>,
        frag_reuse: FragReuseUpdate,
    ) -> Self {
        Self {
            manifest_version,
            original,
            prepared,
            frag_reuse,
            derived: DerivedIndexUpdate::default(),
        }
    }

    /// Attach index-segment replacements derived for this attempt.
    pub fn with_derived_update(mut self, derived: DerivedIndexUpdate) -> Self {
        self.derived = derived;
        self
    }

    /// The version of the manifest the lists were prepared against; `None`
    /// when the dataset is being created.
    pub fn manifest_version(&self) -> Option<u64> {
        self.manifest_version
    }

    /// The index list as read from the current manifest, never mutated.
    pub fn original(&self) -> &[IndexMetadata] {
        &self.original
    }

    /// The original list with the in-place rewrite step applied: the list
    /// the manifest builder starts from.
    pub fn prepared(&self) -> &[IndexMetadata] {
        &self.prepared
    }

    /// What the commit path settled about the fragment reuse entry.
    pub fn frag_reuse_update(&self) -> &FragReuseUpdate {
        &self.frag_reuse
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<IndexMetadata>,
        Vec<IndexMetadata>,
        FragReuseUpdate,
        DerivedIndexUpdate,
    ) {
        (self.original, self.prepared, self.frag_reuse, self.derived)
    }
}
