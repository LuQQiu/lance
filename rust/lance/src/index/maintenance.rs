// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Commit-time derived-index maintenance.
//!
//! A registered hook lets an index derive segment replacements inside the
//! SAME commit that writes the data those segments describe, with no separate
//! transaction and no scheduler. The commit path invokes hooks once per
//! commit attempt, against the manifest current at that attempt, so conflict
//! retries re-derive automatically. The manifest build validates each update
//! (a new segment may claim only fragments this commit creates plus coverage
//! the replaced segments already claimed) and drops it with a warning on any
//! mismatch: maintenance is best effort and never fails a data commit.

use std::sync::{Arc, RwLock};

use lance_core::Result;
use lance_table::format::Fragment;
use lance_table::transaction::DerivedIndexUpdate;

use crate::dataset::transaction::{Operation, Transaction};

use crate::dataset::Dataset;

/// Derives index-segment replacements for one commit attempt.
///
/// Implementations typically read statistics the write path left on the new
/// data files (for example footer seeds), fold them into index records, and
/// return replacement segments. They must tolerate any dataset state: a
/// failure or an invalid update only costs index coverage, never the commit.
#[async_trait::async_trait]
pub trait CommitIndexMaintenance: Send + Sync + std::fmt::Debug {
    /// Cheap filter: `derive` runs only when this returns true.
    fn applies_to(&self, operation: &Operation) -> bool;

    /// Derive segment replacements for this attempt.
    ///
    /// `new_fragments_with_ids` pairs each of the operation's new fragments
    /// with the FINAL id this attempt's manifest build will assign it,
    /// computed by the host with the same rule the build uses.
    async fn derive(
        &self,
        dataset: &Dataset,
        transaction: &Transaction,
        new_fragments_with_ids: &[(u64, Fragment)],
    ) -> Result<DerivedIndexUpdate>;
}

static HOOKS: RwLock<Vec<Arc<dyn CommitIndexMaintenance>>> = RwLock::new(Vec::new());

/// Register a commit-time maintenance hook for this process.
///
/// Like scalar index plugin registration this belongs at process startup;
/// commits running concurrently with registration may or may not see the
/// hook.
pub fn register_commit_index_maintenance(hook: Arc<dyn CommitIndexMaintenance>) {
    HOOKS
        .write()
        .expect("commit index maintenance registry poisoned")
        .push(hook);
}

pub(crate) fn commit_index_maintenance_hooks() -> Vec<Arc<dyn CommitIndexMaintenance>> {
    HOOKS
        .read()
        .expect("commit index maintenance registry poisoned")
        .clone()
}
