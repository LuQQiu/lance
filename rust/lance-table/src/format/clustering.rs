// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Runtime form of the manifest's clustering definitions.
//!
//! A definition is immutable once allocated: fragments and index segments
//! reference it by `clustering_id`, and the provider-specific layout
//! description lives under provider-owned keys in `Manifest::table_metadata`.
//! Lance carries the definitions and the references; it never interprets them.

use std::collections::HashSet;

use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, Result};

use crate::format::pb;
use crate::format::{IndexMetadata, Manifest};

/// An immutable clustering definition referenced by fragment and index
/// segment `clustering_id`.
#[derive(Debug, Clone, PartialEq, Eq, DeepSizeOf)]
pub struct ClusteringDefinition {
    /// Non-zero table-local ID allocated through `Manifest::max_clustering_id`.
    pub id: u32,
    /// Ordered, non-empty list of distinct schema field IDs the layout is keyed on.
    pub columns: Vec<i32>,
    /// Non-empty, case-sensitive clustering implementation name.
    pub provider: String,
    /// Non-zero layout/configuration identifier owned by the provider. It is
    /// neither a dataset version nor a software version.
    pub version: u64,
}

impl ClusteringDefinition {
    /// Check the invariants every definition must hold, wherever it came from.
    pub fn validate(&self) -> Result<()> {
        if self.id == 0 {
            return Err(Error::invalid_input(
                "clustering definition has id 0; ids are allocated from 1",
            ));
        }
        if self.columns.is_empty() {
            return Err(Error::invalid_input(format!(
                "clustering definition {} has no columns",
                self.id
            )));
        }
        let mut seen = HashSet::with_capacity(self.columns.len());
        if let Some(duplicate) = self.columns.iter().find(|column| !seen.insert(**column)) {
            return Err(Error::invalid_input(format!(
                "clustering definition {} repeats field id {} in {:?}",
                self.id, duplicate, self.columns
            )));
        }
        if self.provider.is_empty() {
            return Err(Error::invalid_input(format!(
                "clustering definition {} has an empty provider",
                self.id
            )));
        }
        if self.version == 0 {
            return Err(Error::invalid_input(format!(
                "clustering definition {} (provider {}) has version 0",
                self.id, self.provider
            )));
        }
        Ok(())
    }
}

impl TryFrom<pb::Clustering> for ClusteringDefinition {
    type Error = Error;

    fn try_from(p: pb::Clustering) -> Result<Self> {
        let definition = Self {
            id: p.id,
            columns: p.columns,
            provider: p.provider,
            version: p.version,
        };
        definition
            .validate()
            .map_err(|error| Error::corrupt_file_named("manifest", error.to_string()))?;
        Ok(definition)
    }
}

/// Check the clustering invariants of a manifest and its index section.
///
/// Definitions are well formed with unique ids at or below the high-water
/// mark, and every fragment or index segment reference names a definition in
/// the manifest. A definition's key columns are not checked against the
/// current schema: a column may have been dropped after the definition was
/// allocated, and the objects still referencing it are not thereby invalid.
pub fn validate_clustering(manifest: &Manifest, indices: &[IndexMetadata]) -> Result<()> {
    let mut ids = HashSet::with_capacity(manifest.clustering.len());
    for definition in &manifest.clustering {
        definition.validate()?;
        if definition.id > manifest.max_clustering_id {
            return Err(Error::invalid_input(format!(
                "clustering definition {} is above max_clustering_id {}",
                definition.id, manifest.max_clustering_id
            )));
        }
        if !ids.insert(definition.id) {
            return Err(Error::invalid_input(format!(
                "clustering definition {} is defined more than once",
                definition.id
            )));
        }
    }
    for fragment in manifest.fragments.iter() {
        if let Some(id) = fragment.clustering_id
            && !ids.contains(&id)
        {
            return Err(Error::invalid_input(format!(
                "fragment {} references clustering definition {}, which the manifest does not define",
                fragment.id, id
            )));
        }
    }
    for index in indices {
        if let Some(id) = index.clustering_id
            && !ids.contains(&id)
        {
            return Err(Error::invalid_input(format!(
                "index segment {} ({}) references clustering definition {}, which the manifest does not define",
                index.uuid, index.name, id
            )));
        }
    }
    Ok(())
}

/// Apply a writer's COMPLETE desired definitions list to `manifest`.
///
/// The writer built `desired` from the manifest it read: every definition that
/// manifest had, unchanged, plus the ones it wants to add. Against the manifest
/// the transaction actually lands on, this requires that every current
/// definition appears in `desired` with identical content (matched by id;
/// order is not significant), that nothing else reuses a current id, and that
/// the remaining entries, in list order, carry `max_clustering_id + 1, +2, ...`.
/// New entries are validated and must be keyed on columns the schema has. A
/// list that adds nothing leaves the manifest untouched.
///
/// A dropped or changed current definition, a reused id or an id that no
/// longer continues the mark means the list was built from a stale snapshot;
/// that is a commit conflict the writer must resolve by reloading and
/// rebuilding the list, not by retrying the same transaction. `commit_version`
/// is only used to label that error.
pub fn apply_complete_clustering_definitions(
    manifest: &mut Manifest,
    desired: &[ClusteringDefinition],
    commit_version: u64,
) -> Result<()> {
    let stale = |message: String| Error::commit_conflict_source(commit_version, message.into());
    let mut seen = HashSet::with_capacity(desired.len());
    if let Some(duplicate) = desired.iter().find(|d| !seen.insert(d.id)) {
        return Err(Error::invalid_input(format!(
            "desired clustering definitions list names id {} more than once",
            duplicate.id
        )));
    }
    for current in &manifest.clustering {
        match desired.iter().find(|d| d.id == current.id) {
            Some(d) if d == current => {}
            Some(_) => {
                return Err(stale(format!(
                    "desired clustering definitions list changes definition {}, which is immutable; \
                     reload the manifest and rebuild the list",
                    current.id
                )));
            }
            None => {
                return Err(stale(format!(
                    "desired clustering definitions list drops definition {}; definitions are \
                     append-only, reload the manifest and rebuild the list",
                    current.id
                )));
            }
        }
    }
    let mut next = manifest.max_clustering_id;
    let mut added = Vec::new();
    for definition in desired {
        if manifest.clustering.iter().any(|c| c.id == definition.id) {
            continue;
        }
        definition.validate()?;
        let Some(expected) = next.checked_add(1) else {
            return Err(Error::invalid_input(
                "clustering definition ids are exhausted",
            ));
        };
        if definition.id != expected {
            return Err(stale(format!(
                "new clustering definition {} does not continue max_clustering_id {} (expected {}); \
                 reload the manifest and rebuild the list",
                definition.id, manifest.max_clustering_id, expected
            )));
        }
        if let Some(missing) = definition
            .columns
            .iter()
            .find(|column| manifest.schema.field_by_id(**column).is_none())
        {
            return Err(Error::invalid_input(format!(
                "clustering definition {} is keyed on field id {missing}, which is not in the schema",
                definition.id
            )));
        }
        next = expected;
        added.push(definition.clone());
    }
    if added.is_empty() {
        return Ok(());
    }
    manifest.clustering.extend(added);
    manifest.max_clustering_id = next;
    Ok(())
}

/// Add every definition of `other` that `target` lacks, keeping ids sorted.
///
/// A definition is immutable, so the same id must carry the same content on
/// both sides; anything else is an error rather than an overwrite.
pub fn merge_clustering_definitions(
    target: &mut Vec<ClusteringDefinition>,
    other: &[ClusteringDefinition],
) -> Result<()> {
    for definition in other {
        match target.iter().find(|existing| existing.id == definition.id) {
            Some(existing) if existing == definition => {}
            Some(existing) => {
                return Err(Error::invalid_input(format!(
                    "clustering definition {} differs between manifests: {:?} versus {:?}",
                    definition.id, existing, definition
                )));
            }
            None => target.push(definition.clone()),
        }
    }
    target.sort_by_key(|definition| definition.id);
    Ok(())
}

impl From<&ClusteringDefinition> for pb::Clustering {
    fn from(definition: &ClusteringDefinition) -> Self {
        Self {
            id: definition.id,
            columns: definition.columns.clone(),
            provider: definition.provider.clone(),
            version: definition.version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn valid() -> pb::Clustering {
        pb::Clustering {
            id: 3,
            columns: vec![4, 1],
            provider: "org.example.cluster".into(),
            version: 2,
        }
    }

    #[test]
    fn round_trips_a_valid_definition() {
        let definition = ClusteringDefinition::try_from(valid()).unwrap();
        assert_eq!(definition.id, 3);
        assert_eq!(definition.columns, vec![4, 1]);
        assert_eq!(pb::Clustering::from(&definition), valid());
    }

    #[rstest]
    #[case::zero_id(pb::Clustering { id: 0, ..valid() }, "id 0")]
    #[case::no_columns(pb::Clustering { columns: vec![], ..valid() }, "no columns")]
    #[case::duplicate_column(pb::Clustering { columns: vec![1, 2, 1], ..valid() }, "repeats field id 1")]
    #[case::empty_provider(pb::Clustering { provider: String::new(), ..valid() }, "empty provider")]
    #[case::zero_version(pb::Clustering { version: 0, ..valid() }, "version 0")]
    fn rejects_an_invalid_definition(#[case] proto: pb::Clustering, #[case] message: &str) {
        let error = ClusteringDefinition::try_from(proto).unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }

    fn manifest_with(definitions: Vec<ClusteringDefinition>, max_id: u32) -> Manifest {
        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int32,
            false,
        )]);
        let mut manifest = Manifest::new(
            lance_core::datatypes::Schema::try_from(&schema).unwrap(),
            std::sync::Arc::new(vec![crate::format::Fragment::new(0)]),
            crate::format::DataStorageFormat::new(lance_file::version::ConcreteFileVersion::V2_0),
            std::collections::HashMap::new(),
        );
        manifest.clustering = definitions;
        manifest.max_clustering_id = max_id;
        manifest
    }

    fn definition(id: u32) -> ClusteringDefinition {
        ClusteringDefinition {
            id,
            columns: vec![0],
            provider: "org.example.cluster".into(),
            version: 1,
        }
    }

    #[test]
    fn validates_definitions_and_references() {
        let manifest = manifest_with(vec![definition(1), definition(2)], 2);
        validate_clustering(&manifest, &[]).unwrap();

        let above_mark = manifest_with(vec![definition(3)], 2);
        let error = validate_clustering(&above_mark, &[]).unwrap_err();
        assert!(
            error.to_string().contains("above max_clustering_id"),
            "{error}"
        );

        let duplicate = manifest_with(vec![definition(1), definition(1)], 1);
        let error = validate_clustering(&duplicate, &[]).unwrap_err();
        assert!(error.to_string().contains("more than once"), "{error}");

        let mut dangling = manifest_with(vec![definition(1)], 1);
        std::sync::Arc::make_mut(&mut dangling.fragments)[0].clustering_id = Some(2);
        let error = validate_clustering(&dangling, &[]).unwrap_err();
        assert!(
            error.to_string().contains("fragment 0 references"),
            "{error}"
        );

        let mut zero = manifest_with(vec![definition(1)], 1);
        std::sync::Arc::make_mut(&mut zero.fragments)[0].clustering_id = Some(0);
        assert!(validate_clustering(&zero, &[]).is_err());

        // A key column dropped after allocation does not invalidate the definition.
        let dropped_column = manifest_with(
            vec![ClusteringDefinition {
                columns: vec![42],
                ..definition(1)
            }],
            1,
        );
        validate_clustering(&dropped_column, &[]).unwrap();
    }

    #[test]
    fn validates_index_segment_references() {
        let manifest = manifest_with(vec![definition(1)], 1);
        let mut segment = crate::transaction::test_support::sample_index_metadata("idx");
        segment.clustering_id = Some(1);
        validate_clustering(&manifest, std::slice::from_ref(&segment)).unwrap();
        segment.clustering_id = Some(7);
        let error = validate_clustering(&manifest, &[segment]).unwrap_err();
        assert!(error.to_string().contains("index segment"), "{error}");
    }

    #[test]
    fn merges_definitions_by_id_and_rejects_conflicts() {
        let mut target = vec![definition(2)];
        merge_clustering_definitions(&mut target, &[definition(1), definition(2)]).unwrap();
        assert_eq!(target, vec![definition(1), definition(2)]);

        let conflicting = ClusteringDefinition {
            provider: "org.example.other".into(),
            ..definition(2)
        };
        let error = merge_clustering_definitions(&mut target, &[conflicting]).unwrap_err();
        assert!(
            error.to_string().contains("differs between manifests"),
            "{error}"
        );
    }
}
