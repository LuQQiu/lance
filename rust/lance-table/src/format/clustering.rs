// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Runtime form of the manifest's clustering definitions.
//!
//! A definition is immutable once allocated: fragments and index segments
//! reference it by `clustering_id`, and the provider-specific layout
//! description lives under provider-owned keys in `Manifest::table_metadata`.
//! Lance carries the definitions and the references; it never interprets them.

use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, Result};

use crate::format::pb;

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

impl TryFrom<pb::Clustering> for ClusteringDefinition {
    type Error = Error;

    fn try_from(p: pb::Clustering) -> Result<Self> {
        if p.id == 0 {
            return Err(Error::corrupt_file_named(
                "manifest",
                "clustering definition has id 0; ids are allocated from 1",
            ));
        }
        if p.columns.is_empty() {
            return Err(Error::corrupt_file_named(
                "manifest",
                format!("clustering definition {} has no columns", p.id),
            ));
        }
        let mut seen = std::collections::HashSet::with_capacity(p.columns.len());
        if let Some(duplicate) = p.columns.iter().find(|column| !seen.insert(**column)) {
            return Err(Error::corrupt_file_named(
                "manifest",
                format!(
                    "clustering definition {} repeats field id {} in {:?}",
                    p.id, duplicate, p.columns
                ),
            ));
        }
        if p.provider.is_empty() {
            return Err(Error::corrupt_file_named(
                "manifest",
                format!("clustering definition {} has an empty provider", p.id),
            ));
        }
        if p.version == 0 {
            return Err(Error::corrupt_file_named(
                "manifest",
                format!(
                    "clustering definition {} (provider {}) has version 0",
                    p.id, p.provider
                ),
            ));
        }
        Ok(Self {
            id: p.id,
            columns: p.columns,
            provider: p.provider,
            version: p.version,
        })
    }
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
}
