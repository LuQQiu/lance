// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Clustering definitions and generic object metadata through the real
//! dataset lifecycle: same-object operations keep them, new objects do not
//! inherit them, and restore, branch and clone carry them.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use lance_core::utils::tempfile::TempStrDir;
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::ScalarIndexParams;
use lance_table::feature_flags::FLAG_OBJECT_METADATA;
use lance_table::format::{ClusteringDefinition, IndexMetadata, Manifest};
use lance_table::io::manifest::read_manifest_indexes;

use crate::Dataset;
use crate::dataset::optimize::{CompactionOptions, compact_files};
use crate::dataset::transaction::Operation;
use crate::dataset::write::{CommitBuilder, InsertBuilder, WriteMode, WriteParams};
use crate::dataset::{ManifestWriteConfig, NewColumnTransform, write_manifest_file};
use crate::index::DatasetIndexExt;

const PROVIDER: &str = "org.example.cluster";
const BUCKET_KEY: &str = "org.example.cluster.bucket";
const UNKNOWN_KEY: &str = "org.example.unknown";

fn schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::Int32,
        false,
    )]))
}

fn rows(range: std::ops::Range<i32>) -> impl RecordBatchReader + Send + 'static {
    let batch = RecordBatch::try_new(
        schema(),
        vec![Arc::new(Int32Array::from_iter_values(range))],
    )
    .unwrap();
    RecordBatchIterator::new(vec![Ok(batch)], schema())
}

/// Two fragments of fifty rows each.
async fn write_dataset(uri: &str) -> Dataset {
    Dataset::write(
        rows(0..100),
        uri,
        Some(WriteParams {
            max_rows_per_file: 50,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
}

fn definition(id: u32) -> ClusteringDefinition {
    ClusteringDefinition {
        id,
        columns: vec![0],
        provider: PROVIDER.into(),
        version: 1,
    }
}

fn object_metadata(bucket: &str) -> HashMap<String, String> {
    HashMap::from([
        (BUCKET_KEY.to_string(), bucket.to_string()),
        (UNKNOWN_KEY.to_string(), "opaque".to_string()),
        ("org.example.empty".to_string(), String::new()),
    ])
}

/// Commit a hand-edited manifest. No transaction allocates definitions yet,
/// so tests stamp them (and object metadata) the way a provider's commit
/// eventually will, then exercise the real operations on top.
async fn commit_edited_manifest(
    dataset: &Dataset,
    edit: impl FnOnce(&mut Manifest, &mut Vec<IndexMetadata>),
) -> Dataset {
    let mut manifest = dataset.manifest().clone();
    let mut indices =
        read_manifest_indexes(&dataset.object_store, &dataset.manifest_location, &manifest)
            .await
            .unwrap();
    edit(&mut manifest, &mut indices);
    manifest.version += 1;
    write_manifest_file(
        dataset.object_store.as_ref(),
        dataset.commit_handler.as_ref(),
        &dataset.base,
        &mut manifest,
        Some(indices),
        &ManifestWriteConfig {
            auto_set_feature_flags: true,
            timestamp: None,
            use_stable_row_ids: false,
            use_legacy_format: None,
            storage_format: None,
            disable_transaction_file: false,
            migration_next_row_id: None,
            activate_non_reusable_field_ids: false,
            tagged_frag_reuse_trim: false,
        },
        dataset.manifest_location.naming_scheme,
        None,
        false,
    )
    .await
    .unwrap();
    Dataset::open(dataset.uri()).await.unwrap()
}

fn stamp_fragment(manifest: &mut Manifest, position: usize, clustering_id: u32, bucket: &str) {
    let fragment = &mut Arc::make_mut(&mut manifest.fragments)[position];
    fragment.clustering_id = Some(clustering_id);
    fragment.metadata = object_metadata(bucket);
}

fn fragment_by_id(dataset: &Dataset, id: u64) -> &lance_table::format::Fragment {
    dataset
        .fragments()
        .iter()
        .find(|fragment| fragment.id == id)
        .unwrap_or_else(|| panic!("fragment {id} missing"))
}

#[tokio::test]
async fn same_object_operations_keep_fragment_metadata_and_set_the_writer_flag() {
    let dir = TempStrDir::default();
    let dataset = write_dataset(&dir).await;
    let mut dataset = commit_edited_manifest(&dataset, |manifest, _| {
        manifest.clustering = vec![definition(1)];
        manifest.max_clustering_id = 1;
        stamp_fragment(manifest, 0, 1, "3");
    })
    .await;
    assert_ne!(
        dataset.manifest().writer_feature_flags & FLAG_OBJECT_METADATA,
        0
    );
    assert_eq!(
        dataset.manifest().reader_feature_flags & FLAG_OBJECT_METADATA,
        0
    );

    // Delete: same fragment, new deletion file.
    dataset.delete("i < 5").await.unwrap();
    let fragment = fragment_by_id(&dataset, 0);
    assert!(fragment.deletion_file.is_some());
    assert_eq!(fragment.clustering_id, Some(1));
    assert_eq!(fragment.metadata, object_metadata("3"));
    assert_eq!(fragment_by_id(&dataset, 1).clustering_id, None);
    assert!(fragment_by_id(&dataset, 1).metadata.is_empty());

    // Add a column: same fragments, merged files.
    dataset
        .add_columns(
            NewColumnTransform::SqlExpressions(vec![("j".to_string(), "i + 1".to_string())]),
            None,
            None,
        )
        .await
        .unwrap();
    let fragment = fragment_by_id(&dataset, 0);
    assert_eq!(fragment.files.len(), 2);
    assert_eq!(fragment.clustering_id, Some(1));
    assert_eq!(fragment.metadata, object_metadata("3"));
    assert_eq!(dataset.manifest().clustering, vec![definition(1)]);
    assert_eq!(dataset.manifest().max_clustering_id, 1);
    assert_ne!(
        dataset.manifest().writer_feature_flags & FLAG_OBJECT_METADATA,
        0
    );
    dataset.validate().await.unwrap();
}

#[tokio::test]
async fn compaction_outputs_do_not_inherit_fragment_metadata() {
    let dir = TempStrDir::default();
    let dataset = write_dataset(&dir).await;
    let mut dataset = commit_edited_manifest(&dataset, |manifest, _| {
        manifest.clustering = vec![definition(1)];
        manifest.max_clustering_id = 1;
        stamp_fragment(manifest, 0, 1, "3");
        stamp_fragment(manifest, 1, 1, "3");
    })
    .await;

    compact_files(
        &mut dataset,
        CompactionOptions {
            target_rows_per_fragment: 1_000,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();

    assert_eq!(dataset.fragments().len(), 1);
    let output = &dataset.fragments()[0];
    assert_eq!(output.clustering_id, None);
    assert!(output.metadata.is_empty());
    // The definitions and the high-water mark outlive the fragments that used them.
    assert_eq!(dataset.manifest().clustering, vec![definition(1)]);
    assert_eq!(dataset.manifest().max_clustering_id, 1);
    assert_ne!(
        dataset.manifest().writer_feature_flags & FLAG_OBJECT_METADATA,
        0
    );
}

#[tokio::test]
async fn append_commit_validates_fragment_references() {
    let dir = TempStrDir::default();
    let dataset = write_dataset(&dir).await;
    let dataset = commit_edited_manifest(&dataset, |manifest, _| {
        manifest.clustering = vec![definition(1)];
        manifest.max_clustering_id = 1;
    })
    .await;

    let stamped = |clustering_id: u32| {
        let dataset = dataset.clone();
        async move {
            let mut transaction = InsertBuilder::new(Arc::new(dataset.clone()))
                .with_params(&WriteParams {
                    mode: WriteMode::Append,
                    ..Default::default()
                })
                .execute_uncommitted(vec![
                    RecordBatch::try_new(
                        schema(),
                        vec![Arc::new(Int32Array::from_iter_values(100..110))],
                    )
                    .unwrap(),
                ])
                .await
                .unwrap();
            let Operation::Append { fragments } = &mut transaction.operation else {
                unreachable!()
            };
            for fragment in fragments {
                fragment.clustering_id = Some(clustering_id);
                fragment.metadata = object_metadata("5");
            }
            CommitBuilder::new(Arc::new(dataset))
                .execute(transaction)
                .await
        }
    };

    let error = stamped(9).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("references clustering definition 9"),
        "{error}"
    );

    let committed = stamped(1).await.unwrap();
    let appended = committed.fragments().last().unwrap();
    assert_eq!(appended.clustering_id, Some(1));
    assert_eq!(appended.metadata, object_metadata("5"));
}

#[tokio::test]
async fn restore_keeps_the_high_water_mark_and_merges_definitions() {
    let dir = TempStrDir::default();
    let dataset = write_dataset(&dir).await; // version 1: nothing allocated
    let before_allocation = dataset.version().version;

    let dataset = commit_edited_manifest(&dataset, |manifest, _| {
        manifest.clustering = vec![definition(1)];
        manifest.max_clustering_id = 1;
        manifest
            .table_metadata
            .insert("org.example.cluster.active_id".into(), "1".into());
        stamp_fragment(manifest, 0, 1, "3");
    })
    .await;
    let first_definition = dataset.version().version;

    let dataset = commit_edited_manifest(&dataset, |manifest, _| {
        manifest.clustering.push(definition(2));
        manifest.max_clustering_id = 2;
        manifest
            .table_metadata
            .insert("org.example.cluster.active_id".into(), "2".into());
    })
    .await;

    // Restore to before any allocation: the historical selection comes back
    // (none), the mark does not rewind, and every definition this lineage
    // handed out stays defined.
    let mut restored = dataset.checkout_version(before_allocation).await.unwrap();
    restored.restore().await.unwrap();
    assert!(
        !restored
            .manifest()
            .table_metadata
            .contains_key("org.example.cluster.active_id")
    );
    assert_eq!(restored.manifest().max_clustering_id, 2);
    assert_eq!(
        restored.manifest().clustering,
        vec![definition(1), definition(2)]
    );
    assert_eq!(restored.fragments()[0].clustering_id, None);
    assert_ne!(
        restored.manifest().writer_feature_flags & FLAG_OBJECT_METADATA,
        0
    );

    // Allocating again continues above the mark; id 1 and 2 are never reused.
    let restored = commit_edited_manifest(&restored, |manifest, _| {
        manifest.clustering.push(definition(3));
        manifest.max_clustering_id = 3;
    })
    .await;
    assert_eq!(restored.manifest().max_clustering_id, 3);

    // Restore to the first definition's version: its selection and its
    // fragment references come back, with the later definitions retained.
    let mut restored = restored.checkout_version(first_definition).await.unwrap();
    restored.restore().await.unwrap();
    assert_eq!(
        restored.manifest().table_metadata["org.example.cluster.active_id"],
        "1"
    );
    assert_eq!(restored.manifest().max_clustering_id, 3);
    assert_eq!(
        restored.manifest().clustering,
        vec![definition(1), definition(2), definition(3)]
    );
    assert_eq!(restored.fragments()[0].clustering_id, Some(1));
    assert_eq!(restored.fragments()[0].metadata, object_metadata("3"));
}

#[tokio::test]
async fn restore_rejects_a_definition_that_changed_meaning() {
    let dir = TempStrDir::default();
    let dataset = write_dataset(&dir).await;
    let dataset = commit_edited_manifest(&dataset, |manifest, _| {
        manifest.clustering = vec![definition(1)];
        manifest.max_clustering_id = 1;
    })
    .await;
    let original = dataset.version().version;
    // Simulate a writer that redefined id 1 in place, which the contract forbids.
    let dataset = commit_edited_manifest(&dataset, |manifest, _| {
        manifest.clustering[0].provider = "org.example.other".into();
    })
    .await;

    let mut restored = dataset.checkout_version(original).await.unwrap();
    let error = restored.restore().await.unwrap_err();
    assert!(
        error.to_string().contains("differs between manifests"),
        "{error}"
    );
}

#[tokio::test]
async fn branch_and_shallow_clone_carry_definitions_and_metadata() {
    let dir = TempStrDir::default();
    let dataset = write_dataset(&dir).await;
    let mut dataset = commit_edited_manifest(&dataset, |manifest, _| {
        manifest.clustering = vec![definition(1)];
        manifest.max_clustering_id = 1;
        stamp_fragment(manifest, 0, 1, "3");
    })
    .await;
    let version = dataset.version().version;

    let branch = dataset.create_branch("b", version, None).await.unwrap();
    assert_eq!(branch.manifest().clustering, vec![definition(1)]);
    assert_eq!(branch.manifest().max_clustering_id, 1);
    assert_eq!(branch.fragments()[0].clustering_id, Some(1));
    assert_eq!(branch.fragments()[0].metadata, object_metadata("3"));

    let clone_dir = TempStrDir::default();
    let clone = dataset
        .shallow_clone(&clone_dir, version, None)
        .await
        .unwrap();
    assert_eq!(clone.manifest().clustering, vec![definition(1)]);
    assert_eq!(clone.manifest().max_clustering_id, 1);
    assert_eq!(clone.fragments()[0].clustering_id, Some(1));
    assert_eq!(clone.fragments()[0].metadata, object_metadata("3"));
    assert_ne!(
        clone.manifest().writer_feature_flags & FLAG_OBJECT_METADATA,
        0
    );
}

async fn named_segments(dataset: &Dataset, name: &str) -> Vec<IndexMetadata> {
    read_manifest_indexes(
        &dataset.object_store,
        &dataset.manifest_location,
        dataset.manifest(),
    )
    .await
    .unwrap()
    .into_iter()
    .filter(|index| index.name == name)
    .collect()
}

/// A remapped segment is the same segment under new addresses and keeps its
/// metadata, even though it gets a new UUID; a merged segment is a new
/// object and carries only what its creator set.
#[tokio::test]
async fn index_segment_metadata_survives_remap_but_not_merge() {
    let dir = TempStrDir::default();
    let mut dataset = write_dataset(&dir).await;
    dataset
        .create_index(
            &["i"],
            IndexType::BTree,
            Some("idx".into()),
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();
    let dataset = commit_edited_manifest(&dataset, |manifest, indices| {
        manifest.clustering = vec![definition(1)];
        manifest.max_clustering_id = 1;
        let segment = indices.iter_mut().find(|idx| idx.name == "idx").unwrap();
        segment.clustering_id = Some(1);
        segment.metadata = object_metadata("3");
    })
    .await;
    let original_uuid = named_segments(&dataset, "idx").await[0].uuid;

    // Compaction remaps the BTree segment inline: same segment, new UUID.
    let mut dataset = dataset;
    compact_files(
        &mut dataset,
        CompactionOptions {
            target_rows_per_fragment: 1_000,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    let remapped = named_segments(&dataset, "idx").await;
    assert_eq!(remapped.len(), 1);
    assert_ne!(remapped[0].uuid, original_uuid);
    assert_eq!(remapped[0].clustering_id, Some(1));
    assert_eq!(remapped[0].metadata, object_metadata("3"));

    // New rows and an appended delta segment: the new segment carries nothing.
    Dataset::write(
        rows(100..150),
        dataset.uri(),
        Some(WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    dataset.checkout_latest().await.unwrap();
    dataset
        .optimize_indices(&OptimizeOptions::append())
        .await
        .unwrap();
    let mut segments = named_segments(&dataset, "idx").await;
    segments.sort_by_key(|segment| segment.clustering_id.is_none());
    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0].clustering_id, Some(1));
    assert_eq!(segments[0].metadata, object_metadata("3"));
    assert_eq!(segments[1].clustering_id, None);
    assert!(segments[1].metadata.is_empty());

    // Merging both into one segment creates a new object: no inheritance.
    dataset
        .optimize_indices(&OptimizeOptions::default().num_indices_to_merge(Some(2)))
        .await
        .unwrap();
    let merged = named_segments(&dataset, "idx").await;
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].clustering_id, None);
    assert!(merged[0].metadata.is_empty());
    // The definition and mark remain even with nothing referencing them.
    assert_eq!(dataset.manifest().clustering, vec![definition(1)]);
    assert_eq!(dataset.manifest().max_clustering_id, 1);
}

/// The explicit remap route (`remap_column_index`) builds a new segment entry
/// for the same content: the nonzero reference and the map survive the new UUID.
#[tokio::test]
async fn explicit_remap_keeps_reference_and_metadata() {
    let dir = TempStrDir::default();
    let mut dataset = write_dataset(&dir).await;
    dataset
        .create_index(
            &["i"],
            IndexType::BTree,
            Some("idx".into()),
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();
    let mut dataset = commit_edited_manifest(&dataset, |manifest, indices| {
        manifest.clustering = vec![definition(1)];
        manifest.max_clustering_id = 1;
        let segment = indices.iter_mut().find(|idx| idx.name == "idx").unwrap();
        segment.clustering_id = Some(1);
        segment.metadata = object_metadata("3");
    })
    .await;
    let before = named_segments(&dataset, "idx").await[0].clone();

    // Deferred remap leaves the segment addressing the old fragments; the
    // explicit remap then re-addresses it under a new UUID.
    compact_files(
        &mut dataset,
        CompactionOptions {
            target_rows_per_fragment: 1_000,
            defer_index_remap: true,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    assert_eq!(named_segments(&dataset, "idx").await[0].uuid, before.uuid);
    crate::dataset::optimize::remapping::remap_column_index(
        &mut dataset,
        &["i"],
        Some("idx".into()),
    )
    .await
    .unwrap();
    let after = named_segments(&dataset, "idx").await;
    assert_eq!(after.len(), 1);
    assert_ne!(after[0].uuid, before.uuid);
    assert_eq!(after[0].clustering_id, Some(1));
    assert_eq!(after[0].metadata, object_metadata("3"));
}

/// A table whose ONLY clustering feature is a map on one index segment keeps
/// its writer fence through the clone and branch paths, which publish the
/// manifest without `build_manifest`.
#[tokio::test]
async fn index_only_metadata_keeps_its_writer_fence_through_clone_and_branch() {
    let dir = TempStrDir::default();
    let mut dataset = write_dataset(&dir).await;
    dataset
        .create_index(
            &["i"],
            IndexType::BTree,
            Some("idx".into()),
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();
    let mut dataset = commit_edited_manifest(&dataset, |_, indices| {
        let segment = indices.iter_mut().find(|idx| idx.name == "idx").unwrap();
        segment.metadata = object_metadata("3");
    })
    .await;
    assert!(dataset.manifest().clustering.is_empty());
    assert_eq!(dataset.manifest().max_clustering_id, 0);
    assert!(dataset.fragments().iter().all(|f| f.metadata.is_empty()));
    assert_ne!(
        dataset.manifest().writer_feature_flags & FLAG_OBJECT_METADATA,
        0
    );
    let version = dataset.version().version;

    let clone_dir = TempStrDir::default();
    let clone = dataset
        .shallow_clone(&clone_dir, version, None)
        .await
        .unwrap();
    assert_ne!(
        clone.manifest().writer_feature_flags & FLAG_OBJECT_METADATA,
        0
    );
    assert_eq!(
        clone.manifest().reader_feature_flags & FLAG_OBJECT_METADATA,
        0
    );
    assert_eq!(
        named_segments(&clone, "idx").await[0].metadata,
        object_metadata("3")
    );

    let branch = dataset.create_branch("b", version, None).await.unwrap();
    assert_ne!(
        branch.manifest().writer_feature_flags & FLAG_OBJECT_METADATA,
        0
    );
    assert_eq!(
        branch.manifest().reader_feature_flags & FLAG_OBJECT_METADATA,
        0
    );
    assert_eq!(
        named_segments(&branch, "idx").await[0].metadata,
        object_metadata("3")
    );
}

#[tokio::test]
async fn metadata_only_on_an_index_segment_sets_the_writer_flag() {
    let dir = TempStrDir::default();
    let mut dataset = write_dataset(&dir).await;
    dataset
        .create_index(
            &["i"],
            IndexType::BTree,
            Some("idx".into()),
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();
    let mut dataset = commit_edited_manifest(&dataset, |_, indices| {
        let segment = indices.iter_mut().find(|idx| idx.name == "idx").unwrap();
        segment.metadata.insert(UNKNOWN_KEY.into(), "opaque".into());
    })
    .await;
    assert!(dataset.manifest().clustering.is_empty());
    assert_eq!(dataset.manifest().max_clustering_id, 0);

    // The next real commit derives the bit from the final index section.
    dataset.delete("i < 1").await.unwrap();
    assert_ne!(
        dataset.manifest().writer_feature_flags & FLAG_OBJECT_METADATA,
        0
    );
    assert_eq!(
        dataset.manifest().reader_feature_flags & FLAG_OBJECT_METADATA,
        0
    );
    assert_eq!(
        named_segments(&dataset, "idx").await[0].metadata[UNKNOWN_KEY],
        "opaque"
    );
}
