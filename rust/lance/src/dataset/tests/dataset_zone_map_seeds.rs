// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Zone map write seeds across column rewrites.
//!
//! An index merge reads a seed only from the data file that serves the
//! indexed column, and only when the seed provably describes that column;
//! otherwise it scans. These tests rewrite a seeded column through every path
//! that writes a replacement column file and check that the merged index
//! equals a scan-built one and that the merge reported what it did.

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use futures::stream;
use lance_core::datatypes::Schema as LanceSchema;
use lance_core::utils::tempfile::TempStrDir;
use lance_core::utils::tracing::{
    INDEX_SEEDS_FALLBACK_EVENT, INDEX_SEEDS_HARVESTED_EVENT, SEED_FALLBACK_SEED_MISSING,
    TRACE_DATASET_EVENTS,
};
use lance_datafusion::utils::reader_to_stream;
use lance_file::reader::{FileReader, FileReaderOptions};
use lance_index::IndexType;
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_index::scalar::seed::{SEED_META_KEY_PREFIX, seed_field_id};
use lance_index::scalar::{BuiltinIndexType, IndexStore, ScalarIndexParams};
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::utils::CachedFileSize;
use tracing_subscriber::layer::SubscriberExt;

use crate::Dataset;
use crate::dataset::index::LanceIndexStoreExt;
use crate::dataset::schema_evolution::{ColumnAlteration, NewColumnTransform};
use crate::dataset::transaction::{DataReplacementGroup, Operation};
use crate::dataset::write::merge_insert::{WhenMatched, WhenNotMatched};
use crate::dataset::write::seeds::SEEDS_ENABLED_CONFIG_KEY;
use crate::dataset::{
    DATA_DIR, MergeInsertBuilder, MergeInsertWriteMode, WriteDestination, WriteParams,
};
use crate::index::DatasetIndexExt;
use lance_table::format::overlay::OverlayCoverage;
use roaring::RoaringBitmap;

use super::dataset_overlay_index_masking::commit_overlay;

const ROWS_PER_ZONE: u64 = 4;
const ROWS_PER_FILE: usize = 10;

fn schema(seeded: &[&str]) -> Arc<ArrowSchema> {
    let mut fields: Vec<ArrowField> = seeded
        .iter()
        .map(|name| ArrowField::new(*name, DataType::Int32, true))
        .collect();
    fields.insert(0, ArrowField::new("id", DataType::Int32, false));
    fields.push(ArrowField::new("untouched", DataType::Utf8, true));
    Arc::new(ArrowSchema::new(fields))
}

fn rows(
    seeded: &[&str],
    ids: std::ops::Range<i32>,
    value: impl Fn(i32) -> Option<i32>,
) -> RecordBatch {
    let mut columns: Vec<arrow_array::ArrayRef> =
        vec![Arc::new(Int32Array::from_iter_values(ids.clone()))];
    for (position, _) in seeded.iter().enumerate() {
        let offset = position as i32 * 100;
        columns.push(Arc::new(Int32Array::from_iter(
            ids.clone().map(|i| value(i).map(|v| v + offset)),
        )));
    }
    columns.push(Arc::new(StringArray::from_iter_values(
        ids.map(|i| format!("u{i}")),
    )));
    RecordBatch::try_new(schema(seeded), columns).unwrap()
}

fn base_value(i: i32) -> Option<i32> {
    (i % 7 != 3).then_some(i)
}

fn zone_map_params() -> ScalarIndexParams {
    ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap)
        .with_params(&serde_json::json!({"rows_per_zone": ROWS_PER_ZONE, "use_seeds": true}))
}

/// Twenty rows in two fragments, a seeded zone map on each column of
/// `seeded`, then a third fragment appended so its data file carries seeds.
async fn seeded_dataset(uri: &str, seeded: &[&str]) -> Dataset {
    let mut dataset = Dataset::write(
        RecordBatchIterator::new([Ok(rows(seeded, 0..20, base_value))], schema(seeded)),
        uri,
        Some(WriteParams {
            max_rows_per_file: ROWS_PER_FILE,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    for column in seeded {
        dataset
            .create_index(
                &[column],
                IndexType::ZoneMap,
                None,
                &zone_map_params(),
                false,
            )
            .await
            .unwrap();
    }
    dataset
        .append(
            RecordBatchIterator::new([Ok(rows(seeded, 20..30, base_value))], schema(seeded)),
            None,
        )
        .await
        .unwrap();
    dataset
}

fn field_id(dataset: &Dataset, column: &str) -> i32 {
    dataset.schema().field(column).unwrap().id
}

/// The seed metadata value stored for `column` in the data file at `path`.
async fn seed_value_in(dataset: &Dataset, path: &str, column: &str) -> Option<String> {
    let scheduler = ScanScheduler::new(
        dataset.object_store.clone(),
        SchedulerConfig::max_bandwidth(&dataset.object_store),
    );
    let path = dataset.base.clone().join(DATA_DIR).join(path);
    let file_scheduler = scheduler
        .open_file(&path, &CachedFileSize::unknown())
        .await
        .unwrap();
    let reader = FileReader::try_open(
        file_scheduler,
        None,
        Default::default(),
        &dataset.metadata_cache.file_metadata_cache(&path),
        FileReaderOptions::default(),
    )
    .await
    .unwrap();
    reader
        .metadata()
        .file_schema
        .metadata
        .get(&format!("{SEED_META_KEY_PREFIX}{column}"))
        .cloned()
}

/// Counts the seed harvest and fallback events emitted by an index merge.
#[derive(Clone, Default)]
struct SeedEventCounter {
    harvested: Arc<std::sync::atomic::AtomicUsize>,
    fallback_reasons: Arc<std::sync::Mutex<Vec<String>>>,
}

impl SeedEventCounter {
    fn harvested(&self) -> usize {
        self.harvested.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn fallbacks(&self) -> Vec<String> {
        self.fallback_reasons.lock().unwrap().clone()
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SeedEventCounter {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        #[derive(Default)]
        struct Fields {
            event: Option<String>,
            reason: Option<String>,
        }
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                let text = format!("{value:?}").trim_matches('"').to_string();
                match field.name() {
                    "event" => self.event = Some(text),
                    "reason" => self.reason = Some(text),
                    _ => {}
                }
            }
        }

        if event.metadata().target() != TRACE_DATASET_EVENTS {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        match fields.event.as_deref() {
            Some(name) if name == INDEX_SEEDS_HARVESTED_EVENT => {
                self.harvested
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Some(name) if name == INDEX_SEEDS_FALLBACK_EVENT => {
                self.fallback_reasons
                    .lock()
                    .unwrap()
                    .push(fields.reason.unwrap_or_default());
            }
            _ => {}
        }
    }
}

/// Merge the dataset's indices while counting seed events.
async fn optimize_counting_seed_events(dataset: &mut Dataset) -> SeedEventCounter {
    let counter = SeedEventCounter::default();
    let subscriber = tracing_subscriber::registry().with(counter.clone());
    // tracing caches each callsite's interest when it first fires. While
    // exactly one dispatcher is registered that cache is built from the
    // firing thread's default, so a merge running in another test would pin
    // the seed callsites to "never" before this thread-local subscriber ever
    // sees them. A second live dispatcher makes the cache consult every
    // registered dispatcher instead.
    let _second_dispatcher = tracing::Dispatch::new(tracing_subscriber::registry());
    let _guard = tracing::subscriber::set_default(subscriber);
    dataset.optimize_indices(&Default::default()).await.unwrap();
    counter
}

/// The zones of `index_name`, sorted by fragment and zone start.
async fn zone_map_rows(dataset: &Dataset, index_name: &str) -> RecordBatch {
    let indices = dataset.load_indices().await.unwrap();
    let index = indices.iter().find(|i| i.name == index_name).unwrap();
    let store = LanceIndexStore::from_dataset_for_existing(dataset, index)
        .await
        .unwrap();
    let reader = store.open_index_file("zonemap.lance").await.unwrap();
    let zones = reader.read_range(0..reader.num_rows(), None).await.unwrap();
    let sort_columns = vec![
        arrow_ord::sort::SortColumn {
            values: zones.column_by_name("fragment_id").unwrap().clone(),
            options: None,
        },
        arrow_ord::sort::SortColumn {
            values: zones.column_by_name("zone_start").unwrap().clone(),
            options: None,
        },
    ];
    let order = arrow_ord::sort::lexsort_to_indices(&sort_columns, None).unwrap();
    arrow_select::take::take_record_batch(&zones, &order).unwrap()
}

/// The merged index must hold exactly the zones a full scan produces.
async fn assert_merge_matches_scan(dataset: &mut Dataset, column: &str, index_name: &str) {
    let merged = zone_map_rows(dataset, index_name).await;
    assert!(merged.num_rows() > 0);
    dataset
        .create_index(
            &[column],
            IndexType::ZoneMap,
            None,
            &zone_map_params(),
            true,
        )
        .await
        .unwrap();
    let scanned = zone_map_rows(dataset, index_name).await;
    assert_eq!(merged, scanned, "merged zones differ from scan-built zones");
}

/// Stage a replacement for `column` in every fragment and commit it as a
/// data replacement, the way computed and function column refreshes do.
async fn replace_column(
    dataset: Dataset,
    column: &str,
    rewrite: impl Fn(u64) -> bool,
    value: impl Fn(i32, i32) -> Option<i32>,
) -> Dataset {
    let column_schema = LanceSchema {
        fields: vec![dataset.schema().field(column).unwrap().clone()],
        metadata: Default::default(),
    };
    let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        column,
        DataType::Int32,
        true,
    )]));
    let mut replacements: Vec<DataReplacementGroup> = Vec::new();
    for fragment in dataset.get_fragments() {
        if !rewrite(fragment.id() as u64) {
            continue;
        }
        let rows = fragment.physical_rows().await.unwrap() as i32;
        let fragment_id = fragment.id() as i32;
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from_iter(
                (0..rows).map(|i| value(fragment_id, i)),
            ))],
        )
        .unwrap();
        replacements.push(
            fragment
                .write_columns(stream::iter([Ok(batch)]), &column_schema)
                .await
                .unwrap(),
        );
    }
    let read_version = dataset.manifest.version;
    Dataset::commit(
        WriteDestination::Dataset(Arc::new(dataset)),
        Operation::DataReplacement { replacements },
        Some(read_version),
        None,
        None,
        Arc::new(Default::default()),
        false,
    )
    .await
    .unwrap()
}

fn replacement_value(fragment_id: i32, i: i32) -> Option<i32> {
    (i % 4 != 1).then_some(fragment_id * 1000 + i)
}

/// merge_insert that updates only `val` writes a replacement column file. The
/// merge harvests that file's seed, not the stale one left in the first file.
#[tokio::test]
async fn test_merge_insert_column_rewrite_reseeds_and_harvests() {
    let dir = TempStrDir::default();
    let dataset = seeded_dataset(dir.as_str(), &["val"]).await;

    let source_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("val", DataType::Int32, true),
    ]));
    // Rows 10..30 are fragments 1 and 2; fragment 0 stays as indexed.
    let source = RecordBatch::try_new(
        source_schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(10..30)),
            Arc::new(Int32Array::from_iter(
                (10..30).map(|i| (i % 5 != 0).then_some(i + 1000)),
            )),
        ],
    )
    .unwrap();
    let job = MergeInsertBuilder::try_new(Arc::new(dataset), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::DoNothing)
        .write_mode(MergeInsertWriteMode::RewriteColumns)
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new([Ok(source)], source_schema));
    let (dataset, _) = job.execute(reader_to_stream(reader)).await.unwrap();
    let mut dataset = dataset.as_ref().clone();

    let val = field_id(&dataset, "val");
    let fragments = dataset.fragments();
    assert_eq!(fragments[0].files.len(), 1, "fragment 0 was not rewritten");
    for fragment in &fragments[1..] {
        let serving = fragment
            .data_file_serving_field(val)
            .expect("exactly one file serves val");
        assert_ne!(serving.path, fragment.files[0].path);
        let value = seed_value_in(&dataset, &serving.path, "val")
            .await
            .expect("the replacement file carries a seed");
        assert_eq!(seed_field_id(&value), Some(val));
    }
    // The appended fragment's original file still carries its stale seed.
    let stale = &dataset.fragments()[2].files[0];
    assert!(seed_value_in(&dataset, &stale.path, "val").await.is_some());

    let events = optimize_counting_seed_events(&mut dataset).await;
    assert_eq!(events.harvested(), 1, "fallbacks: {:?}", events.fallbacks());
    assert!(events.fallbacks().is_empty());
    assert_merge_matches_scan(&mut dataset, "val", "val_idx").await;
}

/// `FileFragment::write_columns`, used by computed and function column
/// refreshes, stages a replacement column file that carries a seed; the
/// column is then served by the last of several files.
#[tokio::test]
async fn test_write_columns_replacement_reseeds_and_harvests() {
    let dir = TempStrDir::default();
    let mut dataset = seeded_dataset(dir.as_str(), &["val"]).await;
    // A derived column first, so `val` is not the only column of its fragment
    // and its replacement lands in a third file.
    dataset
        .add_columns(
            NewColumnTransform::SqlExpressions(vec![("twice".to_string(), "val * 2".to_string())]),
            None,
            None,
        )
        .await
        .unwrap();
    let mut dataset = replace_column(dataset, "val", |id| id >= 1, replacement_value).await;

    let val = field_id(&dataset, "val");
    let fragments = dataset.fragments();
    assert_eq!(fragments[0].files.len(), 2, "fragment 0 was not rewritten");
    for fragment in &fragments[1..] {
        assert_eq!(fragment.files.len(), 3);
        let serving = fragment.data_file_serving_field(val).unwrap();
        assert_eq!(serving.path, fragment.files[2].path);
        let value = seed_value_in(&dataset, &serving.path, "val").await.unwrap();
        assert_eq!(seed_field_id(&value), Some(val));
        assert!(
            seed_value_in(&dataset, &fragment.files[1].path, "twice")
                .await
                .is_none(),
            "a column without an index gets no seed"
        );
    }

    let events = optimize_counting_seed_events(&mut dataset).await;
    assert_eq!(events.harvested(), 1, "fallbacks: {:?}", events.fallbacks());
    assert_merge_matches_scan(&mut dataset, "val", "val_idx").await;
}

/// A replacement file written without a seed leaves the stale seed in the
/// old file. The merge must scan rather than harvest it.
#[tokio::test]
async fn test_stale_seed_in_replaced_file_is_not_harvested() {
    let dir = TempStrDir::default();
    let mut dataset = seeded_dataset(dir.as_str(), &["val"]).await;
    dataset
        .update_config([(SEEDS_ENABLED_CONFIG_KEY, "false")])
        .await
        .unwrap();
    let mut dataset = replace_column(dataset, "val", |id| id >= 1, replacement_value).await;

    let val = field_id(&dataset, "val");
    for fragment in &dataset.fragments()[1..] {
        let serving = fragment.data_file_serving_field(val).unwrap();
        assert!(
            seed_value_in(&dataset, &serving.path, "val")
                .await
                .is_none()
        );
    }
    let stale = &dataset.fragments()[2].files[0];
    assert!(seed_value_in(&dataset, &stale.path, "val").await.is_some());

    let events = optimize_counting_seed_events(&mut dataset).await;
    assert_eq!(events.harvested(), 0);
    assert_eq!(
        events.fallbacks(),
        vec![SEED_FALLBACK_SEED_MISSING.to_string()]
    );
    assert_merge_matches_scan(&mut dataset, "val", "val_idx").await;
}

/// After renames that give another field the seeded column's old name, the
/// seed under that name no longer matches the field and the merge scans.
#[tokio::test]
async fn test_rename_swapping_names_falls_back_to_scan() {
    let dir = TempStrDir::default();
    let mut dataset = seeded_dataset(dir.as_str(), &["a", "c"]).await;
    let a = field_id(&dataset, "a");
    let c = field_id(&dataset, "c");
    for (from, to) in [("a", "tmp"), ("c", "a"), ("tmp", "b")] {
        dataset
            .alter_columns(&[ColumnAlteration::new(from.to_string()).rename(to.to_string())])
            .await
            .unwrap();
    }
    assert_eq!(field_id(&dataset, "b"), a);
    assert_eq!(field_id(&dataset, "a"), c);
    // The appended file still carries `lance.seed.a`, which describes field `a` (now `b`).
    let appended = &dataset.fragments()[2].files[0];
    let value = seed_value_in(&dataset, &appended.path, "a").await.unwrap();
    assert_eq!(seed_field_id(&value), Some(a));

    let events = optimize_counting_seed_events(&mut dataset).await;
    assert_eq!(events.harvested(), 0);
    assert_eq!(
        events.fallbacks(),
        vec![SEED_FALLBACK_SEED_MISSING.to_string(); 2]
    );
    assert_merge_matches_scan(&mut dataset, "b", "a_idx").await;
    assert_merge_matches_scan(&mut dataset, "a", "c_idx").await;
}

/// Sorted `id` values matching `filter`, with or without indices.
async fn ids_matching(dataset: &Dataset, filter: &str, use_index: bool) -> Vec<i32> {
    let mut scanner = dataset.scan();
    scanner
        .filter(filter)
        .unwrap()
        .project(&["id"])
        .unwrap()
        .use_scalar_index(use_index);
    let batch = scanner.try_into_batch().await.unwrap();
    let mut ids: Vec<i32> = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .values()
        .to_vec();
    ids.sort_unstable();
    ids
}

/// An overlay changes the values readers see without touching the data file,
/// so the file's seed no longer describes the field and the merge must scan.
#[tokio::test]
async fn test_overlaid_field_seed_is_not_harvested() {
    let dir = TempStrDir::default();
    let dataset = seeded_dataset(dir.as_str(), &["val"]).await;
    let val = field_id(&dataset, "val");
    // Fragment 2, row 1 (id 21) moves far outside its zone's range and row 2
    // (id 22) becomes null.
    let mut dataset = commit_overlay(
        dataset,
        "val_overlay",
        2,
        &[val],
        OverlayCoverage::dense(RoaringBitmap::from_iter([1u32, 2])),
        vec![Arc::new(Int32Array::from(vec![Some(999_999), None]))],
    )
    .await;

    let events = optimize_counting_seed_events(&mut dataset).await;
    assert_eq!(events.harvested(), 0);
    assert_eq!(
        events.fallbacks(),
        vec![SEED_FALLBACK_SEED_MISSING.to_string()]
    );
    for filter in [
        "val = 999999",
        "val = 21",
        "val = 22",
        "val IS NULL",
        "val IS NOT NULL",
        "val > 900000",
    ] {
        assert_eq!(
            ids_matching(&dataset, filter, true).await,
            ids_matching(&dataset, filter, false).await,
            "{filter}"
        );
    }
}
