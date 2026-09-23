// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Build path for the fragment column statistics index.
//!
//! Builds one record per fragment by folding the zone-level statistics
//! embedded in each fragment's data file footers at write time. No column
//! data is scanned. A fragment whose payload is missing, unreadable, or
//! provably incomplete simply gets no record: it stays out of the committed
//! fragment bitmap and remains a scan candidate at query time.

use lance_core::{Error, Result};
use lance_file::reader::FileReaderOptions;
use lance_index::scalar::CreatedIndex;
use lance_index::scalar::fragstats::build_fragment_stats_from_seeds;
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_index::scalar::seed::{FragmentSeed, SEED_META_KEY_PREFIX};
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::utils::CachedFileSize;
use uuid::Uuid;

use crate::dataset::Dataset;
use crate::dataset::index::LanceIndexStoreExt;

/// Build the fragment column statistics index files for `column` from the
/// fragments' embedded zone statistics.
///
/// Returns the created index and the fragment ids that actually received
/// records; only those may appear in the committed fragment bitmap.
pub(crate) async fn build_fragment_stats_index_from_seeds(
    dataset: &Dataset,
    column: &str,
    index_uuid: Uuid,
    fragment_ids: Option<Vec<u32>>,
) -> Result<(CreatedIndex, Vec<u32>)> {
    let field = dataset.schema().field(column).ok_or_else(|| {
        Error::invalid_input(format!("column {column} does not exist in the schema"))
    })?;
    let data_type = field.data_type();
    let meta_key = format!("{}{}", SEED_META_KEY_PREFIX, column);

    let scheduler = ScanScheduler::new(
        dataset.object_store.clone(),
        SchedulerConfig::max_bandwidth(&dataset.object_store),
    );

    let mut seeds: Vec<(FragmentSeed, u64)> = Vec::new();
    for fragment in dataset.fragments().iter() {
        if let Some(requested) = &fragment_ids
            && !requested.contains(&(fragment.id as u32))
        {
            continue;
        }
        // Every failure below is per-fragment tolerant: the fragment simply
        // has no statistics record and stays a scan candidate.
        let Some(expected_rows) = fragment.physical_rows else {
            continue;
        };
        // The payload lives in the data file that carries this column.
        let Some(data_file) = fragment
            .files
            .iter()
            .find(|file| file.fields.contains(&field.id))
        else {
            continue;
        };

        let path = dataset
            .base
            .clone()
            .join(crate::dataset::DATA_DIR)
            .join(data_file.path.as_str());
        let Ok(file_scheduler) = scheduler.open_file(&path, &CachedFileSize::unknown()).await
        else {
            continue;
        };
        let Ok(reader) = lance_file::reader::FileReader::try_open(
            file_scheduler,
            None,
            Default::default(),
            &dataset.metadata_cache.file_metadata_cache(&path),
            FileReaderOptions::default(),
        )
        .await
        else {
            continue;
        };

        let Some(meta_value) = reader
            .metadata()
            .file_schema
            .metadata
            .get(&meta_key)
            .cloned()
        else {
            continue;
        };
        let Some(buf_index) = meta_value
            .split(':')
            .next()
            .and_then(|s| s.parse::<u32>().ok())
        else {
            continue;
        };
        let Ok(bytes) = reader.read_global_buffer(buf_index).await else {
            continue;
        };

        seeds.push((
            FragmentSeed {
                fragment_id: fragment.id,
                bytes,
                metadata_value: meta_value,
            },
            expected_rows as u64,
        ));
    }

    let store = LanceIndexStore::from_dataset_for_new(dataset, &index_uuid)?;
    build_fragment_stats_from_seeds(seeds, &data_type, &store).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{WriteMode, WriteParams};
    use crate::index::DatasetIndexExt;
    use crate::index::DatasetIndexInternalExt;
    use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use datafusion::common::ScalarValue;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_index::IndexType;
    use lance_index::metrics::NoOpMetricsCollector;
    use lance_index::scalar::SargableQuery;
    use lance_index::scalar::ScalarIndexParams;
    use lance_index::scalar::fragstats::FragmentColumnStatsIndex;
    use std::ops::Bound;
    use std::sync::Arc;

    fn int_batch(start: i64, end: i64) -> (Arc<ArrowSchema>, RecordBatch) {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "i",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(start..end))],
        )
        .unwrap();
        (schema, batch)
    }

    #[tokio::test]
    async fn test_build_from_write_time_stats_end_to_end() {
        let test_uri = TempStrDir::default();
        let (schema, batch) = int_batch(0, 300);
        let params = WriteParams {
            max_rows_per_file: 100,
            ..Default::default()
        };
        let mut dataset = crate::Dataset::write(
            RecordBatchIterator::new([Ok(batch)], schema.clone()),
            &test_uri,
            Some(params),
        )
        .await
        .unwrap();
        assert_eq!(dataset.fragments().len(), 3);

        // Append one more fragment WITHOUT write-time statistics.
        let (_, batch) = int_batch(300, 400);
        let append_params = WriteParams {
            mode: WriteMode::Append,
            collect_fragment_stats: Some(false),
            ..Default::default()
        };
        let mut dataset = crate::Dataset::write(
            RecordBatchIterator::new([Ok(batch)], schema),
            Arc::new(dataset),
            Some(append_params),
        )
        .await
        .unwrap();
        assert_eq!(dataset.fragments().len(), 4);

        dataset
            .create_index(
                &["i"],
                IndexType::FragmentColumnStats,
                None,
                &ScalarIndexParams::new("FragmentColumnStats".to_string()),
                true,
            )
            .await
            .unwrap();

        let indices = dataset.load_indices().await.unwrap();
        let index_meta = indices
            .iter()
            .find(|idx| idx.name.contains("i_"))
            .or_else(|| indices.first())
            .unwrap();
        // Coverage must claim exactly the three fragments that carry
        // write-time statistics; the opted-out fragment stays unindexed.
        let bitmap = index_meta.fragment_bitmap.as_ref().unwrap();
        assert_eq!(bitmap.len(), 3);
        assert!(bitmap.contains(0) && bitmap.contains(1) && bitmap.contains(2));
        assert!(!bitmap.contains(3));

        let index = dataset
            .open_scalar_index(&"i", &index_meta.uuid, &NoOpMetricsCollector)
            .await
            .unwrap();
        let stats = index
            .as_any()
            .downcast_ref::<FragmentColumnStatsIndex>()
            .unwrap();
        assert_eq!(stats.num_fragments(), 3);

        // i >= 250 matches only fragment 2 (rows 200..300) among covered
        // fragments; fragments 0 and 1 are provably excludable. Fragment 3
        // has no record, so it must never be excluded even though its values
        // (300..400) do match.
        let query = SargableQuery::Range(
            Bound::Included(ScalarValue::Int64(Some(250))),
            Bound::Unbounded,
        );
        let excluded = stats.excluded_fragments(&query).unwrap();
        assert!(excluded.contains(0) && excluded.contains(1));
        assert!(!excluded.contains(2));
        assert!(!excluded.contains(3));

        // i >= 9999 matches nothing: all three covered fragments excludable,
        // the uncovered one still retained.
        let query = SargableQuery::Range(
            Bound::Included(ScalarValue::Int64(Some(9999))),
            Bound::Unbounded,
        );
        let excluded = stats.excluded_fragments(&query).unwrap();
        assert_eq!(excluded.len(), 3);
        assert!(!excluded.contains(3));
    }

    #[tokio::test]
    async fn test_incremental_update_adds_segment_without_touching_old() {
        use crate::index::create::CreateIndexBuilder;

        let test_uri = TempStrDir::default();
        let (schema, batch) = int_batch(0, 200);
        let params = WriteParams {
            max_rows_per_file: 100,
            ..Default::default()
        };
        let mut dataset = crate::Dataset::write(
            RecordBatchIterator::new([Ok(batch)], schema.clone()),
            &test_uri,
            Some(params),
        )
        .await
        .unwrap();

        let index_params = ScalarIndexParams::new("FragmentColumnStats".to_string());
        let first_segment = CreateIndexBuilder::new(
            &mut dataset,
            &["i"],
            IndexType::FragmentColumnStats,
            &index_params,
        )
        .name("i_stats".to_string())
        .execute_uncommitted()
        .await
        .unwrap();
        dataset
            .commit_existing_index_segments("i_stats", "i", vec![first_segment])
            .await
            .unwrap();
        let first_uuid = dataset.load_indices_by_name("i_stats").await.unwrap()[0].uuid;

        // Append a new fragment, then backfill ONLY that fragment into a new
        // add-only segment.
        let (_, batch) = int_batch(200, 300);
        let append_params = WriteParams {
            mode: WriteMode::Append,
            max_rows_per_file: 100,
            ..Default::default()
        };
        let mut dataset = crate::Dataset::write(
            RecordBatchIterator::new([Ok(batch)], schema),
            Arc::new(dataset),
            Some(append_params),
        )
        .await
        .unwrap();
        let new_segment = CreateIndexBuilder::new(
            &mut dataset,
            &["i"],
            IndexType::FragmentColumnStats,
            &index_params,
        )
        .name("i_stats".to_string())
        .replace(true)
        .fragments(vec![2])
        .execute_uncommitted()
        .await
        .unwrap();
        assert_eq!(
            new_segment.fragment_bitmap.as_ref().unwrap().len(),
            1,
            "the new segment covers only the appended fragment"
        );
        dataset
            .commit_existing_index_segments("i_stats", "i", vec![new_segment])
            .await
            .unwrap();

        let segments = dataset.load_indices_by_name("i_stats").await.unwrap();
        assert_eq!(segments.len(), 2, "old segment retained, new one added");
        assert!(
            segments.iter().any(|s| s.uuid == first_uuid),
            "the original segment UUID must survive the incremental update"
        );
        let mut union = roaring::RoaringBitmap::new();
        for segment in &segments {
            union |= segment.fragment_bitmap.as_ref().unwrap();
        }
        assert_eq!(union.len(), 3);

        // Both segments feed the resolver: a filter on the appended range
        // excludes the two old fragments.
        let mut scanner = dataset.scan();
        scanner.filter("i >= 250").unwrap();
        let analyzed = scanner.analyze_plan().await.unwrap();
        assert!(
            analyzed.contains("num_fragments=1"),
            "expected num_fragments=1 in plan:\n{analyzed}"
        );
    }
}
