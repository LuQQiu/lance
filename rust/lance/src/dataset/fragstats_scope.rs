// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Planning-time fragment scope from the fragment column statistics index.
//!
//! Given a query filter, computes the set of fragments that provably contain
//! no matching row, using the per-fragment min/max/null records of any
//! `fragment_column_stats` indices on the filtered columns. The resolution is
//! subtractive: a fragment is only removed from a scan on proof, so fragments
//! without statistics (or with stale statistics) always stay candidates.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::logical_expr::Expr;
use lance_core::Result;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::SargableQuery;
use lance_index::scalar::expression::{
    IndexInformationProvider, MultiQueryParser, SargableQueryParser, ScalarIndexExpr,
    apply_scalar_indices,
};
use lance_index::scalar::fragstats::FragmentColumnStatsIndex;
use lance_table::format::IndexMetadata;
use roaring::RoaringBitmap;

use super::Dataset;
use super::overlay::{collect_overlay_stale_frags, overlaid_fragments};
use crate::index::DatasetIndexExt;
use crate::index::DatasetIndexInternalExt;

const FRAGSTATS_DETAILS_SUFFIX: &str = "FragmentColumnStatsIndexDetails";

struct FragStatsIndexInfo {
    columns: HashMap<String, (DataType, MultiQueryParser)>,
}

impl IndexInformationProvider for FragStatsIndexInfo {
    fn get_index(&self, col: &str) -> Option<(&DataType, &MultiQueryParser)> {
        self.columns
            .get(col)
            .map(|(data_type, parser)| (data_type, parser))
    }
}

/// One opened statistics segment plus the fragments whose statistics are
/// invalidated by newer column overlays (validity: same fragment id, changed
/// column data means the old record must not prune).
struct LoadedSegment {
    index: Arc<dyn lance_index::scalar::ScalarIndex>,
    stale_fragments: RoaringBitmap,
}

/// Compute the fragments provably excluded by `full_expr`.
///
/// Returns an empty bitmap when no fragment column statistics index applies.
/// Only fragments with valid, complete statistics records can ever appear in
/// the result.
pub(crate) async fn fragment_stats_excluded(
    dataset: &Dataset,
    full_expr: &Expr,
) -> Result<RoaringBitmap> {
    let indices = dataset.load_indices().await?;
    // column -> statistics index segments for that column
    let mut segments_by_column: HashMap<String, Vec<IndexMetadata>> = HashMap::new();
    let mut info = FragStatsIndexInfo {
        columns: HashMap::new(),
    };
    for index in indices.iter() {
        let Some(details) = &index.index_details else {
            continue;
        };
        if !details.type_url.ends_with(FRAGSTATS_DETAILS_SUFFIX) {
            continue;
        }
        let Some(field_id) = index.keyed_field() else {
            continue;
        };
        let Ok(field_path) = dataset.schema().field_path(field_id) else {
            continue;
        };
        let Some(data_type) = dataset.schema().field(&field_path).map(|f| f.data_type()) else {
            continue;
        };
        info.columns.entry(field_path.clone()).or_insert_with(|| {
            (
                data_type,
                MultiQueryParser::single(Box::new(SargableQueryParser::new(
                    index.name.clone(),
                    "FragmentColumnStats".to_string(),
                    true,
                ))),
            )
        });
        segments_by_column
            .entry(field_path)
            .or_default()
            .push(index.clone());
    }
    if info.columns.is_empty() {
        return Ok(RoaringBitmap::new());
    }

    // Reuse the shared predicate decomposition so the statistics see exactly
    // the sargable pieces of the filter; anything it cannot parse simply
    // provides no exclusion proof.
    let indexed = apply_scalar_indices(full_expr.clone(), &info)?;
    let Some(query_tree) = indexed.scalar_query else {
        return Ok(RoaringBitmap::new());
    };

    // Open the referenced segments (warm after the first query via the index
    // cache) and compute overlay staleness per segment.
    let overlaid = overlaid_fragments(&dataset.manifest.fragments);
    let mut loaded: HashMap<String, Vec<LoadedSegment>> = HashMap::new();
    for (column, segments) in segments_by_column {
        let mut loaded_segments = Vec::with_capacity(segments.len());
        for meta in &segments {
            let Ok(index) = dataset
                .open_scalar_index(&column, &meta.uuid, &NoOpMetricsCollector)
                .await
            else {
                continue;
            };
            if index
                .as_any()
                .downcast_ref::<FragmentColumnStatsIndex>()
                .is_none()
            {
                continue;
            }
            let mut stale_fragments = RoaringBitmap::new();
            if collect_overlay_stale_frags(meta, &overlaid, &mut stale_fragments, dataset.schema())
                .is_err()
            {
                // Unknown staleness means the whole segment cannot prove
                // anything: skip it entirely.
                continue;
            }
            loaded_segments.push(LoadedSegment {
                index,
                stale_fragments,
            });
        }
        loaded.insert(column, loaded_segments);
    }

    excluded_by_expr(&query_tree, &loaded)
}

/// Fold the parsed predicate tree into an excluded-fragments bitmap.
///
/// Combinators keep the subtractive discipline sound:
/// - AND: a row must satisfy both sides, so a proof from either side excludes
///   the fragment (union).
/// - OR: both branches must be impossible (intersection).
/// - NOT: no exclusion proof is derived (empty).
fn excluded_by_expr(
    expr: &ScalarIndexExpr,
    loaded: &HashMap<String, Vec<LoadedSegment>>,
) -> Result<RoaringBitmap> {
    match expr {
        ScalarIndexExpr::Not(_) => Ok(RoaringBitmap::new()),
        ScalarIndexExpr::And(lhs, rhs) => {
            Ok(excluded_by_expr(lhs, loaded)? | excluded_by_expr(rhs, loaded)?)
        }
        ScalarIndexExpr::Or(lhs, rhs) => {
            Ok(excluded_by_expr(lhs, loaded)? & excluded_by_expr(rhs, loaded)?)
        }
        ScalarIndexExpr::Query(search) => {
            let Some(query) = search.query.as_any().downcast_ref::<SargableQuery>() else {
                return Ok(RoaringBitmap::new());
            };
            let Some(segments) = loaded.get(&search.column) else {
                return Ok(RoaringBitmap::new());
            };
            let mut excluded = RoaringBitmap::new();
            for segment in segments {
                let Some(stats) = segment
                    .index
                    .as_any()
                    .downcast_ref::<FragmentColumnStatsIndex>()
                else {
                    continue;
                };
                let mut segment_excluded = stats.excluded_fragments(query)?;
                segment_excluded -= &segment.stale_fragments;
                excluded |= segment_excluded;
            }
            Ok(excluded)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::dataset::{WriteMode, WriteParams};
    use crate::index::DatasetIndexExt;
    use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use futures::TryStreamExt;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_index::IndexType;
    use lance_index::scalar::ScalarIndexParams;
    use std::sync::Arc;

    async fn stats_dataset(test_uri: &str) -> crate::Dataset {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "i",
            ArrowDataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(0..300))],
        )
        .unwrap();
        let params = WriteParams {
            max_rows_per_file: 100,
            ..Default::default()
        };
        let dataset = crate::Dataset::write(
            RecordBatchIterator::new([Ok(batch)], schema.clone()),
            test_uri,
            Some(params),
        )
        .await
        .unwrap();

        // Fragment 3 (300..400) opts out of statistics collection.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(300..400))],
        )
        .unwrap();
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
        dataset
    }

    async fn collect_values(dataset: &crate::Dataset, filter: &str, use_stats: bool) -> Vec<i64> {
        let mut scanner = dataset.scan();
        scanner.filter(filter).unwrap();
        scanner.use_fragment_stats(use_stats);
        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut values: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("i")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        values.sort_unstable();
        values
    }

    #[tokio::test]
    async fn test_scan_prunes_fragments_and_keeps_results_identical() {
        let test_uri = TempStrDir::default();
        let dataset = stats_dataset(&test_uri).await;

        for filter in [
            "i >= 250",
            "i < 42",
            "i >= 120 AND i < 160",
            "i < 50 OR i >= 350",
            "i = 305",
            "i >= 9999",
        ] {
            let with_stats = collect_values(&dataset, filter, true).await;
            let without_stats = collect_values(&dataset, filter, false).await;
            assert_eq!(
                with_stats, without_stats,
                "results must be identical for filter {filter}"
            );
        }

        // Pruning is visible in the plan: i >= 250 keeps fragment 2 (covered,
        // matching) and fragment 3 (no statistics, always retained).
        let mut scanner = dataset.scan();
        scanner.filter("i >= 250").unwrap();
        let analyzed = scanner.analyze_plan().await.unwrap();
        assert!(
            analyzed.contains("num_fragments=2"),
            "expected num_fragments=2 in plan:\n{analyzed}"
        );

        // Without statistics all four fragments are read.
        let mut scanner = dataset.scan();
        scanner.filter("i >= 250").unwrap();
        scanner.use_fragment_stats(false);
        let analyzed = scanner.analyze_plan().await.unwrap();
        assert!(
            analyzed.contains("num_fragments=4"),
            "expected num_fragments=4 in plan:\n{analyzed}"
        );

        // A filter nothing can match still scans the statistics-less fragment.
        let mut scanner = dataset.scan();
        scanner.filter("i >= 9999").unwrap();
        let analyzed = scanner.analyze_plan().await.unwrap();
        assert!(
            analyzed.contains("num_fragments=1"),
            "expected num_fragments=1 in plan:\n{analyzed}"
        );
    }

    #[tokio::test]
    async fn test_scan_after_delete_stays_correct() {
        let test_uri = TempStrDir::default();
        let mut dataset = stats_dataset(&test_uri).await;
        // Delete the only rows in fragment 2 that match i >= 250.
        dataset.delete("i >= 250 AND i < 300").await.unwrap();

        let with_stats = collect_values(&dataset, "i >= 240", true).await;
        let without_stats = collect_values(&dataset, "i >= 240", false).await;
        assert_eq!(with_stats, without_stats);
        // 240..250 from fragment 2, 300..400 from fragment 3.
        assert_eq!(with_stats.len(), 110);
    }
}
