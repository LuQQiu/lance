// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Fragment Column Statistics Index
//!
//! One record per fragment for a single column: typed min/max bounds, null and
//! NaN counts, and the covered row span. Records are folded from the zone-level
//! statistics embedded in each fragment's data file footers at write time (the
//! seed payload produced by [`ZoneMapSeedWriter`]); building the index does not
//! rescan column data. A fragment without a provably complete seed simply has
//! no record and always remains a scan candidate.
//!
//! The index serves planning-time pruning: given a sargable predicate it
//! answers "which fragments provably contain no matching row". Like a zone map
//! it is an inexact filter: it may keep false positives, never false negatives.
//!
//! The existing global ZoneMap index is unrelated: this module reuses the
//! generic zone aggregation primitives (`ZoneTrainer`, `ZoneMapProcessor`,
//! `search_zones`, the statistics evaluation rules), not the ZoneMap index.

use crate::pb;
use crate::scalar::expression::ScalarQueryParser;
use crate::scalar::registry::{
    BasicTrainer, ScalarIndexPlugin, TrainingCriteria, TrainingOrdering, TrainingRequest,
};
use crate::scalar::seed::FragmentSeed;
use crate::scalar::zonemap::ZoneMapStatistics;
use crate::scalar::zonemap::{ZoneMapIndex, ZoneMapProcessor, ZoneMapSeedWriter};
use crate::scalar::{CreatedIndex, IndexFile, SargableQuery, ScalarIndexParams, UpdateCriteria};
use lance_core::cache::LanceCache;
use lance_core::utils::row_addr_remap::RowAddrRemap;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::ScalarValue;
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, Result};
use roaring::RoaringBitmap;

use super::zoned::{ZoneBound, ZoneTrainer, search_zones};
use super::{AnyQuery, IndexStore, MetricsCollector, RowIdRemapper, ScalarIndex, SearchResult};
use crate::{Index, IndexType};

pub const FRAGMENT_STATS_FILENAME: &str = "fragment_stats.lance";
pub const FRAGMENT_COLUMN_STATS_INDEX_VERSION: u32 = 0;

/// Default zone size for the fragment-local statistics written with data
/// files. An experiment parameter, not a correctness dependency: folding is
/// defined over whatever zones a fragment carries.
pub const FRAGMENT_STATS_DEFAULT_ROWS_PER_ZONE: u64 = 8192;

/// Zone capacity used when (re)computing records from a data stream: large
/// enough that a fragment never splits into multiple zones (row offsets are
/// 32-bit), so the trainer's fragment-boundary flush yields one record per
/// fragment.
const ONE_ZONE_PER_FRAGMENT_CAPACITY: u64 = u32::MAX as u64;

pub fn make_fragment_column_stats_details() -> prost_types::Any {
    prost_types::Any::from_msg(&pb::FragmentColumnStatsIndexDetails {}).unwrap()
}

fn scalar_is_nan(v: &ScalarValue) -> bool {
    match v {
        ScalarValue::Float16(Some(f)) => f.is_nan(),
        ScalarValue::Float32(Some(f)) => f.is_nan(),
        ScalarValue::Float64(Some(f)) => f.is_nan(),
        _ => false,
    }
}

/// Fold a fragment's zone statistics into one per-fragment record.
///
/// Soundness mirrors `ZoneMapIndex::value_range_over`: a zone with missing
/// extrema that still holds comparable values poisons the fold (`None`), since
/// the folded bounds would silently drop live values. All-null / all-NaN zones
/// contribute counts but no bounds. `ScalarValue`'s total order ranks NaN above
/// every finite value, so a NaN zone max propagates into the folded max and the
/// shared evaluation rules stay conservative.
///
/// `expected_row_span` is the completeness proof: the zones must cover exactly
/// that many row offsets or no record is produced (partial coverage must not
/// prune).
fn fold_zones_to_record(
    fragment_id: u64,
    zones: &[ZoneMapStatistics],
    expected_row_span: u64,
    data_type: &DataType,
) -> Option<ZoneMapStatistics> {
    if zones.is_empty() {
        return None;
    }
    let covered: u64 = zones.iter().map(|z| z.bound.length as u64).sum();
    if covered != expected_row_span {
        return None;
    }

    let mut null_count: u64 = 0;
    let mut nan_count: u64 = 0;
    let mut min: Option<ScalarValue> = None;
    let mut max: Option<ScalarValue> = None;

    for zone in zones {
        null_count += u64::from(zone.null_count);
        nan_count += u64::from(zone.nan_count);

        if data_type.is_nested() {
            // Nested types track null counts only; bounds stay null.
            continue;
        }

        let missing_extrema = zone.min.is_null() || zone.max.is_null();
        let comparable =
            zone.bound.length as u128 > u128::from(zone.null_count) + u128::from(zone.nan_count);
        if missing_extrema {
            if comparable {
                // Bounds unknown but live comparable values exist: folding the
                // other zones would produce a subset. No sound record.
                return None;
            }
            continue;
        }
        if min
            .as_ref()
            .is_none_or(|cur| zone.min.partial_cmp(cur).is_some_and(|o| o.is_lt()))
        {
            min = Some(zone.min.clone());
        }
        if max
            .as_ref()
            .is_none_or(|cur| zone.max.partial_cmp(cur).is_some_and(|o| o.is_gt()))
        {
            max = Some(zone.max.clone());
        }
    }

    let (null_count, nan_count) = (
        u32::try_from(null_count).ok()?,
        u32::try_from(nan_count).ok()?,
    );
    let null_scalar = ScalarValue::try_new_null(data_type).ok()?;
    Some(ZoneMapStatistics {
        min: min.unwrap_or_else(|| null_scalar.clone()),
        max: max.unwrap_or(null_scalar),
        null_count,
        nan_count,
        bound: ZoneBound {
            fragment_id,
            start: 0,
            length: expected_row_span as usize,
        },
    })
}

/// Fold one harvested seed into a per-fragment record.
///
/// `expected_row_span` must be the fragment's physical row count as recorded in
/// its metadata; a seed that does not cover it completely yields `None`.
fn fold_seed_to_record(
    seed: &FragmentSeed,
    expected_row_span: u64,
    data_type: &DataType,
) -> Result<Option<ZoneMapStatistics>> {
    // metadata_value convention: "<buf_index>:<rows_per_zone>"
    let Some(rows_per_zone) = seed
        .metadata_value
        .split(':')
        .nth(1)
        .and_then(|s| s.parse::<u64>().ok())
    else {
        return Ok(None);
    };
    let (zones, _null_bitmap) =
        match ZoneMapSeedWriter::deserialize_seed(seed.fragment_id, &seed.bytes, rows_per_zone) {
            Ok(parsed) => parsed,
            // Unreadable or incompatible seed payloads mean "no statistics",
            // never an error: the fragment simply stays a candidate.
            Err(_) => return Ok(None),
        };
    Ok(fold_zones_to_record(
        seed.fragment_id,
        &zones,
        expected_row_span,
        data_type,
    ))
}

fn records_as_batch(records: &[ZoneMapStatistics], data_type: &DataType) -> Result<RecordBatch> {
    let mins = if records.is_empty() {
        arrow_array::new_empty_array(data_type)
    } else {
        ScalarValue::iter_to_array(records.iter().map(|r| r.min.clone()))?
    };
    let maxs = if records.is_empty() {
        arrow_array::new_empty_array(data_type)
    } else {
        ScalarValue::iter_to_array(records.iter().map(|r| r.max.clone()))?
    };
    let fragment_ids = UInt64Array::from_iter_values(records.iter().map(|r| r.bound.fragment_id));
    let null_counts = UInt32Array::from_iter_values(records.iter().map(|r| r.null_count));
    let nan_counts = UInt32Array::from_iter_values(records.iter().map(|r| r.nan_count));
    let row_spans = UInt64Array::from_iter_values(records.iter().map(|r| r.bound.length as u64));

    let schema = Arc::new(arrow_schema::Schema::new(vec![
        Field::new("fragment_id", DataType::UInt64, false),
        Field::new("min", data_type.clone(), true),
        Field::new("max", data_type.clone(), true),
        Field::new("null_count", DataType::UInt32, false),
        Field::new("nan_count", DataType::UInt32, false),
        Field::new("row_span", DataType::UInt64, false),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(fragment_ids),
            mins,
            maxs,
            Arc::new(null_counts),
            Arc::new(nan_counts),
            Arc::new(row_spans),
        ],
    )?)
}

async fn write_records(
    mut records: Vec<ZoneMapStatistics>,
    data_type: &DataType,
    index_store: &dyn IndexStore,
) -> Result<Vec<IndexFile>> {
    // Deterministic layout and cheap duplicate detection.
    records.sort_by_key(|r| r.bound.fragment_id);
    records.dedup_by_key(|r| r.bound.fragment_id);
    let batch = records_as_batch(&records, data_type)?;
    let mut index_file = index_store
        .new_index_file(FRAGMENT_STATS_FILENAME, batch.schema())
        .await?;
    index_file.write_record_batch(batch).await?;
    let file = index_file.finish_with_metadata(HashMap::new()).await?;
    Ok(vec![file])
}

/// Build the index files from pre-harvested fragment seeds.
///
/// `seeds` pairs each harvested seed with the fragment's physical row count
/// (the completeness proof). Returns the created index and the fragment ids
/// that actually received records; fragments whose seed was missing, partial,
/// or unsound are absent from the coverage list and must not be claimed by the
/// committed fragment bitmap.
pub async fn build_fragment_stats_from_seeds(
    seeds: Vec<(FragmentSeed, u64)>,
    data_type: &DataType,
    index_store: &dyn IndexStore,
) -> Result<(CreatedIndex, Vec<u32>)> {
    let mut records = Vec::with_capacity(seeds.len());
    for (seed, expected_row_span) in &seeds {
        if let Some(record) = fold_seed_to_record(seed, *expected_row_span, data_type)? {
            records.push(record);
        }
    }
    let covered = records
        .iter()
        .map(|r| r.bound.fragment_id as u32)
        .collect::<Vec<_>>();
    let files = write_records(records, data_type, index_store).await?;
    Ok((
        CreatedIndex {
            index_details: make_fragment_column_stats_details(),
            index_version: FRAGMENT_COLUMN_STATS_INDEX_VERSION,
            files,
        },
        covered,
    ))
}

/// Fragment column statistics index: one summary per fragment for one column.
pub struct FragmentColumnStatsIndex {
    records: Vec<ZoneMapStatistics>,
    data_type: DataType,
}

impl std::fmt::Debug for FragmentColumnStatsIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FragmentColumnStatsIndex")
            .field("num_fragments", &self.records.len())
            .field("data_type", &self.data_type)
            .finish()
    }
}

impl DeepSizeOf for FragmentColumnStatsIndex {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.records
            .iter()
            .map(|r| r.deep_size_of_children(context) + std::mem::size_of::<ZoneMapStatistics>())
            .sum()
    }
}

impl FragmentColumnStatsIndex {
    pub async fn load(store: Arc<dyn IndexStore>) -> Result<Arc<Self>> {
        let index_file = store.open_index_file(FRAGMENT_STATS_FILENAME).await?;
        let data = index_file
            .read_range(0..index_file.num_rows(), None)
            .await?;
        Self::try_from_serialized(data)
    }

    fn try_from_serialized(data: RecordBatch) -> Result<Arc<Self>> {
        let get = |name: &str| {
            data.column_by_name(name).ok_or_else(|| {
                Error::invalid_input(format!("FragmentColumnStatsIndex: missing '{name}' column"))
            })
        };
        let fragment_ids = get("fragment_id")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                Error::invalid_input("FragmentColumnStatsIndex: 'fragment_id' is not UInt64")
            })?;
        let min_col = get("min")?;
        let max_col = get("max")?;
        let null_counts = get("null_count")?
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| {
                Error::invalid_input("FragmentColumnStatsIndex: 'null_count' is not UInt32")
            })?;
        let nan_counts = get("nan_count")?
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| {
                Error::invalid_input("FragmentColumnStatsIndex: 'nan_count' is not UInt32")
            })?;
        let row_spans = get("row_span")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                Error::invalid_input("FragmentColumnStatsIndex: 'row_span' is not UInt64")
            })?;

        let data_type = min_col.data_type().clone();
        let mut records = Vec::with_capacity(data.num_rows());
        for i in 0..data.num_rows() {
            let (min, max) = if data_type.is_nested() {
                (
                    ScalarValue::try_new_null(&data_type)?,
                    ScalarValue::try_new_null(&data_type)?,
                )
            } else {
                (
                    ScalarValue::try_from_array(min_col, i)?,
                    ScalarValue::try_from_array(max_col, i)?,
                )
            };
            records.push(ZoneMapStatistics {
                min,
                max,
                null_count: null_counts.value(i),
                nan_count: nan_counts.value(i),
                bound: ZoneBound {
                    fragment_id: fragment_ids.value(i),
                    start: 0,
                    length: row_spans.value(i) as usize,
                },
            });
        }
        Ok(Arc::new(Self { records, data_type }))
    }

    /// Fragments that have a record in this index.
    pub fn covered_fragments(&self) -> RoaringBitmap {
        self.records
            .iter()
            .map(|r| r.bound.fragment_id as u32)
            .collect()
    }

    /// Fragments whose record proves the query cannot match any of their rows.
    ///
    /// The complement discipline is the caller's: fragments WITHOUT records are
    /// never excludable, so the candidate scope is
    /// `live_fragments - excluded_fragments(query)`.
    pub fn excluded_fragments(&self, query: &SargableQuery) -> Result<RoaringBitmap> {
        let mut excluded = RoaringBitmap::new();
        for record in &self.records {
            if !ZoneMapIndex::evaluate_stats_against_query(&self.data_type, record, query)? {
                excluded.insert(record.bound.fragment_id as u32);
            }
        }
        Ok(excluded)
    }

    pub fn num_fragments(&self) -> usize {
        self.records.len()
    }

    /// Bench-only synthetic index: `count` fragments holding sequential,
    /// disjoint Int64 ranges of `rows_per_fragment` values each. Used by the
    /// scale microbenchmark to measure resident memory and scope-evaluation
    /// CPU without materializing real data. Not part of the public API.
    #[doc(hidden)]
    pub fn synthetic_i64(count: u32, rows_per_fragment: u64) -> Self {
        let records = (0..count)
            .map(|fragment_id| {
                let lo = fragment_id as i64 * rows_per_fragment as i64;
                let hi = lo + rows_per_fragment as i64 - 1;
                ZoneMapStatistics {
                    min: ScalarValue::Int64(Some(lo)),
                    max: ScalarValue::Int64(Some(hi)),
                    null_count: 0,
                    nan_count: 0,
                    bound: ZoneBound {
                        fragment_id: fragment_id as u64,
                        start: 0,
                        length: rows_per_fragment as usize,
                    },
                }
            })
            .collect();
        Self {
            records,
            data_type: DataType::Int64,
        }
    }

    pub fn value_data_type(&self) -> &DataType {
        &self.data_type
    }
}

#[async_trait]
impl Index for FragmentColumnStatsIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_index(self: Arc<Self>) -> Arc<dyn Index> {
        self
    }

    async fn prewarm(&self) -> Result<()> {
        Ok(())
    }

    fn statistics(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({
            "num_fragments": self.records.len(),
        }))
    }

    fn index_type(&self) -> IndexType {
        IndexType::FragmentColumnStats
    }

    async fn calculate_included_frags(&self) -> Result<RoaringBitmap> {
        Ok(self.covered_fragments())
    }
}

#[async_trait]
impl ScalarIndex for FragmentColumnStatsIndex {
    async fn search(
        &self,
        query: &dyn AnyQuery,
        metrics: &dyn MetricsCollector,
    ) -> Result<SearchResult> {
        let query = query
            .as_any()
            .downcast_ref::<SargableQuery>()
            .ok_or_else(|| {
                Error::invalid_input("FragmentColumnStatsIndex only supports sargable queries")
            })?;
        search_zones(&self.records, metrics, |record| {
            ZoneMapIndex::evaluate_stats_against_query(&self.data_type, record, query)
        })
    }

    fn results_are_row_addresses(&self) -> bool {
        true
    }

    fn can_remap(&self) -> bool {
        false
    }

    async fn remap(
        &self,
        _mapping: &RowAddrRemap,
        _dest_store: &dyn IndexStore,
    ) -> Result<CreatedIndex> {
        Err(Error::invalid_input_source(
            "FragmentColumnStatsIndex does not support remap; rewritten fragments \
             get fresh statistics from their own write-time zone stats"
                .into(),
        ))
    }

    async fn update(
        &self,
        new_data: SendableRecordBatchStream,
        dest_store: &dyn IndexStore,
        _old_data_filter: Option<super::OldIndexDataFilter>,
    ) -> Result<CreatedIndex> {
        // Generic fallback used by blanket index maintenance: scan the new
        // fragments' column data and append one record per fragment. The
        // preferred, scan-free path is `update_from_seeds`.
        let value_type = new_data.schema().field(0).data_type().clone();
        let processor = ZoneMapProcessor::new(value_type.clone())?;
        let trainer = ZoneTrainer::new(processor, ONE_ZONE_PER_FRAGMENT_CAPACITY)?;
        let (new_records, _null_rows) = trainer.train(new_data).await?;

        let mut combined = self.records.clone();
        combined.extend(new_records);
        let files = write_records(combined, &self.data_type, dest_store).await?;
        Ok(CreatedIndex {
            index_details: make_fragment_column_stats_details(),
            index_version: FRAGMENT_COLUMN_STATS_INDEX_VERSION,
            files,
        })
    }

    fn update_criteria(&self) -> UpdateCriteria {
        UpdateCriteria::only_new_data(
            TrainingCriteria::new(TrainingOrdering::Addresses).with_row_addr(),
        )
    }

    fn derive_index_params(&self) -> Result<ScalarIndexParams> {
        Ok(ScalarIndexParams::new("FragmentColumnStats".to_string()))
    }
}

pub struct FragmentColumnStatsTrainingRequest {
    criteria: TrainingCriteria,
}

impl Default for FragmentColumnStatsTrainingRequest {
    fn default() -> Self {
        Self {
            criteria: TrainingCriteria::new(TrainingOrdering::Addresses).with_row_addr(),
        }
    }
}

impl TrainingRequest for FragmentColumnStatsTrainingRequest {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn criteria(&self) -> &TrainingCriteria {
        &self.criteria
    }
}

#[derive(Debug, Default)]
pub struct FragmentColumnStatsIndexPlugin;

#[async_trait]
impl BasicTrainer for FragmentColumnStatsIndexPlugin {
    fn new_training_request(
        &self,
        _params: &str,
        _field: &Field,
    ) -> Result<Box<dyn TrainingRequest>> {
        Ok(Box::new(FragmentColumnStatsTrainingRequest::default()))
    }

    async fn train_index(
        &self,
        data: SendableRecordBatchStream,
        index_store: &dyn IndexStore,
        _request: Box<dyn TrainingRequest>,
        _fragment_ids: Option<Vec<u32>>,
        _progress: Arc<dyn crate::progress::IndexBuildProgress>,
    ) -> Result<CreatedIndex> {
        let value_type = data.schema().field(0).data_type().clone();
        let processor = ZoneMapProcessor::new(value_type.clone())?;
        let trainer = ZoneTrainer::new(processor, ONE_ZONE_PER_FRAGMENT_CAPACITY)?;
        let (records, _null_rows) = trainer.train(data).await?;
        let files = write_records(records, &value_type, index_store).await?;
        Ok(CreatedIndex {
            index_details: make_fragment_column_stats_details(),
            index_version: FRAGMENT_COLUMN_STATS_INDEX_VERSION,
            files,
        })
    }
}

#[async_trait]
impl ScalarIndexPlugin for FragmentColumnStatsIndexPlugin {
    fn basic_trainer(&self) -> Option<&dyn BasicTrainer> {
        Some(self)
    }

    fn name(&self) -> &str {
        "FragmentColumnStats"
    }

    fn provides_exact_answer(&self) -> bool {
        false
    }

    fn version(&self) -> u32 {
        FRAGMENT_COLUMN_STATS_INDEX_VERSION
    }

    /// No query parser: this index does not participate in generic predicate
    /// planning. The scanner's fragment-scope resolver opens it directly.
    fn new_query_parser(
        &self,
        _index_name: String,
        _index_details: &prost_types::Any,
    ) -> Option<Box<dyn ScalarQueryParser>> {
        None
    }

    async fn load_index(
        &self,
        index_store: Arc<dyn IndexStore>,
        _index_details: &prost_types::Any,
        _frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        _cache: &LanceCache,
    ) -> Result<Arc<dyn ScalarIndex>> {
        Ok(FragmentColumnStatsIndex::load(index_store).await? as Arc<dyn ScalarIndex>)
    }

    fn might_use_seeds(&self, _index_details: &prost_types::Any) -> bool {
        true
    }

    async fn create_seed_writer(
        &self,
        field_path: &str,
        data_type: &DataType,
        _index_details: &prost_types::Any,
    ) -> Result<Option<Box<dyn crate::scalar::seed::IndexSeedWriter>>> {
        Ok(Some(Box::new(ZoneMapSeedWriter::new(
            field_path,
            FRAGMENT_STATS_DEFAULT_ROWS_PER_ZONE,
            data_type.clone(),
        )?)))
    }

    async fn update_from_seeds(
        &self,
        seeds: Vec<FragmentSeed>,
        reference_index: Arc<dyn ScalarIndex>,
        _index_details: &prost_types::Any,
        dest_store: &dyn IndexStore,
    ) -> Result<Option<CreatedIndex>> {
        let Some(reference) = reference_index
            .as_any()
            .downcast_ref::<FragmentColumnStatsIndex>()
        else {
            return Ok(None);
        };
        let mut combined = reference.records.clone();
        for seed in &seeds {
            // Seeds observed every row written to the fragment, so the span
            // they cover is self-consistent; fold rejects internally unsound
            // payloads. Rejected seeds leave the fragment unknown, which is
            // safe, so the harvest fast path never has to fall back.
            let Some(rows_per_zone) = seed
                .metadata_value
                .split(':')
                .nth(1)
                .and_then(|s| s.parse::<u64>().ok())
            else {
                continue;
            };
            let Ok((zones, _)) =
                ZoneMapSeedWriter::deserialize_seed(seed.fragment_id, &seed.bytes, rows_per_zone)
            else {
                continue;
            };
            let covered: u64 = zones.iter().map(|z| z.bound.length as u64).sum();
            if let Some(record) =
                fold_zones_to_record(seed.fragment_id, &zones, covered, &reference.data_type)
            {
                combined.push(record);
            }
        }
        let files = write_records(combined, &reference.data_type, dest_store).await?;
        Ok(Some(CreatedIndex {
            index_details: make_fragment_column_stats_details(),
            index_version: FRAGMENT_COLUMN_STATS_INDEX_VERSION,
            files,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::NoOpMetricsCollector;
    use lance_select::RowSetOps;

    fn record(
        fragment_id: u64,
        min: ScalarValue,
        max: ScalarValue,
        null_count: u32,
        rows: usize,
    ) -> ZoneMapStatistics {
        ZoneMapStatistics {
            min,
            max,
            null_count,
            nan_count: 0,
            bound: ZoneBound {
                fragment_id,
                start: 0,
                length: rows,
            },
        }
    }

    fn int_index(records: Vec<ZoneMapStatistics>) -> FragmentColumnStatsIndex {
        FragmentColumnStatsIndex {
            records,
            data_type: DataType::Int64,
        }
    }

    fn int_scalar(v: i64) -> ScalarValue {
        ScalarValue::Int64(Some(v))
    }

    #[test]
    fn excluded_fragments_range() {
        let index = int_index(vec![
            record(0, int_scalar(0), int_scalar(9), 0, 10),
            record(1, int_scalar(10), int_scalar(19), 0, 10),
            record(3, int_scalar(90), int_scalar(100), 0, 10),
        ]);
        let query = SargableQuery::Range(
            std::ops::Bound::Included(int_scalar(15)),
            std::ops::Bound::Unbounded,
        );
        let excluded = index.excluded_fragments(&query).unwrap();
        assert!(excluded.contains(0));
        assert!(!excluded.contains(1));
        assert!(!excluded.contains(3));
        // Fragment 2 has no record: it must not appear in the excluded set.
        assert!(!excluded.contains(2));
    }

    #[test]
    fn fold_rejects_partial_coverage() {
        let zones = vec![record(7, int_scalar(0), int_scalar(4), 0, 5)];
        // Fragment actually spans 10 rows: only 5 covered -> no record.
        assert!(fold_zones_to_record(7, &zones, 10, &DataType::Int64).is_none());
        // Exact coverage -> record produced.
        let folded = fold_zones_to_record(7, &zones, 5, &DataType::Int64).unwrap();
        assert_eq!(folded.bound.fragment_id, 7);
        assert_eq!(folded.min, int_scalar(0));
        assert_eq!(folded.max, int_scalar(4));
    }

    #[test]
    fn fold_multiple_zones() {
        let zones = vec![
            record(2, int_scalar(5), int_scalar(9), 1, 5),
            record(2, int_scalar(0), int_scalar(3), 2, 5),
        ];
        let folded = fold_zones_to_record(2, &zones, 10, &DataType::Int64).unwrap();
        assert_eq!(folded.min, int_scalar(0));
        assert_eq!(folded.max, int_scalar(9));
        assert_eq!(folded.null_count, 3);
        assert_eq!(folded.bound.length, 10);
    }

    #[test]
    fn fold_all_null_fragment() {
        let null = ScalarValue::Int64(None);
        let zones = vec![record(4, null.clone(), null.clone(), 5, 5)];
        let folded = fold_zones_to_record(4, &zones, 5, &DataType::Int64).unwrap();
        assert!(folded.min.is_null());
        assert_eq!(folded.null_count, 5);
        // An all-null fragment is excludable for a finite equality query...
        let index = int_index(vec![folded]);
        let excluded = index
            .excluded_fragments(&SargableQuery::Equals(int_scalar(1)))
            .unwrap();
        assert!(excluded.contains(4));
        // ...but not for IsNull.
        let excluded = index.excluded_fragments(&SargableQuery::IsNull()).unwrap();
        assert!(!excluded.contains(4));
    }

    #[test]
    fn fold_rejects_missing_extrema_with_comparable_values() {
        let null = ScalarValue::Int64(None);
        // 5 rows, only 1 null: 4 comparable values but no bounds -> unsound.
        let zones = vec![record(9, null.clone(), null, 1, 5)];
        assert!(fold_zones_to_record(9, &zones, 5, &DataType::Int64).is_none());
    }

    #[tokio::test]
    async fn search_returns_fragment_spans() {
        let index = int_index(vec![
            record(0, int_scalar(0), int_scalar(9), 0, 10),
            record(1, int_scalar(10), int_scalar(19), 0, 10),
        ]);
        let query = SargableQuery::Equals(int_scalar(12));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();
        match result {
            SearchResult::AtMost(rows) => {
                let selected = rows.selected_rows();
                assert!(selected.contains(1 << 32));
                assert!(selected.contains((1 << 32) + 9));
                assert!(!selected.contains(0));
            }
            _ => panic!("expected AtMost"),
        }
    }
}
