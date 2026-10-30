// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use super::utils::{IndexMetrics, InstrumentedRecordBatchStreamAdapter};
use crate::{
    Dataset,
    dataset::rowids::{load_row_id_sequences, translate_addr_treemap_to_row_ids},
    index::{
        prefilter::DatasetPreFilter,
        scalar_logical::{
            open_named_scalar_index, open_scalar_index_segments, scalar_index_fragment_bitmap,
        },
    },
};
use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array, cast::AsArray, types::UInt64Type};
use arrow_schema::{Schema, SchemaRef};
use async_recursion::async_recursion;
use async_trait::async_trait;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::{
    execution::memory_pool::{MemoryConsumer, MemoryReservation},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
        stream::RecordBatchStreamAdapter,
    },
    scalar::ScalarValue,
};
use datafusion_physical_expr::EquivalenceProperties;
use futures::{StreamExt, TryFutureExt, TryStreamExt, stream::BoxStream};
use lance_core::{Error, ROW_ID_FIELD, Result, deepsize::DeepSizeOf, utils::address::RowAddress};
use lance_datafusion::{
    chunker::break_stream,
    signed_zero::zero_encodings,
    utils::{
        ExecutionPlanMetricsSetExt, SCALAR_INDEX_MAX_ALLOWED_METRIC,
        SCALAR_INDEX_MAX_BLOCKED_METRIC, SCALAR_INDEX_MIN_ALLOWED_METRIC,
        SCALAR_INDEX_MIN_BLOCKED_METRIC, SCALAR_INDEX_SEARCH_TIME_METRIC,
        SCALAR_INDEX_SER_TIME_METRIC,
    },
};
use lance_index::{
    metrics::MetricsCollector,
    scalar::{
        SargableQuery, ScalarIndex,
        expression::{ScalarIndexExpr, ScalarIndexLoader, ScalarIndexSearch},
    },
};
use lance_select::{
    IndexExprResult, NullableIndexExprResult, NullableRowAddrMask, NullableRowAddrSet, RowAddrMask,
    RowAddrTreeMap, RowSetOps, result::IndexExprResultWireFormat,
};
use lance_table::format::Fragment;
use roaring::RoaringBitmap;
use tracing::{debug_span, instrument};

#[async_trait]
impl ScalarIndexLoader for Dataset {
    async fn load_index(
        &self,
        column: &str,
        index_name: &str,
        metrics: &dyn MetricsCollector,
    ) -> Result<Arc<dyn ScalarIndex>> {
        open_named_scalar_index(self, column, index_name, metrics).await
    }

    async fn row_addr_result_to_row_ids(
        &self,
        result: NullableIndexExprResult,
    ) -> Result<NullableIndexExprResult> {
        // Addresses and row ids only diverge under stable row ids; otherwise the
        // address is the row id and there is nothing to translate.
        if !self.manifest.uses_stable_row_ids() {
            return Ok(result);
        }

        let NullableIndexExprResult { lower, upper, .. } = result;
        let lower = translate_addr_mask_to_row_ids(self, lower).await?;
        let upper = translate_addr_mask_to_row_ids(self, upper).await?;
        Ok(NullableIndexExprResult::new(lower, upper))
    }
}

struct ScopedScalarIndexLoader<'a> {
    dataset: &'a Dataset,
    fragments: Option<&'a RoaringBitmap>,
    segment_uuids: Option<&'a HashSet<uuid::Uuid>>,
}

#[async_trait]
impl ScalarIndexLoader for ScopedScalarIndexLoader<'_> {
    async fn load_index(
        &self,
        column: &str,
        index_name: &str,
        metrics: &dyn MetricsCollector,
    ) -> Result<Arc<dyn ScalarIndex>> {
        open_scalar_index_segments(
            self.dataset,
            column,
            index_name,
            self.fragments,
            self.segment_uuids,
            metrics,
        )
        .await
    }

    async fn row_addr_result_to_row_ids(
        &self,
        result: NullableIndexExprResult,
    ) -> Result<NullableIndexExprResult> {
        self.dataset.row_addr_result_to_row_ids(result).await
    }
}

/// Translate an address-domain [`NullableRowAddrMask`] into the row-id domain
///
/// Address-domain index results are always positive allow-lists (`AtMost`), so
/// a block-list here would mean a boolean op was applied before translation,
/// which is unsupported.
async fn translate_addr_mask_to_row_ids(
    dataset: &Dataset,
    mask: NullableRowAddrMask,
) -> Result<NullableRowAddrMask> {
    match mask {
        NullableRowAddrMask::AllowList(set) => Ok(NullableRowAddrMask::AllowList(
            translate_addr_set_to_row_ids(dataset, set).await?,
        )),
        NullableRowAddrMask::BlockList(_) => Err(Error::internal(
            "cannot translate a block-list address mask to the row-id domain",
        )),
    }
}

async fn translate_addr_set_to_row_ids(
    dataset: &Dataset,
    set: NullableRowAddrSet,
) -> Result<NullableRowAddrSet> {
    let selected = translate_addr_treemap_to_row_ids(dataset, set.selected_rows()).await?;
    let nulls = translate_addr_treemap_to_row_ids(dataset, set.null_rows()).await?;
    Ok(NullableRowAddrSet::new(selected, nulls))
}

/// Intersect both bounds of `result` with the live rows of the `scope`
/// fragments of `dataset`'s snapshot, which `scope` must be a subset of.
///
/// The universe is a positive allow list built from the scoped fragments
/// alone: in the address domain the row range of each fragment less its
/// deletions ([`DatasetPreFilter::do_create_restricted_deletion_mask`]),
/// under stable row ids the live row ids of each fragment
/// ([`DatasetPreFilter::do_create_deletion_mask_row_id`]). Intersecting
/// drops everything the masks say about other fragments, fragments the
/// snapshot no longer has, and deleted rows; an allow list is narrowed and
/// a block list becomes the universe less the blocked rows. An exact result
/// stays exact; the bounds of an inexact one keep their order.
pub(crate) async fn confine_to_fragments(
    result: IndexExprResult,
    dataset: &Arc<Dataset>,
    scope: &RoaringBitmap,
) -> Result<IndexExprResult> {
    let universe = if dataset.manifest.uses_stable_row_ids() {
        DatasetPreFilter::do_create_deletion_mask_row_id(dataset.clone(), Some(scope.clone()))
            .await?
    } else {
        DatasetPreFilter::do_create_restricted_deletion_mask(dataset.clone(), scope.clone()).await?
    };
    let confine = |mask: RowAddrMask| mask.intersect(universe.as_ref().clone());
    let exact = result.is_exact();
    let IndexExprResult { lower, upper, .. } = result;
    Ok(if exact {
        IndexExprResult::exact(confine(lower))
    } else {
        IndexExprResult::new(confine(lower), confine(upper))
    })
}

/// An execution node that performs a scalar index search
///
/// This does not actually scan any data.  We only look through the index to determine
/// the row ids that match the query.  The output of this node is a row id mask (serialized
/// into a record batch)
///
/// If the actual IDs are needed then use MaterializeIndexExec instead
#[derive(Debug)]
pub struct ScalarIndexExec {
    dataset: Arc<Dataset>,
    expr: ScalarIndexExpr,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    result_format: IndexExprResultWireFormat,
    fragment_scope: Option<Arc<RoaringBitmap>>,
    /// Whether `fragment_scope` also bounds the emitted masks (see
    /// [`Self::with_fragment_scope`]) or only selects the segments to load
    /// (see [`Self::with_loading_scope`]).
    confine_output: bool,
    segment_uuids: Option<Arc<HashSet<uuid::Uuid>>>,
}

impl DisplayAs for ScalarIndexExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ScalarIndexQuery: query={}", self.expr)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "ScalarIndexQuery\nquery={}", self.expr)
            }
        }
    }
}

impl ScalarIndexExec {
    pub fn new(
        dataset: Arc<Dataset>,
        expr: ScalarIndexExpr,
        result_format: IndexExprResultWireFormat,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(result_format.schema().clone()),
            Partitioning::RoundRobinBatch(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            dataset,
            expr: expr.optimize(),
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            result_format,
            fragment_scope: None,
            confine_output: false,
            segment_uuids: None,
        }
    }

    /// Restrict the node to `fragments`: load only the index segments that
    /// can intersect them, and emit masks that admit no row outside them.
    ///
    /// Both bounds of the result are intersected, after the complete
    /// expression (including any `NOT`) has been evaluated, with the live
    /// rows of the scoped fragments of this node's dataset snapshot. That
    /// positive set is the whole universe of the output: rows of other
    /// fragments, of fragments the snapshot no longer has, and rows deleted
    /// in the scoped fragments are all outside it. Exactness is kept, as is
    /// the predicate's own NULL semantics (an `IS NULL` leaf still selects
    /// the null-valued rows of the scope, a NULL comparison still selects
    /// nothing), and `fragments_covered` is intersected with the scope too.
    /// The scanner's own callers keep loading-only selection
    /// ([`Self::with_loading_scope`]); this contract is for callers that
    /// consume the raw masks. Both bounds
    /// come out as allow lists. An empty scope, or one naming only fragments
    /// the snapshot lacks, yields an exact empty allow list; without a scope
    /// the output is unrestricted and the node behaves as before.
    ///
    /// Cost: the universe is built once per execution from the scoped
    /// fragments only, never from the rest of the table. In the address
    /// domain it is one row range per scoped fragment (plus its deletion
    /// vector, when it has one); under stable row ids the masks carry row
    /// ids, so it is each scoped fragment's row-id sequence, as ranges where
    /// the sequence is contiguous and as bitmaps where it has holes, read
    /// through the dataset's metadata cache. The consumer remains responsible
    /// for any data-level recheck the result asks for.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use lance::Dataset;
    /// # use lance::io::exec::scalar_index::ScalarIndexExec;
    /// # use lance_index::scalar::expression::ScalarIndexExpr;
    /// # use lance_select::result::IndexExprResultWireFormat;
    /// # use roaring::RoaringBitmap;
    /// # fn plan(dataset: Arc<Dataset>, expr: ScalarIndexExpr) -> ScalarIndexExec {
    /// // Only fragments 3 and 7 may appear in the emitted masks, whatever
    /// // the expression says about the others.
    /// ScalarIndexExec::new(dataset, expr, IndexExprResultWireFormat::default())
    ///     .with_fragment_scope(RoaringBitmap::from_iter([3u32, 7]))
    /// # }
    /// ```
    pub fn with_fragment_scope(mut self, fragments: RoaringBitmap) -> Self {
        self.fragment_scope = Some(Arc::new(fragments));
        self.confine_output = true;
        self
    }

    /// Restrict which physical scalar index segments are loaded, and nothing
    /// else: the result mask may still name rows of other fragments. For a
    /// consumer that applies the exact fragment scope itself (the filtered
    /// read does) this avoids building the output universe of
    /// [`Self::with_fragment_scope`].
    pub(crate) fn with_loading_scope(mut self, fragments: RoaringBitmap) -> Self {
        self.fragment_scope = Some(Arc::new(fragments));
        self.confine_output = false;
        self
    }

    /// Restrict index loading to these physical scalar index segments.
    pub(crate) fn with_segment_uuids(mut self, segment_uuids: Arc<HashSet<uuid::Uuid>>) -> Self {
        self.segment_uuids = Some(segment_uuids);
        self
    }

    pub fn dataset(&self) -> &Arc<Dataset> {
        &self.dataset
    }

    /// The parsed scalar-index expression this node will evaluate.
    pub fn expr(&self) -> &ScalarIndexExpr {
        &self.expr
    }

    /// Return the wire format used when serializing this exec's
    /// [`IndexExprResult`] output.
    pub fn result_format(&self) -> IndexExprResultWireFormat {
        self.result_format
    }

    #[async_recursion]
    pub async fn fragments_covered_by_index_query(
        index_expr: &ScalarIndexExpr,
        dataset: &Dataset,
    ) -> Result<RoaringBitmap> {
        match index_expr {
            ScalarIndexExpr::And(lhs, rhs) => {
                Ok(Self::fragments_covered_by_index_query(lhs, dataset).await?
                    & Self::fragments_covered_by_index_query(rhs, dataset).await?)
            }
            ScalarIndexExpr::Or(lhs, rhs) => {
                Ok(Self::fragments_covered_by_index_query(lhs, dataset).await?
                    & Self::fragments_covered_by_index_query(rhs, dataset).await?)
            }
            ScalarIndexExpr::Not(expr) => {
                Self::fragments_covered_by_index_query(expr, dataset).await
            }
            ScalarIndexExpr::Query(search_key) => {
                scalar_index_fragment_bitmap(dataset, &search_key.column, &search_key.index_name)
                    .await?
                    .ok_or_else(|| {
                        Error::internal(format!(
                            "Index not found even though it must have been found earlier: {}",
                            search_key.index_name
                        ))
                    })
            }
        }
    }

    async fn do_execute(
        expr: ScalarIndexExpr,
        dataset: Arc<Dataset>,
        plan_metrics: ExecutionPlanMetricsSet,
        result_format: IndexExprResultWireFormat,
        fragment_scope: Option<Arc<RoaringBitmap>>,
        confine_output: bool,
        segment_uuids: Option<Arc<HashSet<uuid::Uuid>>>,
    ) -> Result<RecordBatch> {
        let metrics = IndexMetrics::new(&plan_metrics, 0);
        // A scope naming fragments the snapshot does not have is the scope of
        // the ones it does have; none at all is an empty scope.
        let fragment_scope = fragment_scope.map(|scope| {
            let snapshot: RoaringBitmap = dataset
                .fragments()
                .iter()
                .map(|fragment| fragment.id as u32)
                .collect();
            Arc::new(scope.as_ref() & snapshot)
        });
        let query_result = {
            let search_time = plan_metrics.new_time(SCALAR_INDEX_SEARCH_TIME_METRIC, 0);
            let _timer = search_time.timer();
            match (fragment_scope.as_deref(), segment_uuids.as_deref()) {
                (Some(fragments), _) if fragments.is_empty() => {
                    IndexExprResult::exact(RowAddrMask::allow_nothing())
                }
                (Some(fragments), segment_uuids) => {
                    expr.evaluate(
                        &ScopedScalarIndexLoader {
                            dataset: dataset.as_ref(),
                            fragments: Some(fragments),
                            segment_uuids,
                        },
                        &metrics,
                    )
                    .await?
                }
                (None, Some(segment_uuids)) => {
                    expr.evaluate(
                        &ScopedScalarIndexLoader {
                            dataset: dataset.as_ref(),
                            fragments: None,
                            segment_uuids: Some(segment_uuids),
                        },
                        &metrics,
                    )
                    .await?
                }
                (None, None) => expr.evaluate(dataset.as_ref(), &metrics).await?,
            }
        };
        let query_result = match fragment_scope.as_deref() {
            Some(fragments) if confine_output && !fragments.is_empty() => {
                confine_to_fragments(query_result, &dataset, fragments).await?
            }
            _ => query_result,
        };
        Self::record_match_bounds(&query_result, &plan_metrics);
        let mut fragments_covered_by_result =
            Self::fragments_covered_by_index_query(&expr, dataset.as_ref()).await?;
        if let Some(fragment_scope) = fragment_scope {
            fragments_covered_by_result &= fragment_scope.as_ref();
        }
        {
            let ser_time = plan_metrics.new_time(SCALAR_INDEX_SER_TIME_METRIC, 0);
            let _timer = ser_time.timer();
            query_result.serialize(&fragments_covered_by_result, result_format)
        }
    }

    /// Record how many rows each bound of the index result covers
    ///
    /// The node serializes the result into a two-row batch regardless of how
    /// selective the query was, so `output_rows` is always 2.  These metrics
    /// describe the two bounds instead: `min_*` sizes the lower bound (rows
    /// guaranteed to match) and `max_*` the upper bound (rows that may match).
    ///
    /// Each bound is a mask that either allows or blocks the rows it names, and
    /// the metric is suffixed to match, so exactly two of the four are
    /// reported.  A block list is not converted into a number of matching rows
    /// because that needs the count of rows the scan will actually consider,
    /// which is more than this node can know; deletions are only one of the
    /// things that take rows out of a scan.
    fn record_match_bounds(result: &IndexExprResult, plan_metrics: &ExecutionPlanMetricsSet) {
        record_bound(
            &result.lower,
            SCALAR_INDEX_MIN_ALLOWED_METRIC,
            SCALAR_INDEX_MIN_BLOCKED_METRIC,
            plan_metrics,
        );
        record_bound(
            &result.upper,
            SCALAR_INDEX_MAX_ALLOWED_METRIC,
            SCALAR_INDEX_MAX_BLOCKED_METRIC,
            plan_metrics,
        );
    }
}

impl ExecutionPlan for ScalarIndexExec {
    fn name(&self) -> &str {
        "ScalarIndexExec"
    }

    fn schema(&self) -> SchemaRef {
        self.result_format.schema().clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion::error::Result<TreeNodeRecursion>,
    ) -> datafusion::error::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            Err(datafusion::error::DataFusionError::Internal(
                "ScalarIndexExec does not have children".to_string(),
            ))
        } else {
            Ok(self)
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::context::TaskContext>,
    ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let batch_fut = Self::do_execute(
            self.expr.clone(),
            self.dataset.clone(),
            self.metrics.clone(),
            self.result_format,
            self.fragment_scope.clone(),
            self.confine_output,
            self.segment_uuids.clone(),
        );
        let stream = futures::stream::iter(vec![batch_fut])
            .then(|batch_fut| batch_fut.map_err(|err| err.into()))
            .boxed()
            as BoxStream<'static, datafusion::common::Result<RecordBatch>>;
        Ok(Box::pin(InstrumentedRecordBatchStreamAdapter::new(
            self.result_format.schema().clone(),
            stream,
            partition,
            &self.metrics,
        )))
    }

    fn partition_statistics(
        &self,
        _partition: Option<usize>,
    ) -> datafusion::error::Result<Arc<datafusion::physical_plan::Statistics>> {
        Ok(Arc::new(datafusion::physical_plan::Statistics {
            num_rows: datafusion::common::stats::Precision::Exact(2),
            ..datafusion::physical_plan::Statistics::new_unknown(self.result_format.schema())
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }
}

/// Record the size of one bound under the metric matching how it selects rows
///
/// A mask that selects whole fragments does not know how many rows it names, in
/// which case nothing is recorded rather than a guess.
fn record_bound(
    mask: &RowAddrMask,
    allowed_metric: &'static str,
    blocked_metric: &'static str,
    plan_metrics: &ExecutionPlanMetricsSet,
) {
    let (metric, rows) = match mask {
        RowAddrMask::AllowList(allowed) => (allowed_metric, allowed.len()),
        RowAddrMask::BlockList(blocked) => (blocked_metric, blocked.len()),
    };
    if let Some(rows) = rows {
        plan_metrics.new_count(metric, 0).add(rows as usize);
    }
}

pub static INDEX_LOOKUP_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new(vec![ROW_ID_FIELD.clone()])));

/// A single scalar-index lookup used by [`MapIndexExec`].
///
/// `column` identifies a column whose values will be probed against the
/// index named `index_name`. Multiple lookups are intersected with logical
/// AND semantics inside `MapIndexExec`.
#[derive(Debug, Clone)]
pub struct IndexLookup {
    pub column: String,
    pub index_name: String,
}

const MAP_INDEX_CANDIDATES_MEMORY_CONSUMER: &str = "MapIndexExecCandidates";

/// Per-address allowance for cooperative pool accounting of the retained map.
/// This conservatively bounds its approximate [`DeepSizeOf`] growth, but is not
/// an allocator-level peak-memory bound. The reservation is shrunk to the
/// measured retained-map size after each input batch.
const ROW_ADDR_INSERT_RESERVATION_BYTES: usize = 256;

#[derive(Debug)]
struct DistinctRowAddrs {
    emitted: RowAddrTreeMap,
    reservation: MemoryReservation,
}

impl DistinctRowAddrs {
    fn new(reservation: MemoryReservation) -> Self {
        Self {
            emitted: RowAddrTreeMap::new(),
            reservation,
        }
    }

    fn retain_unseen(&mut self, row_addrs: &UInt64Array) -> datafusion::error::Result<UInt64Array> {
        let initial_reservation_size = self.reservation.size();
        // The first batch also has to account for the empty map's inline size,
        // which is not part of the initially empty reservation.
        let retained_size = initial_reservation_size.max(std::mem::size_of::<RowAddrTreeMap>());
        let unaccounted_candidates = row_addrs
            .values()
            .iter()
            .filter(|row_addr| !self.emitted.contains(**row_addr))
            .count();
        let provisional_size = unaccounted_candidates
            .checked_mul(ROW_ADDR_INSERT_RESERVATION_BYTES)
            .and_then(|additional| retained_size.checked_add(additional))
            .ok_or_else(|| {
                datafusion::error::DataFusionError::ResourcesExhausted(format!(
                    "Candidate memory reservation overflowed for {MAP_INDEX_CANDIDATES_MEMORY_CONSUMER}"
                ))
            })?;
        self.reservation.try_resize(provisional_size)?;

        // Allocate output storage only after the complete retained-state
        // reservation succeeds.
        let mut unseen = Vec::with_capacity(unaccounted_candidates);
        for row_addr in row_addrs.values() {
            if self.emitted.insert(*row_addr) {
                unseen.push(*row_addr);
            }
        }

        let measured_size = self.emitted.deep_size_of();
        if measured_size > provisional_size {
            self.rollback_batch(&unseen, initial_reservation_size);
            return Err(datafusion::error::DataFusionError::ResourcesExhausted(
                format!(
                    "MapIndexExecCandidates batch exceeded its {ROW_ADDR_INSERT_RESERVATION_BYTES}-byte per-candidate reservation"
                ),
            ));
        }
        self.reservation.resize(measured_size);
        Ok(UInt64Array::from(unseen))
    }

    fn rollback_batch(&mut self, unseen: &[u64], reservation_size: usize) {
        for row_addr in unseen {
            let is_removed = self.emitted.remove(*row_addr);
            debug_assert!(
                is_removed,
                "a newly inserted MapIndexExec candidate must be removable"
            );
        }
        self.reservation.resize(reservation_size);
    }
}

impl IndexLookup {
    pub fn new(column: impl Into<String>, index_name: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            index_name: index_name.into(),
        }
    }
}

/// An execution node that translates index values into row addresses
///
/// This can be combined with TakeExec to perform an "indexed take".
///
/// Multiple `(column, index_name)` lookups can be supplied: the operator
/// expects one input column per lookup (in matching order) and emits the
/// row addresses that could match on every column. The result is an upper
/// bound, not an exact set — the probes are evaluated one key at a time,
/// most-selective first, and stop as soon as the candidate set is no larger
/// than the input batch, so a caller must still filter on the full key.
/// This lets a composite-key join trim the candidate row set before the
/// downstream take without paying for a probe that prunes nothing. A row
/// address reached by more than one input batch is emitted only once.
#[derive(Debug)]
pub struct MapIndexExec {
    dataset: Arc<Dataset>,
    lookups: Vec<IndexLookup>,
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl DisplayAs for MapIndexExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "IndexedLookup")?;
                if self.lookups.len() > 1 {
                    let cols = self
                        .lookups
                        .iter()
                        .map(|l| l.column.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    write!(f, " [{cols}]")?;
                }
                Ok(())
            }
        }
    }
}

impl MapIndexExec {
    /// Convenience constructor for the common single-column case.
    pub fn new(
        dataset: Arc<Dataset>,
        column_name: String,
        index_name: String,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self::new_multi(
            dataset,
            vec![IndexLookup::new(column_name, index_name)],
            input,
        )
    }

    /// Build a `MapIndexExec` that probes one or more scalar indices and emits
    /// an upper bound on the row addresses matching every one of them (see the
    /// type docs). `lookups` must be non-empty and `input` must produce one
    /// column per lookup, in the same order.
    pub fn new_multi(
        dataset: Arc<Dataset>,
        lookups: Vec<IndexLookup>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        debug_assert!(
            !lookups.is_empty(),
            "MapIndexExec requires at least one index lookup"
        );
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(INDEX_LOOKUP_SCHEMA.clone()),
            Partitioning::RoundRobinBatch(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            dataset,
            lookups,
            input,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    async fn build_stream(
        input: datafusion::physical_plan::SendableRecordBatchStream,
        partition: usize,
        dataset: Arc<Dataset>,
        lookups: Vec<IndexLookup>,
        index_metrics: Arc<IndexMetrics>,
        metrics_set: ExecutionPlanMetricsSet,
        candidate_reservation: MemoryReservation,
    ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        // A row can be found by the composite probe only if it lives in a
        // fragment covered by *every* index in `lookups`; restrict the
        // deletion mask to that intersection so we only filter deletes we
        // could actually see.
        //
        // This loop must keep covering every lookup even though `map_batch`
        // may skip some probes. A skipped probe leaves candidates from
        // fragments that its index does not cover, and the restricted mask is
        // the only thing that then blocks them. Those fragments are read
        // separately by the unindexed-fragment scan in
        // `create_indexed_scan_joined_stream`, so letting candidates through
        // would feed the same target row into the join twice, which the
        // default `SourceDedupeBehavior::Fail` reports as an error.
        let mut fragment_bitmap: Option<RoaringBitmap> = None;
        for lookup in &lookups {
            let bm = scalar_index_fragment_bitmap(&dataset, &lookup.column, &lookup.index_name)
                .await?
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Internal(format!(
                        "IndexedLookupExec: index '{}' on column '{}' disappeared after planning",
                        lookup.index_name, lookup.column,
                    ))
                })?;
            fragment_bitmap = Some(match fragment_bitmap {
                None => bm,
                Some(acc) => acc & bm,
            });
        }
        let fragment_bitmap = fragment_bitmap.expect("MapIndexExec built with no lookups");
        let deletion_mask_fut =
            DatasetPreFilter::create_restricted_deletion_mask(dataset.clone(), fragment_bitmap);
        let deletion_mask = if let Some(fut) = deletion_mask_fut {
            Some(fut.await?)
        } else {
            None
        };

        let baseline = BaselineMetrics::new(&metrics_set, partition);
        let elapsed_compute = baseline.elapsed_compute().clone();
        let stream = input.then(move |batch_result| {
            let lookups = lookups.clone();
            let dataset = dataset.clone();
            let deletion_mask = deletion_mask.clone();
            let metrics = index_metrics.clone();
            let elapsed_compute = elapsed_compute.clone();
            async move {
                let batch = batch_result?;
                // Timer spans `map_batch`'s `.await` on purpose: that await is
                // the per-batch sargable index evaluation, which is the work
                // we want attributed here.
                let _t = elapsed_compute.timer();
                Self::map_batch(lookups, dataset, deletion_mask, batch, metrics).await
            }
        });
        let mut distinct_row_addrs = DistinctRowAddrs::new(candidate_reservation);
        let stream = stream.and_then(move |batch| {
            // Each batch's index result is already a set. Retain first
            // occurrences here to make the complete candidate stream a set too.
            let row_addrs = batch.column(0).as_primitive::<UInt64Type>();
            let result = distinct_row_addrs
                .retain_unseen(row_addrs)
                .and_then(|unseen| {
                    RecordBatch::try_new(INDEX_LOOKUP_SCHEMA.clone(), vec![Arc::new(unseen)])
                        .map_err(datafusion::error::DataFusionError::from)
                });
            futures::future::ready(result)
        });
        let stream = stream.map(move |batch| {
            let poll = baseline.record_poll(std::task::Poll::Ready(Some(batch)));
            match poll {
                std::task::Poll::Ready(Some(b)) => b,
                _ => unreachable!("record_poll preserves Ready(Some) input"),
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            INDEX_LOOKUP_SCHEMA.clone(),
            stream,
        )))
    }

    /// The values of one input column, deduped when `dedupe` is set.
    ///
    /// Deduping earns its keep once there is more than one key: the distinct
    /// count doubles as the probe-ordering signal in [`Self::map_batch`], and a
    /// repeated value only adds an `IsIn` entry that re-selects index pages
    /// already selected. With a single key there is nothing to order, so
    /// hashing every value would be pure overhead on the most common path.
    ///
    /// One NULL survives dedupe on purpose: an index reads a NULL in the list
    /// as "also match null rows", which is a flag rather than a count.
    ///
    /// A float zero is looked up under both encodings. The index compares keys
    /// by encoding, but the hash join downstream matches `-0.0` and `+0.0` per
    /// IEEE 754, so probing one encoding would miss rows the join would match.
    fn key_values(column: &ArrayRef, dedupe: bool) -> datafusion::error::Result<Vec<ScalarValue>> {
        let mut values = Vec::with_capacity(column.len());
        let mut seen = HashSet::with_capacity(if dedupe { column.len() } else { 0 });
        for row in 0..column.len() {
            let value = ScalarValue::try_from_array(column, row)?;
            let encodings = match zero_encodings(&value) {
                Some((negative, positive)) => vec![negative, positive],
                None => vec![value],
            };
            for value in encodings {
                if dedupe {
                    if seen.contains(&value) {
                        continue;
                    }
                    seen.insert(value.clone());
                }
                values.push(value);
            }
        }
        Ok(values)
    }

    /// Build the `IsIn` query for one join key against its matching index.
    fn build_key_query(lookup: &IndexLookup, values: Vec<ScalarValue>) -> ScalarIndexExpr {
        ScalarIndexExpr::Query(ScalarIndexSearch {
            column: lookup.column.clone(),
            index_name: lookup.index_name.clone(),
            // Internal IndexedLookup-style query — type is unknown at this layer
            index_type: String::new(),
            query: Arc::new(SargableQuery::IsIn(values)),
            needs_recheck: false,
            fragment_bitmap: None,
        })
    }

    async fn map_batch(
        lookups: Vec<IndexLookup>,
        dataset: Arc<Dataset>,
        deletion_mask: Option<Arc<RowAddrMask>>,
        batch: RecordBatch,
        metrics: Arc<IndexMetrics>,
    ) -> datafusion::error::Result<RecordBatch> {
        // The operator's contract is one input column per lookup, in order (see
        // `new_multi`). Check it here rather than letting `batch.column` panic
        // deep inside a DataFusion stream, and check it before the probe loop:
        // the loop can skip the offending lookup, which would turn a broken
        // plan into a failure that depends on the data.
        if lookups.len() != batch.num_columns() {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "MapIndexExec has {} lookups but its input produced {} columns",
                lookups.len(),
                batch.num_columns()
            )));
        }

        // Probe the keys one at a time and intersect, rather than evaluating an
        // AND of every probe at once. A join key whose values repeat across the
        // target (a bucket or status column) matches most of the table, so its
        // probe materializes a candidate set the size of the dataset while
        // pruning almost nothing: on a 10M-row table, probing a 1024-distinct
        // key asked for 2.4 GB of candidates.
        // With one key there is nothing to order and nothing a second probe
        // could intersect away, so that path stays exactly as it was.
        let several_keys = lookups.len() > 1;
        let mut values_per_key = Vec::with_capacity(lookups.len());
        for column in batch.columns() {
            values_per_key.push(Self::key_values(column, several_keys)?);
        }

        // Probe the most selective key first. The key with the most distinct
        // source values partitions the target most finely, so its probe is the
        // one most likely to leave a candidate set small enough to skip the
        // rest. Following the caller's `on` order instead would run the
        // expensive probe first, because the natural way to write a composite
        // key is coarse-to-fine (`["tenant_id", "row_id"]`). The distinct count
        // is a source-side proxy for target-side selectivity, and it only knows
        // how many values a probe will look up, not how many target rows each
        // of them matches. A skewed batch — many distinct values that each
        // match many rows, sitting next to few values that each match few —
        // can therefore be ordered worse than the caller wrote it. The floor is
        // the old behaviour's probe set — run sequentially, see below — because
        // the break only ever removes probes.
        let mut probe_order: Vec<usize> = (0..lookups.len()).collect();
        if several_keys {
            // Stable, so keys with equally distinct values keep `on` order.
            probe_order.sort_by_key(|&idx| std::cmp::Reverse(values_per_key[idx].len()));
        }

        // Stop once the candidate set is no larger than the source batch. A
        // further probe could still remove false positives, but what is left to
        // remove is bounded by the source batch, so it cannot save more work
        // downstream than the probe itself costs. That bound is the point: per
        // batch the emitted set is either the full intersection or at most
        // `batch.num_rows()` rows, which is also what keeps the cross-batch
        // candidate set in `DistinctRowAddrs` bounded.
        //
        // Skipping a probe only leaves extra candidates, never drops a match:
        // the downstream hash join filters on the full composite key (see
        // `create_indexed_scan_joined_stream`), the same reason an unindexed
        // `on` column is allowed to prune nothing. Extra candidates stay inside
        // the index-covered fragments because the restricted deletion mask
        // applied below is built from *every* lookup's fragment bitmap.
        //
        // The probes run in sequence where the old `ScalarIndexExpr::And` ran
        // them concurrently. That is the price of being able to stop, and it
        // shows up only when no probe is skipped and the index cache is cold.
        let source_rows = batch.num_rows() as u64;
        // `all_rows()` is the identity for `intersect`, so the first probe
        // needs no special case.
        let mut row_addr_mask = RowAddrMask::all_rows();
        for idx in probe_order {
            if row_addr_mask
                .max_len()
                .is_some_and(|len| len <= source_rows)
            {
                break;
            }
            let values = std::mem::take(&mut values_per_key[idx]);
            let query = Self::build_key_query(&lookups[idx], values);
            let query_result = query.evaluate(dataset.as_ref(), metrics.as_ref()).await?;
            if !query_result.is_exact() {
                todo!("Support for non-exact query results as input for merge_insert")
            }
            row_addr_mask = row_addr_mask.intersect(query_result.upper);
        }

        if let Some(deletion_mask) = deletion_mask.as_ref() {
            row_addr_mask = row_addr_mask & deletion_mask.as_ref().clone();
        }

        let row_id_iter = row_addr_mask
            .iter_addrs()
            .ok_or(datafusion::error::DataFusionError::Internal(
                "IndexedLookupExec: Cannot iterate over row addresses (BlockList or contains full fragments)".to_string(),
            ))?;
        let allow_list: UInt64Array = row_id_iter.map(u64::from).collect();
        Ok(RecordBatch::try_new(
            INDEX_LOOKUP_SCHEMA.clone(),
            vec![Arc::new(allow_list)],
        )?)
    }
}

impl ExecutionPlan for MapIndexExec {
    fn name(&self) -> &str {
        "MapIndexExec"
    }

    fn schema(&self) -> SchemaRef {
        INDEX_LOOKUP_SCHEMA.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion::error::Result<TreeNodeRecursion>,
    ) -> datafusion::error::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            Err(datafusion::error::DataFusionError::Internal(
                "MapIndexExec requires exactly one child".to_string(),
            ))
        } else {
            Ok(Arc::new(Self::new_multi(
                self.dataset.clone(),
                self.lookups.clone(),
                children.into_iter().next().unwrap(),
            )))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        // Cross-batch deduplication retains every emitted candidate until the
        // stream ends, so this state is not covered by per-batch reservations.
        let candidate_reservation = MemoryConsumer::new(MAP_INDEX_CANDIDATES_MEMORY_CONSUMER)
            .register(context.memory_pool());
        let input = self.input.execute(partition, context)?;
        let stream_fut = Self::build_stream(
            input,
            partition,
            self.dataset.clone(),
            self.lookups.clone(),
            Arc::new(IndexMetrics::new(&self.metrics, partition)),
            self.metrics.clone(),
            candidate_reservation,
        );
        let stream = futures::stream::once(stream_fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            INDEX_LOOKUP_SCHEMA.clone(),
            stream,
        )))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }
}

pub static MATERIALIZE_INDEX_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new(vec![ROW_ID_FIELD.clone()])));

/// An execution node that performs a scalar index search and materializes the mask into row ids
///
/// First, the index is searched to determine the mask that should be applied.  Then, we take the
/// list of fragments, iterate through all possible row ids, and materialize the row ids that satisfy
/// the mask.  The output of this node is a list of row ids suitable for use in a take operation.
#[derive(Debug)]
pub struct MaterializeIndexExec {
    dataset: Arc<Dataset>,
    expr: ScalarIndexExpr,
    fragments: Arc<Vec<Fragment>>,
    /// Physical segments to open. `None` loads the whole logical index.
    segment_uuids: Option<Arc<HashSet<uuid::Uuid>>>,
    /// Row addresses blocked from the index result due to data overlay files committed after the
    /// index was built. ANDead into the candidate mask before row ID materialisation so that stale
    /// index entries never reach downstream operators.
    overlay_block: Option<RowAddrMask>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl DisplayAs for MaterializeIndexExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MaterializeIndex: query={}", self.expr)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "MaterializeIndex\nquery={}", self.expr)
            }
        }
    }
}

struct FragIdIter<'a> {
    src: &'a [Fragment],
    frag_idx: usize,
    idx_in_frag: usize,
}

impl<'a> FragIdIter<'a> {
    fn new(src: &'a [Fragment]) -> Self {
        Self {
            src,
            frag_idx: 0,
            idx_in_frag: 0,
        }
    }
}

impl Iterator for FragIdIter<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        while self.frag_idx < self.src.len() {
            let frag = &self.src[self.frag_idx];
            if self.idx_in_frag
                < frag
                    .physical_rows
                    .expect("Fragment doesn't have physical rows recorded")
            {
                let next_id =
                    RowAddress::new_from_parts(frag.id as u32, self.idx_in_frag as u32).into();
                self.idx_in_frag += 1;
                return Some(next_id);
            }
            self.frag_idx += 1;
            self.idx_in_frag = 0;
        }
        None
    }
}

impl MaterializeIndexExec {
    pub fn new(
        dataset: Arc<Dataset>,
        expr: ScalarIndexExpr,
        fragments: Arc<Vec<Fragment>>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(MATERIALIZE_INDEX_SCHEMA.clone()),
            Partitioning::RoundRobinBatch(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            dataset,
            expr,
            fragments,
            segment_uuids: None,
            overlay_block: None,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Restrict index loading to these physical scalar index segments.
    pub(crate) fn with_segment_uuids(mut self, segment_uuids: Arc<HashSet<uuid::Uuid>>) -> Self {
        self.segment_uuids = Some(segment_uuids);
        self
    }

    /// Block specific row addresses (see the `overlay_block` field) from the index result.
    pub fn with_overlay_block(mut self, block: RowAddrMask) -> Self {
        self.overlay_block = Some(block);
        self
    }

    #[instrument(name = "materialize_scalar_index", skip_all, level = "debug")]
    async fn do_execute(
        expr: ScalarIndexExpr,
        dataset: Arc<Dataset>,
        fragments: Arc<Vec<Fragment>>,
        segment_uuids: Option<Arc<HashSet<uuid::Uuid>>>,
        overlay_block: Option<RowAddrMask>,
        metrics: Arc<IndexMetrics>,
    ) -> Result<RecordBatch> {
        // Legacy storage materializes index hits here. UUID selection has to be applied at load
        // time; evaluating against the dataset opens every segment of the logical index.
        let loader = ScopedScalarIndexLoader {
            dataset: dataset.as_ref(),
            fragments: None,
            segment_uuids: segment_uuids.as_deref(),
        };
        let expr_result = expr.evaluate(&loader, metrics.as_ref());
        let span = debug_span!("create_prefilter");
        let prefilter = span.in_scope(|| {
            let fragment_bitmap =
                RoaringBitmap::from_iter(fragments.iter().map(|frag| frag.id as u32));
            // The user-requested `fragments` is guaranteed to be stricter than the index's fragment
            // bitmap.  This node only runs on indexed fragments and any fragments that were deleted
            // when the index was trained will still be deleted when the index is queried.
            DatasetPreFilter::create_deletion_mask(dataset.clone(), fragment_bitmap)
        });
        // MaterializeIndexExec emits a deterministic set of row ids. The
        // `upper` mask of the interval is the candidate set (the answer is
        // a subset of `upper`). For `Exact` results this is the exact
        // answer; for `AtMost` and Refined results it's a superset that
        // gets pruned downstream by `LanceFilterExec` (the full filter
        // runs on the materialized batches via the scan plan, so any
        // non-matching candidates in `upper` are dropped before they
        // reach the user). `AtLeast` carries an unbounded upper, so the
        // candidate set is the whole row space — not actionable here.
        let take_upper = |result: IndexExprResult| -> Result<RowAddrMask> {
            if result.is_at_least() && !result.is_exact() {
                todo!("Support AtLeast in MaterializeIndexExec")
            }
            Ok(result.upper)
        };
        let mut mask = if let Some(prefilter) = prefilter {
            let (expr_result, prefilter) = futures::try_join!(expr_result, prefilter)?;
            take_upper(expr_result)? & (*prefilter).clone()
        } else {
            take_upper(expr_result.await?)?
        };
        if let Some(block) = overlay_block {
            mask = mask & block;
        }
        let ids = row_ids_for_mask(mask, &dataset, &fragments).await?;
        let ids = UInt64Array::from(ids);
        Ok(RecordBatch::try_new(
            MATERIALIZE_INDEX_SCHEMA.clone(),
            vec![Arc::new(ids)],
        )?)
    }
}

#[instrument(name = "make_row_ids", skip(mask, dataset, fragments))]
async fn row_ids_for_mask(
    mask: RowAddrMask,
    dataset: &Dataset,
    fragments: &[Fragment],
) -> Result<Vec<u64>> {
    match mask {
        RowAddrMask::BlockList(block_list) if block_list.is_empty() => {
            // Matches all row ids in the given fragments.
            if dataset.manifest.uses_stable_row_ids() {
                let sequences = load_row_id_sequences(dataset, fragments)
                    .map_ok(|(_frag_id, sequence)| sequence)
                    .try_collect::<Vec<_>>()
                    .await?;

                let capacity = sequences.iter().map(|seq| seq.len() as usize).sum();
                let mut row_ids = Vec::with_capacity(capacity);
                for sequence in sequences {
                    row_ids.extend(sequence.iter());
                }
                Ok(row_ids)
            } else {
                Ok(FragIdIter::new(fragments).collect::<Vec<_>>())
            }
        }
        RowAddrMask::AllowList(mut allow_list) => {
            retain_fragments(&mut allow_list, fragments, dataset).await?;

            if let Some(allow_list_iter) = allow_list.row_addrs() {
                Ok(allow_list_iter.map(u64::from).collect::<Vec<_>>())
            } else {
                // We shouldn't hit this branch if the row ids are stable.
                debug_assert!(!dataset.manifest.uses_stable_row_ids());
                Ok(FragIdIter::new(fragments)
                    .filter(|row_id| allow_list.contains(*row_id))
                    .collect())
            }
        }
        RowAddrMask::BlockList(block_list) => {
            if dataset.manifest.uses_stable_row_ids() {
                let sequences = load_row_id_sequences(dataset, fragments)
                    .map_ok(|(_frag_id, sequence)| sequence)
                    .try_collect::<Vec<_>>()
                    .await?;

                let mut capacity = sequences.iter().map(|seq| seq.len() as usize).sum();
                capacity -= block_list.len().expect("unknown block list len") as usize;
                let mut row_ids = Vec::with_capacity(capacity);
                for sequence in sequences {
                    row_ids.extend(
                        sequence
                            .iter()
                            .filter(|row_id| !block_list.contains(*row_id)),
                    );
                }
                Ok(row_ids)
            } else {
                Ok(FragIdIter::new(fragments)
                    .filter(|row_id| !block_list.contains(*row_id))
                    .collect())
            }
        }
    }
}

async fn retain_fragments(
    allow_list: &mut RowAddrTreeMap,
    fragments: &[Fragment],
    dataset: &Dataset,
) -> Result<()> {
    if dataset.manifest.uses_stable_row_ids() {
        let fragment_ids = load_row_id_sequences(dataset, fragments)
            .map_ok(|(_frag_id, sequence)| RowAddrTreeMap::from(sequence.as_ref()))
            .try_fold(RowAddrTreeMap::new(), |mut acc, tree| async {
                acc |= tree;
                Ok(acc)
            })
            .await?;
        *allow_list &= &fragment_ids;
    } else {
        // Assume row ids are addresses, so we can filter out fragments by their ids.
        allow_list.retain_fragments(fragments.iter().map(|frag| frag.id as u32));
    }
    Ok(())
}

impl ExecutionPlan for MaterializeIndexExec {
    fn name(&self) -> &str {
        "MaterializeIndexExec"
    }

    fn schema(&self) -> SchemaRef {
        MATERIALIZE_INDEX_SCHEMA.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion::error::Result<TreeNodeRecursion>,
    ) -> datafusion::error::Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            Err(datafusion::error::DataFusionError::Internal(
                "MaterializeIndexExec does not have children".to_string(),
            ))
        } else {
            Ok(self)
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let metrics = Arc::new(IndexMetrics::new(&self.metrics, partition));
        let batch_fut = Self::do_execute(
            self.expr.clone(),
            self.dataset.clone(),
            self.fragments.clone(),
            self.segment_uuids.clone(),
            self.overlay_block.clone(),
            metrics,
        );
        let stream = futures::stream::iter(vec![batch_fut])
            .then(|batch_fut| batch_fut.map_err(|err| err.into()))
            .boxed()
            as BoxStream<'static, datafusion::common::Result<RecordBatch>>;
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            MATERIALIZE_INDEX_SCHEMA.clone(),
            stream,
        ));
        let stream = break_stream(stream, context.session_config().batch_size());
        Ok(Box::pin(InstrumentedRecordBatchStreamAdapter::new(
            MATERIALIZE_INDEX_SCHEMA.clone(),
            stream.map_err(|err| err.into()),
            partition,
            &self.metrics,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use datafusion::physical_plan::StatisticsArgs;
    use datafusion::physical_plan::StatisticsContext;
    use std::{ops::Bound, sync::Arc};

    use crate::index::DatasetIndexExt;
    use arrow::datatypes::UInt64Type;
    use arrow::record_batch::RecordBatchIterator;
    use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, UInt64Array, cast::AsArray};
    use arrow_schema::Schema;
    use datafusion::{
        error::DataFusionError,
        execution::{
            TaskContext,
            memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool},
        },
        physical_plan::ExecutionPlan,
        prelude::SessionConfig,
        scalar::ScalarValue,
    };
    use futures::TryStreamExt;
    use lance_core::{
        deepsize::DeepSizeOf,
        utils::{address::RowAddress, tempfile::TempStrDir},
    };
    use lance_datafusion::utils::{
        MetricsExt, SCALAR_INDEX_MAX_ALLOWED_METRIC, SCALAR_INDEX_MAX_BLOCKED_METRIC,
        SCALAR_INDEX_MIN_ALLOWED_METRIC, SCALAR_INDEX_MIN_BLOCKED_METRIC,
    };
    use lance_datagen::gen_batch;
    use lance_index::{
        IndexType,
        scalar::{
            SargableQuery, ScalarIndexParams,
            expression::{ScalarIndexExpr, ScalarIndexSearch},
        },
    };
    use lance_select::{
        RowAddrTreeMap, RowSetOps,
        result::{IndexExprResult, IndexExprResultWireFormat},
    };
    use roaring::RoaringBitmap;

    use crate::{
        Dataset,
        dataset::{
            WriteParams,
            optimize::{CompactionOptions, compact_files},
        },
        io::exec::scalar_index::MaterializeIndexExec,
        utils::test::{DatagenExt, FragmentCount, FragmentRowCount, NoContextTestFixture},
    };

    use super::{
        DistinctRowAddrs, MAP_INDEX_CANDIDATES_MEMORY_CONSUMER, MapIndexExec,
        ROW_ADDR_INSERT_RESERVATION_BYTES, ScalarIndexExec,
    };

    struct TestFixture {
        dataset: Arc<Dataset>,
        _tmp_dir_guard: TempStrDir,
    }

    async fn test_fixture() -> TestFixture {
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let mut dataset = gen_batch()
            .col("ordered", lance_datagen::array::step::<UInt64Type>())
            .into_dataset(
                test_uri,
                FragmentCount::from(10),
                FragmentRowCount::from(10),
            )
            .await
            .unwrap();

        dataset
            .create_index(
                &["ordered"],
                IndexType::BTree,
                None,
                &ScalarIndexParams::default(),
                true,
            )
            .await
            .unwrap();

        TestFixture {
            dataset: Arc::new(dataset),
            _tmp_dir_guard: test_dir,
        }
    }

    #[test]
    fn key_values_probes_both_zero_encodings() {
        fn float_bits(values: &[ScalarValue]) -> Vec<u64> {
            values
                .iter()
                .map(|value| match value {
                    ScalarValue::Float64(Some(v)) => v.to_bits(),
                    other => panic!("unexpected key value {other:?}"),
                })
                .collect()
        }
        fn bits(values: &[f64]) -> Vec<u64> {
            values.iter().map(|v| v.to_bits()).collect()
        }

        let floats: ArrayRef = Arc::new(arrow_array::Float64Array::from(vec![
            1.0,
            0.0,
            1.0,
            f64::NAN,
        ]));

        // A zero row yields both encodings, so without dedupe there are more
        // values than rows. The values only build an `IsIn` set, so nothing
        // depends on them lining up with input rows.
        let values = MapIndexExec::key_values(&floats, false).unwrap();
        assert_eq!(float_bits(&values), bits(&[1.0, -0.0, 0.0, 1.0, f64::NAN]));

        let deduped = MapIndexExec::key_values(&floats, true).unwrap();
        assert_eq!(float_bits(&deduped), bits(&[1.0, -0.0, 0.0, f64::NAN]));

        // Integer zero has a single encoding and is not expanded.
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![0, 1]));
        assert_eq!(
            MapIndexExec::key_values(&ints, false).unwrap(),
            vec![ScalarValue::Int32(Some(0)), ScalarValue::Int32(Some(1))]
        );
    }

    #[test]
    fn test_map_index_candidates_accept_empty_first_batch() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
        let reservation = MemoryConsumer::new(MAP_INDEX_CANDIDATES_MEMORY_CONSUMER).register(&pool);
        let mut candidates = DistinctRowAddrs::new(reservation);

        let empty = candidates
            .retain_unseen(&UInt64Array::from(Vec::<u64>::new()))
            .unwrap();
        assert!(empty.is_empty());
        assert_eq!(pool.reserved(), RowAddrTreeMap::new().deep_size_of());
    }

    #[test]
    fn test_map_index_candidates_respect_memory_pool() {
        let mut first_candidate = RowAddrTreeMap::new();
        first_candidate.insert(1);
        let first_candidate_size = first_candidate.deep_size_of();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(
            ROW_ADDR_INSERT_RESERVATION_BYTES + first_candidate_size,
        ));
        let reservation = MemoryConsumer::new(MAP_INDEX_CANDIDATES_MEMORY_CONSUMER).register(&pool);
        let mut candidates = DistinctRowAddrs::new(reservation);

        let first = candidates
            .retain_unseen(&UInt64Array::from(vec![1]))
            .unwrap();
        assert_eq!(first.values(), &[1]);
        assert_eq!(pool.reserved(), first_candidate_size);

        let second = candidates
            .retain_unseen(&UInt64Array::from(vec![2]))
            .unwrap();
        assert_eq!(second.values(), &[2]);
        let retained_size = pool.reserved();
        assert!(retained_size > first_candidate_size);

        let error = candidates
            .retain_unseen(&UInt64Array::from(vec![3]))
            .unwrap_err();
        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
        assert!(
            error
                .to_string()
                .contains(MAP_INDEX_CANDIDATES_MEMORY_CONSUMER)
        );
        assert!(candidates.emitted.contains(1));
        assert!(candidates.emitted.contains(2));
        assert!(!candidates.emitted.contains(3));
        assert_eq!(pool.reserved(), retained_size);

        let duplicate = candidates
            .retain_unseen(&UInt64Array::from(vec![1, 2]))
            .unwrap();
        assert!(duplicate.values().is_empty());

        drop(candidates);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn test_materialize_index_exec() {
        let TestFixture {
            dataset,
            _tmp_dir_guard,
        } = test_fixture().await;

        let query = ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "ordered".to_string(),
            index_name: "ordered_idx".to_string(),
            index_type: "BTree".to_string(),
            query: Arc::new(SargableQuery::Range(
                Bound::Unbounded,
                Bound::Excluded(ScalarValue::UInt64(Some(47))),
            )),
            needs_recheck: false,
            fragment_bitmap: None,
        });
        let fragments = dataset.fragments().clone();

        let plan = MaterializeIndexExec::new(dataset, query, fragments);

        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();

        let batches = stream.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 47);

        let context =
            TaskContext::default().with_session_config(SessionConfig::default().with_batch_size(5));
        let stream = plan.execute(0, Arc::new(context)).unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(batches.len(), 10);
        assert_eq!(batches[0].num_rows(), 5);
    }

    #[tokio::test]
    async fn test_translate_addr_treemap_to_stable_row_ids() {
        let test_dir = TempStrDir::default();
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int32Array::from((0..10).collect::<Vec<_>>())) as ArrayRef,
        )])
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let write_params = WriteParams {
            enable_stable_row_ids: true,
            max_rows_per_file: 5,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, test_dir.as_str(), Some(write_params))
            .await
            .unwrap();
        let fragment_id = dataset.get_fragments()[1].id() as u32;

        let mut full_fragment = RowAddrTreeMap::new();
        full_fragment.insert_fragment(fragment_id);
        let translated = super::translate_addr_treemap_to_row_ids(&dataset, &full_fragment)
            .await
            .unwrap();
        let row_ids = translated
            .get_fragment_bitmap(0)
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(row_ids, vec![5, 6, 7, 8, 9]);

        let mut partial_fragment = RowAddrTreeMap::new();
        partial_fragment.insert(RowAddress::new_from_parts(fragment_id, 1).into());
        partial_fragment.insert(RowAddress::new_from_parts(fragment_id, 3).into());
        let translated = super::translate_addr_treemap_to_row_ids(&dataset, &partial_fragment)
            .await
            .unwrap();
        let row_ids = translated
            .get_fragment_bitmap(0)
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(row_ids, vec![6, 8]);
    }

    /// `ScalarIndexExec::schema()` (and the stream it emits) must advertise
    /// the same schema the batch actually carries — otherwise downstream
    /// consumers that trust `ExecutionPlan::schema()` will see a different
    /// shape than they receive.
    ///
    /// The schema depends on the `IndexExprResultWireFormat` passed to `ScalarIndexExec::new`.
    #[tokio::test]
    async fn test_scalar_index_exec_advertises_correct_schema() {
        let TestFixture {
            dataset,
            _tmp_dir_guard,
        } = test_fixture().await;

        let query = ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "ordered".to_string(),
            index_name: "ordered_idx".to_string(),
            index_type: "BTree".to_string(),
            query: Arc::new(SargableQuery::Range(
                Bound::Unbounded,
                Bound::Excluded(ScalarValue::UInt64(Some(47))),
            )),
            needs_recheck: false,
            fragment_bitmap: None,
        });

        let verify = async |plan: ScalarIndexExec, schema: Arc<Schema>| {
            assert_eq!(plan.schema(), schema);
            assert_eq!(
                StatisticsContext::new()
                    .compute(&plan, &StatisticsArgs::new())
                    .unwrap()
                    .column_statistics
                    .len(),
                schema.fields().len(),
            );

            let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
            assert_eq!(stream.schema(), schema);
            let batches = stream.try_collect::<Vec<_>>().await.unwrap();
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].schema(), schema);
        };

        let plan = ScalarIndexExec::new(
            dataset.clone(),
            query.clone(),
            IndexExprResultWireFormat::ThreeVariant,
        );
        let schema = IndexExprResultWireFormat::ThreeVariant.schema().clone();

        verify(plan, schema).await;

        let plan = ScalarIndexExec::new(dataset, query, IndexExprResultWireFormat::TwoMask);
        let schema = IndexExprResultWireFormat::TwoMask.schema().clone();

        verify(plan, schema).await;
    }

    /// The node always emits a two-row batch, so `output_rows` says nothing
    /// about selectivity.  The `min_` / `max_` metrics size the two bounds of
    /// the result instead, under the name matching how each bound selects rows.
    #[tokio::test]
    async fn test_scalar_index_exec_reports_match_bounds() {
        let TestFixture {
            dataset,
            _tmp_dir_guard,
        } = test_fixture().await;

        // 100 rows, of which 47 match
        let less_than_47 = ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "ordered".to_string(),
            index_name: "ordered_idx".to_string(),
            index_type: "BTree".to_string(),
            query: Arc::new(SargableQuery::Range(
                Bound::Unbounded,
                Bound::Excluded(ScalarValue::UInt64(Some(47))),
            )),
            needs_recheck: false,
            fragment_bitmap: None,
        });

        let run = async |expr: ScalarIndexExpr| {
            let plan =
                ScalarIndexExec::new(dataset.clone(), expr, IndexExprResultWireFormat::default());
            let batches = plan
                .execute(0, Arc::new(TaskContext::default()))
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            // The output batch shape is fixed no matter how many rows matched
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
            let metrics = plan.metrics().unwrap();
            let bound = |name| metrics.find_count(name).map(|count| count.value());
            [
                bound(SCALAR_INDEX_MIN_ALLOWED_METRIC),
                bound(SCALAR_INDEX_MIN_BLOCKED_METRIC),
                bound(SCALAR_INDEX_MAX_ALLOWED_METRIC),
                bound(SCALAR_INDEX_MAX_BLOCKED_METRIC),
            ]
        };

        // An exact result allows the matching rows at both bounds
        let bounds = run(less_than_47.clone()).await;
        assert_eq!(bounds, [Some(47), None, Some(47), None]);

        // Negating it turns both bounds into block lists of the same rows
        let bounds = run(ScalarIndexExpr::Not(Box::new(less_than_47))).await;
        assert_eq!(bounds, [None, Some(47), None, Some(47)]);
    }

    #[test]
    fn no_context_scalar_index() {
        // These tests ensure we can create nodes and call execute without a tokio Runtime
        // being active.  This is a requirement for proper implementation of a Datafusion foreign
        // table provider.
        let fixture = NoContextTestFixture::new();
        let arc_dasaset = Arc::new(fixture.dataset);

        let query = ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "ordered".to_string(),
            index_name: "ordered_idx".to_string(),
            index_type: "BTree".to_string(),
            query: Arc::new(SargableQuery::Range(
                Bound::Unbounded,
                Bound::Excluded(ScalarValue::UInt64(Some(47))),
            )),
            needs_recheck: false,
            fragment_bitmap: None,
        });

        // These plans aren't even valid but it appears we defer all work (even validation) until
        // read time.
        let plan = ScalarIndexExec::new(
            arc_dasaset.clone(),
            query.clone(),
            IndexExprResultWireFormat::default(),
        );
        plan.execute(0, Arc::new(TaskContext::default())).unwrap();

        let plan = MapIndexExec::new(
            arc_dasaset.clone(),
            "ordered".to_string(),
            "ordered_idx".to_string(),
            Arc::new(plan),
        );
        plan.execute(0, Arc::new(TaskContext::default())).unwrap();

        let plan =
            MaterializeIndexExec::new(arc_dasaset.clone(), query, arc_dasaset.fragments().clone());
        plan.execute(0, Arc::new(TaskContext::default())).unwrap();
    }

    /// Every row of `dataset`: row id, row address and `columns` as i64 (NULL as None).
    async fn rows_of(
        dataset: &Dataset,
        columns: &[&str],
    ) -> Vec<(u64, RowAddress, Vec<Option<i64>>)> {
        let batch = dataset
            .scan()
            .project(columns)
            .unwrap()
            .with_row_id()
            .with_row_address()
            .try_into_batch()
            .await
            .unwrap();
        let values: Vec<_> = columns
            .iter()
            .map(|column| {
                arrow::compute::cast(
                    batch.column_by_name(column).unwrap(),
                    &arrow_schema::DataType::Int64,
                )
                .unwrap()
            })
            .collect();
        let ids = batch.column_by_name("_rowid").unwrap();
        let ids = ids.as_primitive::<UInt64Type>();
        let addrs = batch.column_by_name("_rowaddr").unwrap();
        let addrs = addrs.as_primitive::<UInt64Type>();
        (0..batch.num_rows())
            .map(|i| {
                let row = values
                    .iter()
                    .map(|array| {
                        let array = array.as_primitive::<arrow::datatypes::Int64Type>();
                        array.is_valid(i).then(|| array.value(i))
                    })
                    .collect();
                (ids.value(i), RowAddress::from(addrs.value(i)), row)
            })
            .collect()
    }

    async fn run_scoped(
        dataset: &Arc<Dataset>,
        expr: ScalarIndexExpr,
        scope: Option<&RoaringBitmap>,
    ) -> (IndexExprResult, RoaringBitmap) {
        let mut plan =
            ScalarIndexExec::new(dataset.clone(), expr, IndexExprResultWireFormat::default());
        if let Some(scope) = scope {
            plan = plan.with_fragment_scope(scope.clone());
        }
        let batches = plan
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        IndexExprResult::deserialize(&batches[0]).unwrap()
    }

    /// Check `result` row by row against `pred` (SQL three-valued: `None` is NULL)
    /// and `scope`: no row outside the scope may be admitted by either bound, an
    /// exact result must select exactly the in-scope matches, and an inexact one
    /// must keep every in-scope match in its upper bound and only in-scope matches
    /// in its lower bound. Returns the number of in-scope matches.
    fn check_confined(
        result: &IndexExprResult,
        rows: &[(u64, RowAddress, Vec<Option<i64>>)],
        scope: Option<&RoaringBitmap>,
        pred: &dyn Fn(&[Option<i64>]) -> Option<bool>,
    ) -> usize {
        let mut in_scope_matches = 0;
        for (row_id, addr, values) in rows {
            let in_scope = scope.is_none_or(|scope| scope.contains(addr.fragment_id()));
            let matches = pred(values) == Some(true);
            let context = format!("row id {row_id} at {addr:?} with {values:?}");
            if result.is_exact() {
                assert_eq!(
                    result.lower.selected(*row_id),
                    matches && in_scope,
                    "{context}"
                );
            } else {
                if matches && in_scope {
                    assert!(result.upper.selected(*row_id), "{context}");
                }
                if result.lower.selected(*row_id) {
                    assert!(matches && in_scope, "{context}");
                }
            }
            if !in_scope {
                assert!(
                    !result.upper.selected(*row_id) && !result.lower.selected(*row_id),
                    "{context}"
                );
            }
            in_scope_matches += usize::from(matches && in_scope);
        }
        in_scope_matches
    }

    fn search(column: &str, index_type: &str, query: SargableQuery) -> ScalarIndexExpr {
        ScalarIndexExpr::Query(ScalarIndexSearch {
            column: column.to_string(),
            index_name: format!("{column}_idx"),
            index_type: index_type.to_string(),
            query: Arc::new(query),
            needs_recheck: false,
            fragment_bitmap: None,
        })
    }

    fn below(column: &str, value: ScalarValue) -> ScalarIndexExpr {
        search(
            column,
            "BTree",
            SargableQuery::Range(Bound::Unbounded, Bound::Excluded(value)),
        )
    }

    fn not(expr: ScalarIndexExpr) -> ScalarIndexExpr {
        ScalarIndexExpr::Not(Box::new(expr))
    }

    fn and(lhs: ScalarIndexExpr, rhs: ScalarIndexExpr) -> ScalarIndexExpr {
        ScalarIndexExpr::And(Box::new(lhs), Box::new(rhs))
    }

    fn or(lhs: ScalarIndexExpr, rhs: ScalarIndexExpr) -> ScalarIndexExpr {
        ScalarIndexExpr::Or(Box::new(lhs), Box::new(rhs))
    }

    fn not3(p: Option<bool>) -> Option<bool> {
        p.map(|p| !p)
    }

    fn and3(p: Option<bool>, q: Option<bool>) -> Option<bool> {
        match (p, q) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        }
    }

    fn or3(p: Option<bool>, q: Option<bool>) -> Option<bool> {
        match (p, q) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        }
    }

    /// A three-valued predicate over one row's column values.
    type Pred = Box<dyn Fn(&[Option<i64>]) -> Option<bool>>;

    fn scope(fragments: impl IntoIterator<Item = u32>) -> RoaringBitmap {
        fragments.into_iter().collect()
    }

    /// The scope confines the complete expression: a complement evaluated over
    /// the scoped segments must not re-admit rows of other fragments, and every
    /// bound keeps its form without enumerating rows.
    #[tokio::test]
    async fn fragment_scope_confines_the_complete_expression() {
        let TestFixture {
            dataset,
            _tmp_dir_guard,
        } = test_fixture().await;
        let rows = rows_of(&dataset, &["ordered"]).await;
        let u = |v: u64| ScalarValue::UInt64(Some(v));
        let lt = |v: u64| move |row: &[Option<i64>]| row[0].map(|x| x < v as i64);
        let cases: Vec<(ScalarIndexExpr, Pred)> = vec![
            (below("ordered", u(47)), Box::new(lt(47))),
            (
                not(below("ordered", u(47))),
                Box::new(move |r| not3(lt(47)(r))),
            ),
            (
                and(not(below("ordered", u(47))), below("ordered", u(60))),
                Box::new(move |r| and3(not3(lt(47)(r)), lt(60)(r))),
            ),
            (
                or(below("ordered", u(15)), not(below("ordered", u(80)))),
                Box::new(move |r| or3(lt(15)(r), not3(lt(80)(r)))),
            ),
        ];
        let scopes = [
            None,
            Some(scope([2, 3])),
            Some(scope([4])),
            Some(scope([7])),
            Some(scope(0..10)),
            Some(scope([3, 4, 5])),
            // A fragment the snapshot does not have: nothing is in scope.
            Some(scope([12])),
        ];
        for (expr, pred) in &cases {
            for scope in &scopes {
                let (result, covered) = run_scoped(&dataset, expr.clone(), scope.as_ref()).await;
                assert!(result.is_exact(), "{expr}");
                check_confined(&result, &rows, scope.as_ref(), pred.as_ref());
                if let Some(scope) = scope {
                    assert!(covered.is_subset(scope), "{expr} covered {covered:?}");
                }
            }
        }

        // Form: the allow list of `ordered < 47` over fragment 4 names only that
        // fragment; its complement over fragments 3..5 is an allow list too,
        // holding exactly the scoped rows that are not below 47, and neither
        // admits an address of a fragment the snapshot never had.
        let foreign: u64 = RowAddress::new_from_parts(99, 0).into();
        let (result, _) = run_scoped(&dataset, below("ordered", u(47)), Some(&scope([4]))).await;
        let allow = result.lower.allow_list().unwrap();
        assert!(allow.iter().all(|(fragment, _)| *fragment == 4));
        assert_eq!(allow.len(), Some(7));
        assert!(!result.lower.selected(foreign));
        let (result, _) = run_scoped(
            &dataset,
            not(below("ordered", u(47))),
            Some(&scope([3, 4, 5])),
        )
        .await;
        let allow = result.lower.allow_list().unwrap();
        assert!(
            allow.get(&3).is_none(),
            "every row of fragment 3 is below 47"
        );
        assert_eq!(allow.get_fragment_bitmap(4).unwrap().len(), 3);
        assert!(allow.get(&5).is_some());
        assert_eq!(allow.len(), Some(13));
        assert!(!result.lower.selected(foreign) && !result.upper.selected(foreign));
        // Without a scope the complement admits every other fragment, and
        // addresses outside the snapshot as well: the node is unchanged there.
        let (result, _) = run_scoped(&dataset, not(below("ordered", u(47))), None).await;
        assert!(
            result
                .lower
                .selected(RowAddress::new_from_parts(9, 5).into())
        );
        assert!(result.lower.selected(foreign));
        // An empty scope is an exact, empty allow list.
        let (result, _) =
            run_scoped(&dataset, not(below("ordered", u(47))), Some(&scope([]))).await;
        assert!(result.is_exact());
        assert!(
            result
                .lower
                .allow_list()
                .is_some_and(|allow| allow.is_empty())
        );
    }

    /// Inexact results are confined on both bounds and stay inexact.
    #[tokio::test]
    async fn fragment_scope_confines_inexact_results() {
        let TestFixture {
            dataset,
            _tmp_dir_guard,
        } = test_fixture().await;
        let mut dataset = Arc::try_unwrap(dataset).unwrap();
        dataset
            .create_index(
                &["ordered"],
                IndexType::ZoneMap,
                Some("ordered_zm".to_string()),
                &ScalarIndexParams::for_builtin(lance_index::scalar::BuiltinIndexType::ZoneMap),
                true,
            )
            .await
            .unwrap();
        let dataset = Arc::new(dataset);
        let rows = rows_of(&dataset, &["ordered"]).await;
        let zone_below = |v: u64| {
            ScalarIndexExpr::Query(ScalarIndexSearch {
                column: "ordered".to_string(),
                index_name: "ordered_zm".to_string(),
                index_type: "ZoneMap".to_string(),
                query: Arc::new(SargableQuery::Range(
                    Bound::Unbounded,
                    Bound::Excluded(ScalarValue::UInt64(Some(v))),
                )),
                needs_recheck: true,
                fragment_bitmap: None,
            })
        };
        let lt47 = |row: &[Option<i64>]| row[0].map(|x| x < 47);
        for scope in [None, Some(scope([4, 5])), Some(scope([8]))] {
            let (result, _) = run_scoped(&dataset, zone_below(47), scope.as_ref()).await;
            check_confined(&result, &rows, scope.as_ref(), &lt47);
            let (result, _) = run_scoped(&dataset, not(zone_below(47)), scope.as_ref()).await;
            check_confined(&result, &rows, scope.as_ref(), &|r| not3(lt47(r)));
        }
        // Over fragments 4 and 5 the candidate set is fragment 4 whole (its zone
        // starts below 47; fragment 5's does not), the lower bound stays empty.
        let (result, _) = run_scoped(&dataset, zone_below(47), Some(&scope([4, 5]))).await;
        assert!(!result.is_exact());
        assert!(
            result
                .lower
                .allow_list()
                .is_some_and(|allow| allow.is_empty())
        );
        assert_eq!(result.upper.allow_list().unwrap().len(), Some(10));
        // A scope the candidates miss entirely: neither bound admits a row.
        let (result, _) = run_scoped(&dataset, zone_below(47), Some(&scope([8]))).await;
        assert!(
            result
                .lower
                .allow_list()
                .is_some_and(|allow| allow.is_empty())
        );
        for (row_id, _, _) in &rows {
            assert!(!result.upper.selected(*row_id));
        }
    }

    /// Two indexes under a negation with NULL operands: NULL rows never match,
    /// in or out of scope, and the complement stays inside the scope.
    #[tokio::test]
    async fn fragment_scope_confines_multi_index_negation_with_nulls() {
        let test_dir = TempStrDir::default();
        // `a` is NULL on every fifth row and throughout fragment 2 (rows 20..29).
        let a = Int32Array::from_iter(
            (0..40).map(|i| (i % 5 != 0 && !(20..30).contains(&i)).then_some(i % 7)),
        );
        let b = Int32Array::from_iter((0..40).map(|i| (i % 13 != 0).then_some(i % 11)));
        let batch = RecordBatch::try_from_iter(vec![
            ("a", Arc::new(a) as ArrayRef),
            ("b", Arc::new(b) as ArrayRef),
        ])
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let mut dataset = Dataset::write(
            reader,
            test_dir.as_str(),
            Some(WriteParams {
                max_rows_per_file: 10,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        for column in ["a", "b"] {
            dataset
                .create_index(
                    &[column],
                    IndexType::BTree,
                    None,
                    &ScalarIndexParams::default(),
                    true,
                )
                .await
                .unwrap();
        }
        let dataset = Arc::new(dataset);
        assert_eq!(dataset.fragments().len(), 4);
        let rows = rows_of(&dataset, &["a", "b"]).await;
        let eq = |column: &str, v: i32| {
            search(
                column,
                "BTree",
                SargableQuery::Equals(ScalarValue::Int32(Some(v))),
            )
        };
        let eq3 = |x: Option<i64>, v: i64| x.map(|x| x == v);
        let is_null = |column: &str| search(column, "BTree", SargableQuery::IsNull());
        let cases: Vec<(ScalarIndexExpr, Pred)> = vec![
            (is_null("a"), Box::new(|r| Some(r[0].is_none()))),
            (not(is_null("a")), Box::new(|r| not3(Some(r[0].is_none())))),
            (
                and(is_null("a"), eq("b", 7)),
                Box::new(move |r| and3(Some(r[0].is_none()), eq3(r[1], 7))),
            ),
            (
                not(or(eq("a", 5), eq("b", 7))),
                Box::new(move |r| not3(or3(eq3(r[0], 5), eq3(r[1], 7)))),
            ),
            (
                and(not(eq("a", 5)), eq("b", 7)),
                Box::new(move |r| and3(not3(eq3(r[0], 5)), eq3(r[1], 7))),
            ),
            (
                or(eq("a", 5), not(eq("b", 7))),
                Box::new(move |r| or3(eq3(r[0], 5), not3(eq3(r[1], 7)))),
            ),
        ];
        let mut matched = std::collections::HashMap::new();
        for (expr, pred) in &cases {
            for scope in [
                None,
                Some(scope([1])),
                Some(scope([2])),
                Some(scope([0, 2])),
                Some(scope([])),
            ] {
                let (result, _) = run_scoped(&dataset, expr.clone(), scope.as_ref()).await;
                assert!(result.is_exact(), "{expr}");
                let matches = check_confined(&result, &rows, scope.as_ref(), pred.as_ref());
                matched.insert((expr.to_string(), format!("{scope:?}")), matches);
            }
        }
        let key = |expr: &ScalarIndexExpr, scope: RoaringBitmap| {
            (expr.to_string(), format!("{:?}", Some(scope)))
        };
        // Not vacuous: the all-null fragment is selected whole by IS NULL and
        // not at all by its negation, and the other cases keep rows in scope.
        assert_eq!(matched[&key(&is_null("a"), scope([2]))], 10);
        assert_eq!(matched[&key(&not(is_null("a")), scope([2]))], 0);
        for (expr, _) in &cases {
            assert!(
                matched[&key(expr, scope([0, 2]))] > 0,
                "{expr} over fragments 0 and 2 should keep some rows"
            );
        }
        // A NULL operand in scope: `a` is NULL at row 10 (fragment 1) and `b` is
        // 10 there, so NOT (a = 5 OR b = 7) is NULL and the row is not selected.
        let (result, _) =
            run_scoped(&dataset, not(or(eq("a", 5), eq("b", 7))), Some(&scope([1]))).await;
        assert!(
            !result
                .lower
                .selected(RowAddress::new_from_parts(1, 0).into())
        );
        assert!(result.lower.allow_list().is_some());
    }

    /// A fragment withdrawn from the snapshot (deleted whole) still has its
    /// rows in the index until a remap; the output universe is the scoped
    /// fragments of the snapshot, so those rows never appear, not even when
    /// the scope names every remaining fragment. Without a scope the node is
    /// unchanged and still names them.
    #[tokio::test]
    async fn fragment_scope_excludes_withdrawn_fragments() {
        let TestFixture {
            dataset,
            _tmp_dir_guard,
        } = test_fixture().await;
        let mut dataset = Arc::try_unwrap(dataset).unwrap();
        dataset.delete("ordered < 10").await.unwrap();
        let remaining: Vec<u32> = dataset.fragments().iter().map(|f| f.id as u32).collect();
        assert_eq!(remaining, (1..10).collect::<Vec<_>>());
        let dataset = Arc::new(dataset);
        let rows = rows_of(&dataset, &["ordered"]).await;
        let withdrawn: u64 = RowAddress::new_from_parts(0, 7).into();
        let u = |v: u64| ScalarValue::UInt64(Some(v));
        let lt5 = |row: &[Option<i64>]| row[0].map(|x| x < 5);
        for scope in [Some(scope([1, 2])), Some(scope(remaining.iter().copied()))] {
            let (result, _) =
                run_scoped(&dataset, not(below("ordered", u(5))), scope.as_ref()).await;
            check_confined(&result, &rows, scope.as_ref(), &|r| not3(lt5(r)));
            assert!(!result.lower.selected(withdrawn), "{scope:?}");
            assert!(
                result
                    .lower
                    .selected(RowAddress::new_from_parts(1, 0).into())
            );
        }
        let (result, _) = run_scoped(&dataset, not(below("ordered", u(5))), None).await;
        assert!(result.lower.selected(withdrawn));
    }

    /// Stable row ids: masks carry row ids, so the scope is translated through the
    /// row-id sequences of whichever side is smaller; after a delete and a
    /// compaction the scope names the new fragments while the ids stay.
    #[tokio::test]
    async fn fragment_scope_confines_stable_row_id_results() {
        let test_dir = TempStrDir::default();
        // `n` is NULL on every third row and throughout fragment 5 (rows 25..29).
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from((0..30).collect::<Vec<_>>())) as ArrayRef,
            ),
            (
                "n",
                Arc::new(Int32Array::from_iter(
                    (0..30).map(|i| (i % 3 != 0 && !(25..30).contains(&i)).then_some(i)),
                )) as ArrayRef,
            ),
        ])
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        let mut dataset = Dataset::write(
            reader,
            test_dir.as_str(),
            Some(WriteParams {
                enable_stable_row_ids: true,
                max_rows_per_file: 5,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        for column in ["id", "n"] {
            dataset
                .create_index(
                    &[column],
                    IndexType::BTree,
                    None,
                    &ScalarIndexParams::default(),
                    true,
                )
                .await
                .unwrap();
        }
        assert_eq!(dataset.fragments().len(), 6);
        let i = |v: i32| ScalarValue::Int32(Some(v));
        let lt = |v: i64| move |row: &[Option<i64>]| row[0].map(|x| x < v);
        let n_is_null = || search("n", "BTree", SargableQuery::IsNull());
        let cases: Vec<(ScalarIndexExpr, Pred)> = vec![
            (below("id", i(12)), Box::new(lt(12))),
            (not(below("id", i(12))), Box::new(move |r| not3(lt(12)(r)))),
            (
                or(below("id", i(4)), not(below("id", i(26)))),
                Box::new(move |r| or3(lt(4)(r), not3(lt(26)(r)))),
            ),
            (n_is_null(), Box::new(|r| Some(r[1].is_none()))),
            (not(n_is_null()), Box::new(|r| not3(Some(r[1].is_none())))),
        ];
        let check_all = |dataset: Arc<Dataset>, scopes: Vec<Option<RoaringBitmap>>| {
            let cases = &cases;
            async move {
                let rows = rows_of(&dataset, &["id", "n"]).await;
                // Row ids and addresses differ for every fragment but the first.
                assert!(rows.iter().any(|(id, addr, _)| *id != u64::from(*addr)));
                for (expr, pred) in cases {
                    for scope in &scopes {
                        let (result, _) = run_scoped(&dataset, expr.clone(), scope.as_ref()).await;
                        assert!(result.is_exact(), "{expr}");
                        check_confined(&result, &rows, scope.as_ref(), pred.as_ref());
                    }
                }
            }
        };
        let dataset = Arc::new(dataset);
        check_all(
            dataset.clone(),
            vec![
                None,
                // A small scope, a large one, the all-null fragment, nothing.
                Some(scope([1, 2])),
                Some(scope([0, 1, 2, 3, 4])),
                Some(scope([5])),
                Some(scope([])),
            ],
        )
        .await;
        // Complements come out as allow lists of translated row ids, whatever
        // the scope's size, and never name a row id outside the snapshot.
        for fragments in [vec![0, 1, 2, 3, 4], vec![2]] {
            let (result, _) =
                run_scoped(&dataset, not(below("id", i(12))), Some(&scope(fragments))).await;
            assert!(result.lower.allow_list().is_some());
            assert!(!result.lower.selected(10_000));
        }

        let mut dataset = Arc::try_unwrap(dataset).unwrap();
        dataset.delete("id % 4 = 0").await.unwrap();
        compact_files(
            &mut dataset,
            CompactionOptions {
                target_rows_per_fragment: 10,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        let fragments: Vec<u32> = dataset.fragments().iter().map(|f| f.id as u32).collect();
        assert!(fragments.iter().all(|id| *id >= 6), "{fragments:?}");
        let dataset = Arc::new(dataset);
        check_all(
            dataset.clone(),
            vec![
                None,
                Some(scope([fragments[0]])),
                Some(scope(fragments[1..].iter().copied())),
            ],
        )
        .await;
        // Row ids of the deleted rows (id % 4 = 0, stable ids equal the ids
        // here) are gone from every sequence and so from every scoped result.
        for fragments in [vec![fragments[0]], fragments.clone()] {
            let (result, _) =
                run_scoped(&dataset, not(below("id", i(12))), Some(&scope(fragments))).await;
            for deleted in [12u64, 16, 20, 24, 28] {
                assert!(!result.lower.selected(deleted));
            }
        }
    }

    /// Two index segments with disjoint coverage: a scope inside one, the other,
    /// or both, is honoured by the loader and by the emitted masks.
    #[tokio::test]
    async fn fragment_scope_spans_index_segments() {
        let TestFixture {
            dataset,
            _tmp_dir_guard,
        } = test_fixture().await;
        let mut dataset = Arc::try_unwrap(dataset).unwrap();
        let more = gen_batch()
            .col(
                "ordered",
                lance_datagen::array::step_custom::<UInt64Type>(100, 1),
            )
            .into_reader_rows(
                lance_datagen::RowCount::from(100),
                lance_datagen::BatchCount::from(1),
            );
        dataset
            .append(
                more,
                Some(WriteParams {
                    max_rows_per_file: 10,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        dataset
            .optimize_indices(&lance_index::optimize::OptimizeOptions::append())
            .await
            .unwrap();
        let segments = dataset
            .load_indices()
            .await
            .unwrap()
            .iter()
            .filter(|index| index.name == "ordered_idx")
            .count();
        assert_eq!(segments, 2);
        assert_eq!(dataset.fragments().len(), 20);
        let dataset = Arc::new(dataset);
        let rows = rows_of(&dataset, &["ordered"]).await;
        let u = |v: u64| ScalarValue::UInt64(Some(v));
        let lt150 = |row: &[Option<i64>]| row[0].map(|x| x < 150);
        for scope in [
            None,
            Some(scope([5, 15])),
            Some(scope([15])),
            Some(scope([5])),
            Some(scope(10..20)),
        ] {
            let (result, covered) =
                run_scoped(&dataset, below("ordered", u(150)), scope.as_ref()).await;
            check_confined(&result, &rows, scope.as_ref(), &lt150);
            let (result, _) =
                run_scoped(&dataset, not(below("ordered", u(150))), scope.as_ref()).await;
            check_confined(&result, &rows, scope.as_ref(), &|r| not3(lt150(r)));
            if let Some(scope) = scope {
                assert_eq!(covered, scope);
            }
        }
    }
}
