// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Planning-time fragment pruning from index-held exclusion proofs.
//!
//! Given a query filter, computes the set of fragments that provably contain
//! no matching row, by consulting every index whose plugin advertises the
//! [`FragmentPruner`] capability on the filtered columns. The resolution is
//! subtractive: a fragment is only removed from a scan on proof, so fragments
//! without usable index records (or with stale coverage) always stay
//! candidates. Which index types participate is a plugin property; this
//! module knows none of them.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::logical_expr::Expr;
use lance_core::Result;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::expression::{
    IndexInformationProvider, MultiQueryParser, SargableQueryParser, ScalarIndexExpr,
    apply_scalar_indices,
};
use lance_table::format::IndexMetadata;
use lance_table::format::overlay::staleness::{collect_overlay_stale_frags, overlaid_fragments};
use roaring::RoaringBitmap;

use super::Dataset;
use crate::index::DatasetIndexExt;
use crate::index::DatasetIndexInternalExt;
use crate::index::scalar::SCALAR_INDEX_PLUGIN_REGISTRY;

struct PruningIndexInfo {
    columns: HashMap<String, (DataType, MultiQueryParser)>,
}

impl IndexInformationProvider for PruningIndexInfo {
    fn get_index(&self, col: &str) -> Option<(&DataType, &MultiQueryParser)> {
        self.columns
            .get(col)
            .map(|(data_type, parser)| (data_type, parser))
    }
}

/// One opened pruning-capable segment plus the fragments whose records are
/// invalidated by newer column overlays (validity: same fragment id, changed
/// column data means the old record must not prune).
struct LoadedSegment {
    index: Arc<dyn lance_index::scalar::ScalarIndex>,
    /// The segment's claimed coverage. Records outside it are coverage that
    /// central maintenance withdrew without rewriting the file (in-place
    /// column rewrites) and must never prune.
    coverage: RoaringBitmap,
    stale_fragments: RoaringBitmap,
}

/// Compute the fragments provably excluded by `full_expr`.
///
/// Returns an empty bitmap when no pruning-capable index applies. Only
/// fragments with valid, covered index records can ever appear in the result.
pub(crate) async fn index_pruned_fragments(
    dataset: &Dataset,
    full_expr: &Expr,
) -> Result<RoaringBitmap> {
    let indices = dataset.load_indices().await?;
    // column -> pruning-capable index segments for that column
    let mut segments_by_column: HashMap<String, Vec<IndexMetadata>> = HashMap::new();
    let mut info = PruningIndexInfo {
        columns: HashMap::new(),
    };
    for index in indices.iter() {
        let Some(details) = &index.index_details else {
            continue;
        };
        let Ok(plugin) = SCALAR_INDEX_PLUGIN_REGISTRY.get_plugin_by_details(details) else {
            continue;
        };
        if !plugin.supports_fragment_pruning(details) {
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
        let display_type = plugin.name().to_string();
        info.columns.entry(field_path.clone()).or_insert_with(|| {
            (
                data_type,
                MultiQueryParser::single(Box::new(SargableQueryParser::new(
                    index.name.clone(),
                    display_type,
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

    // Reuse the shared predicate decomposition so the pruning sees exactly
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
            if index.as_fragment_pruner().is_none() {
                continue;
            }
            // No bitmap means unknown coverage: the segment proves nothing.
            let Some(coverage) = meta.fragment_bitmap.clone() else {
                continue;
            };
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
                coverage,
                stale_fragments,
            });
        }
        loaded.insert(column, loaded_segments);
    }

    excluded_by_expr(&query_tree, &loaded)
}

impl Dataset {
    /// Of `fragment_ids`, the fragments that may contain rows matching
    /// `filter` (a SQL predicate), in input order.
    ///
    /// Drops only fragments whose valid index records PROVE no live row can
    /// match. Fragments without records, with stale or withdrawn coverage,
    /// or under unsupported predicates are always retained: the result is a
    /// candidate set, not a match set, and every returned fragment still
    /// needs predicate evaluation.
    pub async fn candidate_fragments(
        &self,
        fragment_ids: impl IntoIterator<Item = u64>,
        filter: &str,
    ) -> Result<Vec<u64>> {
        let schema = Arc::new(arrow_schema::Schema::from(self.schema()));
        let planner = lance_datafusion::planner::Planner::new(schema);
        let expr = planner.parse_filter(filter)?;
        let excluded = index_pruned_fragments(self, &expr).await?;
        Ok(fragment_ids
            .into_iter()
            .filter(|id| u32::try_from(*id).map_or(true, |id32| !excluded.contains(id32)))
            .collect())
    }
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
            let Some(segments) = loaded.get(&search.column) else {
                return Ok(RoaringBitmap::new());
            };
            let mut excluded = RoaringBitmap::new();
            for segment in segments {
                let Some(pruner) = segment.index.as_fragment_pruner() else {
                    continue;
                };
                let mut segment_excluded = pruner.excluded_fragments(search.query.as_ref())?;
                segment_excluded &= &segment.coverage;
                segment_excluded -= &segment.stale_fragments;
                excluded |= segment_excluded;
            }
            Ok(excluded)
        }
    }
}
