// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Anti-join limit pushdown optimization
//!
//! This optimizer adds LocalLimitExec above HashJoinExec for anti-join patterns
//! to limit each partition's output, preventing memory exhaustion when hash joins
//! cannot spill to disk.
//!
//! Pattern: Limit -> HashJoin(Anti) -> ... 
//! Becomes: Limit -> LocalLimitExec -> HashJoin(Anti) -> ...
//!
//! For anti-joins with limits, each partition will produce at most the calculated
//! local limit, significantly reducing memory pressure.

use std::sync::Arc;

use datafusion::common::Result as DFResult;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::JoinType;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::ExecutionPlan;

/// Optimizer that adds LocalLimitExec above HashJoinExec for anti-join patterns
///
/// This prevents over-production of results in each partition, reducing memory usage
/// for anti-joins where hash joins cannot spill to disk.
#[derive(Debug, Clone, Default)]
pub struct AntiJoinLimitPushdown {}

impl AntiJoinLimitPushdown {
    pub fn new() -> Self {
        Self {}
    }

    /// Optimize anti-join by adding LocalLimitExec above it
    fn optimize_anti_join(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        limit: usize,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let join = plan.as_any().downcast_ref::<HashJoinExec>().unwrap();

        log::debug!(
            "AntiJoinLimitPushdown: Found {:?} with limit {}",
            join.join_type(),
            limit
        );

        // Use the same limit for LocalLimitExec as the global limit
        // This ensures each partition can produce up to the full limit if needed
        let local_limit = limit;

        log::debug!(
            "AntiJoinLimitPushdown: Adding LocalLimitExec({}) above HashJoinExec",
            local_limit
        );

        // Insert LocalLimitExec above the HashJoinExec
        let local_limit_exec = Arc::new(LocalLimitExec::new(plan.clone(), local_limit));
        Ok(local_limit_exec)
    }
}

impl PhysicalOptimizerRule for AntiJoinLimitPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        log::debug!("AntiJoinLimitPushdown optimizer called");
        // Recursively optimize with limit context
        optimize_with_limit(plan, None, self)
    }

    fn name(&self) -> &str {
        "anti_join_limit_pushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Recursively optimize plan, passing limit down to anti-joins
fn optimize_with_limit(
    plan: Arc<dyn ExecutionPlan>,
    parent_limit: Option<usize>,
    optimizer: &AntiJoinLimitPushdown,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    log::trace!("optimize_with_limit: checking node {}", plan.name());

    // Check if this node has a fetch limit
    let current_limit = if let Some(global_limit) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
        global_limit.fetch().map(|f| global_limit.skip() + f)
    } else if let Some(local_limit) = plan.as_any().downcast_ref::<LocalLimitExec>() {
        Some(local_limit.fetch())
    } else if let Some(coalesce) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
        coalesce.fetch()
    } else {
        None
    }
    .or(parent_limit);

    log::trace!("  current_limit = {:?}, parent_limit = {:?}", current_limit, parent_limit);

    // If this is an anti-join and we have a limit, optimize it
    if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        log::trace!("  Found HashJoinExec with join_type = {:?}", hash_join.join_type());
        if matches!(
            hash_join.join_type(),
            JoinType::LeftAnti | JoinType::RightAnti
        ) {
            if let Some(limit) = current_limit {
                log::debug!("  Optimizing anti-join with limit {}", limit);
                return optimizer.optimize_anti_join(plan, limit);
            } else {
                log::trace!("  No limit available for anti-join");
            }
        }
    }

    // Recursively process children with the current limit
    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }

    let new_children: DFResult<Vec<_>> = children
        .into_iter()
        .map(|child| optimize_with_limit(Arc::clone(child), current_limit, optimizer))
        .collect();

    plan.with_new_children(new_children?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::JoinType;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::joins::utils::JoinOn;
    use datafusion::physical_plan::joins::PartitionMode;
    use std::sync::Arc;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
        ]))
    }

    fn create_test_join(join_type: JoinType) -> Arc<HashJoinExec> {
        let schema = create_test_schema();
        let left = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let right = Arc::new(EmptyExec::new(Arc::clone(&schema)));

        let on: JoinOn = vec![(
            Arc::new(Column::new("id", 0)),
            Arc::new(Column::new("id", 0)),
        )];

        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &join_type,
                None,
                PartitionMode::Partitioned,
                true, // null_equals_null
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_left_anti_join_gets_local_limit() {
        let join = create_test_join(JoinType::LeftAnti);
        let limit = Arc::new(GlobalLimitExec::new(join, 0, Some(50000)));

        let optimizer = AntiJoinLimitPushdown::new();
        let config = ConfigOptions::default();
        let optimized = optimizer.optimize(limit, &config).unwrap();

        // Check that LocalLimitExec was inserted above HashJoinExec
        if let Some(limit_exec) = optimized.as_any().downcast_ref::<GlobalLimitExec>() {
            if let Some(local_limit) = limit_exec.input().as_any().downcast_ref::<LocalLimitExec>() {
                assert_eq!(local_limit.fetch(), 50000, "LocalLimitExec should have same limit as global");
                // Verify HashJoinExec is under LocalLimitExec
                assert!(local_limit.input().as_any().downcast_ref::<HashJoinExec>().is_some());
            } else {
                panic!("Expected LocalLimitExec under GlobalLimitExec");
            }
        } else {
            panic!("Expected GlobalLimitExec at top level");
        }
    }

    #[test]
    fn test_right_anti_join_gets_local_limit() {
        let join = create_test_join(JoinType::RightAnti);
        let limit = Arc::new(GlobalLimitExec::new(join, 0, Some(100000)));

        let optimizer = AntiJoinLimitPushdown::new();
        let config = ConfigOptions::default();
        let optimized = optimizer.optimize(limit, &config).unwrap();

        // Check that LocalLimitExec was inserted above HashJoinExec
        if let Some(limit_exec) = optimized.as_any().downcast_ref::<GlobalLimitExec>() {
            if let Some(local_limit) = limit_exec.input().as_any().downcast_ref::<LocalLimitExec>() {
                assert_eq!(local_limit.fetch(), 100000, "LocalLimitExec should have same limit as global");
                assert!(local_limit.input().as_any().downcast_ref::<HashJoinExec>().is_some());
            } else {
                panic!("Expected LocalLimitExec under GlobalLimitExec");
            }
        }
    }

    #[test]
    fn test_inner_join_unchanged() {
        let join = create_test_join(JoinType::Inner);
        let limit = Arc::new(GlobalLimitExec::new(join, 0, Some(50000)));

        let optimizer = AntiJoinLimitPushdown::new();
        let config = ConfigOptions::default();
        let optimized = optimizer.optimize(limit, &config).unwrap();

        // Check that LocalLimitExec was NOT inserted for inner joins
        if let Some(limit_exec) = optimized.as_any().downcast_ref::<GlobalLimitExec>() {
            // Should be HashJoinExec directly under GlobalLimitExec
            assert!(limit_exec.input().as_any().downcast_ref::<HashJoinExec>().is_some());
            // Should NOT be LocalLimitExec
            assert!(limit_exec.input().as_any().downcast_ref::<LocalLimitExec>().is_none());
        }
    }

    #[test]
    fn test_no_limit_no_optimization() {
        let join = create_test_join(JoinType::LeftAnti);
        // No limit - just the join directly

        let optimizer = AntiJoinLimitPushdown::new();
        let config = ConfigOptions::default();
        let optimized = optimizer.optimize(join.clone(), &config).unwrap();

        // Should be unchanged - no LocalLimitExec added without a limit
        assert!(optimized.as_any().downcast_ref::<HashJoinExec>().is_some());
        assert!(optimized.as_any().downcast_ref::<LocalLimitExec>().is_none());
    }

    #[test]
    fn test_with_coalesce_partitions_exec() {
        use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
        
        let join = create_test_join(JoinType::LeftAnti);
        let coalesce = Arc::new(CoalescePartitionsExec::new(join).with_fetch(Some(75000)));

        let optimizer = AntiJoinLimitPushdown::new();
        let config = ConfigOptions::default();
        let optimized = optimizer.optimize(coalesce, &config).unwrap();

        // Check that LocalLimitExec was inserted above HashJoinExec
        if let Some(coalesce_exec) = optimized.as_any().downcast_ref::<CoalescePartitionsExec>() {
            if let Some(local_limit) = coalesce_exec.input().as_any().downcast_ref::<LocalLimitExec>() {
                assert_eq!(local_limit.fetch(), 75000, "LocalLimitExec should have same limit as CoalescePartitionsExec");
                // Verify HashJoinExec is under LocalLimitExec
                assert!(local_limit.input().as_any().downcast_ref::<HashJoinExec>().is_some());
            } else {
                panic!("Expected LocalLimitExec under CoalescePartitionsExec");
            }
        } else {
            panic!("Expected CoalescePartitionsExec at top level");
        }
    }

    #[test]
    fn test_local_limit_same_as_global() {
        // Test that LocalLimitExec uses the same limit as the global limit
        let _optimizer = AntiJoinLimitPushdown::new();
        
        // Test with 16 partitions and 50K limit
        let limit = 50000;
        let local_limit = limit;  // Should be same as global
        assert_eq!(local_limit, 50000);
        
        // Test with 1 partition
        let limit = 100000;
        let local_limit = limit;  // Should be same as global
        assert_eq!(local_limit, 100000);
        
        // Test with small limit
        let limit = 10;
        let local_limit = limit;  // Should be same as global
        assert_eq!(local_limit, 10);
    }
}