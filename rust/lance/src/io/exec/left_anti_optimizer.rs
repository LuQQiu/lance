// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! LeftAnti optimizer
//!
//! This optimizer converts LeftAnti joins to RightAnti with swapped streams.
//! This ensures the right side (typically smaller exclusion set) becomes the 
//! build side, significantly reducing memory usage.
//!
//! Pattern: HashJoin(LeftAnti, large_left, small_right)  
//! Becomes: HashJoin(RightAnti, small_right, large_left)
//!
//! This is critical for exclusion patterns like:
//! SELECT * FROM large_table WHERE id NOT IN (SELECT id FROM small_exclusion_list)

use std::sync::Arc;

use datafusion::common::Result as DFResult;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::JoinType;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::ExecutionPlan;

/// Optimizer that converts LeftAnti to RightAnti with swapped streams
///
/// Since DataFusion always uses LEFT as build side:
/// - LeftAnti(large, small) builds hash table from large (BAD!)
/// - RightAnti(small, large) builds hash table from small (GOOD!)
///
/// This optimizer always converts LeftAnti to RightAnti to ensure
/// the smaller exclusion set is on the build side.
#[derive(Debug, Clone, Default)]
pub struct LeftAntiOptimizer {}

impl LeftAntiOptimizer {
    pub fn new() -> Self {
        Self {}
    }

    /// Convert LeftAnti to RightAnti with swapped streams
    fn optimize_left_anti(&self, plan: Arc<dyn ExecutionPlan>) -> DFResult<Arc<dyn ExecutionPlan>> {
        let hash_join = plan.as_any().downcast_ref::<HashJoinExec>().unwrap();
        
        // Get the left and right inputs
        let left = hash_join.left();
        let right = hash_join.right();
        
        log::info!(
            "LeftAntiOptimizer: Converting LeftAnti to RightAnti with swapped streams"
        );

        // Convert LeftAnti to RightAnti and swap the streams
        // This makes RIGHT the build side (which is now the original LEFT)
        let swapped = HashJoinExec::try_new(
            Arc::clone(right),  // Right becomes left (build side)
            Arc::clone(left),   // Left becomes right (probe side)
            hash_join.on().iter().map(|(l, r)| {
                // Swap the on columns as well
                (Arc::clone(r), Arc::clone(l))
            }).collect(),
            hash_join.filter().clone(),
            &JoinType::RightAnti,  // Always RightAnti
            hash_join.projection().map(|p| p.to_vec()),
            hash_join.partition_mode().clone(),
            hash_join.null_equals_null(),
        )?;

        Ok(Arc::new(swapped))
    }
}

impl PhysicalOptimizerRule for LeftAntiOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        log::debug!("LeftAntiOptimizer called");
        
        // Check if this is a LeftAnti join
        if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
            if hash_join.join_type() == &JoinType::LeftAnti {
                let optimized = self.optimize_left_anti(Arc::clone(&plan))?;
                
                // Recursively optimize children of the swapped join
                let children = optimized.children();
                if !children.is_empty() {
                    let new_children: DFResult<Vec<_>> = children
                        .into_iter()
                        .map(|child| self.optimize(Arc::clone(child), _config))
                        .collect();
                    return optimized.with_new_children(new_children?);
                }
                
                return Ok(optimized);
            }
        }
        
        // Recursively optimize children
        let children = plan.children();
        if children.is_empty() {
            return Ok(plan);
        }

        let new_children: DFResult<Vec<_>> = children
            .into_iter()
            .map(|child| self.optimize(Arc::clone(child), _config))
            .collect();

        plan.with_new_children(new_children?)
    }

    fn name(&self) -> &str {
        "left_anti_optimizer"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
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
                true,
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_left_anti_gets_converted_to_right_anti() {
        let join = create_test_join(JoinType::LeftAnti);
        
        let optimizer = LeftAntiOptimizer::new();
        let config = ConfigOptions::default();
        let optimized = optimizer.optimize(join, &config).unwrap();
        
        // Check that it was converted to RightAnti
        if let Some(hash_join) = optimized.as_any().downcast_ref::<HashJoinExec>() {
            assert_eq!(
                hash_join.join_type(),
                &JoinType::RightAnti,
                "LeftAnti should be converted to RightAnti"
            );
        } else {
            panic!("Expected HashJoinExec");
        }
    }

    #[test]
    fn test_right_anti_not_affected() {
        let join = create_test_join(JoinType::RightAnti);
        
        let optimizer = LeftAntiOptimizer::new();
        let config = ConfigOptions::default();
        let optimized = optimizer.optimize(join.clone(), &config).unwrap();
        
        // Check that RightAnti remains unchanged
        if let Some(hash_join) = optimized.as_any().downcast_ref::<HashJoinExec>() {
            assert_eq!(
                hash_join.join_type(),
                &JoinType::RightAnti,
                "RightAnti should remain unchanged"
            );
        }
        
        // Verify it's the same object (no modification)
        assert!(Arc::ptr_eq(&optimized, &join));
    }

    #[test]
    fn test_inner_join_not_affected() {
        let join = create_test_join(JoinType::Inner);
        
        let optimizer = LeftAntiOptimizer::new();
        let config = ConfigOptions::default();
        let optimized = optimizer.optimize(join.clone(), &config).unwrap();
        
        // Check that Inner join remains unchanged
        if let Some(hash_join) = optimized.as_any().downcast_ref::<HashJoinExec>() {
            assert_eq!(
                hash_join.join_type(),
                &JoinType::Inner,
                "Inner join should remain unchanged"
            );
        }
        
        // Verify it's the same object (no modification)
        assert!(Arc::ptr_eq(&optimized, &join));
    }

    #[test]
    fn test_left_semi_not_affected() {
        let join = create_test_join(JoinType::LeftSemi);
        
        let optimizer = LeftAntiOptimizer::new();
        let config = ConfigOptions::default();
        let optimized = optimizer.optimize(join.clone(), &config).unwrap();
        
        // Check that LeftSemi remains unchanged
        if let Some(hash_join) = optimized.as_any().downcast_ref::<HashJoinExec>() {
            assert_eq!(
                hash_join.join_type(),
                &JoinType::LeftSemi,
                "LeftSemi should remain unchanged"
            );
        }
        
        // Verify it's the same object (no modification)
        assert!(Arc::ptr_eq(&optimized, &join));
    }
}