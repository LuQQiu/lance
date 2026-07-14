// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Build a distributed IVF-PQ index with shared centroids.
//!
//! Creates a small test dataset and builds a 2-segment distributed index
//! following the pattern from lance PR #6220.
//!
//! Run with: `cargo run --release --example distributed_index_build`

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow::array::{Array, AsArray, FixedSizeListArray, FixedSizeListBuilder, Float32Builder, Int64Array, RecordBatch, RecordBatchIterator};
use arrow::datatypes::{DataType, Field, Schema};
use lance::Dataset;
use lance_arrow::FixedSizeListArrayExt;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::kmeans::{train_kmeans, KMeansParams};
// use lance_index::vector::pq::PQBuildParams; // Not needed for IVF-FLAT
use lance_index::{DatasetIndexExt, IndexType};
use lance_linalg::distance::MetricType;

const ROOT: &str = "/tmp/lance-multi-segment-test";
const TABLE_NAME: &str = "vectors_100k";
const DIM: usize = 384;
const N_ROWS: usize = 100_000;
const BATCH_SIZE: usize = 1_000; // 100 fragments
const N_PARTITIONS: usize = 256;
// const NUM_SUB_VECTORS: usize = 48; // Not needed for IVF-FLAT
const STAGING_UUID: &str = "00000000-0000-0000-0000-000000000001"; // Valid UUID format

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    println!("============================================================");
    println!("DISTRIBUTED INDEX BUILD");
    println!("============================================================");

    let uri = format!("{}/{}.lance", ROOT, TABLE_NAME);

    // Step 1: Create dataset with 100 fragments
    println!("\n[1/5] Creating {} vectors ({} batches)...", N_ROWS, N_ROWS / BATCH_SIZE);
    create_dataset(&uri).await?;

    let mut dataset = Dataset::open(&uri).await?;
    let fragments = dataset.get_fragments();
    println!("  Dataset has {} fragments", fragments.len());

    // Step 2: Train centroids on sample data
    println!("\n[2/5] Training {} centroids...", N_PARTITIONS);
    let centroids = train_centroids(&dataset).await?;
    println!("  Centroids shape: {} x {}", centroids.len(), DIM);

    let ivf_params = IvfBuildParams::try_with_centroids(N_PARTITIONS, centroids)?;

    // Step 3: Build partial shards (2 workers)
    println!("\n[3/5] Building partial shards...");

    // Worker 1: fragments 0-49
    println!("  Worker 1: building shard for fragments 0-49...");
    let worker1_fragments: Vec<u32> = fragments[0..50].iter().map(|f| f.id() as u32).collect();

    // Use IVF-FLAT for simpler distributed indexing (no PQ codebook needed)
    let vector_params = lance::index::vector::VectorIndexParams::with_ivf_flat_params(
        MetricType::L2,
        ivf_params.clone(),
    );

    let shard1_metadata = dataset
        .create_index_builder(&["vector"], IndexType::Vector, &vector_params)
        .index_uuid(STAGING_UUID.to_string())
        .fragments(worker1_fragments)
        // train=true but centroids provided, so it will use them instead of training
        .execute_uncommitted()
        .await?;

    println!("    Shard 1 UUID: {}", shard1_metadata.uuid);
    println!("    Fragment bitmap size: {}", shard1_metadata.fragment_bitmap.as_ref().map(|b| b.len()).unwrap_or(0));

    // Worker 2: fragments 50-99
    println!("  Worker 2: building shard for fragments 50-99...");
    let worker2_fragments: Vec<u32> = fragments[50..].iter().map(|f| f.id() as u32).collect();

    // Use IVF-FLAT for simpler distributed indexing (no PQ codebook needed)
    let vector_params2 = lance::index::vector::VectorIndexParams::with_ivf_flat_params(
        MetricType::L2,
        ivf_params,
    );

    let shard2_metadata = dataset
        .create_index_builder(&["vector"], IndexType::Vector, &vector_params2)
        .index_uuid(STAGING_UUID.to_string())
        .fragments(worker2_fragments)
        // train=true but centroids provided, so it will use them instead of training
        .execute_uncommitted()
        .await?;

    println!("    Shard 2 UUID: {}", shard2_metadata.uuid);
    println!("    Fragment bitmap size: {}", shard2_metadata.fragment_bitmap.as_ref().map(|b| b.len()).unwrap_or(0));

    // Step 4: Plan and build segments
    println!("\n[4/5] Planning and building segments...");

    let segment_builder = dataset.create_index_segment_builder(STAGING_UUID.to_string());
    let segment_builder = segment_builder
        .with_partial_indices(vec![shard1_metadata, shard2_metadata]);

    let plans = segment_builder.plan().await?;
    println!("  Created {} segment plan(s)", plans.len());

    for (i, plan) in plans.iter().enumerate() {
        println!("    Plan {}: {} partial indices", i, plan.partial_indices().len());
    }

    println!("  Building segments...");
    let segments = segment_builder.build_all().await?;
    println!("  Built {} segment(s)", segments.len());

    for (i, seg) in segments.iter().enumerate() {
        println!("    Segment {}: UUID={}", i, seg.uuid());
    }

    // Step 5: Commit
    println!("\n[5/5] Committing index...");

    dataset
        .commit_existing_index_segments("vector_idx", "vector", segments)
        .await?;

    println!("\n✓ Index committed successfully");

    // Verify
    println!("\nVerifying index...");
    let indices = dataset.load_indices_by_name("vector_idx").await?;
    println!("  Loaded {} index segment(s):", indices.len());
    for idx in indices {
        println!("    - UUID: {}, name: {}", idx.uuid, idx.name);
    }

    println!("\n============================================================");
    println!("INDEX BUILD COMPLETE");
    println!("Dataset location: {}", uri);
    println!("============================================================");

    Ok(())
}

async fn create_dataset(uri: &str) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            false,
        ),
    ]));

    // Write each batch separately to create separate fragments
    for batch_idx in 0..(N_ROWS / BATCH_SIZE) {
        let start_id = batch_idx * BATCH_SIZE;
        let ids: Int64Array = (start_id..(start_id + BATCH_SIZE))
            .map(|i| i as i64)
            .collect();

        // Build vector column using FixedSizeListBuilder
        let v_builder = Float32Builder::new();
        let mut list_builder = FixedSizeListBuilder::new(v_builder, DIM as i32);

        for _ in 0..BATCH_SIZE {
            for _ in 0..DIM {
                list_builder.values().append_value(rand::random::<f32>());
            }
            list_builder.append(true);
        }
        let vectors = list_builder.finish();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ids), Arc::new(vectors)],
        )?;

        // Write each batch separately
        let batch_iter = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema.clone());

        if batch_idx == 0 {
            // First batch: create dataset
            Dataset::write(batch_iter, uri, None).await?;
        } else {
            // Subsequent batches: append to dataset
            let mut dataset = Dataset::open(uri).await?;
            dataset.append(batch_iter, None).await?;
        }

        if (batch_idx + 1) % 10 == 0 {
            println!("  Written {} batches ({} fragments)", batch_idx + 1, batch_idx + 1);
        }
    }

    Ok(())
}

async fn train_centroids(
    dataset: &Dataset,
) -> Result<Arc<FixedSizeListArray>, Box<dyn std::error::Error>> {
    // Sample 10K vectors for training
    let sample_size = 10_000;
    let sample_indices: Vec<_> = (0..sample_size).collect();

    let projection = lance::dataset::ProjectionRequest::from_columns(["vector"], dataset.schema());
    let sample_batch = dataset.take(&sample_indices, projection).await?;

    let vector_col = sample_batch
        .column(0)
        .as_fixed_size_list()
        .values()
        .as_primitive::<arrow::datatypes::Float32Type>();

    // Train KMeans
    let kmeans = train_kmeans(
        vector_col,
        KMeansParams::default(),
        DIM,
        N_PARTITIONS,
        256, // sample_rate
    )?;

    // KMeans returns flattened centroids (N_PARTITIONS * DIM floats)
    // We need to reshape to FixedSizeListArray (N_PARTITIONS rows of DIM floats each)
    let flat_centroids = kmeans.centroids
        .as_any()
        .downcast_ref::<arrow::array::Float32Array>()
        .ok_or("Failed to downcast centroids to Float32Array")?;

    let centroids = FixedSizeListArray::try_new_from_values(
        flat_centroids.clone(),
        DIM as i32,
    )?;

    Ok(Arc::new(centroids))
}
