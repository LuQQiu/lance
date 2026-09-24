// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Fragment column statistics benchmark.
//!
//! Tier 1: real end-to-end scans on random vs clustered layouts, with and
//! without the statistics index, reporting fragments read, rows scanned, and
//! latency, plus the raw before/after analyze_plan output.
//!
//! Tier 2: synthetic per-fragment summaries at increasing fragment counts,
//! reporting resident memory and scope-evaluation CPU.
//!
//! Run: cargo run --release -p lance --example fragstats_bench
//! Env: ROWS (default 1_000_000), FRAG_ROWS (default 10_000), TIER (1|2|both)

use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::WriteParams;
use lance::index::DatasetIndexExt;
use lance_core::utils::tempfile::TempStrDir;
use lance_index::IndexType;
use lance_index::scalar::SargableQuery;
use lance_index::scalar::ScalarIndexParams;
use lance_index::scalar::fragstats::FragmentColumnStatsIndex;
use rand::seq::SliceRandom;
use std::sync::Arc;
use std::time::Instant;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn extract_metric(analyzed: &str, key: &str) -> String {
    // Collect every "key=value" occurrence so multi-node plans stay visible.
    analyzed
        .match_indices(key)
        .map(|(pos, _)| {
            let tail = &analyzed[pos + key.len()..];
            let end = tail
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(tail.len());
            &tail[..end]
        })
        .collect::<Vec<_>>()
        .join("/")
}

async fn build_dataset(uri: &str, rows: usize, frag_rows: usize, clustered: bool) -> Dataset {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "ts",
        DataType::Int64,
        false,
    )]));
    let mut values: Vec<i64> = (0..rows as i64).collect();
    if !clustered {
        let mut rng = rand::rng();
        values.shuffle(&mut rng);
    }
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap();
    let params = WriteParams {
        max_rows_per_file: frag_rows,
        ..Default::default()
    };
    let mut dataset = Dataset::write(
        RecordBatchIterator::new([Ok(batch)], schema),
        uri,
        Some(params),
    )
    .await
    .unwrap();
    dataset
        .create_index(
            &["ts"],
            IndexType::FragmentColumnStats,
            None,
            &ScalarIndexParams::new("FragmentColumnStats".to_string()),
            true,
        )
        .await
        .unwrap();
    dataset
}

async fn run_scan(dataset: &Dataset, filter: &str, use_stats: bool) -> (usize, f64, String) {
    let mut scanner = dataset.scan();
    scanner.filter(filter).unwrap();
    scanner.use_fragment_stats(use_stats);
    let start = Instant::now();
    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    let result_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    let mut scanner = dataset.scan();
    scanner.filter(filter).unwrap();
    scanner.use_fragment_stats(use_stats);
    let analyzed = scanner.analyze_plan().await.unwrap();
    (result_rows, elapsed, analyzed)
}

async fn tier1(rows: usize, frag_rows: usize) {
    println!("\n== Tier 1: end-to-end scans ({rows} rows, {frag_rows} rows/fragment) ==\n");
    println!(
        "| layout | selectivity | stats | result rows | fragments | rows_scanned | latency ms |"
    );
    println!("|---|---|---|---|---|---|---|");
    for clustered in [true, false] {
        let layout = if clustered { "clustered" } else { "random" };
        let dir = TempStrDir::default();
        let dataset = build_dataset(&dir, rows, frag_rows, clustered).await;
        for selectivity in [0.001f64, 0.01, 0.1] {
            let threshold = ((1.0 - selectivity) * rows as f64) as i64;
            let filter = format!("ts >= {threshold}");
            for use_stats in [false, true] {
                let (result_rows, ms, analyzed) = run_scan(&dataset, &filter, use_stats).await;
                let fragments = extract_metric(&analyzed, "num_fragments=");
                let scanned = extract_metric(&analyzed, "rows_scanned=");
                println!(
                    "| {layout} | {selectivity} | {} | {result_rows} | {fragments} | {scanned} | {ms:.1} |",
                    if use_stats { "on" } else { "off" },
                );
                if selectivity == 0.001 {
                    let tag = format!("{layout}-sel0.001-stats-{use_stats}");
                    println!("\n--- analyze_plan [{tag}] ---\n{analyzed}\n");
                }
            }
        }
    }
}

async fn tier1_vector(rows: usize, frag_rows: usize) {
    use arrow_array::{FixedSizeListArray, Float32Array};
    use lance::index::vector::VectorIndexParams;
    use lance_arrow::FixedSizeListArrayExt;
    use lance_linalg::distance::MetricType;

    const DIM: usize = 16;
    println!("\n== Tier 1v: vector search + prefilter (clustered, {rows} rows) ==\n");
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            true,
        ),
    ]));
    let make_batch = |start: i64, end: i64| {
        let values = Float32Array::from_iter_values(
            (start..end).flat_map(|v| std::iter::repeat_n(v as f32, DIM)),
        );
        let vecs = FixedSizeListArray::try_new_from_values(values, DIM as i32).unwrap();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(start..end)),
                Arc::new(vecs),
            ],
        )
        .unwrap()
    };

    let dir = TempStrDir::default();
    // First 60% of rows written and covered by the vector index; the last 40%
    // appended afterwards stay unindexed (flat fallback territory).
    let split = (rows as i64) * 6 / 10;
    let params = WriteParams {
        max_rows_per_file: frag_rows,
        ..Default::default()
    };
    let mut dataset = Dataset::write(
        RecordBatchIterator::new([Ok(make_batch(0, split))], schema.clone()),
        dir.as_str(),
        Some(params),
    )
    .await
    .unwrap();
    dataset
        .create_index(
            &["vec"],
            IndexType::Vector,
            None,
            &VectorIndexParams::ivf_flat(8, MetricType::L2),
            true,
        )
        .await
        .unwrap();
    let append = WriteParams {
        mode: lance::dataset::WriteMode::Append,
        max_rows_per_file: frag_rows,
        ..Default::default()
    };
    let mut dataset = Dataset::write(
        RecordBatchIterator::new([Ok(make_batch(split, rows as i64))], schema.clone()),
        Arc::new(dataset),
        Some(append),
    )
    .await
    .unwrap();
    dataset
        .create_index(
            &["ts"],
            IndexType::FragmentColumnStats,
            None,
            &ScalarIndexParams::new("FragmentColumnStats".to_string()),
            true,
        )
        .await
        .unwrap();

    println!("| filter | stats | deltas_searched | fallback fragments | latency ms |");
    println!("|---|---|---|---|---|");
    // Three regimes: candidates entirely in indexed range, entirely in the
    // unindexed tail, and straddling the boundary.
    let filters = [
        format!("ts < {}", rows / 100),
        format!("ts >= {}", rows - rows / 100),
        format!("ts >= {} AND ts < {}", split - 5000, split as usize + 5000),
    ];
    for filter in &filters {
        for use_stats in [false, true] {
            let query = Float32Array::from_iter_values(std::iter::repeat_n(1.0f32, DIM));
            let mut scanner = dataset.scan();
            scanner.nearest("vec", &query, 10).unwrap();
            scanner.prefilter(true);
            scanner.filter(filter).unwrap();
            scanner.use_fragment_stats(use_stats);
            let start = Instant::now();
            let _batches: Vec<RecordBatch> = scanner
                .try_into_stream()
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let ms = start.elapsed().as_secs_f64() * 1000.0;

            let mut scanner = dataset.scan();
            scanner.nearest("vec", &query, 10).unwrap();
            scanner.prefilter(true);
            scanner.filter(filter).unwrap();
            scanner.use_fragment_stats(use_stats);
            let analyzed = scanner.analyze_plan().await.unwrap();
            let deltas = extract_metric(&analyzed, "deltas_searched=");
            let frags = extract_metric(&analyzed, "num_fragments=");
            println!(
                "| {filter} | {} | {deltas} | {frags} | {ms:.1} |",
                if use_stats { "on" } else { "off" },
            );
            if filter == &filters[1] {
                println!(
                    "\n--- analyze_plan [vector, {filter}, stats={use_stats}] ---\n{analyzed}\n"
                );
            }
        }
    }
}

fn tier2() {
    use lance_core::deepsize::DeepSizeOf;
    println!("\n== Tier 2: synthetic summary scale ==\n");
    println!("| fragments | resident bytes | bytes/record | eval ms (1 predicate) |");
    println!("|---|---|---|---|");
    for count in [1_000u32, 10_000, 100_000, 1_000_000] {
        let index = FragmentColumnStatsIndex::synthetic_i64(count, 1_000_000);
        let bytes = index.deep_size_of();
        // A 1% range at the top of the domain.
        let lo = (count as i64) * 1_000_000 * 99 / 100;
        let query = SargableQuery::Range(
            std::ops::Bound::Included(datafusion::common::ScalarValue::Int64(Some(lo))),
            std::ops::Bound::Unbounded,
        );
        let start = Instant::now();
        let excluded = index.excluded_fragments(&query).unwrap();
        let eval_ms = start.elapsed().as_secs_f64() * 1000.0;
        assert!(excluded.len() > 0);
        println!(
            "| {count} | {bytes} | {:.0} | {eval_ms:.2} |",
            bytes as f64 / count as f64
        );
    }
}

#[tokio::main]
async fn main() {
    let rows = env_usize("ROWS", 1_000_000);
    let frag_rows = env_usize("FRAG_ROWS", 10_000);
    let tier = std::env::var("TIER").unwrap_or_else(|_| "both".to_string());
    if tier == "1" || tier == "both" {
        tier1(rows, frag_rows).await;
    }
    if tier == "1v" || tier == "both" {
        tier1_vector(rows, frag_rows).await;
    }
    if tier == "2" || tier == "both" {
        tier2();
    }
}
