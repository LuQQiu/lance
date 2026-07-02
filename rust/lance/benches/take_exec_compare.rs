// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
//
// FilteredReadExec vs TakeExec, apples-to-apples.
//
// Both materialize the SAME projection for the SAME 100 row addresses per query.
// A fixed list of 1000 queries (each = 100 random row addrs out of the dataset)
// is replayed identically against both execs. We report QPS and P50/P90/P99
// latency (a real latency harness, not criterion's mean), plus an optional
// pprof flamegraph.
//
// Run:
//   LANCE_BENCH_DATASET=/page-cache/persistent/datasets/<name>.lance \
//   LANCE_BENCH_COLUMN=full_content \
//   cargo run --release --bench take_exec_compare
//
// Env:
//   LANCE_BENCH_DATASET   path to the .lance dataset (required)
//   LANCE_BENCH_COLUMN    column to take (default: full_content)
//   LANCE_BENCH_NQUERIES  number of queries in the fixed list (default: 1000)
//   LANCE_BENCH_ROWS      rows per take (default: 100)
//   LANCE_BENCH_WARMUP    warmup queries per exec (default: 100)

use std::sync::Arc;
use std::time::Instant;

use futures::TryStreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::Dataset;
use lance_core::datatypes::OnMissing;
use lance_datafusion::exec::{execute_plan, LanceExecutionOptions};
use lance_table::format::Fragment;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Build `n` queries, each a Vec of `rows_per` random row addresses
/// `(frag<<32)|offset` drawn size-weighted across the dataset's fragments.
async fn build_queries(
    dataset: &Dataset,
    n: usize,
    rows_per: usize,
    seed: u64,
) -> Vec<Vec<u64>> {
    // (fragment_id, physical_rows) for size-weighted sampling.
    let frags: Vec<&Fragment> = dataset.manifest().fragments.iter().collect();
    let mut frag_sizes: Vec<(u64, u64)> = Vec::with_capacity(frags.len());
    for f in &frags {
        let n = dataset
            .get_fragment(f.id as usize)
            .unwrap()
            .physical_rows()
            .await
            .unwrap_or(0) as u64;
        if n > 0 {
            frag_sizes.push((f.id, n));
        }
    }
    let total: u64 = frag_sizes.iter().map(|(_, n)| *n).sum();
    println!(
        "dataset: {} fragments, {} rows total",
        frag_sizes.len(),
        total
    );

    let mut rng = StdRng::seed_from_u64(seed);
    let mut queries = Vec::with_capacity(n);
    for _ in 0..n {
        let mut q = Vec::with_capacity(rows_per);
        for _ in 0..rows_per {
            let mut target = rng.gen_range(0..total);
            for (fid, fsz) in &frag_sizes {
                if target < *fsz {
                    q.push((fid << 32) | target);
                    break;
                }
                target -= *fsz;
            }
        }
        queries.push(q);
    }
    queries
}

fn pctl(sorted_us: &[u128], p: f64) -> f64 {
    if sorted_us.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_us.len() as f64) * p) as usize;
    let idx = idx.min(sorted_us.len() - 1);
    sorted_us[idx] as f64 / 1000.0 // -> ms
}

/// Replay all queries against one exec builder, timing each. Returns per-query
/// latencies (µs) and total rows fetched (sanity).
async fn run_bench<F>(
    name: &str,
    dataset: &Arc<Dataset>,
    queries: &[Vec<u64>],
    warmup: usize,
    build: F,
) where
    F: Fn(&Arc<Dataset>, Vec<u64>) -> Arc<dyn datafusion_physical_plan::ExecutionPlan>,
{
    // Warmup (populate OS/page cache, JIT paths).
    for q in queries.iter().take(warmup) {
        let plan = build(dataset, q.clone());
        let stream = execute_plan(plan, LanceExecutionOptions::default()).unwrap();
        let _ = stream.try_collect::<Vec<_>>().await.unwrap();
    }

    let mut lats = Vec::with_capacity(queries.len());
    let mut total_rows = 0usize;
    let wall = Instant::now();
    for q in queries {
        let plan = build(dataset, q.clone());
        let t = Instant::now();
        let stream = execute_plan(plan, LanceExecutionOptions::default()).unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        lats.push(t.elapsed().as_micros());
        total_rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
    }
    let wall = wall.elapsed().as_secs_f64();

    lats.sort_unstable();
    let qps = queries.len() as f64 / wall;
    println!(
        "\n=== {name} ===\n  queries={} rows/take~{} total_rows={} wall={:.2}s\n  QPS={:.1}\n  P50={:.2}ms  P90={:.2}ms  P99={:.2}ms  max={:.2}ms  mean={:.2}ms",
        queries.len(),
        queries.first().map(|q| q.len()).unwrap_or(0),
        total_rows,
        wall,
        qps,
        pctl(&lats, 0.50),
        pctl(&lats, 0.90),
        pctl(&lats, 0.99),
        pctl(&lats, 1.0),
        lats.iter().sum::<u128>() as f64 / lats.len() as f64 / 1000.0,
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let path = std::env::var("LANCE_BENCH_DATASET")
        .expect("set LANCE_BENCH_DATASET to the .lance path");
    let column = std::env::var("LANCE_BENCH_COLUMN").unwrap_or_else(|_| "full_content".into());
    let nqueries = env_usize("LANCE_BENCH_NQUERIES", 1000);
    let rows_per = env_usize("LANCE_BENCH_ROWS", 100);
    let warmup = env_usize("LANCE_BENCH_WARMUP", 100);

    // ~10% of pod memory (1.2TB) for metadata+index caches.
    let cache = 120usize * 1024 * 1024 * 1024;
    let dataset = Arc::new(
        DatasetBuilder::from_uri(&path)
            .with_index_cache_size_bytes(cache)
            .with_metadata_cache_size_bytes(cache)
            .load()
            .await
            .expect("open dataset"),
    );
    println!(
        "opened {path}\n  column={column} nqueries={nqueries} rows/take={rows_per} warmup={warmup}"
    );

    let queries = build_queries(&dataset, nqueries, rows_per, 42).await;

    let col = column.clone();
    let build_filtered = move |ds: &Arc<Dataset>, addrs: Vec<u64>| {
        let scanner = ds.scan();
        let projection = ds
            .empty_projection()
            .union_column(&col, OnMissing::Error)
            .unwrap();
        scanner
            .bench_build_filtered_read_take(addrs, projection)
            .unwrap()
    };
    let col2 = column.clone();
    let build_take = move |ds: &Arc<Dataset>, addrs: Vec<u64>| {
        let scanner = ds.scan();
        let projection = ds
            .empty_projection()
            .union_column(&col2, OnMissing::Error)
            .unwrap();
        scanner.bench_build_take_exec(addrs, projection).unwrap()
    };

    run_bench("FilteredReadExec", &dataset, &queries, warmup, build_filtered).await;
    run_bench("TakeExec", &dataset, &queries, warmup, build_take).await;
}
