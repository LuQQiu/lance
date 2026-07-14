// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
//
// Root-cause harness: TakeExec vs FilteredReadExec (mask) vs FilteredReadExec
// (row-stream), replaying identical scattered row addresses and reporting
// QPS/P50 plus PHYSICAL I/O counts (lance_io global counters, post-coalescing).
//
// Env:
//   LANCE_BENCH_DATASET      path to the .lance dataset (required)
//   LANCE_BENCH_COLUMN       column to take (default: row_index)
//   LANCE_BENCH_NQUERIES     queries in the fixed list (default: 10)
//   LANCE_BENCH_ROWS         rows per take (default: 100000)
//   LANCE_BENCH_WARMUP       warmup queries per arm (default: 2)
//   LANCE_BENCH_ARMS         comma list: take_exec,take_exec_8ki,mask,take
//                            (default: all)
//   LANCE_BENCH_BATCH_SIZE   with_batch_size on the row-stream arm (input
//                            coalescing; also flows to the inner fragment read)
//   LANCE_BENCH_CREATE_ROWS  create synthetic narrow dataset if missing:
//                            row_index UInt64 + rating Int32, 100k rows/fragment
//
// The single-variable toggle LANCE_BENCH_INNER_MAX=1 lives in
// src/io/exec/filtered_read.rs (test checkout only): forces the inner
// ScopedFragmentRead.batch_size to u32::MAX without touching input coalescing.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt};
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::io::exec::TakeExec;
use lance::io::exec::filtered_read::{FilteredReadExec, FilteredReadOptions};
use lance_core::ROW_ADDR;
use lance_core::datatypes::{OnMissing, Projection};
use lance_datafusion::exec::{LanceExecutionOptions, OneShotExec, execute_plan};
use lance_select::result::IndexExprResultWireFormat;
use lance_select::{IndexExprResult, RowAddrMask, RowAddrTreeMap};
use lance_table::format::Fragment;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

async fn build_queries(dataset: &Dataset, n: usize, rows_per: usize, seed: u64) -> Vec<Vec<u64>> {
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
    // LANCE_BENCH_SINGLE_FRAG=1: draw every key from the first fragment, so a
    // query touches exactly one fragment (isolates per-fragment machinery)
    let single_frag = std::env::var("LANCE_BENCH_SINGLE_FRAG").is_ok();
    let mut queries = Vec::with_capacity(n);
    for _ in 0..n {
        let mut q = Vec::with_capacity(rows_per);
        for _ in 0..rows_per {
            if single_frag {
                let (fid, fsz) = frag_sizes[0];
                q.push((fid << 32) | rng.random_range(0..fsz));
                continue;
            }
            let mut target = rng.random_range(0..total);
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
    sorted_us[idx] as f64 / 1000.0
}

fn projection(dataset: &Arc<Dataset>, column: &str) -> Projection {
    dataset
        .empty_projection()
        .union_column(column, OnMissing::Error)
        .unwrap()
}

fn row_addr_input(addrs: Vec<u64>) -> Arc<OneShotExec> {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        ROW_ADDR,
        DataType::UInt64,
        true,
    )]));
    // LANCE_BENCH_INPUT_CHUNK: pre-split the input into real batches of this
    // many rows (CoalesceBatchesExec does NOT split oversized batches)
    let chunk = std::env::var("LANCE_BENCH_INPUT_CHUNK")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(usize::MAX);
    let batches: Vec<_> = addrs
        .chunks(chunk.max(1))
        .map(|c| {
            Ok(RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(UInt64Array::from(c.to_vec()))],
            )
            .unwrap())
        })
        .collect();
    let stream = futures::stream::iter(batches);
    Arc::new(OneShotExec::new(Box::pin(RecordBatchStreamAdapter::new(
        schema, stream,
    ))))
}

fn mask_input(dataset: &Arc<Dataset>, addrs: Vec<u64>) -> Arc<OneShotExec> {
    let fragments_covered: roaring::RoaringBitmap =
        dataset.fragments().iter().map(|f| f.id as u32).collect();
    let mask = RowAddrMask::from_allowed(RowAddrTreeMap::from_iter(addrs));
    let batch = IndexExprResult::exact(mask)
        .serialize(&fragments_covered, IndexExprResultWireFormat::TwoMask)
        .unwrap();
    let schema = batch.schema();
    let stream = futures::stream::once(async move { Ok(batch) });
    Arc::new(OneShotExec::new(Box::pin(RecordBatchStreamAdapter::new(
        schema, stream,
    ))))
}

async fn run_bench<F>(
    name: &str,
    dataset: &Arc<Dataset>,
    queries: &[Vec<u64>],
    warmup: usize,
    build: F,
) where
    F: Fn(&Arc<Dataset>, Vec<u64>) -> Arc<dyn datafusion_physical_plan::ExecutionPlan>,
{
    for q in queries.iter().take(warmup) {
        let plan = build(dataset, q.clone());
        let stream = execute_plan(plan, LanceExecutionOptions::default()).unwrap();
        let _ = stream.try_collect::<Vec<_>>().await.unwrap();
    }

    // LANCE_BENCH_CONCURRENCY: how many queries run at once (default 1)
    let concurrency = env_usize("LANCE_BENCH_CONCURRENCY", 1);
    // LANCE_BENCH_PROFILE=/path/prefix: write a pprof flamegraph per arm
    #[cfg(target_os = "linux")]
    let profiler = std::env::var("LANCE_BENCH_PROFILE").ok().map(|prefix| {
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(499)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .unwrap();
        (prefix, guard)
    });
    let io0 = lance_io::iops_counter();
    let bytes0 = lance_io::bytes_read_counter();
    let wall = Instant::now();
    let results: Vec<(u128, usize)> = futures::stream::iter(queries.iter().cloned())
        .map(|q| {
            let plan = build(dataset, q);
            async move {
                let t = Instant::now();
                let stream = execute_plan(plan, LanceExecutionOptions::default()).unwrap();
                let batches = stream.try_collect::<Vec<_>>().await.unwrap();
                let rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
                (t.elapsed().as_micros(), rows)
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;
    let wall = wall.elapsed().as_secs_f64();
    let iops = lance_io::iops_counter() - io0;
    let bytes = lance_io::bytes_read_counter() - bytes0;
    #[cfg(target_os = "linux")]
    if let Some((prefix, guard)) = profiler {
        let report = guard.report().build().unwrap();
        let slug: String = name
            .chars()
            .map(|c| if c.is_alphanumeric() { c } else { '_' })
            .collect();
        let path = format!("{prefix}_{slug}.svg");
        let file = std::fs::File::create(&path).unwrap();
        report.flamegraph(file).unwrap();
        println!("flamegraph written to {path}");
    }

    let mut lats: Vec<u128> = results.iter().map(|(lat, _)| *lat).collect();
    let total_rows: usize = results.iter().map(|(_, rows)| *rows).sum();
    lats.sort_unstable();
    let qps = queries.len() as f64 / wall;
    println!(
        "\n=== {name} ===\n  queries={} concurrency={} rows/take~{} total_rows={} wall={:.2}s\n  QPS={:.1}\n  P50={:.2}ms  P90={:.2}ms  P99={:.2}ms  max={:.2}ms  mean={:.2}ms\n  IOPS_PER_QUERY={:.0}  MB_READ_PER_QUERY={:.1}  BYTES_PER_READ={:.0}",
        queries.len(),
        concurrency,
        queries.first().map(|q| q.len()).unwrap_or(0),
        total_rows,
        wall,
        qps,
        pctl(&lats, 0.50),
        pctl(&lats, 0.90),
        pctl(&lats, 0.99),
        pctl(&lats, 1.0),
        lats.iter().sum::<u128>() as f64 / lats.len() as f64 / 1000.0,
        iops as f64 / queries.len() as f64,
        bytes as f64 / queries.len() as f64 / 1e6,
        if iops > 0 { bytes as f64 / iops as f64 } else { 0.0 },
    );
    // EXPERIMENTAL: per-stage wall-time totals accumulated inside lance
    let stage_report = lance::io::exec::filtered_read::exp_timing::report_and_reset();
    if !stage_report.is_empty() {
        print!("{stage_report}");
    }
}

async fn maybe_create_dataset(path: &str) {
    let Ok(rows) = std::env::var("LANCE_BENCH_CREATE_ROWS") else {
        return;
    };
    if std::path::Path::new(path).exists() {
        return;
    }
    let rows: u64 = rows
        .parse()
        .expect("LANCE_BENCH_CREATE_ROWS must be a number");
    println!("creating synthetic dataset at {path} with {rows} rows");
    use lance_datagen::{BatchCount, RowCount, array, gen_batch};
    let reader = gen_batch()
        .col(
            "row_index",
            array::step::<arrow_array::types::UInt64Type>(),
        )
        .col("rating", array::step::<arrow_array::types::Int32Type>())
        .into_reader_rows(
            RowCount::from(100_000),
            BatchCount::from((rows / 100_000).max(1) as u32),
        );
    let params = lance::dataset::WriteParams {
        max_rows_per_file: 100_000,
        ..Default::default()
    };
    Dataset::write(reader, path, Some(params)).await.unwrap();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let path =
        std::env::var("LANCE_BENCH_DATASET").expect("set LANCE_BENCH_DATASET to the .lance path");
    let column = std::env::var("LANCE_BENCH_COLUMN").unwrap_or_else(|_| "row_index".into());
    let nqueries = env_usize("LANCE_BENCH_NQUERIES", 10);
    let rows_per = env_usize("LANCE_BENCH_ROWS", 100_000);
    let warmup = env_usize("LANCE_BENCH_WARMUP", 2);
    let arms = std::env::var("LANCE_BENCH_ARMS")
        .unwrap_or_else(|_| "take_exec,take_exec_8ki,mask,take".into());
    let batch_size: Option<u32> = std::env::var("LANCE_BENCH_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok());

    maybe_create_dataset(&path).await;

    let cache = 8usize * 1024 * 1024 * 1024;
    let dataset = Arc::new(
        DatasetBuilder::from_uri(&path)
            .with_index_cache_size_bytes(cache)
            .with_metadata_cache_size_bytes(cache)
            .load()
            .await
            .expect("open dataset"),
    );
    println!(
        "opened {path}\n  column={column} nqueries={nqueries} rows/take={rows_per} warmup={warmup} arms={arms} batch_size={batch_size:?} inner_max={}",
        std::env::var("LANCE_BENCH_INNER_MAX").is_ok(),
    );

    let queries = build_queries(&dataset, nqueries, rows_per, 42).await;

    if arms.contains("take_exec_8ki") {
        // TakeExec fed 8Ki coalesced batches — the fair production shape, and
        // the probe for whether TakeExec re-reads chunks across batches
        let col = column.clone();
        let build = move |ds: &Arc<Dataset>, addrs: Vec<u64>| {
            let coalesced = Arc::new(CoalesceBatchesExec::new(row_addr_input(addrs), 8192));
            Arc::new(
                TakeExec::try_new(ds.clone(), coalesced, projection(ds, &col))
                    .unwrap()
                    .unwrap(),
            ) as Arc<dyn datafusion_physical_plan::ExecutionPlan>
        };
        run_bench("TakeExec (8Ki batches)", &dataset, &queries, warmup, build).await;
    }
    if arms.contains("take_exec,") || arms.ends_with("take_exec") {
        let col = column.clone();
        let build = move |ds: &Arc<Dataset>, addrs: Vec<u64>| {
            Arc::new(
                TakeExec::try_new(ds.clone(), row_addr_input(addrs), projection(ds, &col))
                    .unwrap()
                    .unwrap(),
            ) as Arc<dyn datafusion_physical_plan::ExecutionPlan>
        };
        run_bench("TakeExec (one batch)", &dataset, &queries, warmup, build).await;
    }
    if arms.contains("mask") {
        let col = column.clone();
        // LANCE_BENCH_SCOPED_FRAGS=1: scope options.fragments to the hit set so
        // plan_scan only walks fragments the mask touches (like plan_batch does)
        let scoped_frags = std::env::var("LANCE_BENCH_SCOPED_FRAGS").is_ok();
        let build = move |ds: &Arc<Dataset>, addrs: Vec<u64>| {
            let mut options = FilteredReadOptions::new(projection(ds, &col));
            if scoped_frags {
                let mut hit_ids: Vec<u64> = addrs.iter().map(|addr| addr >> 32).collect();
                hit_ids.sort_unstable();
                hit_ids.dedup();
                let hit_fragments: Vec<_> = ds
                    .fragments()
                    .iter()
                    .filter(|fragment| hit_ids.binary_search(&fragment.id).is_ok())
                    .cloned()
                    .collect();
                options.fragments = Some(Arc::new(hit_fragments));
            }
            let input = mask_input(ds, addrs);
            Arc::new(
                FilteredReadExec::try_new(ds.clone(), options, Some(input)).unwrap(),
            ) as Arc<dyn datafusion_physical_plan::ExecutionPlan>
        };
        run_bench("FilteredReadExec (mask)", &dataset, &queries, warmup, build).await;
    }
    if arms.contains("scan") {
        let col = column.clone();
        let build = move |ds: &Arc<Dataset>, addrs: Vec<u64>| {
            // Dense scan A/B: read a contiguous window of `rows_per` rows
            // starting at a seed-derived offset
            let total: u64 = ds
                .fragments()
                .iter()
                .map(|f| f.physical_rows.expect("physical_rows") as u64)
                .sum();
            let n = addrs.len() as u64;
            let start = addrs[0] % (total.saturating_sub(n)).max(1);
            let options = FilteredReadOptions::new(projection(ds, &col))
                .with_scan_range_before_filter(start..start + n)
                .unwrap();
            Arc::new(FilteredReadExec::try_new(ds.clone(), options, None).unwrap())
                as Arc<dyn datafusion_physical_plan::ExecutionPlan>
        };
        run_bench("FilteredReadExec (scan)", &dataset, &queries, warmup, build).await;
    }
    if arms.contains("take,") || arms.ends_with("take") {
        let col = column.clone();
        let build = move |ds: &Arc<Dataset>, addrs: Vec<u64>| {
            let mut options = FilteredReadOptions::new(projection(ds, &col));
            if let Some(batch_size) = batch_size {
                options = options.with_batch_size(batch_size);
            }
            Arc::new(
                FilteredReadExec::try_new(ds.clone(), options, Some(row_addr_input(addrs)))
                    .unwrap(),
            ) as Arc<dyn datafusion_physical_plan::ExecutionPlan>
        };
        run_bench("FilteredReadExec (row-stream)", &dataset, &queries, warmup, build).await;
    }
}
