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
use roaring::RoaringBitmap;
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

fn median_ms<F: FnMut() -> RoaringBitmap>(mut f: F) -> (f64, RoaringBitmap) {
    let mut times = Vec::with_capacity(5);
    let mut out = RoaringBitmap::new();
    for _ in 0..5 {
        let start = Instant::now();
        out = f();
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (times[2], out)
}

fn mem_tier() {
    use lance_core::deepsize::DeepSizeOf;
    use lance_index::scalar::fragstats::{
        CompactFragmentStats, PackedFragmentStats, synthetic_for_bench,
    };
    use std::ops::Bound;

    println!("\n== Tier M: memory representations (object vs compact vs packed) ==\n");
    println!(
        "| kind | fragments | scheme | resident bytes | bytes/rec | build ms | range eval ms | equals eval ms | candidates identical |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");

    for kind in ["i64", "utf8"] {
        for count in [100_000u32, 1_000_000] {
            let rows_per_fragment = 1_000_000u64;
            let object = synthetic_for_bench(count, rows_per_fragment, kind);
            let live: RoaringBitmap = (0..count).collect();

            let build_start = Instant::now();
            let compact = CompactFragmentStats::try_from_index(&object).unwrap();
            let compact_build = build_start.elapsed().as_secs_f64() * 1000.0;
            let build_start = Instant::now();
            let packed = PackedFragmentStats::try_from_index(&object).unwrap();
            let packed_build = build_start.elapsed().as_secs_f64() * 1000.0;

            // ~1% range at the top of the domain, plus a point query.
            let (range_query, equals_query) = if kind == "utf8" {
                (
                    SargableQuery::Range(
                        Bound::Included(datafusion::common::ScalarValue::Utf8(Some(format!(
                            "user-{:012}-a",
                            count as u64 * 99 / 100
                        )))),
                        Bound::Unbounded,
                    ),
                    SargableQuery::Equals(datafusion::common::ScalarValue::Utf8(Some(format!(
                        "user-{:012}-mmm",
                        count as u64 / 2
                    )))),
                )
            } else {
                let lo = (count as i64) * rows_per_fragment as i64 * 99 / 100;
                let make = |v: i64| {
                    if kind == "timestamp" {
                        datafusion::common::ScalarValue::TimestampMicrosecond(Some(v), None)
                    } else {
                        datafusion::common::ScalarValue::Int64(Some(v))
                    }
                };
                (
                    SargableQuery::Range(Bound::Included(make(lo)), Bound::Unbounded),
                    SargableQuery::Equals(make((count as i64 / 2) * rows_per_fragment as i64 + 17)),
                )
            };

            // Candidate-list generation timed end to end: exclusion scan PLUS
            // the live-minus-excluded bitmap subtraction.
            let schemes: Vec<(&str, usize, f64)> = vec![
                ("object", object.deep_size_of(), 0.0),
                ("compact", compact.resident_bytes(), compact_build),
                ("packed", packed.resident_bytes(), packed_build),
            ];
            let mut range_results: Vec<RoaringBitmap> = Vec::new();
            let mut equals_results: Vec<RoaringBitmap> = Vec::new();
            let mut range_times = Vec::new();
            let mut equals_times = Vec::new();
            for scheme in ["object", "compact", "packed"] {
                let (rt, rres) = median_ms(|| {
                    let excluded = match scheme {
                        "object" => object.excluded_fragments(&range_query).unwrap(),
                        "compact" => compact.excluded_fragments(&range_query),
                        _ => packed.excluded_fragments(&range_query),
                    };
                    &live - &excluded
                });
                let (et, eres) = median_ms(|| {
                    let excluded = match scheme {
                        "object" => object.excluded_fragments(&equals_query).unwrap(),
                        "compact" => compact.excluded_fragments(&equals_query),
                        _ => packed.excluded_fragments(&equals_query),
                    };
                    &live - &excluded
                });
                range_results.push(rres);
                equals_results.push(eres);
                range_times.push(rt);
                equals_times.push(et);
            }
            let identical = range_results.windows(2).all(|w| w[0] == w[1])
                && equals_results.windows(2).all(|w| w[0] == w[1]);

            for (i, (name, bytes, build)) in schemes.iter().enumerate() {
                println!(
                    "| {kind} | {count} | {name} | {bytes} | {:.1} | {:.1} | {:.2} | {:.2} | {identical} |",
                    *bytes as f64 / count as f64,
                    build,
                    range_times[i],
                    equals_times[i],
                );
            }
        }
    }

    // Attribution: at 1M fragments, is the cost the comparisons or the
    // result-bitmap construction? Pure count vs per-item insert vs
    // run-detected insert_range, on the packed i64 scheme.
    {
        use lance_index::scalar::fragstats::{PackedFragmentStats, synthetic_for_bench};
        use std::ops::Bound;
        let object = synthetic_for_bench(1_000_000, 1_000_000, "i64");
        let packed = PackedFragmentStats::try_from_index(&object).unwrap();
        let lo = 1_000_000i64 * 1_000_000 * 99 / 100;
        let query = SargableQuery::Range(
            Bound::Included(datafusion::common::ScalarValue::Int64(Some(lo))),
            Bound::Unbounded,
        );
        let start = Instant::now();
        let mut excl_count = 0u64;
        for _ in 0..5 {
            excl_count = packed.count_excluded(&query);
        }
        let count_ms = start.elapsed().as_secs_f64() * 1000.0 / 5.0;
        let start = Instant::now();
        let mut bitmap_len = 0u64;
        for _ in 0..5 {
            bitmap_len = packed.excluded_fragments(&query).len();
        }
        let insert_ms = start.elapsed().as_secs_f64() * 1000.0 / 5.0;
        let start = Instant::now();
        let mut runs_len = 0u64;
        for _ in 0..5 {
            runs_len = packed.excluded_fragments_runs(&query).len();
        }
        let runs_ms = start.elapsed().as_secs_f64() * 1000.0 / 5.0;
        assert_eq!(excl_count, bitmap_len);
        assert_eq!(bitmap_len, runs_len);
        println!("\nAttribution (packed i64, 1M fragments, 1% range, {excl_count} excluded):");
        println!("| pure comparison scan (count only) | {count_ms:.2} ms |");
        println!("| per-item bitmap insert            | {insert_ms:.2} ms |");
        println!("| run-detected insert_range         | {runs_ms:.2} ms |");
    }

    println!(
        "\nNote: candidate bitmap for the 1% range over 1M fragments serializes to ~{} bytes.",
        {
            let live: RoaringBitmap = (0..1_000_000u32).collect();
            let candidates: RoaringBitmap = (990_000u32..1_000_000).collect();
            let _ = live;
            candidates.serialized_size()
        }
    );
}

fn qn_tier() {
    use lance_index::scalar::fragstats::{PackedFragmentStats, synthetic_for_bench};
    use std::ops::Bound;
    use std::sync::Arc as StdArc;

    println!("\n== Tier QN: compound predicates, multi-column residency, concurrency ==\n");
    let count = 1_000_000u32;
    let rows_per_fragment = 1_000_000u64;
    let make_range = |lo: i64, hi: Option<i64>| {
        SargableQuery::Range(
            Bound::Included(datafusion::common::ScalarValue::Int64(Some(lo))),
            match hi {
                Some(hi) => Bound::Excluded(datafusion::common::ScalarValue::Int64(Some(hi))),
                None => Bound::Unbounded,
            },
        )
    };

    // Two columns with the same clustered domain shape.
    let col_a = StdArc::new(
        PackedFragmentStats::try_from_index(&synthetic_for_bench(count, rows_per_fragment, "i64"))
            .unwrap(),
    );
    let col_b = StdArc::new(
        PackedFragmentStats::try_from_index(&synthetic_for_bench(count, rows_per_fragment, "i64"))
            .unwrap(),
    );
    let live: RoaringBitmap = (0..count).collect();
    let domain = count as i64 * rows_per_fragment as i64;
    // A: top 1%; B: a 5% band overlapping half of A's range.
    let query_a = make_range(domain * 99 / 100, None);
    let query_b = make_range(domain * 985 / 1000, Some(domain * 995 / 1000));

    // Compound candidate generation, run-based exclusion per column.
    let and_ms = {
        let start = Instant::now();
        let mut out = 0u64;
        for _ in 0..5 {
            let ex =
                col_a.excluded_fragments_runs(&query_a) | col_b.excluded_fragments_runs(&query_b);
            out = (&live - &ex).len();
        }
        (start.elapsed().as_secs_f64() * 1000.0 / 5.0, out)
    };
    let or_ms = {
        let start = Instant::now();
        let mut out = 0u64;
        for _ in 0..5 {
            let ex =
                col_a.excluded_fragments_runs(&query_a) & col_b.excluded_fragments_runs(&query_b);
            out = (&live - &ex).len();
        }
        (start.elapsed().as_secs_f64() * 1000.0 / 5.0, out)
    };
    println!("| compound (1M frags, 2 cols) | candidates | total ms |");
    println!("|---|---|---|");
    println!("| A AND B | {} | {:.2} |", and_ms.1, and_ms.0);
    println!("| A OR B | {} | {:.2} |", or_ms.1, or_ms.0);

    // Ten cached columns: actual instances, summed residency.
    let mut ten_total = 0usize;
    let mut ten = Vec::new();
    for i in 0..10 {
        let kind = if i < 6 { "i64" } else { "utf8" };
        let packed = PackedFragmentStats::try_from_index(&synthetic_for_bench(
            count,
            rows_per_fragment,
            kind,
        ))
        .unwrap();
        ten_total += packed.resident_bytes();
        ten.push(packed);
    }
    println!(
        "\n10 cached columns at 1M fragments (6 numeric + 4 short-string): {:.1} MB actual resident",
        ten_total as f64 / 1024.0 / 1024.0
    );
    drop(ten);

    // Concurrency: shared 1M-fragment column, run-based candidate generation.
    println!("\n| threads | QPS | p50 ms | p99 ms |");
    println!("|---|---|---|---|");
    for threads in [8usize, 32, 128] {
        let mut handles = Vec::new();
        let stop = StdArc::new(std::sync::atomic::AtomicBool::new(false));
        for t in 0..threads {
            let col = col_a.clone();
            let live = live.clone();
            let stop = stop.clone();
            handles.push(std::thread::spawn(move || {
                let mut latencies = Vec::with_capacity(4096);
                let mut i = t as i64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let lo = (i * 7919) % 99; // vary the range start
                    let query = SargableQuery::Range(
                        Bound::Included(datafusion::common::ScalarValue::Int64(Some(
                            1_000_000i64 * 1_000_000 * lo / 100,
                        ))),
                        Bound::Excluded(datafusion::common::ScalarValue::Int64(Some(
                            1_000_000i64 * 1_000_000 * (lo + 1) / 100,
                        ))),
                    );
                    let start = Instant::now();
                    let excluded = col.excluded_fragments_runs(&query);
                    let candidates = &live - &excluded;
                    std::hint::black_box(candidates.len());
                    latencies.push(start.elapsed().as_secs_f64() * 1000.0);
                    i += 1;
                }
                latencies
            }));
        }
        std::thread::sleep(std::time::Duration::from_secs(3));
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let mut all: Vec<f64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        all.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let qps = all.len() as f64 / 3.0;
        let p50 = all[all.len() / 2];
        let p99 = all[(all.len() as f64 * 0.99) as usize];
        println!("| {threads} | {qps:.0} | {p50:.2} | {p99:.2} |");
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
    if tier == "mem" {
        mem_tier();
    }
    if tier == "qn" {
        qn_tier();
    }
}
