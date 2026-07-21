// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
//
// Pure-lance FTS QPS bench — opens a real dataset with index cache DISABLED,
// runs tier-100-200 all-lang match_any(5-term) queries at fixed concurrency,
// reports QPS + latency. Used to isolate the single-node FTS throughput ceiling.
//
// Usage:
//   fts_qps <dataset_uri> <words_dir> <concurrency> <duration_secs> [column]
// Env:
//   LANCE_CPU_THREADS   overrides lance compute-pool size (default cpus-2)

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Duration, Instant};

use futures::TryStreamExt;
type BoxErr = Box<dyn std::error::Error + Send + Sync>;
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance_index::scalar::FullTextSearchQuery;
use lance_index::scalar::inverted::query::{FtsQuery, MatchQuery, Operator};

const TIER_LO: usize = 100; // rank 100..200 (0-based -> 99..199)
const TIER_HI: usize = 200;
const TERMS_PER_QUERY: usize = 5;
const K: i64 = 100;

// 42 languages (matches dataset). words_dir holds words_<lang>_6k_common.txt
const LANGS: &[&str] = &[
    "ar","bg","bn","ca","cs","da","de","el","en","es","fa","fi","fil","fr","he",
    "hi","hu","id","is","it","ja","ko","lt","lv","mk","ms","nb","nl","pl","pt","ro",
    "ru","sh","sk","sl","sv","ta","tr","uk","ur","vi","zh",
];

fn load_tier_words(words_dir: &str) -> Vec<Vec<String>> {
    // For each language, the tier-100-200 slice (100 words). Returned per-lang
    // so each query stays monolingual (matches mmlb word-freq-tier semantics).
    let mut per_lang = Vec::new();
    for lang in LANGS {
        let p = PathBuf::from(words_dir).join(format!("words_{lang}_6k_common.txt"));
        if let Ok(content) = std::fs::read_to_string(&p) {
            let words: Vec<String> = content
                .lines()
                .skip(TIER_LO - 1)
                .take(TIER_HI - TIER_LO)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if words.len() >= TERMS_PER_QUERY {
                per_lang.push(words);
            }
        }
    }
    per_lang
}

// Deterministic-ish 5-word pick from a lang's word slice, varied by index.
fn build_query(per_lang: &[Vec<String>], i: u64, column: &str) -> FullTextSearchQuery {
    let lang = &per_lang[(i as usize) % per_lang.len()];
    let mut s = i.wrapping_mul(0x100000001B3).wrapping_add(0xABCDEF);
    let mut chosen: Vec<&str> = Vec::with_capacity(TERMS_PER_QUERY);
    let mut attempts = 0;
    while chosen.len() < TERMS_PER_QUERY && attempts < TERMS_PER_QUERY * 20 {
        s ^= s << 13; s ^= s >> 7; s ^= s << 17; // xorshift
        let w = lang[(s as usize) % lang.len()].as_str();
        if !chosen.contains(&w) { chosen.push(w); }
        attempts += 1;
    }
    while chosen.len() < TERMS_PER_QUERY { chosen.push(lang[chosen.len() % lang.len()].as_str()); }
    let terms = chosen.join(" ");
    // MatchQuery defaults to Operator::Or == match_any
    let mq = MatchQuery::new(terms)
        .with_column(Some(column.to_string()))
        .with_operator(Operator::Or);
    FullTextSearchQuery::new_query(FtsQuery::Match(mq))
}


// Flatten all languages' tier words into one pool for the no-repeat mode.
fn flat_pool(per_lang: &[Vec<String>]) -> Vec<String> {
    let mut v = Vec::new();
    for lang in per_lang { for w in lang { v.push(w.clone()); } }
    v
}

// No-repeat: each query takes 5 DISTINCT words by global stride, so consecutive
// queries share no words (tests whether CPU amplification is word-collision).
fn build_query_norepeat(flat: &[String], i: u64, column: &str) -> FullTextSearchQuery {
    let n = flat.len() as u64;
    let mut chosen: Vec<&str> = Vec::with_capacity(TERMS_PER_QUERY);
    for k in 0..TERMS_PER_QUERY as u64 {
        let idx = ((i.wrapping_mul(TERMS_PER_QUERY as u64) + k) % n) as usize;
        chosen.push(flat[idx].as_str());
    }
    let terms = chosen.join(" ");
    let mq = MatchQuery::new(terms).with_column(Some(column.to_string())).with_operator(Operator::Or);
    FullTextSearchQuery::new_query(FtsQuery::Match(mq))
}

async fn run_one(ds: &Dataset, q: FullTextSearchQuery) -> Result<usize, BoxErr> {
    let mut scanner = ds.scan();
    scanner
        .full_text_search(q)?
        .with_row_id()
        .limit(Some(K), None)?;
    // project nothing heavy: just _rowid + _score (skip_take equivalent)
    let stream = scanner.try_into_stream().await?;
    let batches: Vec<_> = stream.try_collect().await?;
    Ok(batches.iter().map(|b| b.num_rows()).sum())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), BoxErr> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        eprintln!("usage: fts_qps <dataset_uri> <words_dir> <concurrency> <duration_secs> [column]");
        std::process::exit(1);
    }
    let uri = args[1].clone();
    let words_dir = args[2].clone();
    let concurrency: usize = args[3].parse()?;
    let duration_secs: u64 = args[4].parse()?;
    let column = args.get(5).cloned().unwrap_or_else(|| "full_content".to_string());
    let idx_cache_gib: f64 = args.get(6).and_then(|s| s.parse().ok()).unwrap_or(0.0);
    let meta_cache_gib: f64 = args.get(7).and_then(|s| s.parse().ok()).unwrap_or(0.0);
    let idx_cache_bytes = (idx_cache_gib * 1073741824.0) as usize;
    let meta_cache_bytes = (meta_cache_gib * 1073741824.0) as usize;

    let per_lang = Arc::new(load_tier_words(&words_dir));
    eprintln!("loaded tier {}-{} words for {} langs", TIER_LO, TIER_HI, per_lang.len());
    assert!(!per_lang.is_empty(), "no word lists loaded from {words_dir}");
    let flat = Arc::new(flat_pool(&per_lang));
    let norepeat = std::env::var("NOREPEAT").is_ok();
    eprintln!("query mode: {} (pool={} tokens)", if norepeat {"NO-REPEAT"} else {"random-per-lang"}, flat.len());

    // Open dataset with index cache DISABLED (0 bytes) — the "no cache" requirement.
    let ds = Arc::new(
        DatasetBuilder::from_uri(&uri)
            .with_index_cache_size_bytes(idx_cache_bytes)
            .with_metadata_cache_size_bytes(meta_cache_bytes)
            .load()
            .await?,
    );
    eprintln!("opened dataset {uri} (index_cache={idx_cache_gib}GiB, metadata_cache={meta_cache_gib}GiB)");

    // warmup: a few serial queries so the index metadata is resolved (opening cost),
    // but with cache=0 nothing persists between queries.
    for i in 0..3u64 {
        let _ = run_one(&ds, if norepeat { build_query_norepeat(&flat, i, &column) } else { build_query(&per_lang, i, &column) }).await?;
    }

    let done = Arc::new(AtomicBool::new(false));
    let counter = Arc::new(AtomicU64::new(0));
    let lat_ns = Arc::new(AtomicU64::new(0));
    let qid = Arc::new(AtomicU64::new(1000));

    // Optional in-process CPU profiler (captures ALL thread stacks) — set PROFILE=1
    let guard = if std::env::var("PROFILE").is_ok() {
        Some(::pprof::ProfilerGuardBuilder::default().frequency(199).blocklist(&["libc","libgcc","pthread","vdso"]).build().unwrap())
    } else { None };

    // periodic rolling-QPS progress line (PROGRESS=1)
    if std::env::var("PROGRESS").is_ok() {
        let counter = counter.clone();
        let done = done.clone();
        tokio::spawn(async move {
            let mut last = 0u64;
            let mut t = 0u64;
            while !done.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(15)).await;
                t += 15;
                let n = counter.load(Ordering::Relaxed);
                eprintln!("PROGRESS t={t}s window_qps={:.1} total={n}", (n - last) as f64 / 15.0);
                last = n;
            }
        });
    }

    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..concurrency {
        let ds = ds.clone();
        let per_lang = per_lang.clone();
        let flat = flat.clone();
        let done = done.clone();
        let counter = counter.clone();
        let lat_ns = lat_ns.clone();
        let qid = qid.clone();
        let column = column.clone();
        handles.push(tokio::spawn(async move {
            while !done.load(Ordering::Relaxed) {
                let i = qid.fetch_add(1, Ordering::Relaxed);
                let q = if norepeat { build_query_norepeat(&flat, i, &column) } else { build_query(&per_lang, i, &column) };
                let t0 = Instant::now();
                match run_one(&ds, q).await {
                    Ok(_) => {
                        lat_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => { eprintln!("query error: {e}"); }
                }
            }
        }));
    }

    tokio::time::sleep(Duration::from_secs(duration_secs)).await;
    done.store(true, Ordering::Relaxed);
    for h in handles { let _ = h.await; }

    // dump flamegraph if profiling
    if let Some(g) = guard {
        if let Ok(report) = g.report().build() {
            let f = std::fs::File::create("/tmp/fts_flame.svg").unwrap();
            let mut opts = ::pprof::flamegraph::Options::default();
            report.flamegraph_with_options(f, &mut opts).unwrap();
            eprintln!("wrote /tmp/fts_flame.svg");
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    let n = counter.load(Ordering::Relaxed);
    let total_lat_ms = lat_ns.load(Ordering::Relaxed) as f64 / 1e6;
    let qps = n as f64 / elapsed;
    let mean_ms = if n > 0 { total_lat_ms / n as f64 } else { 0.0 };
    println!(
        "RESULT concurrency={concurrency} qps={:.1} mean_latency_ms={:.1} queries={n} elapsed_s={:.1} idx_cache_gib={idx_cache_gib} meta_cache_gib={meta_cache_gib} lance_cpu_threads={}",
        qps, mean_ms, elapsed,
        std::env::var("LANCE_CPU_THREADS").unwrap_or_else(|_| "default".into())
    );
    Ok(())
}
