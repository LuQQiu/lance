// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Env-driven harness for the FTS staged-build finalize path: build one
//! staged intermediate over every fragment, finalize + promote it, commit,
//! and query. Each mode prints a `BENCH {json}` line; the same file runs
//! unchanged on the baseline tree so before/after numbers come from one
//! driver.
//!
//! ```text
//! FTS_BENCH_MODE=build    FTS_BENCH_URI=<dataset uri> FTS_BENCH_ROWS=.. FTS_BENCH_FRAGS=..
//!                         FTS_BENCH_SEGMENT_JSON=<out> [FTS_BENCH_WORKERS=..] [FTS_BENCH_REUSE_DATASET=1]
//! FTS_BENCH_MODE=finalize FTS_BENCH_URI=.. FTS_BENCH_SEGMENT_JSON=<in> [FTS_BENCH_INDEX_UUID=<copy>]
//! FTS_BENCH_MODE=query    FTS_BENCH_URI=.. FTS_BENCH_QUERIES=<file: one query per line, "phrase:" or "and:" prefix>
//!                         FTS_BENCH_REPS=.. FTS_BENCH_HITS_OUT=<out json>
//! ```
//!
//! `cargo test -p lance --release --features substrait --lib fts_finalize_bench -- --ignored --nocapture`

// The harness reports through stdout on purpose: one `BENCH` line per run is
// what the driver scripts collect.
#![allow(clippy::print_stdout)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, UInt64Type};
use futures::TryStreamExt;
use lance_core::{ROW_ID, Result};
use lance_index::IndexType;
use lance_index::progress::IndexBuildProgress;
use lance_index::scalar::FullTextSearchQuery;
use lance_index::scalar::inverted::query::{FtsQuery, MatchQuery, Operator, PhraseQuery};
use lance_index::scalar::inverted::{InvertedIndexParams, SCORE_COL};
use lance_table::format::IndexMetadata;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Dataset;
use crate::dataset::WriteParams;
use crate::dataset::index::LanceIndexStoreExt;
use crate::index::DatasetIndexExt;

const COLUMN: &str = "text";
const INDEX_NAME: &str = "text_idx";

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    env(name)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[derive(Serialize, Deserialize)]
struct SavedSegment {
    uuid: String,
    fields: Vec<i32>,
    covering_fields: Vec<i32>,
    name: String,
    dataset_version: u64,
    fragment_bitmap: Vec<u8>,
    index_details_type_url: String,
    index_details_value: Vec<u8>,
    index_version: i32,
}

impl SavedSegment {
    fn from_metadata(segment: &IndexMetadata) -> Self {
        let mut fragment_bitmap = Vec::new();
        if let Some(bitmap) = &segment.fragment_bitmap {
            bitmap.serialize_into(&mut fragment_bitmap).unwrap();
        }
        let details = segment.index_details.as_ref().expect("index details");
        Self {
            uuid: segment.uuid.to_string(),
            fields: segment.fields.clone(),
            covering_fields: segment.covering_fields.clone(),
            name: segment.name.clone(),
            dataset_version: segment.dataset_version,
            fragment_bitmap,
            index_details_type_url: details.type_url.clone(),
            index_details_value: details.value.clone(),
            index_version: segment.index_version,
        }
    }

    fn into_metadata(self, uuid_override: Option<Uuid>) -> IndexMetadata {
        IndexMetadata {
            uuid: uuid_override.unwrap_or_else(|| Uuid::parse_str(&self.uuid).unwrap()),
            fields: self.fields,
            covering_fields: self.covering_fields,
            name: self.name,
            dataset_version: self.dataset_version,
            fragment_bitmap: Some(
                RoaringBitmap::deserialize_from(self.fragment_bitmap.as_slice()).unwrap(),
            ),
            index_details: Some(Arc::new(prost_types::Any {
                type_url: self.index_details_type_url,
                value: self.index_details_value,
            })),
            index_version: self.index_version,
            created_at: None,
            base_id: None,
            files: None,
        }
    }
}

/// When a stage started and completed, relative to the progress origin, and
/// its last reported progress count.
#[derive(Debug, Default)]
struct StageTiming {
    start: Option<Duration>,
    complete: Option<Duration>,
    last_progress: u64,
}

/// Records when each finalize stage started and completed, so overlapping
/// stages can be reported as intervals rather than summed.
#[derive(Debug, Default)]
struct TimingProgress {
    origin: Option<Instant>,
    stages: Mutex<BTreeMap<String, StageTiming>>,
}

impl TimingProgress {
    fn new() -> Self {
        Self {
            origin: Some(Instant::now()),
            stages: Mutex::default(),
        }
    }

    fn elapsed(&self) -> Duration {
        self.origin
            .map(|origin| origin.elapsed())
            .unwrap_or_default()
    }

    fn report(&self) -> BTreeMap<String, serde_json::Value> {
        self.stages
            .lock()
            .unwrap()
            .iter()
            .map(|(stage, timing)| {
                (
                    stage.clone(),
                    serde_json::json!({
                        "start_s": timing.start.map(|d| d.as_secs_f64()),
                        "complete_s": timing.complete.map(|d| d.as_secs_f64()),
                        "duration_s": match (timing.start, timing.complete) {
                            (Some(s), Some(c)) => Some((c - s).as_secs_f64()),
                            _ => None,
                        },
                        "last_progress": timing.last_progress,
                    }),
                )
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl IndexBuildProgress for TimingProgress {
    async fn stage_start(&self, stage: &str, _total: Option<u64>, _unit: &str) -> Result<()> {
        let now = self.elapsed();
        self.stages
            .lock()
            .unwrap()
            .entry(stage.to_string())
            .or_default()
            .start
            .get_or_insert(now);
        Ok(())
    }

    async fn stage_progress(&self, stage: &str, completed: u64) -> Result<()> {
        self.stages
            .lock()
            .unwrap()
            .entry(stage.to_string())
            .or_default()
            .last_progress = completed;
        Ok(())
    }

    async fn stage_complete(&self, stage: &str) -> Result<()> {
        let now = self.elapsed();
        self.stages
            .lock()
            .unwrap()
            .entry(stage.to_string())
            .or_default()
            .complete = Some(now);
        Ok(())
    }
}

fn process_cpu_secs() -> f64 {
    // SAFETY: getrusage with a zeroed rusage out-param is always sound.
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) != 0 {
            return 0.0;
        }
        let user = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1e6;
        let sys = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1e6;
        user + sys
    }
}

fn current_rss_bytes() -> u64 {
    let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else {
        return 0;
    };
    statm
        .split_whitespace()
        .nth(1)
        .and_then(|pages| pages.parse::<u64>().ok())
        .map(|pages| pages * 4096)
        .unwrap_or(0)
}

struct ResourceSampler {
    stop: Arc<AtomicBool>,
    peak_rss: Arc<AtomicU64>,
    cpu_start: f64,
    wall_start: Instant,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl ResourceSampler {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let peak_rss = Arc::new(AtomicU64::new(current_rss_bytes()));
        let (stop_c, peak_c) = (stop.clone(), peak_rss.clone());
        let handle = std::thread::spawn(move || {
            while !stop_c.load(Ordering::Relaxed) {
                peak_c.fetch_max(current_rss_bytes(), Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(50));
            }
        });
        Self {
            stop,
            peak_rss,
            cpu_start: process_cpu_secs(),
            wall_start: Instant::now(),
            handle: Some(handle),
        }
    }

    fn finish(mut self) -> serde_json::Value {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        let wall = self.wall_start.elapsed().as_secs_f64();
        let cpu = process_cpu_secs() - self.cpu_start;
        serde_json::json!({
            "wall_s": wall,
            "cpu_s": cpu,
            "avg_cores": if wall > 0.0 { cpu / wall } else { 0.0 },
            "peak_rss_bytes": self.peak_rss.load(Ordering::Relaxed),
        })
    }
}

fn io_json(stats: &lance_io::utils::tracking_store::IoStats) -> serde_json::Value {
    serde_json::json!({
        "read_bytes": stats.read_bytes,
        "read_iops": stats.read_iops,
        "written_bytes": stats.written_bytes,
        "write_iops": stats.write_iops,
    })
}

/// Stored sizes of the partitions under `prefix` in the index directory:
/// `part_<id>_*` files grouped by id.
async fn partition_sizes(dataset: &Dataset, uuid: &Uuid, prefix: &str) -> serde_json::Value {
    let index_dir = dataset.indices_dir().join(uuid.to_string());
    let listed = dataset
        .object_store
        .read_dir_all(&index_dir, None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let base = index_dir.as_ref().trim_start_matches('/');
    let mut by_partition: BTreeMap<String, u64> = BTreeMap::new();
    let mut total = 0u64;
    for meta in listed {
        let relative = meta
            .location
            .as_ref()
            .trim_start_matches('/')
            .strip_prefix(base)
            .map(|path| path.trim_start_matches('/'))
            .unwrap_or("");
        let Some(name) = relative.strip_prefix(prefix) else {
            continue;
        };
        let Some(rest) = name.strip_prefix("part_") else {
            continue;
        };
        let Some((id, suffix)) = rest.split_once('_') else {
            continue;
        };
        if suffix.ends_with("metadata.lance") {
            continue;
        }
        *by_partition.entry(id.to_string()).or_default() += meta.size;
        total += meta.size;
    }
    let mut sizes = by_partition.values().copied().collect::<Vec<_>>();
    sizes.sort_unstable();
    let percentile = |p: f64| -> u64 {
        if sizes.is_empty() {
            0
        } else {
            sizes[((sizes.len() - 1) as f64 * p).round() as usize]
        }
    };
    serde_json::json!({
        "count": sizes.len(),
        "total_bytes": total,
        "min_bytes": sizes.first().copied().unwrap_or(0),
        "p10_bytes": percentile(0.1),
        "median_bytes": percentile(0.5),
        "p90_bytes": percentile(0.9),
        "max_bytes": sizes.last().copied().unwrap_or(0),
        "sizes_bytes": sizes,
    })
}

async fn open_dataset(uri: &str) -> Dataset {
    let dataset = Dataset::open(uri).await.unwrap();
    match env("FTS_BENCH_DATASET_VERSION") {
        Some(version) => dataset
            .checkout_version(version.parse::<u64>().unwrap())
            .await
            .unwrap(),
        None => dataset,
    }
}

fn index_params() -> InvertedIndexParams {
    let mut params = InvertedIndexParams::default().with_position(true);
    if let Some(workers) = env("FTS_BENCH_WORKERS") {
        params = params.num_workers(workers.parse().unwrap());
    }
    if let Some(memory_mb) = env("FTS_BENCH_MEMORY_LIMIT_MB") {
        params = params.memory_limit_mb(memory_mb.parse().unwrap());
    }
    params
}

async fn mode_build(uri: &str) {
    use lance_datagen::{BatchCount, RowCount, array, gen_batch};

    let rows: u64 = env_or("FTS_BENCH_ROWS", 1_000_000);
    let frags: u32 = env_or("FTS_BENCH_FRAGS", 10);
    let min_words: usize = env_or("FTS_BENCH_MIN_WORDS", 8);
    let max_words: usize = env_or("FTS_BENCH_MAX_WORDS", 64);
    let rows_per_fragment = rows / frags as u64;
    let segment_json = env("FTS_BENCH_SEGMENT_JSON").expect("FTS_BENCH_SEGMENT_JSON");

    let write_sampler = ResourceSampler::start();
    // A table written by an earlier run whose index build failed can be
    // indexed again instead of regenerated.
    let mut dataset = if env("FTS_BENCH_REUSE_DATASET").is_some() {
        open_dataset(uri).await
    } else {
        let reader = gen_batch()
            .col("id", array::step::<arrow_array::types::Int64Type>())
            .col(COLUMN, array::random_sentence(min_words, max_words, false))
            .into_reader_rows(RowCount::from(rows_per_fragment), BatchCount::from(frags));
        let reader = RareTokenReader::new(reader, env_or("FTS_BENCH_RARE_TOKENS", 3));
        Dataset::write(
            reader,
            uri,
            Some(WriteParams {
                max_rows_per_file: rows_per_fragment as usize,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
    };
    let write = write_sampler.finish();

    let fragment_ids = dataset
        .get_fragments()
        .iter()
        .map(|fragment| fragment.id() as u32)
        .collect::<Vec<_>>();
    let io_before = dataset.object_store.io_stats_snapshot();
    let build_sampler = ResourceSampler::start();
    let params = index_params();
    let segment = dataset
        .create_index_builder(&[COLUMN], IndexType::Inverted, &params)
        .name(INDEX_NAME.to_string())
        .fragments(fragment_ids)
        .execute_uncommitted()
        .await
        .unwrap();
    let build = build_sampler.finish();
    let io_after = dataset.object_store.io_stats_snapshot();
    let staged = partition_sizes(&dataset, &segment.uuid, "staging/").await;

    std::fs::write(
        &segment_json,
        serde_json::to_string_pretty(&SavedSegment::from_metadata(&segment)).unwrap(),
    )
    .unwrap();
    if let Some(queries_out) = env("FTS_BENCH_QUERIES_OUT") {
        std::fs::write(&queries_out, sample_queries(&dataset).await.join("\n")).unwrap();
    }
    println!(
        "BENCH {}",
        serde_json::json!({
            "mode": "build",
            "uri": uri,
            "dataset_version": dataset.version().version,
            "rows": rows,
            "fragments": frags,
            "intermediate_uuid": segment.uuid.to_string(),
            "write": write,
            "tokenize": build,
            "tokenize_io": {
                "read_bytes": io_after.read_bytes - io_before.read_bytes,
                "written_bytes": io_after.written_bytes - io_before.written_bytes,
            },
            "staged_partitions": staged,
            "env": bench_env(),
        })
    );
}

/// The sentence generator draws from about 1,500 words, so on its own the
/// vocabulary stops growing after the first rows and every posting list is
/// long and dense. Real corpora have tens of millions of distinct terms that
/// each occur a handful of times, which is what sizes token files and makes
/// partition decoding expensive. Appending a few tokens derived from the row
/// number gives the vocabulary that long tail: `rare_tokens` per row, each
/// shared by a different small group of rows.
struct RareTokenReader {
    inner: Box<dyn arrow_array::RecordBatchReader + Send>,
    rare_tokens: usize,
    next_row: u64,
}

impl RareTokenReader {
    fn new(
        inner: impl arrow_array::RecordBatchReader + Send + 'static,
        rare_tokens: usize,
    ) -> Self {
        Self {
            inner: Box::new(inner),
            rare_tokens,
            next_row: 0,
        }
    }
}

impl Iterator for RareTokenReader {
    type Item = std::result::Result<arrow_array::RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = match self.inner.next()? {
            Ok(batch) => batch,
            Err(error) => return Some(Err(error)),
        };
        let schema = batch.schema();
        let column = schema.index_of(COLUMN).unwrap();
        let texts = batch.column(column).as_string::<i32>();
        let first_row = self.next_row;
        self.next_row += batch.num_rows() as u64;
        let mut builder = arrow_array::builder::StringBuilder::with_capacity(
            batch.num_rows(),
            texts.values().len() + batch.num_rows() * 16 * self.rare_tokens,
        );
        for (offset, text) in texts.iter().enumerate() {
            let row = first_row + offset as u64;
            let mut doc = text.unwrap_or("").to_string();
            for k in 0..self.rare_tokens as u64 {
                // Token k of row r is shared by the rows whose number agrees
                // with r on all but the low (k+1) digits: group sizes 10, 100...
                let group = row / 10u64.pow(k as u32 + 1);
                doc.push_str(&format!(" r{k}x{group:x}"));
            }
            builder.append_value(doc);
        }
        let mut columns = batch.columns().to_vec();
        columns[column] = Arc::new(builder.finish());
        Some(arrow_array::RecordBatch::try_new(schema, columns))
    }
}

impl arrow_array::RecordBatchReader for RareTokenReader {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.inner.schema()
    }
}

/// Queries drawn from the indexed text itself: frequent single terms, OR and
/// AND pairs, and phrases of adjacent words from sampled sentences.
async fn sample_queries(dataset: &Dataset) -> Vec<String> {
    let batch = dataset
        .scan()
        .project(&[COLUMN])
        .unwrap()
        .limit(Some(2_000), None)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    let texts = batch.column_by_name(COLUMN).unwrap().as_string::<i32>();
    let mut frequency: BTreeMap<String, usize> = BTreeMap::new();
    let mut phrases = Vec::new();
    for (row, text) in texts.iter().enumerate() {
        let Some(text) = text else { continue };
        let words = text
            .split_whitespace()
            .map(|word| word.to_lowercase())
            .filter(|word| word.len() > 3)
            .collect::<Vec<_>>();
        for word in &words {
            *frequency.entry(word.clone()).or_default() += 1;
        }
        if row % 400 == 0 && words.len() >= 3 {
            phrases.push(format!("phrase:{} {} {}", words[0], words[1], words[2]));
        }
    }
    let mut ranked = frequency.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    let words = ranked
        .iter()
        .map(|(word, _)| word.clone())
        .collect::<Vec<_>>();
    let pick = |index: usize| words[index.min(words.len().saturating_sub(1))].clone();
    let mut queries = vec![
        pick(0),
        pick(words.len() / 2),
        pick(words.len().saturating_sub(5)),
        format!("{} {}", pick(1), pick(words.len() / 3)),
        format!("and:{} {}", pick(2), pick(words.len() / 4)),
        format!("and:{} {} {}", pick(0), pick(3), pick(6)),
    ];
    queries.extend(phrases.into_iter().take(4));
    // Rare-tail terms from the appended tokens: one of ~10 rows, one of ~1000.
    queries.push("r0x2a".to_string());
    queries.push("r2x1".to_string());
    queries
}

fn bench_env() -> serde_json::Value {
    let keys = [
        "LANCE_FTS_PARTITION_SIZE",
        "LANCE_FTS_NUM_SHARDS",
        "LANCE_FTS_FORMAT_VERSION",
        "LANCE_IO_THREADS",
        "LANCE_UPLOAD_CONCURRENCY",
        "LANCE_INITIAL_UPLOAD_SIZE",
        "LANCE_SERVER_SIDE_COPY_ENABLED",
        "FTS_BENCH_WORKERS",
        "FTS_BENCH_MEMORY_LIMIT_MB",
    ];
    keys.iter()
        .map(|key| (key.to_string(), serde_json::json!(std::env::var(key).ok())))
        .collect::<serde_json::Map<_, _>>()
        .into()
}

async fn mode_finalize(uri: &str) {
    let segment_json = env("FTS_BENCH_SEGMENT_JSON").expect("FTS_BENCH_SEGMENT_JSON");
    let saved: SavedSegment =
        serde_json::from_str(&std::fs::read_to_string(&segment_json).unwrap()).unwrap();
    let uuid_override = env("FTS_BENCH_INDEX_UUID").map(|uuid| Uuid::parse_str(&uuid).unwrap());
    let segment = saved.into_metadata(uuid_override);
    let mut dataset = open_dataset(uri).await;
    let staged = partition_sizes(&dataset, &segment.uuid, "staging/").await;

    // Stage 1: finalize the staged files (copy and, on the fixed tree,
    // consolidate), observed through the progress callbacks.
    let progress = Arc::new(TimingProgress::new());
    let io_0 = dataset.object_store.io_stats_snapshot();
    let finalize_sampler = ResourceSampler::start();
    let index_dir = dataset.indices_dir().join(segment.uuid.to_string());
    let store = lance_index::scalar::lance_format::LanceIndexStore::from_dataset_for_new(
        &dataset,
        &segment.uuid,
    )
    .unwrap();
    let _ = lance_index::scalar::inverted::builder::merge_index_files(
        dataset.object_store.as_ref(),
        &index_dir,
        Arc::new(store),
        progress.clone(),
    )
    .await
    .unwrap();
    let finalize = finalize_sampler.finish();
    let io_1 = dataset.object_store.io_stats_snapshot();

    // Stage 2: what the merge API does on top of a finalized segment: on the
    // baseline a full rewrite to a new uuid, on the fixed tree a promotion.
    let merge_sampler = ResourceSampler::start();
    let merged = dataset
        .merge_existing_index_segments(vec![segment.clone()])
        .await
        .unwrap();
    let merge = merge_sampler.finish();
    let io_2 = dataset.object_store.io_stats_snapshot();

    // Every finalize run checks out the same build version and commits on
    // top of it, so each commit carries its own index name: two CreateIndex
    // transactions for one name on the same base version conflict.
    let index_name = format!("{INDEX_NAME}_{}", &merged.uuid.simple().to_string()[..8]);
    let commit_sampler = ResourceSampler::start();
    dataset
        .commit_existing_index_segments(&index_name, COLUMN, vec![merged.clone()])
        .await
        .unwrap();
    let commit = commit_sampler.finish();
    let final_partitions = partition_sizes(&dataset, &merged.uuid, "").await;
    let indexed_rows = dataset
        .load_indices()
        .await
        .unwrap()
        .iter()
        .filter(|index| index.uuid == merged.uuid)
        .map(|index| {
            index
                .fragment_bitmap
                .as_ref()
                .map(|bitmap| bitmap.len())
                .unwrap_or(0)
        })
        .sum::<u64>();

    let diff = |a: &lance_io::utils::tracking_store::IoStats,
                b: &lance_io::utils::tracking_store::IoStats| {
        serde_json::json!({
            "read_bytes": b.read_bytes - a.read_bytes,
            "read_iops": b.read_iops - a.read_iops,
            "written_bytes": b.written_bytes - a.written_bytes,
            "write_iops": b.write_iops - a.write_iops,
        })
    };
    println!(
        "BENCH {}",
        serde_json::json!({
            "mode": "finalize",
            "uri": uri,
            "dataset_version": dataset.version().version,
            "intermediate_uuid": segment.uuid.to_string(),
            "final_uuid": merged.uuid.to_string(),
            "same_uuid": merged.uuid == segment.uuid,
            "index_name": index_name,
            "staged_partitions": staged,
            "finalize": finalize,
            "finalize_io": diff(&io_0, &io_1),
            "finalize_stages": progress.report(),
            "merge": merge,
            "merge_io": diff(&io_1, &io_2),
            "commit": commit,
            "total_io": io_json(&dataset.object_store.io_stats_snapshot()),
            "final_partitions": final_partitions,
            "indexed_fragments": indexed_rows,
            "dataset_fragments": dataset.get_fragments().len(),
            "dataset_rows": dataset.count_rows(None).await.unwrap(),
            "env": bench_env(),
        })
    );
}

#[derive(Serialize)]
struct QueryResult {
    query: String,
    hits: usize,
    p50_ms: f64,
    p95_ms: f64,
    max_ms: f64,
    /// `(row_id, score bits)` ordered by score desc then row id.
    results: Vec<(u64, u32)>,
}

async fn mode_query(uri: &str) {
    let dataset = open_dataset(uri).await;
    let queries_file = env("FTS_BENCH_QUERIES").expect("FTS_BENCH_QUERIES");
    let reps: usize = env_or("FTS_BENCH_REPS", 5);
    let limit: i64 = env_or("FTS_BENCH_LIMIT", 100);
    let hits_out = env("FTS_BENCH_HITS_OUT");
    let mut results = Vec::new();
    for line in std::fs::read_to_string(&queries_file).unwrap().lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let query = if let Some(terms) = line.strip_prefix("phrase:") {
            FtsQuery::Phrase(
                PhraseQuery::new(terms.trim().to_string()).with_column(Some(COLUMN.to_string())),
            )
        } else if let Some(terms) = line.strip_prefix("and:") {
            FtsQuery::Match(
                MatchQuery::new(terms.trim().to_string())
                    .with_column(Some(COLUMN.to_string()))
                    .with_operator(Operator::And),
            )
        } else {
            FtsQuery::Match(MatchQuery::new(line.to_string()).with_column(Some(COLUMN.to_string())))
        };
        let mut latencies = Vec::with_capacity(reps);
        let mut last = Vec::new();
        for _ in 0..reps {
            let started = Instant::now();
            let mut scan = dataset.scan();
            scan.full_text_search(FullTextSearchQuery::new_query(query.clone()).limit(Some(limit)))
                .unwrap();
            scan.with_row_id();
            let batch = scan.try_into_batch().await.unwrap();
            latencies.push(started.elapsed().as_secs_f64() * 1e3);
            let row_ids = batch
                .column_by_name(ROW_ID)
                .unwrap()
                .as_primitive::<UInt64Type>();
            let scores = batch
                .column_by_name(SCORE_COL)
                .unwrap()
                .as_primitive::<Float32Type>();
            last = row_ids
                .values()
                .iter()
                .zip(scores.values())
                .map(|(row_id, score)| (*row_id, score.to_bits()))
                .collect();
            last.sort_by(|a, b| {
                f32::from_bits(b.1)
                    .total_cmp(&f32::from_bits(a.1))
                    .then(a.0.cmp(&b.0))
            });
        }
        latencies.sort_by(|a, b| a.total_cmp(b));
        let pick = |p: f64| latencies[((latencies.len() - 1) as f64 * p).round() as usize];
        results.push(QueryResult {
            query: line.to_string(),
            hits: last.len(),
            p50_ms: pick(0.5),
            p95_ms: pick(0.95),
            max_ms: *latencies.last().unwrap(),
            results: last,
        });
    }
    if let Some(path) = hits_out {
        std::fs::write(&path, serde_json::to_string_pretty(&results).unwrap()).unwrap();
    }
    let summary = results
        .iter()
        .map(|result| {
            serde_json::json!({
                "query": result.query,
                "hits": result.hits,
                "p50_ms": result.p50_ms,
                "p95_ms": result.p95_ms,
                "max_ms": result.max_ms,
            })
        })
        .collect::<Vec<_>>();
    println!(
        "BENCH {}",
        serde_json::json!({
            "mode": "query",
            "uri": uri,
            "dataset_version": dataset.version().version,
            "limit": limit,
            "reps": reps,
            "queries": summary,
        })
    );
}

// The index job runs on a multi-threaded runtime; the default current-thread
// test runtime would serialize every copy and merge group on one thread.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "env-driven benchmark; see the module docs"]
async fn fts_finalize_bench() {
    // Finalize logs the stored and decoded size of every partition it merges;
    // the driver logs collect them to calibrate the planning constants.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "lance_index::scalar::inverted::builder=info".into()),
        )
        .with_writer(std::io::stderr)
        .try_init();
    let uri = env("FTS_BENCH_URI").expect("FTS_BENCH_URI");
    match env("FTS_BENCH_MODE").as_deref() {
        Some("build") => mode_build(&uri).await,
        Some("finalize") => mode_finalize(&uri).await,
        Some("query") => mode_query(&uri).await,
        other => panic!("FTS_BENCH_MODE must be build|finalize|query, got {other:?}"),
    }
}
