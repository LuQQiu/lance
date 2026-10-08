// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Cost and benefit of zone map write seeds across data types.
//!
//! A zone map index with `use_seeds` enabled makes every later append observe
//! the indexed column and embed per-zone statistics in the data file footer
//! (`lance.seed.<column>`), so an incremental index update can harvest those
//! statistics instead of scanning the column. This benchmark measures what that
//! costs on the write path and what it saves on the update path, for a matrix
//! of column types and value distributions, so the default enablement rule in
//! `lance_index::scalar::zonemap::default_use_seeds` can be based on numbers.
//!
//! It measures the existing, index-driven seed path only: seeds are produced
//! when the column already has a zone map index with `use_seeds = true` and the
//! write is an append. Collecting seeds for columns without an index is a
//! separate implementation and is not measured here.
//!
//! ## Layers
//!
//! - `micro`: drives `ZoneMapSeedWriter` directly on pre-generated batches,
//!   calling `finish()` at every `ROWS_PER_FILE` boundary exactly as the
//!   dataset writer does. Isolates the seed writer's CPU, memory and output size.
//! - `e2e`: creates a table with a zone map index (`use_seeds` on or off), then
//!   times a `WriteMode::Append` of `ROWS` rows. Only the append is timed; seed
//!   bytes and data bytes are read back afterwards.
//! - `benefit`: opens the dataset the `e2e` layer wrote with a fresh session
//!   and times `optimize_indices`. With seeds the merge harvests the footers,
//!   without seeds it scans the column. Harvest versus scan is reported from
//!   the `index_seeds_harvested` / `index_seeds_fallback_scan` tracing events,
//!   not inferred from timing.
//!
//! Every (case, mode, repetition) runs in a fresh child process so allocator
//! state from earlier cases cannot leak into peak-RSS numbers. Both modes
//! receive byte-identical input: the data generator seed is derived from the
//! case and repetition only.
//!
//! ## Running
//!
//! ```bash
//! cd rust
//! ROWS=4000000 REPEAT=5 OUT=/tmp/zone_map_seeds.jsonl DATA_DIR=/mnt/nvme/zmseed \
//!   cargo bench --profile release-with-debug --bench zone_map_seeds
//! ```
//!
//! ## Configuration
//!
//! - `LAYERS`: comma list of `micro`, `e2e`, `benefit` (default: all three).
//! - `SCENARIOS`: comma list of case names, or `all` (default). `--list`
//!   prints the case names.
//! - `ROWS`: rows appended per repetition (default 4_000_000). Cases whose
//!   values are 2 KiB or wider use `ROWS / 4`.
//! - `ROWS_PER_FILE`: `max_rows_per_file` for the append and the `finish()`
//!   boundary for `micro` (default 1_000_000).
//! - `BATCH_ROWS`: rows per input batch (default 8192).
//! - `BASE_ROWS`: rows in the table before the index is created (default 50_000).
//! - `REPEAT`: repetitions per case and mode (default 5).
//! - `WIDE_COLS`: columns in the `wide` case (default 16).
//! - `OUT`: JSONL file that receives one line per child run.
//! - `DATA_DIR`: where datasets are written (default: a temp dir).
//! - `VALIDATE`: `first` (or any truthy value) makes the `benefit` layer of
//!   the first repetition check that the seed-built index equals the
//!   scan-built index and that filters return the same row ids with and
//!   without the index; `all` checks every repetition.
//! - `KEEP_DATASET`: when truthy, datasets are left in place.
//! - `PID_DIR`: directory where each child's pid is recorded while it runs.

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::BTreeMap;
use std::env;
use std::io::Write;
use std::path::{Path as StdPath, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_array::{
    Array, ArrayRef, Float32Array, Float64Array, RecordBatch, RecordBatchIterator,
    RecordBatchReader, UInt64Array, cast::AsArray,
};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use arrow_select::concat::concat;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::index::LanceIndexStoreExt;
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance::index::DatasetIndexExt;
use lance::session::Session;
use lance_core::cache::LanceCache;
use lance_core::utils::parse::str_is_truthy;
use lance_core::utils::tracing::{
    INDEX_SEEDS_FALLBACK_EVENT, INDEX_SEEDS_HARVESTED_EVENT, TRACE_DATASET_EVENTS,
};
use lance_datagen::{ArrayGeneratorExt, BatchCount, ByteCount, RowCount, Seed, array, gen_batch};
use lance_file::reader::{FileReader, FileReaderOptions};
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_index::scalar::seed::{IndexSeedWriter, SEED_META_KEY_PREFIX};
use lance_index::scalar::zonemap::ZoneMapSeedWriter;
use lance_index::scalar::{BuiltinIndexType, IndexStore, ScalarIndexParams};
use lance_io::object_store::ObjectStore;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::utils::CachedFileSize;
use serde_json::{Value, json};
use tracing_subscriber::Layer as SubscriberLayer;
use tracing_subscriber::layer::SubscriberExt;

const DEFAULT_ROWS: usize = 4_000_000;
const DEFAULT_ROWS_PER_FILE: usize = 1_000_000;
const DEFAULT_BATCH_ROWS: usize = 8192;
const DEFAULT_BASE_ROWS: usize = 50_000;
const DEFAULT_REPEAT: usize = 5;
const DEFAULT_WIDE_COLS: usize = 16;
const DEFAULT_ROWS_PER_ZONE: u64 = 8192;
/// Values at least this wide make a case "big": it runs with `ROWS / 4`.
const BIG_VALUE_BYTES: usize = 2048;
/// The file name of the zone map index inside an index directory.
const ZONEMAP_INDEX_FILE: &str = "zonemap.lance";
const COLUMN: &str = "val";
const RSS_SAMPLE_INTERVAL: Duration = Duration::from_millis(2);

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str) -> bool {
    env::var(key).map(|s| str_is_truthy(&s)).unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Layer {
    Micro,
    E2e,
    Benefit,
}

impl Layer {
    fn name(self) -> &'static str {
        match self {
            Self::Micro => "micro",
            Self::E2e => "e2e",
            Self::Benefit => "benefit",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "micro" => Some(Self::Micro),
            "e2e" => Some(Self::E2e),
            "benefit" => Some(Self::Benefit),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    SeedsOff,
    SeedsOn,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::SeedsOff => "off",
            Self::SeedsOn => "on",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "off" => Some(Self::SeedsOff),
            "on" => Some(Self::SeedsOn),
            _ => None,
        }
    }
}

/// How the value column of a single-column case is generated.
#[derive(Clone, Debug)]
enum ValueGen {
    Int32Rand,
    Int64Rand,
    Int64Step,
    Float64Rand,
    /// One NaN per hundred rows at a fixed position.
    Float64Nan1Pct,
    /// Random values with `-0.0` and `0.0` sprinkled in at fixed positions.
    Float64SignedZero,
    Bool,
    Date32,
    TimestampMicros,
    Decimal128,
    FixedSizeBinary(i32),
    /// 32-character hex UUID strings.
    Uuid,
    LowCardinality(usize),
    PrefixCounter,
    /// Random English sentences of `min..=max` words.
    Sentence(usize, usize),
    /// Random sentences, globally sorted before writing.
    SentenceSorted(usize, usize),
    /// Random sentences appended as `Utf8View` to a `Utf8` column.
    SentenceUtf8View(usize, usize),
    VarBinary(u64, u64),
    LargeBinary(u64),
    LargeBinarySorted(u64),
    FixedSizeListF32(i32),
    ListInt32,
    Struct,
}

#[derive(Clone, Copy, Debug)]
enum NullMode {
    None,
    /// Each row is null with this probability.
    Random(f64),
    /// The first half of every file is null, the second half is not.
    ClusteredHalf,
    All,
}

#[derive(Clone, Debug)]
struct CaseDef {
    name: String,
    values: ValueGen,
    nulls: NullMode,
    rows_per_zone: u64,
    /// Rough bytes per value, used only to decide whether the case is big.
    value_bytes: usize,
    /// Number of columns; more than one means the `wide` table.
    cols: usize,
}

impl CaseDef {
    fn new(name: &str, values: ValueGen, value_bytes: usize) -> Self {
        Self {
            name: name.to_string(),
            values,
            nulls: NullMode::None,
            rows_per_zone: DEFAULT_ROWS_PER_ZONE,
            value_bytes,
            cols: 1,
        }
    }

    fn with_nulls(mut self, suffix: &str, nulls: NullMode) -> Self {
        self.name = format!("{}_{}", self.name, suffix);
        self.nulls = nulls;
        self
    }

    fn with_rows_per_zone(mut self, rows_per_zone: u64) -> Self {
        self.name = format!("{}_rpz{}", self.name, rows_per_zone);
        self.rows_per_zone = rows_per_zone;
        self
    }

    fn is_big(&self) -> bool {
        self.value_bytes >= BIG_VALUE_BYTES
    }

    fn rows(&self, configured: usize) -> usize {
        if self.is_big() {
            configured / 4
        } else {
            configured
        }
    }
}

/// Word counts that produce sentences of roughly the given byte size with the
/// datagen vocabulary (about 5.5 bytes per word including the separator).
const WORDS_256B: (usize, usize) = (40, 52);
const WORDS_2KB: (usize, usize) = (340, 400);
const WORDS_8KB: (usize, usize) = (1400, 1560);

fn all_cases(wide_cols: usize) -> Vec<CaseDef> {
    let text2k = || CaseDef::new("text2k", ValueGen::Sentence(WORDS_2KB.0, WORDS_2KB.1), 2048);
    let uuid = || CaseDef::new("uuid", ValueGen::Uuid, 32);
    let int64 = || CaseDef::new("int64_rand", ValueGen::Int64Rand, 8);

    let mut cases = vec![
        // Narrow fixed-width types: seeds are off by default.
        CaseDef::new("int32_rand", ValueGen::Int32Rand, 4),
        int64(),
        CaseDef::new("int64_step", ValueGen::Int64Step, 8),
        CaseDef::new("f64_rand", ValueGen::Float64Rand, 8),
        CaseDef::new("f64_nan1", ValueGen::Float64Nan1Pct, 8),
        CaseDef::new("f64_signed_zero", ValueGen::Float64SignedZero, 8),
        CaseDef::new("bool", ValueGen::Bool, 1),
        CaseDef::new("date32", ValueGen::Date32, 4),
        CaseDef::new("ts_us", ValueGen::TimestampMicros, 8),
        // Wide fixed-width types: seeds are on by default.
        CaseDef::new("decimal128", ValueGen::Decimal128, 16),
        CaseDef::new("fsb16", ValueGen::FixedSizeBinary(16), 16),
        CaseDef::new("fsb4096", ValueGen::FixedSizeBinary(4096), 4096),
        // Strings.
        uuid(),
        CaseDef::new("lowcard100", ValueGen::LowCardinality(100), 8),
        CaseDef::new("prefix_counter", ValueGen::PrefixCounter, 16),
        CaseDef::new(
            "text256",
            ValueGen::Sentence(WORDS_256B.0, WORDS_256B.1),
            256,
        ),
        text2k(),
        CaseDef::new("text8k", ValueGen::Sentence(WORDS_8KB.0, WORDS_8KB.1), 8192),
        CaseDef::new(
            "text2k_sorted",
            ValueGen::SentenceSorted(WORDS_2KB.0, WORDS_2KB.1),
            2048,
        ),
        CaseDef::new(
            "text2k_utf8view",
            ValueGen::SentenceUtf8View(WORDS_2KB.0, WORDS_2KB.1),
            2048,
        ),
        // Binary.
        CaseDef::new("varbin64_256", ValueGen::VarBinary(64, 256), 160),
        CaseDef::new("bin20k", ValueGen::LargeBinary(20 * 1024), 20 * 1024),
        CaseDef::new(
            "bin20k_sorted",
            ValueGen::LargeBinarySorted(20 * 1024),
            20 * 1024,
        ),
        // Nested: min/max are always null, only null statistics are kept.
        CaseDef::new("fsl_f32_768", ValueGen::FixedSizeListF32(768), 768 * 4),
        CaseDef::new("list_i32", ValueGen::ListInt32, 48),
        CaseDef::new("struct", ValueGen::Struct, 24),
    ];

    // Null distributions: the seed tracks every null position in a bitmap.
    for base in [int64, uuid, text2k] {
        cases.push(base().with_nulls("null1", NullMode::Random(0.01)));
        cases.push(base().with_nulls("null10", NullMode::Random(0.1)));
        cases.push(base().with_nulls("null50", NullMode::Random(0.5)));
        cases.push(base().with_nulls("null100", NullMode::All));
        cases.push(base().with_nulls("null50_clustered", NullMode::ClusteredHalf));
    }

    // Zone size: seed size is linear in the number of zones.
    for base in [uuid, text2k] {
        cases.push(base().with_rows_per_zone(1024));
        cases.push(base().with_rows_per_zone(65536));
    }

    // Wide tables: per-column costs add up.
    for cols in [wide_cols, wide_cols * 4] {
        let mut case = CaseDef::new(&format!("wide{cols}"), ValueGen::Int64Rand, 64);
        case.cols = cols;
        cases.push(case);
    }
    cases
}

/// The generator for column `index` of the wide table.
fn wide_column_gen(index: usize) -> ValueGen {
    match index % 5 {
        0 => ValueGen::Int64Rand,
        1 => ValueGen::Uuid,
        2 => ValueGen::Sentence(WORDS_256B.0, WORDS_256B.1),
        3 => ValueGen::Float64Rand,
        _ => ValueGen::Decimal128,
    }
}

// ---------------------------------------------------------------------------
// Data generation
// ---------------------------------------------------------------------------

/// The datagen seed for a repetition: both modes of the same (case, rep) get
/// identical values, null positions and batch boundaries.
fn datagen_seed(case: &str, rep: usize) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in case.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash ^ ((rep as u64 + 1) << 48)
}

fn base_generator(values: &ValueGen) -> Box<dyn lance_datagen::ArrayGenerator> {
    use arrow_array::types::{
        Date32Type, Float64Type, Int32Type, Int64Type, TimestampMicrosecondType,
    };
    match values {
        ValueGen::Int32Rand => array::rand::<Int32Type>(),
        ValueGen::Int64Rand => array::rand::<Int64Type>(),
        ValueGen::Int64Step => array::step::<Int64Type>(),
        ValueGen::Float64Rand | ValueGen::Float64SignedZero => array::rand::<Float64Type>(),
        ValueGen::Float64Nan1Pct => {
            let mut pattern = vec![false; 100];
            pattern[37] = true;
            array::rand::<Float64Type>().with_nans(&pattern)
        }
        ValueGen::Bool => array::rand_boolean(),
        ValueGen::Date32 => array::rand::<Date32Type>(),
        ValueGen::TimestampMicros => array::rand::<TimestampMicrosecondType>(),
        ValueGen::Decimal128 => array::rand_type(&DataType::Decimal128(38, 10)),
        ValueGen::FixedSizeBinary(size) => array::rand_fsb(*size),
        ValueGen::Uuid => array::rand_pseudo_uuid_hex(),
        ValueGen::LowCardinality(cardinality) => {
            array::low_cardinality(array::random_word(false), *cardinality)
        }
        ValueGen::PrefixCounter => array::utf8_prefix_plus_counter("user_", false),
        ValueGen::Sentence(min, max)
        | ValueGen::SentenceSorted(min, max)
        | ValueGen::SentenceUtf8View(min, max) => array::random_sentence(*min, *max, false),
        ValueGen::VarBinary(min, max) => {
            array::rand_varbin(ByteCount::from(*min), ByteCount::from(*max))
        }
        ValueGen::LargeBinary(size) | ValueGen::LargeBinarySorted(size) => {
            array::rand_fixedbin(ByteCount::from(*size), true)
        }
        ValueGen::FixedSizeListF32(dim) => array::rand_type(&DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)),
            *dim,
        )),
        ValueGen::ListInt32 => array::rand_type(&DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            true,
        )))),
        ValueGen::Struct => array::rand_struct(Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ])),
    }
}

fn apply_nulls(
    values: Box<dyn lance_datagen::ArrayGenerator>,
    nulls: NullMode,
    rows_per_file: usize,
) -> Box<dyn lance_datagen::ArrayGenerator> {
    match nulls {
        NullMode::None => values,
        NullMode::Random(p) => values.with_random_nulls(p),
        NullMode::ClusteredHalf => {
            let mut validity = vec![false; rows_per_file];
            for slot in validity.iter_mut().skip(rows_per_file / 2) {
                *slot = true;
            }
            values.with_validity(&validity)
        }
        NullMode::All => values.with_validity(&[false]),
    }
}

/// Deterministic input for one run: the arrow schema the table is created
/// with and the batches to append.
struct Input {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl Input {
    fn reader(&self) -> impl arrow_array::RecordBatchReader + Send + 'static {
        RecordBatchIterator::new(
            self.batches.clone().into_iter().map(Ok),
            self.schema.clone(),
        )
    }

    fn total_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Bytes of array memory across all batches.
    fn input_bytes(&self) -> usize {
        self.batches.iter().map(|b| b.get_array_memory_size()).sum()
    }
}

fn generate(
    case: &CaseDef,
    seed: u64,
    rows: usize,
    batch_rows: usize,
    rows_per_file: usize,
) -> Input {
    let batches = rows.div_ceil(batch_rows);
    let mut builder = gen_batch().with_seed(Seed(seed));
    if case.cols == 1 {
        builder = builder.col(
            COLUMN,
            apply_nulls(base_generator(&case.values), case.nulls, rows_per_file),
        );
    } else {
        for col in 0..case.cols {
            builder = builder.col(format!("c{col}"), base_generator(&wide_column_gen(col)));
        }
    }
    let reader = builder.into_reader_rows(
        RowCount::from(batch_rows as u64),
        BatchCount::from(batches as u32),
    );
    let schema = reader.schema();
    let mut batches: Vec<RecordBatch> = reader.map(|b| b.expect("datagen")).collect();
    // Trim to exactly `rows` so every mode and layer sees the same row count.
    let mut remaining = rows;
    batches.retain_mut(|batch| {
        if remaining == 0 {
            return false;
        }
        if batch.num_rows() > remaining {
            *batch = batch.slice(0, remaining);
        }
        remaining -= batch.num_rows();
        true
    });

    let (schema, batches) = match &case.values {
        ValueGen::SentenceSorted(..) | ValueGen::LargeBinarySorted(..) => {
            (schema, sort_batches(&batches, batch_rows))
        }
        ValueGen::Float64SignedZero => (schema, sprinkle_signed_zeros(&batches)),
        ValueGen::SentenceUtf8View(..) => {
            let view_schema = Arc::new(Schema::new(vec![Field::new(
                COLUMN,
                DataType::Utf8View,
                true,
            )]));
            let batches = batches
                .iter()
                .map(|batch| {
                    let col = arrow_cast::cast(batch.column(0), &DataType::Utf8View).expect("cast");
                    RecordBatch::try_new(view_schema.clone(), vec![col]).expect("batch")
                })
                .collect();
            (view_schema, batches)
        }
        _ => (schema, batches),
    };
    Input { schema, batches }
}

/// Globally sort the single value column and re-slice it into batches.
fn sort_batches(batches: &[RecordBatch], batch_rows: usize) -> Vec<RecordBatch> {
    let arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(0).as_ref()).collect();
    let all = concat(&arrays).expect("concat");
    let indices = arrow_ord::sort::sort_to_indices(&all, None, None).expect("sort");
    let sorted = arrow_select::take::take(&all, &indices, None).expect("take");
    let schema = batches[0].schema();
    (0..sorted.len())
        .step_by(batch_rows)
        .map(|start| {
            let len = batch_rows.min(sorted.len() - start);
            RecordBatch::try_new(schema.clone(), vec![sorted.slice(start, len)]).expect("batch")
        })
        .collect()
}

/// Replace every 7th value with `-0.0` and every 11th with `0.0`.
fn sprinkle_signed_zeros(batches: &[RecordBatch]) -> Vec<RecordBatch> {
    let mut row = 0usize;
    batches
        .iter()
        .map(|batch| {
            let values = batch
                .column(0)
                .as_primitive::<arrow_array::types::Float64Type>();
            let rewritten: Float64Array = values
                .iter()
                .map(|v| {
                    row += 1;
                    match v {
                        Some(_) if row.is_multiple_of(7) => Some(-0.0),
                        Some(_) if row.is_multiple_of(11) => Some(0.0),
                        other => other,
                    }
                })
                .collect();
            RecordBatch::try_new(batch.schema(), vec![Arc::new(rewritten)]).expect("batch")
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Process resources
// ---------------------------------------------------------------------------

/// User plus system CPU seconds consumed by this process so far.
fn process_cpu_secs() -> f64 {
    #[cfg(unix)]
    // SAFETY: getrusage fills the zeroed out-param and keeps no pointer to it.
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) != 0 {
            return 0.0;
        }
        let user = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1e6;
        let system = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1e6;
        user + system
    }
    #[cfg(not(unix))]
    {
        0.0
    }
}

/// Current resident set size in bytes; 0 where `/proc` is unavailable.
fn current_rss_bytes() -> u64 {
    let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else {
        return 0;
    };
    let resident_pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    // SAFETY: sysconf has no preconditions.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
    resident_pages * page_size
}

/// Samples RSS on a background thread and keeps the maximum.
struct RssSampler {
    stop: Arc<AtomicBool>,
    peak: Arc<AtomicU64>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl RssSampler {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let peak = Arc::new(AtomicU64::new(current_rss_bytes()));
        let handle = {
            let stop = stop.clone();
            let peak = peak.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    peak.fetch_max(current_rss_bytes(), Ordering::Relaxed);
                    std::thread::sleep(RSS_SAMPLE_INTERVAL);
                }
                peak.fetch_max(current_rss_bytes(), Ordering::Relaxed);
            })
        };
        Self {
            stop,
            peak,
            handle: Some(handle),
        }
    }

    fn stop(mut self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.join().expect("rss sampler");
        }
        self.peak.load(Ordering::Relaxed)
    }
}

/// Resource deltas around one timed operation.
struct Measured {
    wall: Duration,
    cpu_secs: f64,
    baseline_rss: u64,
    peak_rss: u64,
}

fn measure<T>(op: impl FnOnce() -> T) -> (T, Measured) {
    let baseline_rss = current_rss_bytes();
    let cpu_before = process_cpu_secs();
    let sampler = RssSampler::start();
    let start = Instant::now();
    let out = op();
    let wall = start.elapsed();
    let peak_rss = sampler.stop();
    let cpu_secs = process_cpu_secs() - cpu_before;
    (
        out,
        Measured {
            wall,
            cpu_secs,
            baseline_rss,
            peak_rss,
        },
    )
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

// ---------------------------------------------------------------------------
// Seed harvest events
// ---------------------------------------------------------------------------

/// Counts the seed harvest and fallback events the index merge emits.
#[derive(Clone, Default)]
struct SeedEventCounter {
    harvested: Arc<AtomicU64>,
    fallback: Arc<AtomicU64>,
    reasons: Arc<Mutex<Vec<String>>>,
}

#[derive(Default)]
struct EventFields {
    event: Option<String>,
    reason: Option<String>,
}

impl tracing::field::Visit for EventFields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let text = format!("{value:?}").trim_matches('"').to_string();
        match field.name() {
            "event" => self.event = Some(text),
            "reason" => self.reason = Some(text),
            _ => {}
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "event" => self.event = Some(value.to_string()),
            "reason" => self.reason = Some(value.to_string()),
            _ => {}
        }
    }
}

impl<S: tracing::Subscriber> SubscriberLayer<S> for SeedEventCounter {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() != TRACE_DATASET_EVENTS {
            return;
        }
        let mut fields = EventFields::default();
        event.record(&mut fields);
        match fields.event.as_deref() {
            Some(name) if name == INDEX_SEEDS_HARVESTED_EVENT => {
                self.harvested.fetch_add(1, Ordering::Relaxed);
            }
            Some(name) if name == INDEX_SEEDS_FALLBACK_EVENT => {
                self.fallback.fetch_add(1, Ordering::Relaxed);
                self.reasons
                    .lock()
                    .unwrap()
                    .push(fields.reason.unwrap_or_default());
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Child runs
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Validate {
    Off,
    FirstRep,
    All,
}

impl Validate {
    fn from_env() -> Self {
        match env::var("VALIDATE").ok().as_deref() {
            None => Self::Off,
            Some("all") => Self::All,
            Some(value) if str_is_truthy(value) || value == "first" => Self::FirstRep,
            Some(_) => Self::Off,
        }
    }

    fn applies(self, rep: usize) -> bool {
        match self {
            Self::Off => false,
            Self::FirstRep => rep == 0,
            Self::All => true,
        }
    }
}

#[derive(Clone)]
struct Config {
    rows: usize,
    rows_per_file: usize,
    batch_rows: usize,
    base_rows: usize,
    repeat: usize,
    wide_cols: usize,
    out: PathBuf,
    data_dir: PathBuf,
    pid_dir: PathBuf,
    validate: Validate,
    keep: bool,
    layers: Vec<Layer>,
    scenarios: Option<Vec<String>>,
}

impl Config {
    fn from_env() -> Self {
        let layers = env::var("LAYERS")
            .ok()
            .map(|s| {
                s.split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| Layer::parse(s.trim()).unwrap_or_else(|| panic!("unknown layer {s}")))
                    .collect()
            })
            .unwrap_or_else(|| vec![Layer::Micro, Layer::E2e, Layer::Benefit]);
        let scenarios = env::var("SCENARIOS")
            .ok()
            .filter(|s| s != "all")
            .map(|s| s.split(',').map(|s| s.trim().to_string()).collect());
        let data_dir = env::var("DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| env::temp_dir().join("zone_map_seeds"));
        let pid_dir = env::var("PID_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| data_dir.join("pids"));
        Self {
            rows: env_usize("ROWS", DEFAULT_ROWS),
            rows_per_file: env_usize("ROWS_PER_FILE", DEFAULT_ROWS_PER_FILE),
            batch_rows: env_usize("BATCH_ROWS", DEFAULT_BATCH_ROWS),
            base_rows: env_usize("BASE_ROWS", DEFAULT_BASE_ROWS),
            repeat: env_usize("REPEAT", DEFAULT_REPEAT),
            wide_cols: env_usize("WIDE_COLS", DEFAULT_WIDE_COLS),
            out: env::var("OUT")
                .map(PathBuf::from)
                .unwrap_or_else(|_| data_dir.join("results.jsonl")),
            data_dir,
            pid_dir,
            validate: Validate::from_env(),
            keep: env_bool("KEEP_DATASET"),
            layers,
            scenarios,
        }
    }

    fn dataset_dir(&self, case: &str, mode: Mode, rep: usize) -> PathBuf {
        self.data_dir
            .join(case)
            .join(mode.name())
            .join(format!("rep{rep}"))
    }
}

struct Run<'a> {
    config: &'a Config,
    case: &'a CaseDef,
    mode: Mode,
    rep: usize,
}

impl Run<'_> {
    fn column_names(&self) -> Vec<String> {
        if self.case.cols == 1 {
            vec![COLUMN.to_string()]
        } else {
            (0..self.case.cols).map(|c| format!("c{c}")).collect()
        }
    }

    /// Every column with the generator that produced it.
    fn column_gens(&self) -> Vec<(String, ValueGen)> {
        if self.case.cols == 1 {
            vec![(COLUMN.to_string(), self.case.values.clone())]
        } else {
            (0..self.case.cols)
                .map(|c| (format!("c{c}"), wide_column_gen(c)))
                .collect()
        }
    }

    fn common_fields(&self, layer: Layer) -> serde_json::Map<String, Value> {
        let mut map = serde_json::Map::new();
        map.insert("layer".into(), json!(layer.name()));
        map.insert("case".into(), json!(self.case.name));
        map.insert("mode".into(), json!(self.mode.name()));
        map.insert("rep".into(), json!(self.rep));
        map.insert(
            "datagen_seed".into(),
            json!(datagen_seed(&self.case.name, self.rep)),
        );
        map.insert("rows".into(), json!(self.case.rows(self.config.rows)));
        map.insert("rows_per_file".into(), json!(self.config.rows_per_file));
        map.insert("batch_rows".into(), json!(self.config.batch_rows));
        map.insert("rows_per_zone".into(), json!(self.case.rows_per_zone));
        map.insert("cols".into(), json!(self.case.cols));
        map
    }

    fn input(&self) -> Input {
        generate(
            self.case,
            datagen_seed(&self.case.name, self.rep),
            self.case.rows(self.config.rows),
            self.config.batch_rows,
            self.config.rows_per_file,
        )
    }

    /// Drive the seed writer directly, finishing at every file boundary.
    fn micro(&self) -> Value {
        let input = self.input();
        let mut record = self.common_fields(Layer::Micro);
        record.insert("input_bytes".into(), json!(input.input_bytes()));
        record.insert("input_rss_mib".into(), json!(mib(current_rss_bytes())));

        let data_type = input.schema.field(0).data_type().clone();
        let rows_per_file = self.config.rows_per_file;
        let rows_per_zone = self.case.rows_per_zone;
        let batches = &input.batches;
        let (out, measured) = measure(|| {
            let mut writer =
                ZoneMapSeedWriter::new(COLUMN, rows_per_zone, data_type).expect("seed writer");
            let mut observe = Duration::ZERO;
            let mut finish = Duration::ZERO;
            let mut seed_bytes = 0u64;
            let mut files = 0u64;
            let mut rows_in_file = 0usize;
            for batch in batches {
                let column = batch.column(0);
                let start = Instant::now();
                writer.observe_batch(column).expect("observe");
                observe += start.elapsed();
                rows_in_file += batch.num_rows();
                if rows_in_file >= rows_per_file {
                    let start = Instant::now();
                    seed_bytes += writer
                        .finish()
                        .expect("finish")
                        .map_or(0, |b| b.len() as u64);
                    finish += start.elapsed();
                    files += 1;
                    rows_in_file = 0;
                }
            }
            if rows_in_file > 0 {
                let start = Instant::now();
                seed_bytes += writer
                    .finish()
                    .expect("finish")
                    .map_or(0, |b| b.len() as u64);
                finish += start.elapsed();
                files += 1;
            }
            (observe, finish, seed_bytes, files)
        });
        let (observe, finish, seed_bytes, files) = out;
        let rows = input.total_rows();
        record.insert("wall_ms".into(), json!(measured.wall.as_secs_f64() * 1e3));
        record.insert("observe_ms".into(), json!(observe.as_secs_f64() * 1e3));
        record.insert("finish_ms".into(), json!(finish.as_secs_f64() * 1e3));
        record.insert(
            "observe_ns_per_row".into(),
            json!(observe.as_nanos() as f64 / rows as f64),
        );
        record.insert(
            "rows_per_s".into(),
            json!(rows as f64 / measured.wall.as_secs_f64()),
        );
        record.insert("cpu_s".into(), json!(measured.cpu_secs));
        record.insert("baseline_rss_mib".into(), json!(mib(measured.baseline_rss)));
        record.insert("peak_rss_mib".into(), json!(mib(measured.peak_rss)));
        record.insert(
            "extra_rss_mib".into(),
            json!(mib(measured.peak_rss.saturating_sub(measured.baseline_rss))),
        );
        record.insert("seed_bytes".into(), json!(seed_bytes));
        record.insert("seed_files".into(), json!(files));
        record.insert(
            "seed_bytes_per_file".into(),
            json!(seed_bytes as f64 / files.max(1) as f64),
        );
        record.insert(
            "seed_ratio".into(),
            json!(seed_bytes as f64 / input.input_bytes().max(1) as f64),
        );
        Value::Object(record)
    }

    /// Create the table and index, then time one append.
    fn e2e(&self, rt: &tokio::runtime::Runtime) -> Value {
        let dir = self
            .config
            .dataset_dir(&self.case.name, self.mode, self.rep);
        if dir.exists() {
            std::fs::remove_dir_all(&dir).expect("remove old dataset");
        }
        std::fs::create_dir_all(&dir).expect("create dataset dir");
        let uri = dir.to_str().expect("utf8 path").to_string();

        let input = self.input();
        // The table is created with the storage type even when the appended
        // batches are views.
        let base_case = match &self.case.values {
            ValueGen::SentenceUtf8View(min, max) => CaseDef {
                values: ValueGen::Sentence(*min, *max),
                ..self.case.clone()
            },
            _ => self.case.clone(),
        };
        let base = generate(
            &base_case,
            datagen_seed(&self.case.name, self.rep) ^ 0x5eed_ba5e,
            self.config.base_rows,
            self.config.batch_rows,
            self.config.rows_per_file,
        );

        let use_seeds = self.mode == Mode::SeedsOn;
        let columns = self.column_names();
        let rows_per_file = self.config.rows_per_file;
        let rows_per_zone = self.case.rows_per_zone;
        rt.block_on(async {
            let mut dataset = Dataset::write(
                base.reader(),
                uri.as_str(),
                Some(WriteParams {
                    mode: WriteMode::Create,
                    max_rows_per_file: rows_per_file,
                    ..Default::default()
                }),
            )
            .await
            .expect("write base table");
            for column in &columns {
                let params = ScalarIndexParams::for_builtin(BuiltinIndexType::ZoneMap)
                    .with_params(&json!({"rows_per_zone": rows_per_zone, "use_seeds": use_seeds}));
                dataset
                    .create_index(&[column.as_str()], IndexType::ZoneMap, None, &params, false)
                    .await
                    .expect("create zone map index");
            }
        });
        drop(base);

        let mut record = self.common_fields(Layer::E2e);
        record.insert("input_bytes".into(), json!(input.input_bytes()));
        record.insert("input_rss_mib".into(), json!(mib(current_rss_bytes())));

        let reader = input.reader();
        let (dataset, measured) = measure(|| {
            rt.block_on(Dataset::write(
                reader,
                uri.as_str(),
                Some(WriteParams {
                    mode: WriteMode::Append,
                    max_rows_per_file: rows_per_file,
                    ..Default::default()
                }),
            ))
            .expect("append")
        });

        let rows = input.total_rows();
        record.insert("wall_ms".into(), json!(measured.wall.as_secs_f64() * 1e3));
        record.insert(
            "rows_per_s".into(),
            json!(rows as f64 / measured.wall.as_secs_f64()),
        );
        record.insert("cpu_s".into(), json!(measured.cpu_secs));
        record.insert("baseline_rss_mib".into(), json!(mib(measured.baseline_rss)));
        record.insert("peak_rss_mib".into(), json!(mib(measured.peak_rss)));
        record.insert(
            "extra_rss_mib".into(),
            json!(mib(measured.peak_rss.saturating_sub(measured.baseline_rss))),
        );

        // Everything below is outside the timed region.
        let base_fragments = 1usize;
        let (data_bytes, seed_bytes, seeded_files, new_fragments) =
            rt.block_on(inspect_seeds(&dataset, &uri, base_fragments, &columns));
        assert_eq!(
            seeded_files,
            if use_seeds {
                new_fragments * columns.len()
            } else {
                0
            },
            "seed buffers present must match the mode"
        );
        record.insert("new_fragments".into(), json!(new_fragments));
        record.insert("data_bytes".into(), json!(data_bytes));
        record.insert("seed_bytes".into(), json!(seed_bytes));
        record.insert(
            "seed_ratio".into(),
            json!(seed_bytes as f64 / data_bytes.max(1) as f64),
        );
        record.insert(
            "file_version".into(),
            json!(format!("{:?}", dataset.manifest.data_storage_format)),
        );
        Value::Object(record)
    }

    /// Open the dataset `e2e` wrote with a fresh session and time the index update.
    fn benefit(&self, rt: &tokio::runtime::Runtime) -> Value {
        let dir = self
            .config
            .dataset_dir(&self.case.name, self.mode, self.rep);
        let uri = dir.to_str().expect("utf8 path").to_string();
        let counter = SeedEventCounter::default();
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(counter.clone()),
        )
        .expect("install tracing subscriber");

        let session = Arc::new(Session::default());
        let mut dataset = rt
            .block_on(
                DatasetBuilder::from_uri(&uri)
                    .with_session(session.clone())
                    .load(),
            )
            .expect("open dataset");
        let stores = session.store_registry().active_stores();
        for store in &stores {
            store.io_stats_incremental();
        }

        let mut record = self.common_fields(Layer::Benefit);
        record.insert("input_rss_mib".into(), json!(mib(current_rss_bytes())));
        let (_, measured) = measure(|| {
            rt.block_on(dataset.optimize_indices(&OptimizeOptions::default()))
                .expect("optimize indices")
        });
        let (read_iops, read_bytes) = stores.iter().fold((0u64, 0u64), |(iops, bytes), store| {
            let stats = store.io_stats_snapshot();
            (iops + stats.read_iops, bytes + stats.read_bytes)
        });
        record.insert(
            "optimize_ms".into(),
            json!(measured.wall.as_secs_f64() * 1e3),
        );
        record.insert("cpu_s".into(), json!(measured.cpu_secs));
        record.insert("baseline_rss_mib".into(), json!(mib(measured.baseline_rss)));
        record.insert("peak_rss_mib".into(), json!(mib(measured.peak_rss)));
        record.insert(
            "extra_rss_mib".into(),
            json!(mib(measured.peak_rss.saturating_sub(measured.baseline_rss))),
        );
        record.insert("read_iops".into(), json!(read_iops));
        record.insert("read_bytes".into(), json!(read_bytes));
        let harvested = counter.harvested.load(Ordering::Relaxed);
        let fallback = counter.fallback.load(Ordering::Relaxed);
        let reasons = counter.reasons.lock().unwrap().clone();
        record.insert("harvest_events".into(), json!(harvested));
        record.insert("fallback_events".into(), json!(fallback));
        record.insert("fallback_reasons".into(), json!(reasons));
        let cols = self.case.cols as u64;
        match self.mode {
            Mode::SeedsOn => assert!(
                harvested == cols && fallback == 0,
                "seeds on: expected {cols} harvests and no fallback, got {harvested}/{fallback} {reasons:?}"
            ),
            Mode::SeedsOff => assert!(
                harvested == 0 && fallback == cols,
                "seeds off: expected {cols} fallbacks and no harvest, got {harvested}/{fallback} {reasons:?}"
            ),
        }

        // Which zone statistics the index actually carries after the update:
        // types without min/max support only contribute null counts.
        if self.mode == Mode::SeedsOn {
            let reopened = rt
                .block_on(
                    DatasetBuilder::from_uri(&uri)
                        .with_session(Arc::new(Session::default()))
                        .load(),
                )
                .expect("reopen dataset");
            let mut zones = 0usize;
            let mut with_bounds = 0usize;
            for column in self.column_names() {
                let batch = rt.block_on(read_zones(&reopened, &column));
                let min = batch.column_by_name("min").expect("min");
                zones += batch.num_rows();
                with_bounds += batch.num_rows() - min.null_count();
            }
            record.insert("zones".into(), json!(zones));
            record.insert("zones_with_bounds".into(), json!(with_bounds));
        }

        if self.config.validate.applies(self.rep) && self.mode == Mode::SeedsOn {
            let other = self
                .config
                .dataset_dir(&self.case.name, Mode::SeedsOff, self.rep);
            let report = rt.block_on(validate(
                &uri,
                other.to_str().expect("utf8 path"),
                &self.column_gens(),
            ));
            record.insert("validated".into(), json!(report));
        }
        Value::Object(record)
    }
}

/// Sum data file sizes and seed buffer sizes of the fragments appended after
/// `base_fragments`. Returns (data bytes, seed bytes, seeded column-files,
/// new fragments).
async fn inspect_seeds(
    dataset: &Dataset,
    uri: &str,
    base_fragments: usize,
    columns: &[String],
) -> (u64, u64, usize, usize) {
    let (store, base) = ObjectStore::from_uri(uri).await.expect("object store");
    let scheduler = ScanScheduler::new(store.clone(), SchedulerConfig::max_bandwidth(&store));
    let cache = LanceCache::no_cache();
    let mut data_bytes = 0u64;
    let mut seed_bytes = 0u64;
    let mut seeded = 0usize;
    let fragments = dataset.fragments();
    let new_fragments = &fragments[base_fragments.min(fragments.len())..];
    for fragment in new_fragments {
        for file in &fragment.files {
            let path = base.clone().join("data").join(file.path.as_str());
            data_bytes += store.size(&path).await.expect("file size");
            let file_scheduler = scheduler
                .open_file(&path, &CachedFileSize::unknown())
                .await
                .expect("open data file");
            let reader = FileReader::try_open(
                file_scheduler,
                None,
                Default::default(),
                &cache,
                FileReaderOptions::default(),
            )
            .await
            .expect("file reader");
            let metadata = reader.metadata().file_schema.metadata.clone();
            for column in columns {
                let key = format!("{SEED_META_KEY_PREFIX}{column}");
                let Some(value) = metadata.get(&key) else {
                    continue;
                };
                let buf_index: u32 = value
                    .split(':')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .expect("seed buffer index");
                seed_bytes += reader
                    .read_global_buffer(buf_index)
                    .await
                    .expect("seed buffer")
                    .len() as u64;
                seeded += 1;
            }
        }
    }
    (data_bytes, seed_bytes, seeded, new_fragments.len())
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Read every zone of every segment of `column`'s zone map, sorted by
/// (fragment_id, zone_start).
async fn read_zones(dataset: &Dataset, column: &str) -> RecordBatch {
    let indices = dataset.load_indices().await.expect("load indices");
    let mut batches = Vec::new();
    for index in indices.iter().filter(|i| i.name == format!("{column}_idx")) {
        let store = LanceIndexStore::from_dataset_for_existing(dataset, index)
            .await
            .expect("index store");
        let reader = store
            .open_index_file(ZONEMAP_INDEX_FILE)
            .await
            .expect("open zonemap file");
        let rows = reader.num_rows();
        if rows == 0 {
            continue;
        }
        batches.push(reader.read_range(0..rows, None).await.expect("read zones"));
    }
    assert!(!batches.is_empty(), "no zone map segments for {column}");
    let schema = batches[0].schema();
    let all = arrow_select::concat::concat_batches(&schema, &batches).expect("concat zones");
    let sort_columns = vec![
        arrow_ord::sort::SortColumn {
            values: all
                .column_by_name("fragment_id")
                .expect("fragment_id")
                .clone(),
            options: None,
        },
        arrow_ord::sort::SortColumn {
            values: all
                .column_by_name("zone_start")
                .expect("zone_start")
                .clone(),
            options: None,
        },
    ];
    let indices = arrow_ord::sort::lexsort_to_indices(&sort_columns, None).expect("sort zones");
    arrow_select::take::take_record_batch(&all, &indices).expect("take zones")
}

/// Float arrays are equal when validity matches and valid values have the
/// same bit pattern, so NaN and signed zeros are compared exactly.
fn arrays_equal(left: &ArrayRef, right: &ArrayRef) -> bool {
    if left.len() != right.len() || left.data_type() != right.data_type() {
        return false;
    }
    match left.data_type() {
        DataType::Float32 => {
            let l = left.as_any().downcast_ref::<Float32Array>().unwrap();
            let r = right.as_any().downcast_ref::<Float32Array>().unwrap();
            (0..l.len()).all(|i| {
                l.is_null(i) == r.is_null(i)
                    && (l.is_null(i) || l.value(i).to_bits() == r.value(i).to_bits())
            })
        }
        DataType::Float64 => {
            let l = left.as_any().downcast_ref::<Float64Array>().unwrap();
            let r = right.as_any().downcast_ref::<Float64Array>().unwrap();
            (0..l.len()).all(|i| {
                l.is_null(i) == r.is_null(i)
                    && (l.is_null(i) || l.value(i).to_bits() == r.value(i).to_bits())
            })
        }
        _ => left == right,
    }
}

/// Filters whose row ids are compared with and without the index.
fn predicates(values: &ValueGen, column: &str) -> Vec<String> {
    let mut out = vec![format!("{column} IS NULL"), format!("{column} IS NOT NULL")];
    match values {
        ValueGen::Int32Rand | ValueGen::Int64Rand | ValueGen::Int64Step => {
            out.push(format!("{column} > 0"));
            out.push(format!("{column} BETWEEN 1000 AND 500000"));
            out.push(format!("{column} = 4096"));
        }
        ValueGen::Float64Rand | ValueGen::Float64Nan1Pct | ValueGen::Float64SignedZero => {
            out.push(format!("{column} > 0.0"));
            out.push(format!("{column} < 0.0"));
            out.push(format!("{column} = 0.0"));
            out.push(format!("{column} >= 0.5"));
        }
        ValueGen::Bool => out.push(format!("{column} = true")),
        ValueGen::Date32 => out.push(format!("{column} > DATE '2000-01-01'")),
        ValueGen::TimestampMicros => {
            out.push(format!("{column} > TIMESTAMP '2000-01-01T00:00:00'"))
        }
        ValueGen::Decimal128 => out.push(format!("{column} > 0")),
        ValueGen::Uuid
        | ValueGen::LowCardinality(_)
        | ValueGen::PrefixCounter
        | ValueGen::Sentence(..)
        | ValueGen::SentenceSorted(..)
        | ValueGen::SentenceUtf8View(..) => {
            out.push(format!("{column} > 'm'"));
            out.push(format!("{column} < 'c'"));
            out.push(format!("{column} = 'the'"));
        }
        ValueGen::FixedSizeBinary(_)
        | ValueGen::VarBinary(..)
        | ValueGen::LargeBinary(_)
        | ValueGen::LargeBinarySorted(_)
        | ValueGen::FixedSizeListF32(_)
        | ValueGen::ListInt32
        | ValueGen::Struct => {}
    }
    out
}

async fn row_ids(dataset: &Dataset, filter: &str, use_index: bool) -> Vec<u64> {
    let mut scanner = dataset.scan();
    scanner
        .filter(filter)
        .expect("filter")
        .project::<&str>(&[])
        .expect("project")
        .with_row_id()
        .use_scalar_index(use_index);
    let batch = scanner.try_into_batch().await.expect("scan");
    let mut ids: Vec<u64> = batch
        .column_by_name("_rowid")
        .expect("_rowid")
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("u64 row ids")
        .values()
        .to_vec();
    ids.sort_unstable();
    ids
}

/// Compare the seed-built index at `seeded_uri` with the scan-built index at
/// `scanned_uri`, zone by zone, then compare filter results with and without
/// the index. Panics on any mismatch and returns a short description otherwise.
async fn validate(seeded_uri: &str, scanned_uri: &str, columns: &[(String, ValueGen)]) -> Value {
    let seeded = DatasetBuilder::from_uri(seeded_uri)
        .with_session(Arc::new(Session::default()))
        .load()
        .await
        .expect("open seeded dataset");
    let scanned = DatasetBuilder::from_uri(scanned_uri)
        .with_session(Arc::new(Session::default()))
        .load()
        .await
        .expect("open scanned dataset");
    let mut zones_checked = 0usize;
    let mut filters_checked = 0usize;
    for (column, values) in columns {
        let from_seeds = read_zones(&seeded, column).await;
        let from_scan = read_zones(&scanned, column).await;
        assert_eq!(
            from_seeds.num_rows(),
            from_scan.num_rows(),
            "{column}: zone count differs"
        );
        for name in [
            "fragment_id",
            "zone_start",
            "zone_length",
            "null_count",
            "nan_count",
            "min",
            "max",
        ] {
            let l = from_seeds.column_by_name(name).expect(name);
            let r = from_scan.column_by_name(name).expect(name);
            assert!(
                arrays_equal(l, r),
                "{column}: zone column {name} differs between seed-built and scan-built index"
            );
        }
        zones_checked += from_seeds.num_rows();

        for filter in predicates(values, column) {
            let with_seeded_index = row_ids(&seeded, &filter, true).await;
            let with_scanned_index = row_ids(&scanned, &filter, true).await;
            let without_index = row_ids(&seeded, &filter, false).await;
            assert!(
                with_seeded_index == without_index,
                "{filter}: seed-built index returns different rows than a scan"
            );
            assert!(
                with_scanned_index == without_index,
                "{filter}: scan-built index returns different rows than a scan"
            );
            filters_checked += 1;
        }
    }
    json!({"zones": zones_checked, "filters": filters_checked})
}

// ---------------------------------------------------------------------------
// Parent
// ---------------------------------------------------------------------------

fn run_child(config: &Config, layer: Layer, case: &CaseDef, mode: Mode, rep: usize) {
    let exe = env::current_exe().expect("current exe");
    let mut command = Command::new(exe);
    command
        .env("ZMSEED_CHILD", "1")
        .env("ZMSEED_LAYER", layer.name())
        .env("ZMSEED_CASE", &case.name)
        .env("ZMSEED_MODE", mode.name())
        .env("ZMSEED_REP", rep.to_string());
    let started = Instant::now();
    let mut child = command.spawn().expect("spawn child");
    let pid_file = config.pid_dir.join(child.id().to_string());
    std::fs::write(
        &pid_file,
        format!(
            "{} {} {} rep{}\n",
            layer.name(),
            case.name,
            mode.name(),
            rep
        ),
    )
    .expect("record pid");
    let status = child.wait().expect("wait for child");
    let _ = std::fs::remove_file(&pid_file);
    println!(
        "{:<8} {:<24} {:<4} rep{} {:>8.1}s {}",
        layer.name(),
        case.name,
        mode.name(),
        rep,
        started.elapsed().as_secs_f64(),
        if status.success() { "ok" } else { "FAILED" }
    );
    assert!(
        status.success(),
        "child failed: {layer:?} {} {mode:?} rep{rep}",
        case.name
    );
}

fn child_main(config: &Config) {
    let layer = Layer::parse(&env::var("ZMSEED_LAYER").unwrap()).unwrap();
    let name = env::var("ZMSEED_CASE").unwrap();
    let mode = Mode::parse(&env::var("ZMSEED_MODE").unwrap()).unwrap();
    let rep: usize = env::var("ZMSEED_REP").unwrap().parse().unwrap();
    let case = all_cases(config.wide_cols)
        .into_iter()
        .find(|c| c.name == name)
        .unwrap_or_else(|| panic!("unknown case {name}"));
    let run = Run {
        config,
        case: &case,
        mode,
        rep,
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let record = match layer {
        Layer::Micro => run.micro(),
        Layer::E2e => run.e2e(&rt),
        Layer::Benefit => run.benefit(&rt),
    };
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&config.out)
        .expect("open OUT");
    writeln!(file, "{record}").expect("write record");
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    values[values.len() / 2]
}

fn summarize(out: &StdPath) {
    let Ok(text) = std::fs::read_to_string(out) else {
        return;
    };
    let mut groups: BTreeMap<(String, String, String), Vec<Value>> = BTreeMap::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let value: Value = serde_json::from_str(line).expect("results line");
        let key = (
            value["layer"].as_str().unwrap().to_string(),
            value["case"].as_str().unwrap().to_string(),
            value["mode"].as_str().unwrap().to_string(),
        );
        groups.entry(key).or_default().push(value);
    }
    println!();
    println!(
        "{:<8} {:<24} {:<4} {:>3} {:>12} {:>12} {:>9} {:>10} {:>12} {:>8}",
        "layer",
        "case",
        "mode",
        "n",
        "wall_ms",
        "rows/s",
        "cpu_s",
        "extra_mib",
        "seed_bytes",
        "harvest"
    );
    for ((layer, case, mode), records) in groups {
        let stat = |key: &str| -> f64 {
            let mut values: Vec<f64> = records
                .iter()
                .filter_map(|r| r.get(key).and_then(Value::as_f64))
                .collect();
            if values.is_empty() {
                f64::NAN
            } else {
                median(&mut values)
            }
        };
        let wall = if layer == "benefit" {
            stat("optimize_ms")
        } else {
            stat("wall_ms")
        };
        println!(
            "{:<8} {:<24} {:<4} {:>3} {:>12.1} {:>12.0} {:>9.2} {:>10.1} {:>12.0} {:>8}",
            layer,
            case,
            mode,
            records.len(),
            wall,
            stat("rows_per_s"),
            stat("cpu_s"),
            stat("extra_rss_mib"),
            stat("seed_bytes"),
            if layer == "benefit" {
                format!("{}/{}", stat("harvest_events"), stat("fallback_events"))
            } else {
                String::new()
            }
        );
    }
}

fn parent_main(config: &Config) {
    std::fs::create_dir_all(&config.data_dir).expect("create DATA_DIR");
    std::fs::create_dir_all(&config.pid_dir).expect("create PID_DIR");
    let cases: Vec<CaseDef> = all_cases(config.wide_cols)
        .into_iter()
        .filter(|c| {
            config
                .scenarios
                .as_ref()
                .is_none_or(|wanted| wanted.iter().any(|w| w == &c.name))
        })
        .collect();
    if let Some(wanted) = &config.scenarios {
        for name in wanted {
            assert!(cases.iter().any(|c| &c.name == name), "unknown case {name}");
        }
    }
    println!(
        "zone_map_seeds: {} cases, rows={} rows_per_file={} batch_rows={} repeat={} layers={:?} out={}",
        cases.len(),
        config.rows,
        config.rows_per_file,
        config.batch_rows,
        config.repeat,
        config.layers.iter().map(|l| l.name()).collect::<Vec<_>>(),
        config.out.display()
    );

    for case in &cases {
        for rep in 0..config.repeat {
            // Alternate modes within a repetition so drift affects both equally.
            let modes = if rep % 2 == 0 {
                [Mode::SeedsOff, Mode::SeedsOn]
            } else {
                [Mode::SeedsOn, Mode::SeedsOff]
            };
            if config.layers.contains(&Layer::Micro) && case.cols == 1 {
                run_child(config, Layer::Micro, case, Mode::SeedsOn, rep);
            }
            if config.layers.contains(&Layer::E2e) {
                for mode in modes {
                    run_child(config, Layer::E2e, case, mode, rep);
                }
            }
            if config.layers.contains(&Layer::Benefit) {
                // Off first so the scan-built index exists when `on` validates.
                run_child(config, Layer::Benefit, case, Mode::SeedsOff, rep);
                run_child(config, Layer::Benefit, case, Mode::SeedsOn, rep);
            }
            if !config.keep {
                for mode in modes {
                    let _ = std::fs::remove_dir_all(config.dataset_dir(&case.name, mode, rep));
                }
            }
        }
    }
    summarize(&config.out);
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let config = Config::from_env();
    if args.iter().any(|a| a == "--list") {
        for case in all_cases(config.wide_cols) {
            println!("{}", case.name);
        }
        return;
    }
    if env::var("ZMSEED_CHILD").is_ok() {
        child_main(&config);
    } else {
        parent_main(&config);
    }
}
