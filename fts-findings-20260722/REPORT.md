# FTS warm-QPS investigation: 225 → 1307 qps @ c64 (2026-07-22)

Single-node bench on lu-take-rig (320-core, 1.2TB RAM, NVMe), dataset
`lu_mmlb_100m_fts_all42lang_100shard_20260715_v3.lance` (100M docs, 42 languages,
bs256/V4 FTS index, ~1.4TiB local), queries = 5-term `match_any` (OR), tier-100-200
words, k=100, distinct words per query (no-repeat), index cache 960GiB, metadata
cache 36GiB, prewarmed, 30s warmup discarded, 180s measured windows, exact scoring
(`wand_factor=1`). Base code: PR #7897 tip (`e35c13320`: late resolution + row-id
column as the sole cache-entry residency).

## Result summary

| stage | c16 | c64 | c128 | c192 |
|---|---|---|---|---|
| baseline (PR #7897, bench projection bug) | 137 / 117ms | 174* / 369ms | 205* / 626ms | 174 / 1106ms |
| + bench fix (`empty_project`) | 227 / 71ms / 20% | 224 / 286ms / 78% | 345 / 371ms / 90% | 174 / 1106ms / 66% |
| + chunked pipeline (`3d842c5`+`8e8aca2`) | 428 / 37ms / 22% | 221 / 290ms / **29%** | 181 / 710ms / 47% | 168 / 1148ms / 66% |
| + WeakSlot (`a2888a2`) | **509 / 31ms / 26%** | **1307 / 49ms / 82%** | **1168 / 110ms / 84%** | **1101 / 174ms / 81%** |

(qps / mean latency / cgroup CPU. * = measured at those concurrencies on the buggy
bench; whole-day arc from the original resolve bug: 4.1 → 137 → 345 → 1307.)

## The three findings, in discovery order

### 1. Bench projection bug (measurement, not engine)

The `fts_qps` bench never set a projection; the lance scanner defaults to ALL
columns, so every query materialized 100 `full_content` documents after top-k.
`scanner.empty_project()` (only `_rowid` + `_score` return) lifted c128 197→345
(+75%) and CPU 76→90%. All prior absolute numbers were understated; relative A/Bs
remain valid since both sides carried the same weight.

### 2. Chunked partition pipeline kills over-scoring (commits `3d842c5`, `8e8aca2`)

Before: one tiny cpu-pool task per partition (353/query; ~80-120K dispatches/sec)
and, at high concurrency, a query's partitions all start scoring in parallel with
shared top-k threshold ≈ 0, so MAXSCORE prunes nothing: `index_comparisons` =
1.31M scored docs per query for k=100 (13,100x over-scoring).

After: partitions are searched in chunks of 16 (`LANCE_FTS_SEARCH_CHUNK`) — the
chunk's postings/DocSets load concurrently, then ONE cpu task searches the 16
partitions sequentially. The shared threshold ratchets between partitions on the
same thread: partition #2 starts with #1's k-th score already published, and so
on. Scoring CPU at ~225 qps dropped **~177 cores → ~6 cores**; c16 doubled to 428
qps @ 37ms. (In-chunk loads must stay concurrent — the first sequential-load
version regressed c128 181 vs 345 by serializing the contended load path.)

Note: chunking exposed rather than lifted the c64 wall — QPS stayed ~225 while
CPU fell to 29%, i.e. 64 in-flight queries × 290ms with idle cores = a lock, not
compute.

### 3. THE WALL: moka read-op bookkeeping; fixed by WeakSlot (commit `a2888a2`)

Flamegraph of the pipelined build at c64 (`flame_c64_pipeline_moka_wall.svg`):
**70.6% of all CPU is self-time in moka `BaseCache::record_read_op`** under
`load_posting_lists → posting_list → MokaCacheBackend::get_or_insert`, plus 14%
more in the row-ids resolve path (same moka get). Actual scoring: 6.3%.

Mechanism: every warm HIT records "this entry was read" into moka's bounded
read-op channel (TinyLFU recency/frequency input). ~2000 cache reads per query ×
225 qps ≈ 500K reads/sec saturate the channel; readers then run inline
housekeeping under a shared lock — a classic convoy (more waiters → slower
critical section), matching c16 (427) > c64 (221) and 29% CPU at 290ms latency.
Same frame was measured at ~40% CPU on a production PE in June; it was masked
then by the resolve bug, materialization, and over-scoring.

Fix: `WeakSlot<T>` — a per-call-site `Weak` in front of the two hot entry kinds
(posting-list groups, row-id columns). A hit upgrades the Weak (one atomic op,
zero cache traffic); every 64th hit deliberately reads through moka so the LRU
still sees recency; the Weak never keeps the value alive, so eviction and
weighed accounting are unchanged (evicted → upgrade fails → single-flight
reload). c64: **225 → 1307 qps**, and the c128+ collapse disappeared.

## Falsified hypotheses (kept for the record)

- `wand_factor > 1` (aggressive/approximate threshold): c128 fell 345 → 124 qps
  at factor 1.2. Killed.
- "Push concurrency to fill CPU": c192/c256 dropped both QPS and CPU
  (66%/58%) pre-fix — oversubscription collapse, not headroom.
- "Streaming load→search overlap helps warm queries": ANALYZE showed a warm
  query is 18.3ms entirely inside MatchQuery with `iops=0, parts_loaded=0` —
  load is free when warm; the old 3-stage pipeline (branch
  `lu/fts_pipeline_experiments`) helps the cold path only. The chunk pipeline's
  value warm is task-rate + threshold propagation, not IO overlap.

## Flamegraphs

- `flame_c64_prechunk_maxscore.svg` — before chunking (160 qps era): 70.9%
  `maxscore_search` (over-scoring visible as scoring CPU), moka ~10%.
- `flame_c64_pipeline_moka_wall.svg` — after chunking (225 qps era): scoring
  6.3%, moka `record_read_op` 70.6% — the wall that WeakSlot removes.

## Branches

- `lu/fts_v3_perf` — working branch, all three commits.
- `lu/fts_20260722_chunk_pipeline` — through the chunked pipeline (`8e8aca2`).
- `lu/fts_20260722_weakslot` — plus WeakSlot (`a2888a2`).
- `lu/fts_pipeline_experiments` — the earlier 3-stage channel pipeline
  (`77f93b6cc`, pre-resolve-fix era).

## Follow-ups

- Productize the three commits as a PR once #7897 merges.
- Recall/result-equivalence spot-check vs baseline (threshold sharing is exact
  WAND semantics; expected identical modulo ties).
- Proper fix for the moka wall at the cache layer: backend-level weak L1 in
  `MokaCacheBackend`, or evaluate a read-optimized backend (e.g. quick_cache)
  behind the existing `CacheBackend` trait; production PE (sophon-caching
  memory tier) likely hits the same wall at scale.
- Remaining headroom: ~18% idle CPU at c64-c192; mild decline past c64.
