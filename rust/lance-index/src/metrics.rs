// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

/// A trait used by the index to report metrics
///
/// Callers can implement this trait to collect metrics
pub trait MetricsCollector: Send + Sync {
    /// Record partition loads
    ///
    /// Many indices consist of partitions that may need to be loaded
    /// into cache.  For example, an inverted index or ngram index has a
    /// posting list for each token.
    ///
    /// In the ideal case, these shards are in the cache and will not need
    /// to be loaded from disk.  This method should not be called if the
    /// shard is in the cache.
    fn record_parts_loaded(&self, num_parts: usize);

    /// Record a shard load
    fn record_part_load(&self) {
        self.record_parts_loaded(1);
    }

    /// Record an index load
    ///
    /// This should be called when a scalar index is loaded from storage.
    /// It should not be called if the index is already in memory.
    fn record_index_loads(&self, num_indexes: usize);

    /// Record an index load
    fn record_index_load(&self) {
        self.record_index_loads(1);
    }

    /// Record the number of "comparisons" made by the index
    ///
    /// What exactly constitutes a comparison depends on the index type.
    /// For example, a B-tree index may make comparisons while searching for a value.
    /// On the other hand, a bitmap index makes comparisons when computing the intersection
    /// of two bitmaps.
    ///
    /// The goal is to provide some visibility into the compute cost of the search
    fn record_comparisons(&self, num_comparisons: usize);

    /// Record time spent checking the scalar-index cache.
    fn record_index_cache_lookup_time(&self, _duration: Duration) {}

    /// Record time spent loading a scalar index after a cache miss.
    fn record_index_load_time(&self, _duration: Duration) {}

    /// Record time spent loading an index part after a part-cache miss.
    fn record_part_load_time(&self, _duration: Duration) {}

    /// Record time spent finding candidate BTree pages from the top-level lookup.
    fn record_btree_lookup_time(&self, _duration: Duration) {}

    /// Record time spent reading BTree top-level lookup data.
    fn record_btree_lookup_read_time(&self, _duration: Duration) {}

    /// Record time spent deserializing BTree top-level lookup data.
    fn record_btree_deserialize_time(&self, _duration: Duration) {}

    /// Record time spent searching BTree leaf pages.
    fn record_btree_page_search_time(&self, _duration: Duration) {}
}

/// A no-op metrics collector that does nothing
pub struct NoOpMetricsCollector;

impl MetricsCollector for NoOpMetricsCollector {
    fn record_parts_loaded(&self, _num_parts: usize) {}
    fn record_index_loads(&self, _num_indexes: usize) {}
    fn record_comparisons(&self, _num_comparisons: usize) {}
}

#[derive(Default)]
pub struct LocalMetricsCollector {
    pub parts_loaded: AtomicUsize,
    pub index_loads: AtomicUsize,
    pub comparisons: AtomicUsize,
    pub index_cache_lookup_time_ns: AtomicU64,
    pub index_load_time_ns: AtomicU64,
    pub part_load_time_ns: AtomicU64,
    pub btree_lookup_time_ns: AtomicU64,
    pub btree_lookup_read_time_ns: AtomicU64,
    pub btree_deserialize_time_ns: AtomicU64,
    pub btree_page_search_time_ns: AtomicU64,
}

impl LocalMetricsCollector {
    pub fn dump_into(self, other: &dyn MetricsCollector) {
        other.record_parts_loaded(self.parts_loaded.load(Ordering::Relaxed));
        other.record_index_loads(self.index_loads.load(Ordering::Relaxed));
        other.record_comparisons(self.comparisons.load(Ordering::Relaxed));
        other.record_index_cache_lookup_time(Duration::from_nanos(
            self.index_cache_lookup_time_ns.load(Ordering::Relaxed),
        ));
        other.record_index_load_time(Duration::from_nanos(
            self.index_load_time_ns.load(Ordering::Relaxed),
        ));
        other.record_part_load_time(Duration::from_nanos(
            self.part_load_time_ns.load(Ordering::Relaxed),
        ));
        other.record_btree_lookup_time(Duration::from_nanos(
            self.btree_lookup_time_ns.load(Ordering::Relaxed),
        ));
        other.record_btree_lookup_read_time(Duration::from_nanos(
            self.btree_lookup_read_time_ns.load(Ordering::Relaxed),
        ));
        other.record_btree_deserialize_time(Duration::from_nanos(
            self.btree_deserialize_time_ns.load(Ordering::Relaxed),
        ));
        other.record_btree_page_search_time(Duration::from_nanos(
            self.btree_page_search_time_ns.load(Ordering::Relaxed),
        ));
    }
}

impl MetricsCollector for LocalMetricsCollector {
    fn record_parts_loaded(&self, num_parts: usize) {
        self.parts_loaded.fetch_add(num_parts, Ordering::Relaxed);
    }

    fn record_index_loads(&self, num_indexes: usize) {
        self.index_loads.fetch_add(num_indexes, Ordering::Relaxed);
    }

    fn record_comparisons(&self, num_comparisons: usize) {
        self.comparisons
            .fetch_add(num_comparisons, Ordering::Relaxed);
    }

    fn record_index_cache_lookup_time(&self, duration: Duration) {
        self.index_cache_lookup_time_ns
            .fetch_add(duration_to_nanos(duration), Ordering::Relaxed);
    }

    fn record_index_load_time(&self, duration: Duration) {
        self.index_load_time_ns
            .fetch_add(duration_to_nanos(duration), Ordering::Relaxed);
    }

    fn record_part_load_time(&self, duration: Duration) {
        self.part_load_time_ns
            .fetch_add(duration_to_nanos(duration), Ordering::Relaxed);
    }

    fn record_btree_lookup_time(&self, duration: Duration) {
        self.btree_lookup_time_ns
            .fetch_add(duration_to_nanos(duration), Ordering::Relaxed);
    }

    fn record_btree_lookup_read_time(&self, duration: Duration) {
        self.btree_lookup_read_time_ns
            .fetch_add(duration_to_nanos(duration), Ordering::Relaxed);
    }

    fn record_btree_deserialize_time(&self, duration: Duration) {
        self.btree_deserialize_time_ns
            .fetch_add(duration_to_nanos(duration), Ordering::Relaxed);
    }

    fn record_btree_page_search_time(&self, duration: Duration) {
        self.btree_page_search_time_ns
            .fetch_add(duration_to_nanos(duration), Ordering::Relaxed);
    }
}

fn duration_to_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
