// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Index seed writers — compact per-fragment summaries embedded in data files.
//!
//! A seed writer observes column values as they are written to a data file,
//! accumulates compact statistics in memory, and serializes them to a byte
//! buffer that is embedded in the data file footer as a global buffer.
//!
//! The buffer can later be read back during index updates to reconstruct index
//! statistics without re-scanning the column data.

use arrow_array::ArrayRef;
use bytes::Bytes;
use lance_core::Result;

/// Schema metadata key prefix for all seed buffers: `"lance.seed.<column_name>"`.
pub const SEED_META_KEY_PREFIX: &str = "lance.seed.";

/// Seed metadata values are colon-separated. The first segment is always the
/// global buffer index; plugin-specific segments follow. Writers that know the
/// field id of the seeded column append it as the last segment so a harvester
/// can prove the seed belongs to the field it is about to update even after the
/// column was renamed. Values with only two segments come from older writers
/// and carry no field id.
pub const SEED_META_VALUE_SEPARATOR: char = ':';

/// Minimum number of segments a value must have for its last segment to be
/// the field id.
const SEED_META_VALUE_SEGMENTS_WITH_FIELD_ID: usize = 3;

/// The global buffer index recorded in a seed metadata value.
pub fn seed_buffer_index(value: &str) -> Option<u32> {
    value
        .split(SEED_META_VALUE_SEPARATOR)
        .next()
        .and_then(|segment| segment.parse().ok())
}

/// The field id recorded in a seed metadata value, if the writer recorded one.
pub fn seed_field_id(value: &str) -> Option<i32> {
    let segments: Vec<&str> = value.split(SEED_META_VALUE_SEPARATOR).collect();
    if segments.len() < SEED_META_VALUE_SEGMENTS_WITH_FIELD_ID {
        return None;
    }
    segments.last().and_then(|segment| segment.parse().ok())
}

/// A hook registered during data file writes that observes column values batch
/// by batch, accumulates compact statistics in memory, and serializes them to
/// a byte buffer that is embedded in the data file footer as a global buffer.
///
/// The buffer can later be read back during index updates to reconstruct index
/// statistics without re-scanning the column data.
pub trait IndexSeedWriter: Send + std::fmt::Debug {
    /// The column this writer is interested in.
    fn column_name(&self) -> &str;

    /// Observe a slice of column values as they are written to the current fragment.
    /// Called once per batch.
    fn observe_batch(&mut self, values: &ArrayRef) -> Result<()>;

    /// Serialize accumulated state to bytes and reset for the next fragment.
    /// Returns `None` if no data was observed (empty fragment).
    fn finish(&mut self) -> Result<Option<Bytes>>;

    /// Schema metadata key used to record that a seed buffer was written.
    /// Convention: `"lance.seed.<column_name>"`.
    fn schema_metadata_key(&self) -> String;

    /// Create a string to store in the file's schema metadata. This will normally
    /// contain the buffer index (provided by the caller after `add_global_buffer`)
    /// as well as any other information needed to validate or understand the seed
    /// (e.g. `rows_per_zone` for zone map seeds).
    fn schema_metadata_value(&self, buf_index: u32) -> String;
}

/// A pre-harvested seed buffer from a single fragment's data file.
#[derive(Debug, Clone)]
pub struct FragmentSeed {
    pub fragment_id: u64,
    pub bytes: Bytes,
    /// Physical row count of the data file the seed was read from. Plugins
    /// must reject a seed whose zones do not cover exactly this many rows.
    pub num_rows: u64,
    /// The raw value that was stored in the data file's schema metadata under
    /// the seed key (i.e. the output of [`IndexSeedWriter::schema_metadata_value`]).
    /// Plugins can inspect this to validate that the seed is compatible with the
    /// current index configuration before consuming `bytes`.
    pub metadata_value: String,
}
