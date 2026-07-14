// Enumerate the ICU CJK (Chinese/Japanese) dictionary that `IcuTokenizer` uses
// for word segmentation, and dump every known word to a plaintext file.
//
// The dictionary is baked into `icu_segmenter` as a `Char16Trie` (a 16-bit-unit
// trie keyed by Han characters). We load it via the crate's `Baked` provider,
// then DFS over the trie: every path from the root that reaches a value node is
// one dictionary word. The trie iterator is not cloneable, so at each node we
// re-walk the accumulated prefix from the root before trying the next char.
//
// Run: cargo run -p lance-tokenizer --example dump_icu_dict
// Output: /Users/lu/Projects/Github/Work/mmlb/data/icu_cjk_dict.txt

use icu_collections::char16trie::{Char16Trie, TrieResult};
use icu_provider::DataRequestMetadata;
use icu_provider::prelude::*;
use icu_segmenter::provider::{Baked, SegmenterDictionaryAutoV1};
use std::fs;

// The CJK dictionary is stored under this marker attribute (see icu_segmenter
// `complex/mod.rs`: `CJ_DICT = "cjdict"`), loaded with prefix-matching on.
const CJ_DICT: &DataMarkerAttributes = DataMarkerAttributes::from_str_or_panic("cjdict");

const OUT: &str = "/Users/lu/Projects/Github/Work/mmlb/data/icu_cjk_dict.txt";

/// The candidate alphabet: CJK code points the dictionary could contain.
/// Covers CJK Unified Ideographs + Ext A + the common radicals/compat range.
/// (Ext B..F are rare in this dict and would blow up the DFS fanout; the base
/// plane covers essentially all words the segmenter uses for modern text.)
fn cjk_alphabet() -> Vec<char> {
    let mut v = Vec::new();
    // CJK Unified Ideographs
    for cp in 0x4E00u32..=0x9FFF {
        if let Some(c) = char::from_u32(cp) {
            v.push(c);
        }
    }
    // CJK Unified Ideographs Extension A
    for cp in 0x3400u32..=0x4DBF {
        if let Some(c) = char::from_u32(cp) {
            v.push(c);
        }
    }
    // CJK Compatibility Ideographs (some dict entries use these)
    for cp in 0xF900u32..=0xFAFF {
        if let Some(c) = char::from_u32(cp) {
            v.push(c);
        }
    }
    v
}

/// Walk `prefix` from the trie root and return the TrieResult of the LAST char
/// (or None if the prefix hits a dead end partway). Fresh iterator each call.
fn walk(trie: &Char16Trie, prefix: &[char]) -> Option<TrieResult> {
    let mut it = trie.iter();
    let mut last = TrieResult::NoValue;
    for &c in prefix {
        last = it.next(c);
        if matches!(last, TrieResult::NoMatch) {
            return None;
        }
    }
    Some(last)
}

fn main() {
    // Load the baked CJK dictionary payload (singleton under the "cjdict" attr).
    let resp: DataResponse<SegmenterDictionaryAutoV1> = Baked
        .load(DataRequest {
            id: DataIdentifierBorrowed::for_marker_attributes(CJ_DICT),
            metadata: {
                let mut m = DataRequestMetadata::default();
                m.attributes_prefix_match = true;
                m
            },
        })
        .expect("load SegmenterDictionaryAutoV1 from baked data");
    let dict = resp.payload.get();
    let trie = Char16Trie::new(dict.trie_data.clone());

    let alphabet = cjk_alphabet();
    println!(
        "alphabet: {} candidate CJK code points; DFS over trie...",
        alphabet.len()
    );

    // Iterative DFS. Each stack entry is a prefix that is a valid trie path
    // (last char returned NoValue / Intermediate / FinalValue, i.e. not NoMatch).
    // We seed with every single char that is a valid first step.
    let mut words: Vec<String> = Vec::new();
    let mut stack: Vec<Vec<char>> = Vec::new();

    for &c in &alphabet {
        match walk(&trie, &[c]) {
            None => {}
            Some(res) => {
                if matches!(res, TrieResult::Intermediate(_) | TrieResult::FinalValue(_)) {
                    words.push(c.to_string());
                }
                // FinalValue means no longer word extends this prefix; don't descend.
                if !matches!(res, TrieResult::FinalValue(_) | TrieResult::NoMatch) {
                    stack.push(vec![c]);
                }
            }
        }
    }

    let mut visited_prefixes: u64 = 0;
    while let Some(prefix) = stack.pop() {
        for &c in &alphabet {
            let mut next = prefix.clone();
            next.push(c);
            visited_prefixes += 1;
            match walk(&trie, &next) {
                None => {} // NoMatch: dead end, prune
                Some(res) => match res {
                    TrieResult::NoMatch => {}
                    TrieResult::NoValue => {
                        // valid prefix, not a word, descend
                        stack.push(next);
                    }
                    TrieResult::Intermediate(_) => {
                        // a word AND a prefix of longer words: record + descend
                        words.push(next.iter().collect());
                        stack.push(next);
                    }
                    TrieResult::FinalValue(_) => {
                        // a word and a leaf: record, don't descend
                        words.push(next.iter().collect());
                    }
                },
            }
        }
        if visited_prefixes % 5_000_000 == 0 {
            println!(
                "  ...{} prefixes probed, {} words so far, {} pending",
                visited_prefixes,
                words.len(),
                stack.len()
            );
        }
    }

    words.sort();
    words.dedup();
    let by_len = |n: usize| words.iter().filter(|w| w.chars().count() == n).count();
    println!(
        "\nDONE: {} unique dictionary words ({} prefixes probed)",
        words.len(),
        visited_prefixes
    );
    println!(
        "  by char length: 1={}, 2={}, 3={}, 4={}, 5+={}",
        by_len(1),
        by_len(2),
        by_len(3),
        by_len(4),
        words.iter().filter(|w| w.chars().count() >= 5).count()
    );

    fs::write(OUT, words.join("\n") + "\n").expect("write dict");
    println!("  wrote: {OUT}");
}
