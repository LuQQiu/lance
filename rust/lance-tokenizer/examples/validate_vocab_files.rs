// Final validation: every word in each vocab file must be a SINGLE token under
// both the simple and ICU tokenizers, and there must be no duplicates within a
// file. Proves the common/search lists are fully deduped and atomic.
//
// Run: cargo run -p lance-tokenizer --release --example validate_vocab_files

use lance_tokenizer::{IcuTokenizer, SimpleTokenizer, TokenStream, Tokenizer};
use std::collections::HashSet;
use std::fs;

const DATA: &str = "/Users/lu/Projects/Github/Work/mmlb/data";

fn ntok<T: Tokenizer>(t: &mut T, w: &str) -> usize {
    let mut s = t.token_stream(w);
    let mut n = 0;
    while s.next().is_some() {
        n += 1;
    }
    n
}

fn check(file: &str, simple: &mut SimpleTokenizer, icu: &mut IcuTokenizer) {
    let path = format!("{DATA}/{file}");
    let words: Vec<String> = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {path}: {e}"))
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();

    let mut seen = HashSet::new();
    let (mut dup, mut split_simple, mut split_icu) = (0usize, 0usize, 0usize);
    for w in &words {
        if !seen.insert(w.clone()) {
            dup += 1;
        }
        if ntok(simple, w) != 1 {
            split_simple += 1;
        }
        if ntok(icu, w) != 1 {
            split_icu += 1;
        }
    }
    let ok = dup == 0 && split_simple == 0 && split_icu == 0;
    println!(
        "  {:<28} {:>6} words | dup={dup} split(simple)={split_simple} split(icu)={split_icu}  {}",
        file,
        words.len(),
        if ok { "OK" } else { "*** FAIL ***" }
    );
}

fn main() {
    let mut simple = SimpleTokenizer::default();
    let mut icu = IcuTokenizer::default();
    println!("Validating vocab files (1 token under simple AND icu, no dups):");
    for f in [
        "words_en_10k_common.txt",
        "words_en_3k_search.txt",
        "words_zh_10k_common.txt",
        "words_zh_3k_search.txt",
    ] {
        check(f, &mut simple, &mut icu);
    }
}
