use lance_tokenizer::{IcuTokenizer, TokenStream, Tokenizer};
use std::fs;

fn ntokens(tk: &mut IcuTokenizer, text: &str) -> usize {
    let mut s = tk.token_stream(text);
    let mut n = 0;
    s.process(&mut |_| n += 1);
    n
}

fn main() {
    let path = std::env::args().nth(1).expect("zh word list path");
    let words: Vec<String> = fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();
    let mut tk = IcuTokenizer::default();

    // Per-tier: how many actual ICU tokens does a "word" from this rank produce, on avg?
    let tiers = [
        (1, 100),
        (100, 200),
        (500, 600),
        (1000, 1100),
        (2000, 2100),
        (3000, 3100),
        (4000, 4100),
        (5000, 5100),
        (9000, 10000),
    ];
    println!(
        "{:<12} {:>6} {:>8} {:>10} {:>10}",
        "tier", "words", "tokens", "tok/word", "split%"
    );
    for (lo, hi) in tiers {
        if hi > words.len() + 1 {
            continue;
        }
        let slice = &words[lo - 1..hi - 1];
        let mut toks = 0usize;
        let mut split = 0usize;
        for w in slice {
            let n = ntokens(&mut tk, w);
            toks += n;
            if n > 1 {
                split += 1;
            }
        }
        let nw = slice.len();
        println!(
            "{:<12} {:>6} {:>8} {:>10.3} {:>9.1}%",
            format!("{lo}-{hi}"),
            nw,
            toks,
            toks as f64 / nw as f64,
            100.0 * split as f64 / nw as f64
        );
    }
}
