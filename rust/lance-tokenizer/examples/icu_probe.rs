// Probe: run the ICU tokenizer exactly as the FTS index does, on mixed en+zh queries.
use lance_tokenizer::{IcuTokenizer, TokenStream, Tokenizer};

fn tokens(text: &str) -> Vec<String> {
    let mut tk = IcuTokenizer::default();
    let mut s = tk.token_stream(text);
    let mut out = Vec::new();
    s.process(&mut |t| out.push(t.text.clone()));
    out
}

fn show(label: &str, text: &str) {
    let toks = tokens(text);
    println!("[{label}] input  = {text:?}");
    println!("[{label}] tokens = {toks:?}   (n={})", toks.len());
    println!();
}

fn main() {
    // The exact example the user asked about
    show("mixed", "语言 government 财政 policy economic");

    // Pure-english control (what an en-only query looks like)
    show("en-only", "government policy economic finance market");

    // Pure-chinese control (5 dictionary words, space separated)
    show("zh-only", "语言 财政 经济 政府 政策");

    // A single chinese "word" from a dictionary to see if ICU re-splits it
    for w in [
        "语言",
        "财政",
        "经济",
        "政策",
        "中华人民共和国",
        "人工智能",
        "机器学习",
    ] {
        let t = tokens(w);
        println!("single zh word {w:?} -> {t:?}  (n={})", t.len());
    }
    println!();

    // A single english word (should always be 1 token)
    for w in ["government", "economic", "policy"] {
        let t = tokens(w);
        println!("single en word {w:?} -> {t:?}  (n={})", t.len());
    }
}
