// Standalone Rust binary to build an inverted index on an existing Lance dataset.
//
// Add as an example in the lance repo at rust/examples/src/build_inverted_index.rs
//
// Usage:
//   build_inverted_index \
//     --uri az://lancedbdatasets/fineweb_edu_full_384_dim.lance \
//     --column text \
//     --with-position \
//     --no-remove-stop-words \
//     --memory-limit-mb 1024000 \
//     --account-name lancedbdevatlas \
//     --account-key <key>

#![allow(clippy::print_stdout)]

use std::time::Instant;

use clap::Parser;
use lance::dataset::builder::DatasetBuilder;
use lance_index::scalar::InvertedIndexParams;
use lance_index::DatasetIndexExt;

#[derive(Parser, Debug)]
#[command(name = "build_inverted_index")]
#[command(about = "Build an inverted (FTS) index on a Lance dataset")]
struct Args {
    /// Dataset URI (e.g. az://bucket/dataset.lance or file:///path/to/dataset.lance)
    #[arg(long)]
    uri: String,

    /// Column to index
    #[arg(long, default_value = "text")]
    column: String,

    /// Store term positions (required for phrase queries)
    #[arg(long, default_value_t = false)]
    with_position: bool,

    /// Remove stop words during indexing
    #[arg(long, default_value_t = true)]
    remove_stop_words: bool,

    /// Build-time memory limit in MiB
    #[arg(long)]
    memory_limit_mb: Option<u64>,

    /// Number of build workers
    #[arg(long)]
    num_workers: Option<usize>,

    /// Replace existing index
    #[arg(long, default_value_t = true)]
    replace: bool,

    /// Azure storage account name (or set AZURE_STORAGE_ACCOUNT_NAME env var)
    #[arg(long, env = "AZURE_STORAGE_ACCOUNT_NAME")]
    account_name: Option<String>,

    /// Azure storage account key (or set AZURE_STORAGE_ACCOUNT_KEY env var)
    #[arg(long, env = "AZURE_STORAGE_ACCOUNT_KEY")]
    account_key: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let args = Args::parse();

    let remove_stop_words = args.remove_stop_words;

    println!("=== Build Inverted Index ===");
    println!("URI:               {}", args.uri);
    println!("Column:            {}", args.column);
    println!("with_position:     {}", args.with_position);
    println!("remove_stop_words: {}", remove_stop_words);
    if let Some(ml) = args.memory_limit_mb {
        println!("memory_limit_mb:   {}", ml);
    }
    if let Some(nw) = args.num_workers {
        println!("num_workers:       {}", nw);
    }
    println!("replace:           {}", args.replace);
    println!();

    // Open dataset
    println!("Opening dataset...");
    let open_start = Instant::now();
    let mut builder = DatasetBuilder::from_uri(&args.uri);
    if let Some(ref name) = args.account_name {
        builder = builder.with_storage_option("account_name", name);
    }
    if let Some(ref key) = args.account_key {
        builder = builder.with_storage_option("account_key", key);
    }
    let mut dataset = builder.load().await?;
    println!("Dataset opened in {:?}", open_start.elapsed());
    println!("  Fragments: {}", dataset.count_fragments());
    println!("  Rows: {}", dataset.count_rows(None).await?);

    // List existing indices
    let indices = dataset.load_indices().await?;
    println!("  Existing indices: {}", indices.len());
    for idx in indices.iter() {
        println!("    - {} (fields: {:?})", idx.name, idx.fields);
    }
    println!();

    // Build index params
    let mut params = InvertedIndexParams::default()
        .with_position(args.with_position)
        .remove_stop_words(remove_stop_words);

    if let Some(ml) = args.memory_limit_mb {
        params = params.memory_limit_mb(ml);
    }
    if let Some(nw) = args.num_workers {
        params = params.num_workers(nw);
    }

    // Create index
    println!("Creating inverted index on column '{}'...", args.column);
    let build_start = Instant::now();
    dataset
        .create_index(
            &[args.column.as_str()],
            lance_index::IndexType::Inverted,
            None,
            &params,
            args.replace,
        )
        .await?;
    let elapsed = build_start.elapsed();
    println!();
    println!("=== Index created ===");
    println!("  Elapsed: {:?}", elapsed);
    println!("  ({:.1} minutes / {:.1} hours)", elapsed.as_secs_f64() / 60.0, elapsed.as_secs_f64() / 3600.0);

    // Verify
    let indices = dataset.load_indices().await?;
    println!("\nFinal indices: {}", indices.len());
    for idx in indices.iter() {
        println!("  - {} (fields: {:?})", idx.name, idx.fields);
    }

    println!("\nDone!");
    Ok(())
}
