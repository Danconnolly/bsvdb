use clap::Parser;
use bsv_blockarchive::{BlockArchive, SimpleFileBasedBlockArchive};
use tokio_stream::StreamExt;

/// Lists all blocks in the block archive.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The root of the block archive.
    #[clap(index=1)]
    root_dir: String,
}

#[tokio::main]
async fn main() {
    let args: Args = Args::parse();
    let root_dir = std::path::PathBuf::from(args.root_dir);
    let mut archive= SimpleFileBasedBlockArchive::new(root_dir).await.unwrap();
    let mut results = archive.block_list().await.unwrap();
    while let Some(block_hash) = results.next().await {
        println!("{}", block_hash);
    }
}
