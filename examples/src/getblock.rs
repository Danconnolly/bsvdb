use clap::Parser;
use bsv_blockarchive::{BlockArchive, SimpleFileBasedBlockArchive};
use bitcoinsv::bitcoin::{BlockHash, FromHex, FullBlockStream};
use tokio_stream::StreamExt;

/// Get a block from the archive, listing all tx in the block.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The root of the block archive.
    #[clap(index=1)]
    root_dir: String,
    /// The block hash.
    #[clap(index=2)]
    block_hash: String,
}

#[tokio::main]
async fn main() {
    let args: Args = Args::parse();
    let root_dir = std::path::PathBuf::from(args.root_dir);
    let block_hash = BlockHash::from_hex(args.block_hash).unwrap();
    let archive= SimpleFileBasedBlockArchive::new(root_dir).await.unwrap();
    let reader = archive.get_block(&block_hash).await.unwrap();
    let mut block = FullBlockStream::new(reader).await.unwrap();
    println!("Header: {:?}", block.block_header);
    println!("Number of transactions: {}", block.num_tx);
    while let Some(tx) = block.next().await {
        let tx = tx.unwrap();
        println!("Tx: {:?}", tx);
    }
}
