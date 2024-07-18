use std::path::PathBuf;
use std::collections::{BTreeSet, VecDeque};
use std::io::Cursor;
use bitcoinsv::bitcoin::{BlockHash, FullBlockStream, ToHex};
use bitcoinsv_rpc::{Auth, Client, GetChainTipsResultStatus, RpcApi};
use clap::{Parser, Subcommand};
use bsv_blockarchive::{BlockArchive, SimpleFileBasedBlockArchive, Result, Error};
use tokio_stream::StreamExt;
use url::Url;

/// A simple CLI for managing block archives.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The root of the block archive.
    #[clap(short = 'r', long, env)]
    root_dir: String,
    /// Emit more status messages.
    #[clap(short = 'v', long, default_value = "false")]
    verbose: bool,
    /// Command to perform
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Perform checks on the archive.
    Check {
        #[command(subcommand)]
        check_cmd: CheckCommands,
    },
    /// Get the header of a block
    Header {
        /// Return hex encoded.
        #[clap(short = 'x', long, default_value = "false")]
        hex: bool,
        /// Block hash.
        block_hash: BlockHash,
    },
    /// Import blocks.
    Import {
        #[command(subcommand)]
        import_cmd: ImportCommands,
    },
    /// List all blocks in the archive.
    List,
}

#[derive(Subcommand, Debug)]
enum CheckCommands {
    /// Check that all blocks are linked in the archive (except the Genesis block).  WARNING: this may take a long time.
    Linked,
    /// Consistency check of a single block.
    ///
    /// The consistency check is not block validation. It checks that the block is consistent which
    /// involves reading every transaction, hashing the transaction, and checking that the merkle
    /// root of the transaction hashes matches the value in the header.
    Block {
        /// Block hash.
        block_hash: BlockHash,
    },
    /// Consistency check of all blocks. WARNING: this may take a long time.
    ///
    /// The consistency check is not block validation. It checks that the block is consistent which
    /// involves reading every transaction, hashing the transaction, and checking that the merkle
    /// root of the transaction hashes matches the value in the header.
    Blocks,
}

#[derive(Subcommand, Debug)]
enum ImportCommands {
    /// Import blocks over an RPC connection from an SV Node.
    Rpc {
        /// RCP Connection URI.
        ///
        /// URI should be something like 'http://username:password@127.0.0.1:8332'
        rpc_uri: String,
    }
}

async fn list_blocks(root_dir: PathBuf) -> Result<()>{
    let mut archive= SimpleFileBasedBlockArchive::new(root_dir).await.unwrap();
    let mut results = archive.block_list().await.unwrap();
    while let Some(block_hash) = results.next().await {
        println!("{}", block_hash);
    }
    Ok(())
}

async fn check_links(root_dir: PathBuf) -> Result<()> {
    let mut archive= SimpleFileBasedBlockArchive::new(root_dir).await.unwrap();
    let mut block_it = archive.block_list().await.unwrap();
    // collect all hashes for checking parents
    let mut block_hashes = BTreeSet::new();
    // headers where we didnt find the parent on the first pass
    let mut not_found = Vec::new();
    // for each block
    while let Some(block_hash) = block_it.next().await {
        block_hashes.insert(block_hash);
        let h = archive.block_header(&block_hash).await.unwrap();
        if ! block_hashes.contains(&h.prev_hash) {
            not_found.push(h);
        }
    }
    // check the ones not found yet
    for h in not_found {
        if ! block_hashes.contains(&h.prev_hash) {
            println!("dont have parent of block {}", h.hash())
        }
    }
    Ok(())
}

// check a single block, returns true if all ok, false otherwise
async fn check_single_block(mut block: FullBlockStream) -> Result<bool>{
    // collect transaction hashes
    let mut hashes = VecDeque::new();
    while let Some(tx) = block.next().await {
        match tx {
            Ok(t) => {
                hashes.push_back(t.hash());
            }
            Err(e) => {
                return Err(Error::from(e));
            }
        }
    }
    // calculate merkle root
    while hashes.len() > 1 {
        let mut n = hashes.len();
        while n > 0 {
            n -= 1;
            let h1 = hashes.pop_front().unwrap();
            let h2 = if n == 0 {
                h1
            } else {
                n -= 1;
                hashes.pop_front().unwrap()
            };
            let h = Vec::with_capacity(64);
            let mut c = Cursor::new(h);
            std::io::Write::write(&mut c, &h1.hash).unwrap();
            std::io::Write::write(&mut c, &h2.hash).unwrap();
            let r = BlockHash::sha256d(c.get_ref());
            hashes.push_back(r);
        }
    }
    let m_root = hashes.pop_front().unwrap();
    return Ok(m_root == block.block_header.merkle_root);
}

// check the consistency of a single block
async fn check_block(root_dir: PathBuf, block_hash: BlockHash) -> Result<()> {
    let archive= SimpleFileBasedBlockArchive::new(root_dir).await.unwrap();
    let reader = archive.get_block(&block_hash).await.unwrap();
    let block = FullBlockStream::new(reader).await.unwrap();
    println!("Block hash: {}", block.block_header.hash());
    println!("Number of transactions: {}", block.num_tx);
    let r = check_single_block(block).await.unwrap();
    if r {
        println!("OK: consistency check succeeded block {}", block_hash);
    } else {
        println!("ERROR: merkle root mismatch for block {}", block_hash);
    }
    Ok(())
}

// check all blocks
async fn check_all_blocks(root_dir: PathBuf, verbose: bool) -> Result<()> {
    let mut archive= SimpleFileBasedBlockArchive::new(root_dir).await.unwrap();
    let mut block_it = archive.block_list().await.unwrap();
    let mut num = 0;
    let mut errs = 0;
    while let Some(block_hash) = block_it.next().await {
        let reader = archive.get_block(&block_hash).await.unwrap();
        let block = FullBlockStream::new(reader).await.unwrap();
        num += 1;
        match check_single_block(block).await {
            Ok(r) => {
                if r {
                    if verbose {
                        println!("OK: block {}", block_hash);
                    }
                } else {
                    println!("ERROR: block {}", block_hash);
                    errs += 1;
                }
            }
            Err(_) => {
                println!("ERROR: error reading block {}", block_hash);
                errs += 1;
            }
        }
    }
    if verbose {
        println!("{} blocks checked, {} errors found", num, errs);
    }
    Ok(())
}

async fn header(root_dir: PathBuf, block_hash: BlockHash, hex: bool) -> Result<()> {
    let archive= SimpleFileBasedBlockArchive::new(root_dir).await.unwrap();
    match archive.block_header(&block_hash).await {
        Ok(h) => {
            if hex {
                let x: String = h.encode_hex();
                println!("{}", x);
            } else {
                println!("{:?}", h);
            }
            Ok(())
        },
        Err(e) => {
            match e {
                Error::BlockNotFound => {
                    println!("Block not found");
                    Ok(())
                },
                _ => {
                    Err(e)
                }
            }
        }
    }
}

// connect to an SV node using RPC and import as many blocks as can be found
// for every chain tip:
//      follow chain down until find a block we already have, putting each block on a stack
//      follow chain back up, popping off stack, fetch the block and store it in block archive
async fn rpc_import(root_dir: PathBuf, rpc_uri: String, verbose: bool) -> Result<()> {
    let uri;
    let username;
    let password;
    match Url::parse(&*rpc_uri) {
        Err(_e) => {
            println!("could not parse RPC URI");
            return Ok(());
        }
        Ok(url) => {
            uri = format!("{}://{}:{}/", url.scheme(), url.host_str().unwrap(), url.port().unwrap());
            username = String::from(url.username());
            password = String::from(url.password().unwrap());
        }
    }
    let archive= SimpleFileBasedBlockArchive::new(root_dir).await.unwrap();
    let rpc_client = Client::new(&*uri, Auth::UserPass(username, password), None).unwrap();
    let chain_tips = rpc_client.get_chain_tips().unwrap();
    let num_tips = chain_tips.len();
    let mut known_hashes = BTreeSet::new();     // set of hashes that are known and we either have it already or will get it
    let mut fetched = 0;
    for t in chain_tips {
        if verbose { println!("checking chain tip {}", t.hash);}
        if t.status == GetChainTipsResultStatus::Active || t.status == GetChainTipsResultStatus::ValidFork
            || t.status == GetChainTipsResultStatus::ValidHeaders {
            // follow chain down
            let mut fetch_hashes = Vec::new();              // stack of hashes of blocks to get
            let mut hash = t.hash;
            while ! known_hashes.contains(&hash) {
                known_hashes.insert(hash.clone());
                if ! archive.block_exists(&hash).await.unwrap() {
                    fetch_hashes.push(hash.clone());
                    let h = rpc_client.get_block_header(&hash).unwrap();
                    hash = h.prev_hash;
                }
            }
            if verbose { println!("found known hash {}, need to fetch {} blocks", hash, fetch_hashes.len());}
            // fetch them
            while let Some(h) = fetch_hashes.pop() {
                let mut fb = rpc_client.get_block_binary(&h).await.unwrap();
                archive.store_block(&h, &mut fb).await.unwrap();
                if verbose { println!("stored block {}", h); }
                fetched += 1
            }
        } else {
            // todo: this ignores the entire chain tip, there might be blocks down there that we should get
            if verbose { println!("ignoring chain tip {}", t.hash);}
        }
    }
    println!("checked {} chain tips, imported {} blocks ", num_tips, fetched);
    Ok(())
}



#[tokio::main]
async fn main() {
    let args: Args = Args::parse();
    let root_dir = std::path::PathBuf::from(args.root_dir);
    match args.cmd {
        Commands::Check{check_cmd} => {
            match check_cmd {
                CheckCommands::Linked => {
                    check_links(root_dir).await.unwrap();
                }
                CheckCommands::Block{block_hash} => {
                    check_block(root_dir, block_hash).await.unwrap();
                }
                CheckCommands::Blocks => {
                    check_all_blocks(root_dir, args.verbose).await.unwrap();
                }
            }
        }
        Commands::Header{hex, block_hash} => {
            header(root_dir, block_hash, hex).await.unwrap();
        }
        Commands::Import {import_cmd} => {
            match import_cmd {
                ImportCommands::Rpc {rpc_uri} => {
                    rpc_import(root_dir, rpc_uri, args.verbose).await.unwrap();
                }
            }
        }
        Commands::List => {
            list_blocks(root_dir).await.unwrap();
        }
    };
}
