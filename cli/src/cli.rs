mod ba;

use bitcoinsv::bitcoin::{BlockHash, ToHex};
use bitcoinsv_rpc::{RpcApi};
use clap::{Parser, Subcommand};
use bsvdb_blockarchive::{BlockArchive};
use tokio_stream::StreamExt;
use bsvdb_base::{BSVDBConfig};
use crate::ba::{check_all_blocks, check_block, check_links, header, list_blocks, rpc_import};

/// A CLI for managing bsvdb components and systems.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Run with this configuration file, instead of default file.
    #[clap(long)]
    config: Option<String>,
    /// Emit more status messages.
    #[clap(short = 'v', long, default_value = "false")]
    verbose: bool,
    /// Command to perform
    #[command(subcommand)]
    cmd: SubSystems,
}

#[derive(Subcommand, Debug)]
enum SubSystems {
    /// Block Archive commands.
    BA {
        #[command(subcommand)]
        ba_cmd: BACommands
    }
}

#[derive(Subcommand, Debug)]
enum BACommands {
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




#[tokio::main]
async fn main() {
    let args: Args = Args::parse();
    let config = BSVDBConfig::new(args.config).unwrap();
    match args.cmd {
        SubSystems::BA{ba_cmd} => {
            if config.block_archive.is_none() {
                println!("BlockArchive configuration not set.");
                return;
            }
            let ba_config = config.block_archive.unwrap();
            match ba_cmd {
                BACommands::Check{check_cmd} => {
                    match check_cmd {
                        CheckCommands::Linked => {
                            check_links(&ba_config).await.unwrap();
                        }
                        CheckCommands::Block{block_hash} => {
                            check_block(&ba_config, block_hash).await.unwrap();
                        }
                        CheckCommands::Blocks => {
                            check_all_blocks(&ba_config, args.verbose).await.unwrap();
                        }
                    }
                }
                BACommands::Header{hex, block_hash} => {
                    header(&ba_config, block_hash, hex).await.unwrap();
                }
                BACommands::Import {import_cmd} => {
                    match import_cmd {
                        ImportCommands::Rpc {rpc_uri} => {
                            rpc_import(&ba_config, rpc_uri, args.verbose).await.unwrap();
                        }
                    }
                }
                BACommands::List => {
                    list_blocks(&ba_config).await.unwrap();
                }
            }
        }
    }
}
