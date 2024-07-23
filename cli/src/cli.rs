mod result;
mod ba;
mod global;
mod cs;

use bitcoinsv::bitcoin::BlockHash;
use clap::{Parser, Subcommand};
use bsvdb_base::{BSVDBConfig};
use crate::ba::{check_all_blocks, check_block, check_links, header, list_blocks, rpc_import};
use crate::cs::{cs_list_blocks, get_block_info};
use crate::global::{sync_piped};

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
    /// Command or sub-system.
    #[command(subcommand)]
    cmd: CommandOrSystem,
}

#[derive(Subcommand, Debug)]
enum CommandOrSystem {
    /// Block Archive commands.
    BA {
        #[command(subcommand)]
        ba_cmd: BACommands
    },
    /// Chain Store commands.
    CS {
        #[command(subcommand)]
        cs_cmd: CSCommands
    },
    /// Synchronize system.
    #[clap(long_about = "synchronizes data between various components, such as importing blocks from blockstore to chainstore.")]
    Sync,
}

/// Block Archive commands.
#[derive(Subcommand, Debug)]
enum BACommands {
    /// Perform checks on the archive.
    Check {
        #[command(subcommand)]
        check_cmd: BACheckCommands,
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
        import_cmd: BAImportCommands,
    },
    /// List all blocks in the archive.
    List,
}

// Block Archive check commands.
#[derive(Subcommand, Debug)]
enum BACheckCommands {
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

/// Block Archive import commands.
#[derive(Subcommand, Debug)]
enum BAImportCommands {
    /// Import blocks over an RPC connection from an SV Node.
    Rpc {
        /// RCP Connection URI.
        ///
        /// URI should be something like 'http://username:password@127.0.0.1:8332'
        rpc_uri: String,
    }
}

/// Chain Store commands.
#[derive(Subcommand, Debug)]
enum CSCommands {
    /// Get information about a block.
    Block {
        /// Block hash.
        block_hash: BlockHash,
    },
    /// List blocks starting at given id and moving up the chain.
    List {
        /// Block ID
        block_id: u64,
    }
}

#[tokio::main]
async fn main() {
    let args: Args = Args::parse();
    let config = BSVDBConfig::new(args.config).unwrap();
    match args.cmd {
        CommandOrSystem::BA{ba_cmd} => {
            if ! config.block_archive.enabled {
                println!("BlockArchive is not enabled.");
                return;
            }
            let ba_config = config.block_archive;
            match ba_cmd {
                BACommands::Check{check_cmd} => {
                    match check_cmd {
                        BACheckCommands::Linked => {
                            check_links(&ba_config).await.unwrap();
                        }
                        BACheckCommands::Block{block_hash} => {
                            check_block(&ba_config, block_hash).await.unwrap();
                        }
                        BACheckCommands::Blocks => {
                            check_all_blocks(&ba_config, args.verbose).await.unwrap();
                        }
                    }
                }
                BACommands::Header{hex, block_hash} => {
                    header(&ba_config, block_hash, hex).await.unwrap();
                }
                BACommands::Import {import_cmd} => {
                    match import_cmd {
                        BAImportCommands::Rpc {rpc_uri} => {
                            rpc_import(&ba_config, rpc_uri, args.verbose).await.unwrap();
                        }
                    }
                }
                BACommands::List => {
                    list_blocks(&ba_config).await.unwrap();
                }
            }
        },
        CommandOrSystem::CS{ cs_cmd } => {
            if ! config.chain_store.enabled {
                println!("ChainStore is not enabled.")
            }
            let network = unsafe { foundationdb::boot() };
            match cs_cmd {
                CSCommands::Block {block_hash} => {
                    get_block_info(&config, block_hash).await;
                },
                CSCommands::List {block_id} => {
                    cs_list_blocks(&config, block_id).await;
                }
            }
            drop(network);
        }
        CommandOrSystem::Sync => {
            sync_piped(&config).await.unwrap();
        }
    }
}
