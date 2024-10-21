use crate::result::{BsvDbBaseError, BsvDbBaseResult};
use bitcoinsv::bitcoin::BlockchainId;
use config::{Config, File, FileFormat};
use serde::Deserialize;
use std::path::Path;

#[derive(Clone, Debug, Deserialize)]
#[allow(unused)]
pub struct BlockArchiveConfig {
    pub enabled: bool,
    pub root_path: String,
}

#[derive(Clone, Debug, Deserialize)]
#[allow(unused)]
pub struct ChainStoreConfig {
    pub enabled: bool,
    pub root_path: String,
}

#[derive(Clone, Debug, Deserialize)]
#[allow(unused)]
pub struct BSVDBConfig {
    pub verbose: bool,
    pub blockchain: String,
    pub block_archive: BlockArchiveConfig,
    pub chain_store: ChainStoreConfig,
}

impl BSVDBConfig {
    pub fn new(config_path: Option<String>) -> BsvDbBaseResult<Self> {
        let s1 = Config::builder()
            .add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Toml))
            .add_source(File::from(Path::new("~/.bsvdb.toml")).required(false))
            .add_source(File::new("bsvdb.toml", FileFormat::Toml).required(false));
        let s2 = match config_path {
            Some(cf) => s1.add_source(File::new(cf.as_str(), FileFormat::Toml)),
            None => s1,
        };
        let r: BSVDBConfig = s2.build()?.try_deserialize()?;
        if r.blockchain != "mainnet"
            && r.blockchain != "testnet"
            && r.blockchain != "stn"
            && r.blockchain != "regtest"
        {
            Err(BsvDbBaseError::BlockchainUnknown)
        } else {
            Ok(r)
        }
    }

    /// Get the BlockChainId from the configuration.
    //
    // It assumes that the blockchain config value is a valid value. This is checked in new().
    pub fn get_blockchain_id(&self) -> BlockchainId {
        // todo: this should be provided as a function by BlockchainId
        if self.blockchain == "mainnet" {
            BlockchainId::Main
        } else if self.blockchain == "testnet" {
            BlockchainId::Test
        } else if self.blockchain == "stn" {
            BlockchainId::Stn
        } else {
            BlockchainId::Regtest
        }
    }

    /// Get the root path for the ChainStore, accounting for default based on BlockChain
    pub fn get_chain_store_root_path(&self) -> String {
        if self.chain_store.root_path.is_empty() {
            String::from(match self.get_blockchain_id() {
                BlockchainId::Main => "bsvmain",
                BlockchainId::Test => "bsvtest",
                BlockchainId::Stn => "bsvstn",
                BlockchainId::Regtest => "bsvregtest",
            })
        } else {
            self.chain_store.root_path.clone()
        }
    }

    /// Check that the BlockArchive is enabled.
    pub fn check_block_archive_enabled(&self) -> BsvDbBaseResult<()> {
        match self.block_archive.enabled {
            true => Ok(()),
            false => Err(BsvDbBaseError::BlockArchiveNotEnabled),
        }
    }

    /// Check that the BlockArchive is enabled.
    pub fn check_chain_store_enabled(&self) -> BsvDbBaseResult<()> {
        match self.chain_store.enabled {
            true => Ok(()),
            false => Err(BsvDbBaseError::ChainStoreNotEnabled),
        }
    }
}

const DEFAULT_CONFIG: &str = r#"
verbose = false
blockchain = "mainnet"

[block_archive]
enabled = false
root_path = "~/.bsvdb/blockstore"

[chain_store]
enabled = false
root_path = ""
"#;
