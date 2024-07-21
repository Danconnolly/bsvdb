use std::future::Future;
use std::pin::Pin;
use async_trait::async_trait;
use bitcoinsv::bitcoin::{BlockchainId, BlockHash, BlockHeader};
use tokio::io::Join;
use tokio::task::JoinHandle;
use crate::ChainStoreResult;

/// A ChainStore stores information about a blockchain.
///
/// The ChainStore stores BlockInfo structures which contain the block header
/// and additional metadata about the block. Each BlockInfo has a database
/// identifier field that is called db_id. This is a unique identifier for the
/// block that is only significant within the context of the ChainStore.
///
/// Each ChainStore is associated with a particular blockchain (e.g. mainnet, testnet, etc).
///
/// All functions return a future that can be safely sent between threads, enabling parallel execution
/// of the futures returned by the functions.
///
/// The initialization of the ChainStore is not defined here, each implementation may have
/// different initialization needs. However, each new ChainStore must be initialized with the genesis block
/// for the blockchain that it is intended for.
///
/// Each implementation of ChainStore is expected to support async parallelization, as described
/// in the development notes (Developing Parallel Access to Databases)[/docs/dev-parallel-dbs.md].
#[async_trait]
pub trait ChainStore {
    /// Returns the current state of the blockchain.
    async fn get_chain_state(&mut self) -> ChainStoreResult<ChainState>;

    /// Get the block info for the block with the given id.
    ///
    /// Returns a future that will produce the results.
    fn get_block_info(&self, db_id: BlockId) -> impl Future<Output = ChainStoreResult<Option<BlockInfo>>> + Send;

    /// Returns the block info for the block with the given hash.
    async fn get_block_info_by_hash(&self, hash: BlockHash) -> JoinHandle<ChainStoreResult<Option<BlockInfo>>>;

    /// Returns the block infos for the block and its ancestors.
    ///
    /// Return at most max_blocks block infos, if given, otherwise return all block infos to the
    /// genesis block.
    async fn get_block_infos(&mut self, db_id: &BlockId, max_blocks: Option<u64>) -> ChainStoreResult<Vec<BlockInfo>>;

    /// Store the block info in the ChainStore, returning an updated BlockInfo structure.
    ///
    /// The block_id field of the BlockInfo structure is ignored and will be set by the ChainStore.
    ///
    /// If the block already exists in the ChainStore then it is updated.
    ///
    /// The block must be a child of a block that is already in the ChainStore.
    ///
    /// If the validity of the parent block is Unknown, then the validity of the child block is also
    /// Unknown.
    ///
    /// If the validity of the parent block is Invalid, then the validity of the child block is
    /// Invalid.
    ///
    /// If the validity of the parent block is Valid, then the validity of the child block can be
    /// Unknown or ValidHeader.
    ///
    /// If the validity of the parent block is ValidHeader, then the validity of the child block can
    /// be Unknown or ValidHeader.
    ///
    /// If the validity of the parent block is InvalidAncestor, then the validity of the child block
    /// is InvalidAncestor.
    ///
    /// If the validity of the parent block is HeaderInvalid, then the validity of the child block is
    /// InvalidAncestor.
    async fn store_block_info(&mut self, block_info: BlockInfo) -> ChainStoreResult<BlockInfo>;
}

/// The BlockId is a unique identifier for a block in the ChainStore.
pub type BlockId = u64;

/// The BlockValidity enum describes the validity of a block.
#[derive(Debug, Clone, PartialEq)]
pub enum BlockValidity {
    /// The validity is unknown
    Unknown,
    /// The block is valid.
    Valid,
    /// The header is valid but the validity of the block is unknown.
    ValidHeader,
    /// The block is invalid.
    Invalid,
    /// The header is invalid.
    HeaderInvalid,
    /// The block has an invalid ancestor.
    InvalidAncestor,
}

/// The BlockInfo struct contains information about a block.
#[derive(Debug, Clone, PartialEq)]
pub struct BlockInfo {
    pub id: BlockId,
    pub hash: BlockHash,
    pub header: BlockHeader,
    pub height: u64,
    pub prev_id: BlockId,
    pub next_ids: Vec<BlockId>,
    pub size: Option<u64>,
    pub num_tx: Option<u64>,
    pub median_time: Option<u64>,
    pub chain_work: Option<Vec<u8>>,
    pub total_tx: Option<u64>,
    pub total_size: Option<u64>,
    pub miner: Option<String>,
    pub validity: BlockValidity,
}

/// The ChainState struct contains the current tips of the blockchain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainState {
    /// The block id of the tip with the most proof-of-work.
    ///
    /// If multiple tips have the most proof-of-work, then this is one of them.
    pub most_work_tip: BlockId,
    /// The block ids of tips that are currently not dormant. Includes the most work tip.
    pub active_tips: Vec<BlockId>,
    /// The block ids of tips that are dormant.
    pub dormant_tips: Vec<BlockId>,
    /// Tips with either invalid headers or invalid blocks.
    pub invalid_tips: Vec<BlockId>,
}

impl From<u8> for BlockValidity {
    fn from(value: u8) -> Self {
        if value == 1 {
            BlockValidity::Valid
        } else if value == 2 {
            BlockValidity::ValidHeader
        } else if value == 3 {
            BlockValidity::Invalid
        } else if value == 4 {
            BlockValidity::HeaderInvalid
        } else if value == 5 {
            BlockValidity::InvalidAncestor
        } else {
            BlockValidity::Unknown
        }
    }
}

impl From<BlockValidity> for u8 {
    fn from(value: BlockValidity) -> Self {
        match value {
            BlockValidity::Unknown => 0,
            BlockValidity::Valid => 1,
            BlockValidity::ValidHeader => 2,
            BlockValidity::Invalid => 3,
            BlockValidity::HeaderInvalid => 4,
            BlockValidity::InvalidAncestor => 5,
        }
    }
}

impl BlockInfo {
    /// Get the BlockInfo for the genesis block.
    pub fn genesis_info(block_chain: BlockchainId) -> BlockInfo {
        let g_hdr = BlockHeader::get_genesis(block_chain);
        let mut info = BlockInfo {
            id: 0,
            hash: g_hdr.hash(),
            header: g_hdr,
            height: 0,
            prev_id: 0,
            next_ids: vec![],
            size: Some(285),
            num_tx: Some(1),
            median_time: Some(1231006505),
            chain_work: Some(hex::decode("0000000000000000000000000000000000000000000000000000000100010001").unwrap()),
            total_tx: Some(1),
            total_size: Some(285),
            miner: Some(String::from("Satoshi Nakamoto")),
            validity: BlockValidity::Valid,
        };
        match block_chain {
            BlockchainId::Mainnet => info,
            BlockchainId::Testnet => {
                info.median_time = Some(1296688602);
                info
            },
            BlockchainId::Stn =>  {
                info.median_time = Some(1296688602);
                info
            },
            BlockchainId::Regtest => {
                info.median_time = Some(1296688602);
                info.chain_work = Some(hex::decode("0000000000000000000000000000000000000000000000000000000000000002").unwrap());
                info
            },
        }
    }
}