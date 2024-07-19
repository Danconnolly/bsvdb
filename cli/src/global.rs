use std::collections::{BTreeMap, BTreeSet};
use std::time;
use std::time::Instant;
use bitcoinsv::bitcoin::hash::{Hash};
use bitcoinsv::bitcoin::{BlockHash, BlockHeader};
use futures::StreamExt;
use bsvdb_base::BSVDBConfig;
use crate::result::CliResult;
use bsvdb_blockarchive::{BlockArchive, SimpleFileBasedBlockArchive};
use bsvdb_chainstore::{BlockInfo, BlockValidity, ChainStore, FDBChainStore};


// synchronize chainstore from blockstore - single-threaded approach
pub async fn sync(config: &BSVDBConfig) -> CliResult<()> {
    // single-threaded approach is achieved 40-43 blocks/sec
    config.check_block_archive_enabled()?;
    config.check_chain_store_enabled()?;

    let fdb_boot = unsafe { foundationdb::boot() };

    let mut block_archive = SimpleFileBasedBlockArchive::new(&config.block_archive).await?;
    let mut chain_store = FDBChainStore::new(&config.chain_store, config.get_blockchain_id()).await?;

    let mut missing_headers = BTreeMap::new();    // headers of missing blocks
    let mut parent_hashes = BTreeMap::new();   // parent hash -> vec of block hash
    let mut known_parents = BTreeSet::new();                        // hashes of parents which are known for blocks which are unknown
    println!("starting sync from blockstore to chainstore");
    let mut i = block_archive.block_list().await?;
    let mut num_found = 0;
    let mut checked = 0;
    let start_time = Instant::now();
    while let Some(block_hash) = i.next().await {
        match chain_store.get_block_info_by_hash(&block_hash).await? {
            Some(_info) => {
                // already have block
                num_found += 1;
                // todo: check whether we need to update the values
            },
            None => {
                let hdr = block_archive.block_header(&block_hash).await.unwrap();
                // insert into parent_hashes index - there can be multiple blocks with the same parent
                match parent_hashes.get(&hdr.prev_hash) {
                    None => {
                        parent_hashes.insert(hdr.prev_hash, vec![hdr.hash()]);
                    },
                    Some(v) => {
                        let mut v2 = v.clone();
                        v2.push(hdr.hash());
                        parent_hashes.insert(hdr.prev_hash, v2);
                    }
                }
                // check if we know the parent
                match chain_store.get_block_info_by_hash(&hdr.prev_hash).await? {
                    Some(p_info) => {
                        known_parents.insert(p_info.hash);
                    },
                    None => {
                        // do nothing
                    }
                }
                // add to missing headers
                missing_headers.insert(hdr.hash(), hdr);
            }
        }
        checked += 1;
        if checked % 10_000 == 0 {
            let dur = start_time.elapsed();
            println!("checked {} blocks, {} blocks/sec", checked, (checked as f32)/dur.as_secs_f32());
        }
    }
    println!("checked {} blocks, found {} blocks, missing {} blocks, analyzing...", checked, num_found, missing_headers.len());

    let mut added = 0;
    while known_parents.len() > 0 {
        let p_hash = known_parents.pop_first();
        if let Some(c_hashes) = parent_hashes.get(&p_hash.unwrap()) {
            for c_hash in c_hashes {
                let hdr = missing_headers.get(c_hash).unwrap();
                let b_info = BlockInfo {
                    id: 0,
                    hash: hdr.hash(),
                    header: hdr.clone(),
                    height: 0,
                    prev_id: 0,
                    next_ids: vec![],
                    size: None,
                    num_tx: None,
                    median_time: None,
                    chain_work: None,
                    total_tx: None,
                    total_size: None,
                    miner: None,
                    validity: BlockValidity::Unknown,
                };
                let b_info = chain_store.store_block_info(b_info).await.unwrap();
                // this can now be a known parent
                known_parents.insert(b_info.hash);
                added += 1;
                if added % 10_000 == 0 {
                    println!("added {} blocks.", added);
                }
            }
            parent_hashes.remove(&p_hash.unwrap());
        }
    }
    println!("finished sync. added {} blocks.", added);

    drop(fdb_boot);
    Ok(())
}

// synchronize chainstore from blockstore, multi-threaded approach
pub async fn sync2(config: &BSVDBConfig) -> CliResult<()> {
    // async sub-function to check for a particular block.
    // if found in chainstore, then it returns None.
    // if not found, then it returns Some((header: BlockHeader, parent_known: bool))
    async fn check_block(block_hash: BlockHash) -> Option<(BlockHeader, bool)> {
        None
    }

    // single-threaded approach is achieved 40-43 blocks/sec

    config.check_block_archive_enabled()?;
    config.check_chain_store_enabled()?;

    let fdb_boot = unsafe { foundationdb::boot() };

    let mut block_archive = SimpleFileBasedBlockArchive::new(&config.block_archive).await?;
    let mut chain_store = FDBChainStore::new(&config.chain_store, config.get_blockchain_id()).await?;

    let mut missing_headers = BTreeMap::new();    // headers of missing blocks
    let mut parent_hashes = BTreeMap::new();   // parent hash -> vec of block hash
    let mut known_parents = BTreeSet::new();                        // hashes of parents which are known for blocks which are unknown
    println!("starting sync from blockstore to chainstore");
    let mut i = block_archive.block_list().await?;
    let mut num_found = 0;
    let mut checked = 0;
    let start_time = Instant::now();
    while let Some(block_hash) = i.next().await {
        match chain_store.get_block_info_by_hash(&block_hash).await? {
            Some(_info) => {
                // already have block
                num_found += 1;
                // todo: check whether we need to update the values
            },
            None => {
                let hdr = block_archive.block_header(&block_hash).await.unwrap();
                // insert into parent_hashes index - there can be multiple blocks with the same parent
                match parent_hashes.get(&hdr.prev_hash) {
                    None => {
                        parent_hashes.insert(hdr.prev_hash, vec![hdr.hash()]);
                    },
                    Some(v) => {
                        let mut v2 = v.clone();
                        v2.push(hdr.hash());
                        parent_hashes.insert(hdr.prev_hash, v2);
                    }
                }
                // check if we know the parent
                match chain_store.get_block_info_by_hash(&hdr.prev_hash).await? {
                    Some(p_info) => {
                        known_parents.insert(p_info.hash);
                    },
                    None => {
                        // do nothing
                    }
                }
                // add to missing headers
                missing_headers.insert(hdr.hash(), hdr);
            }
        }
        checked += 1;
        if checked % 10_000 == 0 {
            let dur = start_time.elapsed();
            println!("checked {} blocks, {} blocks/sec", checked, (checked as f32)/dur.as_secs_f32());
        }
    }
    println!("checked {} blocks, found {} blocks, missing {} blocks, analyzing...", checked, num_found, missing_headers.len());

    let mut added = 0;
    while known_parents.len() > 0 {
        let p_hash = known_parents.pop_first();
        if let Some(c_hashes) = parent_hashes.get(&p_hash.unwrap()) {
            for c_hash in c_hashes {
                let hdr = missing_headers.get(c_hash).unwrap();
                let b_info = BlockInfo {
                    id: 0,
                    hash: hdr.hash(),
                    header: hdr.clone(),
                    height: 0,
                    prev_id: 0,
                    next_ids: vec![],
                    size: None,
                    num_tx: None,
                    median_time: None,
                    chain_work: None,
                    total_tx: None,
                    total_size: None,
                    miner: None,
                    validity: BlockValidity::Unknown,
                };
                let b_info = chain_store.store_block_info(b_info).await.unwrap();
                // this can now be a known parent
                known_parents.insert(b_info.hash);
                added += 1;
                if added % 10_000 == 0 {
                    println!("added {} blocks.", added);
                }
            }
            parent_hashes.remove(&p_hash.unwrap());
        }
    }
    println!("finished sync. added {} blocks.", added);

    drop(fdb_boot);
    Ok(())
}