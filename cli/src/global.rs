use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::process::{id, Output};
use std::time::Instant;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use bitcoinsv::bitcoin::{BlockHash, BlockHeader, FullBlockStream};
use futures::StreamExt;
use bsvdb_base::BSVDBConfig;
use crate::result::CliResult;
use bsvdb_blockarchive::{BlockArchive, SimpleFileBasedBlockArchive};
use bsvdb_chainstore::{BlockInfo, BlockValidity, ChainStore, FDBChainStore};
use bsvdb_chainstore::ChainStoreResult;
use crate::ba::header;

// synchronize chainstore from blockstore, multi-threaded approach
pub async fn sync_piped(config: &BSVDBConfig) -> CliResult<()> {
    // single-threaded approach has achieved 40-43 blocks/sec
    // lets see if we can do dramatically better with a pipeline approach

    // pipeline is a number of stages, where each stage is a spawned task
    // the stages communicate through channels which are configured with reasonably sized buffers
    // a stage will generally receive a future from the source channel, do some processing which culminates in spawning another task,
    // then send the future for the result down the output channel to the next stage
    // the buffers enable multi-processing to continue while a stage waits for the next result
    //
    // 1 - iterate over block hashes from block archive and in launch a task to fetch it from the chain store
    //      send future to next stage
    // 2 - incoming is futures of check for block in chain store
    //      if the block is in chain store then abandon
    //      if the block is not in the chain store then
    //          spawn a task to spool the block, collecting the header and the number of transactions and send to next stage
    // 3 - incoming is future of fetching the header & the number of tx for unknown block
    //      create a BlockInfo and add the header and number of tx
    //      spawn a task to get the block size and send the task and the blockinfo to the next stage
    // 4 - incoming is a future for getting the block size and the partial block info
    //      update the partial block info with the file size
    //      spawn a task to check if the parent is in the chainstore
    //      send future to next stage with the partial block info
    // 5 - collect all inputs - (block info, joinhandle of check for parent)
    //      build map of hash -> block info, parent hash -> child hashes, known_parent set
    //      until pipeline done
    //      return maps & set
    //
    //      while known_parent set not empty:
    //          create empty list of futures
    //          for each known_parent:
    //              for each child of known_parent, if any: (from parent hash -> child hashes)
    //                  get header from hash -> header map
    //                  spawn task to store in chain store, returning new block info, adding future to list of futures
    //          clear known_parent set
    //          wait for each storage task to complete, storing hash of block stored in known_parent set

    const BUFFER_SIZE:usize = 1000;

    type Stage1Result = (Pin<Box<dyn Future<Output=ChainStoreResult<std::option::Option<BlockInfo<<FDBChainStore as ChainStore>::BlockId>>>> + Send>>, BlockHash);
    pub async fn stage1(block_hashes: Vec<BlockHash>, chain_store: FDBChainStore,
                        sender: Sender<Stage1Result>) -> CliResult<()> {
        for block_hash in block_hashes {
            let j = chain_store.get_block_info_by_hash(block_hash);
            sender.send((j, block_hash)).await.expect("TODO: panic message");
        }
        Ok(())
    }

    type Stage2Result = BlockInfo<<FDBChainStore as ChainStore>::BlockId>;
    async fn stage2(mut receiver: Receiver<Stage1Result>, config: BSVDBConfig,
                        sender: Sender<Stage2Result>) -> CliResult<()> {
        // unfortunately we cant send futures for retrieving block data at the moment, so we have to do it in the foreground here
        let mut block_archive = SimpleFileBasedBlockArchive::new(&config.block_archive).await?;
        while let Some((j, block_hash)) = receiver.recv().await {
            let r = j.await.unwrap();
            if r.is_none() {
                let r = block_archive.get_block(&block_hash).await.expect("get_block failed in stage2");
                let it = FullBlockStream::new(r).await.expect("couldnt get block stream");
                let b_info = BlockInfo {
                    id: 0u64,
                    hash: it.block_header.hash(),
                    header: it.block_header,
                    height: 0,
                    prev_id: 0u64,
                    next_ids: vec![],
                    size: None,
                    num_tx: Some(it.num_tx),
                    median_time: None,
                    chain_work: None,
                    total_tx: None,
                    total_size: None,
                    miner: None,
                    validity: BlockValidity::Unknown,
                };
                sender.send(b_info).await.expect("sending failed in stage2");
            }
        }
        Ok(())
    }

    type Stage3Result = BlockInfo<<FDBChainStore as ChainStore>::BlockId>;
    async fn stage3(mut receiver: Receiver<Stage2Result>, config: BSVDBConfig, sender: Sender<Stage3Result>) -> CliResult<()> {
        let mut block_archive = SimpleFileBasedBlockArchive::new(&config.block_archive).await?;
        while let Some(mut r) = receiver.recv().await {
            let sz = block_archive.block_size(&r.hash).await.expect("get block size failed in stage3");
            r.size = Some(sz as u64);
            sender.send(r).await.expect("msg sending failed in stage3");
        }
        Ok(())
    }

    type Stage4Result = (BlockInfo<<FDBChainStore as ChainStore>::BlockId>, Pin<Box<dyn Future<Output=ChainStoreResult<std::option::Option<BlockInfo<<FDBChainStore as ChainStore>::BlockId>>>> + Send>>);
    async fn stage4(mut receiver: Receiver<Stage3Result>, chain_store: FDBChainStore, sender: Sender<Stage4Result>) -> CliResult<()> {
        while let Some(r) = receiver.recv().await {
            let f = chain_store.get_block_info_by_hash(r.header.prev_hash);
            sender.send((r, f)).await.expect("sending failed in stage4");
        }
        Ok(())
    }

    type Stage5Result = (BTreeMap<BlockHash, BlockInfo<<FDBChainStore as ChainStore>::BlockId>>, BTreeMap<BlockHash, Vec<BlockHash>>, BTreeSet<BlockHash>);
    async fn stage5(mut receiver: Receiver<Stage4Result>) -> CliResult<Stage5Result> {
        let mut b_infos = BTreeMap::new();
        let mut parents_children = BTreeMap::new();
        let mut known_parents = BTreeSet::new();
        let start_time = Instant::now();
        let mut found = 0;
        while let Some((b_info, j)) = receiver.recv().await {
            let r = j.await.unwrap();
            if r.is_some() {
                known_parents.insert(r.unwrap().hash);
            }
            match parents_children.get(&b_info.header.prev_hash) {
                None => {
                    parents_children.insert(b_info.header.prev_hash, vec![b_info.hash]);
                },
                Some(v) => {
                    let mut v2 = v.clone();
                    v2.push(b_info.hash);
                    parents_children.insert(b_info.header.prev_hash, v2);
                }
            }
            b_infos.insert(b_info.hash, b_info);
            found += 1;
            if found % 10_000 == 0 {
                let dur = start_time.elapsed();
                println!("found {} blocks, {} blocks/sec", found, (found as f32)/dur.as_secs_f32());
            }
        }
        Ok((b_infos, parents_children, known_parents))
    }

    config.check_block_archive_enabled()?;
    config.check_chain_store_enabled()?;
    let fdb_boot = unsafe { foundationdb::boot() };

    println!("starting sync from blockstore to chainstore");
    let mut block_archive = SimpleFileBasedBlockArchive::new(&config.block_archive).await?;
    println!("fetching all block hashes...");
    let mut i = block_archive.block_list().await?;
    let mut block_hashes = vec![];
    while let Some(b) = i.next().await {
        block_hashes.push(b);
    }
    println!("done got {} hashes", block_hashes.len());

    let (chain_store, _j_chain_store) = FDBChainStore::new(&config.chain_store, config.get_blockchain_id()).await?;
    let (sender, mut receiver) = channel(BUFFER_SIZE);
    let f_stage1 = stage1(block_hashes, chain_store.clone(), sender);
    let j_stage1 = tokio::spawn(f_stage1);
    
    let (s2, r2) = channel(BUFFER_SIZE);
    let c = (*config).clone();
    let f_stage2 = stage2(receiver, c, s2);
    let j_stage2 = tokio::spawn(f_stage2);

    let (s3, r3) = channel(BUFFER_SIZE);
    let f_stage3 = stage3(r2, (*config).clone(), s3);
    let j_stage3 = tokio::spawn(f_stage3);

    let (s4, r4) = channel(BUFFER_SIZE);
    let f_stage4 = stage4(r3, chain_store.clone(), s4);
    let j_stage4 = tokio::spawn(f_stage4);

    let f_stage5 = stage5(r4);
    let j_stage5 = tokio::spawn(f_stage5);

    j_stage1.await?;
    j_stage2.await?;
    j_stage3.await?;
    j_stage4.await?;

    let (mut block_infos, mut parent_children, mut known_parents) = j_stage5.await?.unwrap();
    println!("found {} new headers, with {} known parents", block_infos.len(), known_parents.len());

    let mut added = 0;
    while known_parents.len() > 0 {
        let p_hash = known_parents.pop_first().unwrap();
        if let Some(c_hashes) = parent_children.get(&p_hash) {
            for c_hash in c_hashes {
                let b_info = block_infos.get(c_hash).unwrap();
                let b_info = chain_store.store_block_info(b_info.clone()).await.unwrap();
                // this can now be a known parent
                known_parents.insert(b_info.hash);
                added += 1;
                if added % 10_000 == 0 {
                    println!("added {} blocks.", added);
                }
            }
            parent_children.remove(&p_hash);
        }
    }
    println!("finished sync. added {} blocks.", added);

    drop(fdb_boot);
    Ok(())
}
