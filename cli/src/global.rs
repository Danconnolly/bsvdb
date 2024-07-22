use std::future::Future;
use std::pin::Pin;
use std::time::Instant;
use tokio::sync::mpsc::{channel, Sender};
use bitcoinsv::bitcoin::BlockHash;
use futures::StreamExt;
use bsvdb_base::BSVDBConfig;
use crate::result::CliResult;
use bsvdb_blockarchive::{BlockArchive, SimpleFileBasedBlockArchive};
use bsvdb_chainstore::{BlockInfo, ChainStore, FDBChainStore};
use bsvdb_chainstore::ChainStoreResult;


// synchronize chainstore from blockstore - single-threaded approach
// pub async fn sync(config: &BSVDBConfig) -> CliResult<()> {
//     // single-threaded approach is achieved 40-43 blocks/sec
//     config.check_block_archive_enabled()?;
//     config.check_chain_store_enabled()?;
//
//     let fdb_boot = unsafe { foundationdb::boot() };
//
//     let mut block_archive = SimpleFileBasedBlockArchive::new(&config.block_archive).await?;
//     let mut chain_store = FDBChainStore::new(&config.chain_store, config.get_blockchain_id()).await?;
//
//     let mut missing_headers = BTreeMap::new();    // headers of missing blocks
//     let mut parent_hashes = BTreeMap::new();   // parent hash -> vec of block hash
//     let mut known_parents = BTreeSet::new();                        // hashes of parents which are known for blocks which are unknown
//     println!("starting sync from blockstore to chainstore");
//     let mut i = block_archive.block_list().await?;
//     let mut num_found = 0;
//     let mut checked = 0;
//     let start_time = Instant::now();
//     while let Some(block_hash) = i.next().await {
//         match chain_store.get_block_info_by_hash(&block_hash).await? {
//             Some(_info) => {
//                 // already have block
//                 num_found += 1;
//                 // todo: check whether we need to update the values
//             },
//             None => {
//                 let hdr = block_archive.block_header(&block_hash).await.unwrap();
//                 // insert into parent_hashes index - there can be multiple blocks with the same parent
//                 match parent_hashes.get(&hdr.prev_hash) {
//                     None => {
//                         parent_hashes.insert(hdr.prev_hash, vec![hdr.hash()]);
//                     },
//                     Some(v) => {
//                         let mut v2 = v.clone();
//                         v2.push(hdr.hash());
//                         parent_hashes.insert(hdr.prev_hash, v2);
//                     }
//                 }
//                 // check if we know the parent
//                 match chain_store.get_block_info_by_hash(&hdr.prev_hash).await? {
//                     Some(p_info) => {
//                         known_parents.insert(p_info.hash);
//                     },
//                     None => {
//                         // do nothing
//                     }
//                 }
//                 // add to missing headers
//                 missing_headers.insert(hdr.hash(), hdr);
//             }
//         }
//         checked += 1;
//         if checked % 10_000 == 0 {
//             let dur = start_time.elapsed();
//             println!("checked {} blocks, {} blocks/sec", checked, (checked as f32)/dur.as_secs_f32());
//         }
//     }
//     println!("checked {} blocks, found {} blocks, missing {} blocks, analyzing...", checked, num_found, missing_headers.len());
//
//     let mut added = 0;
//     while known_parents.len() > 0 {
//         let p_hash = known_parents.pop_first();
//         if let Some(c_hashes) = parent_hashes.get(&p_hash.unwrap()) {
//             for c_hash in c_hashes {
//                 let hdr = missing_headers.get(c_hash).unwrap();
//                 let b_info = BlockInfo {
//                     id: 0,
//                     hash: hdr.hash(),
//                     header: hdr.clone(),
//                     height: 0,
//                     prev_id: 0,
//                     next_ids: vec![],
//                     size: None,
//                     num_tx: None,
//                     median_time: None,
//                     chain_work: None,
//                     total_tx: None,
//                     total_size: None,
//                     miner: None,
//                     validity: BlockValidity::Unknown,
//                 };
//                 let b_info = chain_store.store_block_info(b_info).await.unwrap();
//                 // this can now be a known parent
//                 known_parents.insert(b_info.hash);
//                 added += 1;
//                 if added % 10_000 == 0 {
//                     println!("added {} blocks.", added);
//                 }
//             }
//             parent_hashes.remove(&p_hash.unwrap());
//         }
//     }
//     println!("finished sync. added {} blocks.", added);
//
//     drop(fdb_boot);
//     Ok(())
// }

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
    //      if the block is not in the chain store then spawn a task to fetch the header and send to next stage
    // 3 - incoming is future of fetching the header for unknown block
    //      spawn a task to check if the parent is in the chainstore
    //      send future to next stage
    // 4 - collect all inputs - (header, joinhandle of check for parent)
    //      build map of hash -> header, parent hash -> child hashes, known_parent set
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

    type Stage1Result = (Pin<Box<dyn Future<Output=ChainStoreResult<Option<BlockInfo<<FDBChainStore as ChainStore>::BlockId>>>> + Send>>, BlockHash);
    pub async fn stage1(block_hashes: Vec<BlockHash>, chain_store: FDBChainStore,
                        sender: Sender<Stage1Result>) -> CliResult<()> {
        for block_hash in block_hashes {
            let j = chain_store.get_block_info_by_hash(block_hash);
            sender.send((j, block_hash)).await.expect("TODO: panic message");
        }
        Ok(())
    }

    // type Stage2Result = JoinHandle<BlockArchiveResult<BlockHeader>>;
    // pub async fn stage2(mut receiver: Receiver<Stage1Result>, config: &BSVDBConfig,
    //                     sender: Sender<Stage2Result>) -> CliResult<()> {
    //     let mut block_archive = SimpleFileBasedBlockArchive::new(&config.block_archive).await?;
    //     while let Some((j, h)) = receiver.recv().await {
    //         let r = j.await.unwrap().unwrap();
    //         if r.is_none() {
    //             let f = block_archive.block_header(&h);
    //             let j = tokio::spawn(f);
    //             sender.send(j).await;
    //         }
    //     }
    //     Ok(())
    // }
    //
    // type Stage3Result = (BlockHeader,JoinHandle<ChainStoreResult<Option<BlockInfo>>>);
    // async fn stage3(mut receiver: Receiver<Stage2Result>, config: &BSVDBConfig, sender: Sender<Stage3Result>) -> CliResult<()> {
    //     let mut chain_store = FDBChainStore::new(&config.chain_store, config.get_blockchain_id()).await?;
    //     while let Some(j) = receiver.recv().await {
    //         let r = j.await.unwrap().unwrap();
    //         let f = chain_store.get_block_info_by_hash(&r.prev_hash);
    //         let j = tokio::spawn(f);
    //         sender.send((r, j)).await;
    //     }
    //     Ok(())
    // }
    //
    // type Stage4Result = (BTreeMap<BlockHash, BlockHeader>, BTreeMap<BlockHash, Vec<BlockHash>>, BTreeSet<BlockHash>);
    // async fn stage4(mut receiver: Receiver<Stage3Result>) -> CliResult<Stage4Result> {
    //     let mut headers = BTreeMap::new();
    //     let mut parents_children = BTreeMap::new();
    //     let mut known_parents = BTreeSet::new();
    //     let start_time = Instant::now();
    //     let mut found = 0;
    //     while let Some((hdr, j)) = receiver.recv().await {
    //         let r = j.await.unwrap().unwrap();
    //         if r.is_some() {
    //             known_parents.insert(r.unwrap().hash);
    //         }
    //         match parents_children.get(&hdr.prev_hash) {
    //             None => {
    //                 parents_children.insert(hdr.prev_hash, vec![hdr.hash()]);
    //             },
    //             Some(v) => {
    //                 let mut v2 = v.clone();
    //                 v2.push(hdr.hash());
    //                 parents_children.insert(hdr.prev_hash, v2);
    //             }
    //         }
    //         headers.insert(hdr.hash(), hdr);
    //         found += 1;
    //         if found % 10_000 == 0 {
    //             let dur = start_time.elapsed();
    //             println!("found {} blocks, {} blocks/sec", found, (found as f32)/dur.as_secs_f32());
    //         }
    //     }
    //     Ok((headers, parents_children, known_parents))
    // }

    config.check_block_archive_enabled()?;
    config.check_chain_store_enabled()?;
    let fdb_boot = unsafe { foundationdb::boot() };

    println!("starting sync from blockstore to chainstore");
    let mut block_archive = SimpleFileBasedBlockArchive::new(&config.block_archive).await?;
    let (chain_store, _j_chain_store) = FDBChainStore::new(&config.chain_store, config.get_blockchain_id()).await?;

    let mut i = block_archive.block_list().await?;
    let mut r = vec![];
    while let Some(b) = i.next().await {
        r.push(b);
    }
    println!("got all block hashes");
    let (sender, mut receiver) = channel(BUFFER_SIZE);
    let f_stage1 = stage1(r, chain_store.clone(), sender);
    let j_stage1 = tokio::spawn(f_stage1);
    
    let start_time = Instant::now();
    let mut i = 0;
    while let Some((j, h)) = receiver.recv().await {
        let k = j.await;
        i += 1;
        if i % 10_000 == 0 {
            let dur = start_time.elapsed();
            println!("found {} blocks, {} blocks/sec", i, (i as f32)/dur.as_secs_f32());
        }
    }
    let dur = start_time.elapsed();
    println!("found {} blocks, {} blocks/sec", i, (i as f32)/dur.as_secs_f32());

    // let (s2, r2) = channel(BUFFER_SIZE);
    // let f_stage2 = stage2(receiver, config.clone(), s2);
    // let j_stage2 = tokio::spawn(f_stage2);
    //
    // let (s3, r3) = channel(BUFFER_SIZE);
    // let f_stage3 = stage3(r2, config.clone(), s3);
    // let j_stage3 = tokio::spawn(f_stage3);
    //
    // let f_stage4 = stage4(r3);
    // let j_stag4 = tokio::spawn(f_stage4);

    j_stage1.await?.expect("TODO: panic message");
    // j_stage2.await?;
    // j_stage3.await?;
    // let (mut headers, mut parent_children, mut known_parents) = j_stag4.await?.unwrap();

    // let mut added = 0;
    // while known_parents.len() > 0 {
    //     let p_hash = known_parents.pop_first().unwrap();
    //     if let Some(c_hashes) = parent_children.get(&p_hash) {
    //         for c_hash in c_hashes {
    //             let hdr = headers.get(c_hash).unwrap();
    //             let b_info = BlockInfo {
    //                 id: 0,
    //                 hash: hdr.hash(),
    //                 header: hdr.clone(),
    //                 height: 0,
    //                 prev_id: 0,
    //                 next_ids: vec![],
    //                 size: None,
    //                 num_tx: None,
    //                 median_time: None,
    //                 chain_work: None,
    //                 total_tx: None,
    //                 total_size: None,
    //                 miner: None,
    //                 validity: BlockValidity::Unknown,
    //             };
    //             let b_info = chain_store.store_block_info(b_info).await.unwrap();
    //             // this can now be a known parent
    //             known_parents.insert(b_info.hash);
    //             added += 1;
    //             if added % 10_000 == 0 {
    //                 println!("added {} blocks.", added);
    //             }
    //         }
    //         parent_children.remove(&p_hash);
    //     }
    // }
    // println!("finished sync. added {} blocks.", added);

    drop(fdb_boot);
    Ok(())
}