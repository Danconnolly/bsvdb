use bitcoinsv::bitcoin::BlockHash;
use tokio_stream::StreamExt;
use bsvdb_base::BSVDBConfig;
use bsvdb_chainstore::{ChainStore, FDBChainStore};

pub async fn get_block_info(config: &BSVDBConfig, block_hash: BlockHash) {
    let (chain_store, j) = FDBChainStore::new(&config.chain_store, config.get_blockchain_id()).await.unwrap();
    match chain_store.get_block_info_by_hash(block_hash).await.unwrap() {
        None => println!("block not found"),
        Some(b_info) => {
            println!("{:?}", b_info);
        }
    }
    chain_store.shutdown().await.unwrap();
    j.await.unwrap();
}

pub async fn cs_list_blocks(config: &BSVDBConfig, block_id: u64) {
    let (chain_store, j) = FDBChainStore::new(&config.chain_store, config.get_blockchain_id()).await.unwrap();
    let mut stream = chain_store.get_block_infos(block_id, None).await.unwrap();
    while let Some(b_i) = stream.next().await {
        println!("{:?}", b_i);
    }
    chain_store.shutdown().await.unwrap();
    j.await.unwrap();

}