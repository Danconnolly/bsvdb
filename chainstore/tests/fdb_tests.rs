use bitcoinsv::bitcoin::{BlockchainId, BlockHash, BlockHeader};
use foundationdb;
use foundationdb::directory::Directory;
use hex::FromHex;
use bsv_chainstore::{BlockInfo, BlockValidity, ChainStore, FDBChainStore};
use rand::random;

#[tokio::test]
async fn check_store() {
    let network = unsafe { foundationdb::boot() };
    // get a unique root
    let r_id: u16 = random();
    let root = vec![format!("testing{}", r_id)];
    let mut chain_store = FDBChainStore::new(root.clone(), BlockchainId::Mainnet).await.unwrap();

    // get genesis block by id
    let g_block = chain_store.get_block_info(&0).await.unwrap();
    assert!(g_block.is_some());
    let g_block = g_block.unwrap();
    assert_eq!(g_block.hash, BlockHeader::get_genesis(BlockchainId::Mainnet).hash());
    assert_eq!(g_block.height, 0);
    assert_eq!(g_block.size, Some(285));

    // get genesis block by hash
    let g_block2 = chain_store.get_block_info_by_hash(&BlockHeader::get_genesis(BlockchainId::Mainnet).hash()).await.unwrap().unwrap();
    assert_eq!(g_block2.height, 0);

    let hdr1 = BlockHeader::from_hex("010000006fe28c0ab6f1b372c1a6a246ae63f74f931e8365e15a089c68d6190000000000982051fd1e4ba744bbbe680e1fee14677ba1a3c3540bf7b1cdb606e857233e0e61bc6649ffff001d01e36299").unwrap();
    let info1 = BlockInfo {
        id: 0,      // should be updated
        hash: hdr1.hash(),
        header: hdr1,
        height: 0,  // should be updated
        prev_id: 15,    // should be updated
        next_ids: vec![],
        size: Some(215),
        num_tx: Some(1),
        median_time: Some(1231469665),
        chain_work: None,
        total_tx: None,     // should be updated
        total_size: None,   // should be updated
        miner: None,
        validity: BlockValidity::Valid,
    };
    assert_eq!(info1.header.hash(), BlockHash::from_hex("00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048").unwrap());
    assert_eq!(info1.header.prev_hash, BlockHash::from_hex("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f").unwrap());
    let i2 = chain_store.store_block_info(info1).await.unwrap();
    assert_eq!(i2.id, 1);
    assert_eq!(i2.height, 1);
    assert_eq!(i2.prev_id, 0);
    assert_eq!(i2.total_tx, Some(2));
    assert_eq!(i2.total_size, Some(500));
    let g2 = chain_store.get_block_info(&0).await.unwrap().unwrap();
    assert_eq!(g2.next_ids, vec![1]);

    // clear the testing directory
    let fdb = foundationdb::Database::default().unwrap();
    let dir = foundationdb::directory::DirectoryLayer::default();
    let trx = fdb.create_trx().unwrap();
    let r = dir.remove(&trx, &*root).await.unwrap();
    trx.commit().await.unwrap();

    drop(network);
}
