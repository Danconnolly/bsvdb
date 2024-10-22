use bsvdb_base::BlockArchiveConfig;
use bsvdb_blockarchive::{BlockArchive, SimpleFileBasedBlockArchive};
use std::collections::BTreeSet;

/// BlockArchive check links
// todo: incorrectly reports genesis block as unlinked
// todo: make this more extensive, report on length of max chain, number of linked blocks
//      and for each unconnected sequence report the hashes
pub async fn check_links(config: &BlockArchiveConfig) -> bsvdb_blockarchive::Result<()> {
    let mut archive = SimpleFileBasedBlockArchive::new(config).await.unwrap();
    let mut block_it = archive.block_list().await.unwrap();
    // collect all hashes for checking parents
    let mut block_hashes = BTreeSet::new();
    // headers where we didnt find the parent on the first pass
    let mut not_found = Vec::new();
    // for each block
    while let Some(block_hash) = block_it.next().await {
        block_hashes.insert(block_hash);
        let h = archive.block_header(&block_hash).await.unwrap();
        if !block_hashes.contains(&h.prev_hash) {
            not_found.push(h);
        }
    }
    // check the ones not found yet
    for h in not_found {
        if !block_hashes.contains(&h.prev_hash) {
            println!("dont have parent of block {}", h.hash())
        }
    }
    Ok(())
}
