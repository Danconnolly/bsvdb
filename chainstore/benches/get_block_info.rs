// benchmarks on get_block_info

use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
use bitcoinsv::bitcoin::{BlockchainId, BlockHash};
use criterion::{Criterion, criterion_group, criterion_main};
use foundationdb::api::NetworkAutoStop;
use futures::executor::block_on;
use hex::FromHex;
use rand::random;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use bsvdb_base::ChainStoreConfig;
use bsvdb_chainstore::{ChainStore, FDBChainStore};

// on main branch, 2024-07-22, 10_000 hashes
//      get_block_info          time:   [6.1090 s 6.1658 s 6.2220 s]
// = 1622 checks/second
// on parallel branch, same date, parallel version
//      get_block_info          time:   [149.52 ms 155.22 ms 161.60 ms]
// = 64,000 checks/second


// benchmark chainstore.get_block_info()
// load a bunch of block hashes and do a lookup for each one
async fn setup_get_block_info() -> (FDBChainStore, JoinHandle<()>) {
    let r_id: u16 = random();
    let root = format!("benchmark{}", r_id);
    let config = ChainStoreConfig {
        enabled: true,
        root_path: root,
    };
    let mut chain_store = FDBChainStore::new(&config, BlockchainId::Mainnet).await.unwrap();
    chain_store
}

async fn parallel_get_block_info(chain_store: &mut FDBChainStore, block_hashes: &Vec<BlockHash>) {
    let mut v = vec![];
    for b in block_hashes {
        let i = chain_store.get_block_info_by_hash(*b);
        let j = tokio::spawn(i);
        v.push(j);
    }
    for j in v {
        let _i = j.await.unwrap().unwrap();
    }
}

async fn serial_get_block_info(chain_store: &mut FDBChainStore, block_hashes: &Vec<BlockHash>) {
    for b in block_hashes {
        let _i = chain_store.get_block_info_by_hash(*b).await.unwrap();
    }
}

async fn global_setup() -> (NetworkAutoStop, Vec<BlockHash>) {
    // load block list
    let file = File::open("../testdata/blockhashes").expect("failed to open blockhashes file");
    let reader = BufReader::new(file);

    let mut strings: Vec<String> = reader
        .lines()
        .take(10_000)
        .collect::<Result<Vec<String>, io::Error>>().expect("cant load lines");
    let mut hashes = vec![];
    for s in strings {
        hashes.push(BlockHash::from_hex(s).expect("cant convert to hash"));
    }
    println!("loaded {} hashes", hashes.len());

    (unsafe { foundationdb::boot() }, hashes)
}

async fn global_teardown(network: NetworkAutoStop) {
    drop(network);
}

fn benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (network, block_hashes) = rt.block_on(global_setup());
    c.bench_function("parallel_get_block_info", |b| {
        let (mut chain_store, j) = rt.block_on(setup_get_block_info());
        b.iter(|| {
            rt.block_on(parallel_get_block_info(&mut chain_store, &block_hashes))
        });
    });
    c.bench_function("serial_get_block_info", |b| {
        let (mut chain_store, j) = rt.block_on(setup_get_block_info());
        b.iter(|| {
            rt.block_on(serial_get_block_info(&mut chain_store, &block_hashes))
        });
    });
    rt.block_on(global_teardown(network));
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = benchmark
}

criterion_main!(benches);