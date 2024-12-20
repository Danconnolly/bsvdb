use crate::chain_store::{BlockInfoStreamFromChannel, ChainState};
use crate::{BlockInfo, BlockValidity, ChainStore, Error, Result};
use async_trait::async_trait;
use bitcoinsv::bitcoin::{AsyncEncodable, BlockHash, BlockHeader, BlockchainId};
use bsvdb_base::ChainStoreConfig;
use foundationdb::directory::{Directory, DirectoryOutput};
use foundationdb::tuple::{pack, unpack, Bytes, Element};
use foundationdb::Transaction;
use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot::channel as oneshot_channel;
use tokio::sync::oneshot::Sender as OneshotSender;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::sleep;

/// FDBChainStore is an implementation of ChainStore for foundationdb.
///
/// Applications must call foundationdb::boot() before using this struct.
/// See https://docs.rs/foundationdb/latest/foundationdb/fn.boot.html.
///
/// It uses the foundationdb tuple encoding so that the database can be read by multiple
/// languages.
#[derive(Clone)]
pub struct FDBChainStore {
    sender: Sender<(FDBChainStoreMessage, OneshotSender<FDBChainStoreReply>)>,
}

impl FDBChainStore {
    /// Create a new FDBChainStore.
    ///
    /// The root directory supplied as a parameter must be dedicated to the ChainStore. If the
    /// ChainStore is part of a larger system, then this is probably a sub-directory of the larger
    /// systems directory. (e.g.: vec!["bsvmain", "chainstore"])
    pub async fn new(
        config: &ChainStoreConfig,
        chain: BlockchainId,
    ) -> Result<(Self, JoinHandle<()>)> {
        let (tx, rx) = channel(1_000);
        let mut actor = FDBChainStoreActor::new(config, chain, rx).await?;
        let j = tokio::spawn(async move { actor.run().await });
        Ok((FDBChainStore { sender: tx }, j))
    }

    /// Shutdown the FDBChainStore, cleaning up and terminating background processes.
    pub async fn shutdown(&self) -> Result<()> {
        let (tx, rx) = oneshot_channel();
        self.sender
            .send((FDBChainStoreMessage::Shutdown, tx))
            .await
            .map_err(|e| Error::SendError(format!("{}", e)))?;
        match rx.await {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::from(e)),
        }
    }
}

#[async_trait]
impl ChainStore for FDBChainStore {
    type BlockId = u64;

    #[allow(refining_impl_trait)]
    fn get_chain_state(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ChainState<<Self as ChainStore>::BlockId>>> + Send>>
    {
        let sender = self.sender.clone();
        Box::pin(async move {
            let (tx, rx) = oneshot_channel();
            sender
                .send((FDBChainStoreMessage::ChainState, tx))
                .await
                .map_err(|e| Error::SendError(format!("{}", e)))?;
            match rx.await {
                Ok(FDBChainStoreReply::ChainStateReply(s)) => Ok(s),
                Ok(_) => Err(Error::Internal("received unexpected reply".into())),
                Err(e) => Err(Error::from(e)),
            }
        })
    }

    #[allow(refining_impl_trait)]
    fn get_block_info(
        &self,
        db_id: Self::BlockId,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<BlockInfo<<Self as ChainStore>::BlockId>>>> + Send>,
    > {
        let sender = self.sender.clone();
        Box::pin(async move {
            let (tx, rx) = oneshot_channel();
            sender
                .send((FDBChainStoreMessage::BlockInfo(db_id), tx))
                .await
                .map_err(|e| Error::SendError(format!("{}", e)))?;
            match rx.await {
                Ok(FDBChainStoreReply::BlockInfoReply(r)) => Ok(r),
                Ok(_) => Err(Error::Internal("received unexpected reply".into())),
                Err(e) => Err(Error::from(e)),
            }
        })
    }

    #[allow(refining_impl_trait)]
    fn get_block_info_by_hash(
        &self,
        block_hash: BlockHash,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<BlockInfo<<Self as ChainStore>::BlockId>>>> + Send>,
    > {
        let sender = self.sender.clone();
        Box::pin(async move {
            let (tx, rx) = oneshot_channel();
            sender
                .send((FDBChainStoreMessage::BlockInfoByHash(block_hash), tx))
                .await
                .map_err(|e| Error::SendError(format!("{}", e)))?;
            match rx.await {
                Ok(FDBChainStoreReply::BlockInfoReply(r)) => Ok(r),
                Ok(_) => Err(Error::Internal("received unexpected reply".into())),
                Err(e) => Err(Error::from(e)),
            }
        })
    }

    // return a BlockInfoStream which will stream the BlockInfo's from db_id downwards, for max_blocks or until reaching Genesis
    // it returns the BlockInfoStream directly, not a future to the BlockInfoStream.
    //
    // There are three channels involved here, the first two of which are normal for every command and the last one is specfic to this command:
    //  1. The self.sender channel which is used for sending commands to the Actor
    //  2. The (tx,rx) one shot channel which is used for sending back the results - in this case we dont really need this and just send back an empty reply, but we have to conform to the command standard
    //  3. A temporary channel for the stream of results which are sent by a task spawned by the Actor and are presented to the caller through the BlockInfoStream interface.
    async fn get_block_infos(
        &self,
        db_id: Self::BlockId,
        max_blocks: Option<u64>,
    ) -> Result<BlockInfoStreamFromChannel<Self::BlockId>> {
        let sender = self.sender.clone();
        let (tx, rx) = oneshot_channel();
        let (r_tx, r_rx) = channel(1000);
        sender
            .send((
                FDBChainStoreMessage::BlockInfos(db_id, max_blocks, r_tx),
                tx,
            ))
            .await
            .map_err(|e| Error::SendError(format!("{}", e)))?;
        match rx.await {
            Ok(FDBChainStoreReply::BlockInfosReply) => {
                let r = BlockInfoStreamFromChannel::new(r_rx);
                Ok(r)
            }
            Ok(_) => Err(Error::Internal("received unexpected reply".into())),
            Err(e) => Err(Error::from(e)),
        }
    }

    /// Store the block info in the ChainStore, returning an updated BlockInfo structure and updating
    /// the ChainState as required.
    ///
    /// Implementation of [ChainStore::store_block_info()], see there for more information.
    ///
    /// Calls the actor function StoreBlockInfo().
    #[allow(refining_impl_trait)]
    fn store_block_info(
        &self,
        block_info: BlockInfo<Self::BlockId>,
    ) -> Pin<Box<dyn Future<Output = Result<BlockInfo<Self::BlockId>>> + Send>> {
        let sender = self.sender.clone();
        Box::pin(async move {
            let (tx, rx) = oneshot_channel();
            sender
                .send((FDBChainStoreMessage::StoreBlockInfo(block_info), tx))
                .await
                .map_err(|e| Error::SendError(format!("{}", e)))?;
            match rx.await {
                Ok(FDBChainStoreReply::BlockInfoReply(Some(r))) => Ok(r),
                Ok(_) => Err(Error::Internal("received unexpected reply".into())),
                Err(e) => Err(Error::from(e)),
            }
        })
    }
}

#[derive(Debug)]
enum FDBChainStoreMessage {
    ChainState,
    BlockInfo(<FDBChainStore as ChainStore>::BlockId),
    BlockInfoByHash(BlockHash),
    BlockInfos(
        <FDBChainStore as ChainStore>::BlockId,
        Option<u64>,
        Sender<BlockInfo<<FDBChainStore as ChainStore>::BlockId>>,
    ),
    StoreBlockInfo(BlockInfo<<FDBChainStore as ChainStore>::BlockId>),
    Shutdown,
}

#[derive(Debug)]
enum FDBChainStoreReply {
    ChainStateReply(ChainState<<FDBChainStore as ChainStore>::BlockId>),
    BlockInfoReply(Option<BlockInfo<<FDBChainStore as ChainStore>::BlockId>>),
    BlockInfosReply,
    Done,
}

/// the chain store actor
///
/// todo: update to use minactor
struct FDBChainStoreActor {
    receiver: Receiver<(FDBChainStoreMessage, OneshotSender<FDBChainStoreReply>)>,
    db: foundationdb::Database,
    // root directory for chainstore
    chain_dir: DirectoryOutput,
    // BlockInfo directory
    infos_dir: DirectoryOutput,
    // hash index directory
    h_index_dir: DirectoryOutput,
    // next_id with lock
    next_id_lock: Arc<Mutex<u8>>,
}

impl FDBChainStoreActor {
    // BlockInfo directory - key = BlockId, value = BlockInfo
    const INFOS_DIR: &'static str = "infos";
    // Hash index directory - key = BlockHash, value = BlockId
    const H_INDEX_DIR: &'static str = "hindex";
    // ChainState key name
    const STATE_KEY: &'static str = "statekey";
    // NextId key name
    const NEXT_ID_KEY: &'static str = "nextid";

    /// Create a new FDBChainStore.
    ///
    /// The root directory supplied as a parameter must be dedicated to the ChainStore. If the
    /// ChainStore is part of a larger system, then this is probably a sub-directory of the larger
    /// systems directory. (e.g.: vec!["bsvmain", "chainstore"])
    pub async fn new(
        config: &ChainStoreConfig,
        chain: BlockchainId,
        receiver: Receiver<(FDBChainStoreMessage, OneshotSender<FDBChainStoreReply>)>,
    ) -> Result<FDBChainStoreActor> {
        let root_dir: Vec<String> = config.root_path.split('/').map(String::from).collect();
        let db = foundationdb::Database::default()?;
        let r_dir = foundationdb::directory::DirectoryLayer::default();
        // ensure chain dir exists and fetch it
        let trx = db.create_trx()?;
        let chain_dir = r_dir.create_or_open(&trx, &root_dir, None, None).await?;
        trx.commit().await?;
        // ensure infos dir exists and fetch it
        let trx = db.create_trx()?;
        let i = vec![String::from(Self::INFOS_DIR)];
        let infos_dir = chain_dir.create_or_open(&trx, &i, None, None).await?;
        trx.commit().await?;
        // ensure h_index dir exists and fetch it
        let trx = db.create_trx()?;
        let i = vec![String::from(Self::H_INDEX_DIR)];
        let h_index_dir = chain_dir.create_or_open(&trx, &i, None, None).await?;
        trx.commit().await?;
        Self::ensure_db_initialized(&db, &chain_dir, infos_dir.clone(), &h_index_dir, chain)
            .await?;
        Ok(FDBChainStoreActor {
            receiver,
            db,
            chain_dir,
            infos_dir,
            h_index_dir,
            next_id_lock: Arc::new(Mutex::new(0)),
        })
    }

    // ensure that database is initialized
    async fn ensure_db_initialized(
        db: &foundationdb::Database,
        chain_dir: &DirectoryOutput,
        info_dir: DirectoryOutput,
        h_index_dir: &DirectoryOutput,
        chain: BlockchainId,
    ) -> Result<()> {
        let trx = db.create_trx()?;
        let state_key = Self::get_state_key(chain_dir).unwrap();
        let v = trx.get(&state_key, false).await?;
        if v.is_none() {
            // initialize database
            // set chain_state
            let v = Self::encode_chain_state(&ChainState {
                most_work_tip: 0,
                active_tips: vec![0],
                dormant_tips: vec![],
                invalid_tips: vec![],
            });
            trx.set(&state_key, &v);
            // set next_id
            let v = Self::encode_next_id(1);
            let k = Self::get_next_id_key(chain_dir).unwrap();
            trx.set(&k, &v);
            // store genesis BlockInfo
            let gbi = BlockInfo::genesis_info(chain);
            let k2 = Self::get_block_info_key(&info_dir, 0).unwrap();
            let v2 = Self::encode_block_info(&gbi);
            trx.set(&k2, &v2);
            let k3 = Self::get_h_index_key(h_index_dir, &gbi.hash).unwrap();
            let v3 = Self::encode_h_index(0);
            trx.set(&k3, &v3);
            trx.commit().await?;
        } else {
            trx.cancel();
        }
        Ok(())
    }

    // get the key for the state
    fn get_state_key(chain_dir: &DirectoryOutput) -> Result<Vec<u8>> {
        Ok(chain_dir.pack(&Self::STATE_KEY)?)
    }

    // decode ChainState from fdb format
    pub(crate) fn decode_chain_state(
        v: &[u8],
    ) -> ChainState<<FDBChainStore as ChainStore>::BlockId> {
        let (most_work_tip, a, d, i) = unpack::<(u64, Element, Element, Element)>(v)
            .expect("unpack failed in decode_chain_state()");
        let active_tips = a
            .as_tuple()
            .unwrap()
            .iter()
            .map(|e| e.as_i64().unwrap() as u64)
            .collect();
        let dormant_tips = d
            .as_tuple()
            .unwrap()
            .iter()
            .map(|e| e.as_i64().unwrap() as u64)
            .collect();
        let invalid_tips = i
            .as_tuple()
            .unwrap()
            .iter()
            .map(|e| e.as_i64().unwrap() as u64)
            .collect();
        ChainState {
            most_work_tip,
            active_tips,
            dormant_tips,
            invalid_tips,
        }
    }

    // encode ChainState to fdb format
    pub(crate) fn encode_chain_state(
        cs: &ChainState<<FDBChainStore as ChainStore>::BlockId>,
    ) -> Vec<u8> {
        pack(&(
            cs.most_work_tip,
            &cs.active_tips,
            &cs.dormant_tips,
            &cs.invalid_tips,
        ))
    }

    // get the key for the next_id
    fn get_next_id_key(chain_dir: &DirectoryOutput) -> Result<Vec<u8>> {
        Ok(chain_dir.pack(&Self::NEXT_ID_KEY)?)
    }

    // decode the next_id from fdb
    pub(crate) fn decode_next_id(v: &[u8]) -> <FDBChainStore as ChainStore>::BlockId {
        let (i,) = unpack(v).expect("unpack failed in decode_next_id()");
        i
    }

    // encode the next_id into fdb
    pub(crate) fn encode_next_id(v: <FDBChainStore as ChainStore>::BlockId) -> Vec<u8> {
        pack(&(v,))
    }

    // get the key for the BlockInfo
    fn get_block_info_key(
        info_dir: &DirectoryOutput,
        block_id: <FDBChainStore as ChainStore>::BlockId,
    ) -> Result<Vec<u8>> {
        Ok(info_dir.pack(&block_id)?)
    }

    // decode the BlockInfo from fdb
    pub(crate) fn decode_block_info(v: &[u8]) -> BlockInfo<<FDBChainStore as ChainStore>::BlockId> {
        // the tuple is too large for the shortcut implementation
        let i = unpack::<Vec<Element>>(v).expect("unpack failed in decode_block_info()");
        let hash = BlockHash::from(i[1].as_bytes().unwrap().to_vec().as_slice());
        let next_ids = i[5]
            .as_tuple()
            .unwrap()
            .iter()
            .map(|j| j.as_i64().unwrap() as u64)
            .collect();
        let chain_work = i[9].as_bytes().map(|j| j.to_vec());
        let miner = i[12].as_str().map(String::from);
        BlockInfo {
            id: i[0].as_i64().unwrap() as u64,
            hash,
            header: BlockHeader::from_binary_buf(i[2].as_bytes().unwrap()).unwrap(),
            height: i[3].as_i64().unwrap() as u64,
            prev_id: i[4].as_i64().unwrap() as u64,
            next_ids,
            size: i[6].as_i64().map(|j| j as u64),
            num_tx: i[7].as_i64().map(|j| j as u64),
            median_time: i[8].as_i64().map(|j| j as u64),
            chain_work,
            total_tx: i[10].as_i64().map(|j| j as u64),
            total_size: i[11].as_i64().map(|j| j as u64),
            miner,
            validity: BlockValidity::from(i[13].as_i64().unwrap() as u8),
        }
    }

    // encode the block_info into fdb
    pub(crate) fn encode_block_info(
        v: &BlockInfo<<FDBChainStore as ChainStore>::BlockId>,
    ) -> Vec<u8> {
        let hash = Element::Bytes(Bytes::from(Vec::from(v.hash.hash)));
        let hdr = Element::Bytes(Bytes::from(v.header.to_binary_buf().unwrap()));
        let m = match v.miner.clone() {
            Some(k) => Element::String(Cow::from(k)),
            None => Element::Nil,
        };
        let n_i = Element::Tuple(v.next_ids.iter().map(|i| Element::Int(*i as i64)).collect());
        let c_w = v
            .chain_work
            .clone()
            .map(|j| Element::Bytes(Bytes::from(j)))
            .unwrap_or(Element::Nil);
        let i = vec![
            Element::Int(v.id as i64),
            hash,
            hdr,
            Element::Int(v.height as i64),
            Element::Int(v.prev_id as i64),
            n_i,
            v.size
                .map(|j| Element::Int(j as i64))
                .unwrap_or(Element::Nil),
            v.num_tx
                .map(|j| Element::Int(j as i64))
                .unwrap_or(Element::Nil),
            v.median_time
                .map(|j| Element::Int(j as i64))
                .unwrap_or(Element::Nil),
            c_w,
            v.total_tx
                .map(|j| Element::Int(j as i64))
                .unwrap_or(Element::Nil),
            v.total_size
                .map(|j| Element::Int(j as i64))
                .unwrap_or(Element::Nil),
            m,
            Element::Int(u8::from(v.validity.clone()) as i64),
        ];
        pack(&i)
    }

    // get the key for the hash index
    fn get_h_index_key(h_index_dir: &DirectoryOutput, block_hash: &BlockHash) -> Result<Vec<u8>> {
        Ok(h_index_dir.pack(&block_hash.to_binary_buf().unwrap())?)
    }

    // decode the hash index value from fdb
    pub(crate) fn decode_h_index(v: &[u8]) -> <FDBChainStore as ChainStore>::BlockId {
        let (i,) = unpack(v).expect("unpack failed in decode_h_index()");
        i
    }

    // encode the hash index value into fdb
    pub(crate) fn encode_h_index(v: <FDBChainStore as ChainStore>::BlockId) -> Vec<u8> {
        pack(&(v,))
    }

    // get the BlockId from the hash
    pub(crate) async fn get_block_id_from_hash(
        trx: &Transaction,
        block_hash: &BlockHash,
        h_index_dir: &DirectoryOutput,
    ) -> Result<Option<<FDBChainStore as ChainStore>::BlockId>> {
        let k = Self::get_h_index_key(h_index_dir, block_hash)?;
        let v = trx.get(k.as_slice(), false).await?;
        if v.is_none() {
            return Ok(None);
        }
        let i = Self::decode_h_index(&v.unwrap());
        Ok(Some(i))
    }

    async fn get_next_id(
        trx: &Transaction,
        next_id: &Mutex<u8>,
        chain_dir: &DirectoryOutput,
    ) -> Result<<FDBChainStore as ChainStore>::BlockId> {
        // only do one of these at a time to prevent db transaction clashes
        let k = Self::get_next_id_key(chain_dir).unwrap();
        let _lck = next_id.lock().await;
        let v = trx.get(&k, false).await.unwrap().unwrap();
        let id = Self::decode_next_id(&v);
        let v2 = Self::encode_next_id(id + 1);
        trx.set(&k, &v2);
        Ok(id)
    }

    /// Handles the ChainState message.
    async fn handle_get_chain_state(
        &self,
        reply: OneshotSender<FDBChainStoreReply>,
    ) -> Result<JoinHandle<()>> {
        let k = Self::get_state_key(&self.chain_dir)?;
        let trx = self.db.create_trx()?;
        Ok(tokio::spawn(async move {
            let v = trx
                .get(k.as_slice(), false)
                .await
                .unwrap()
                .expect("chainstate missing from db"); // todo: remove
            let r = Self::decode_chain_state(&v);
            reply
                .send(FDBChainStoreReply::ChainStateReply(r))
                .expect("send of reply failed in get_chain_state()"); // todo: remove
        }))
    }

    async fn get_block_info(
        &self,
        db_id: <FDBChainStore as ChainStore>::BlockId,
        reply: OneshotSender<FDBChainStoreReply>,
    ) -> Result<JoinHandle<()>> {
        let k = Self::get_block_info_key(&self.infos_dir, db_id)?;
        let trx = self.db.create_trx()?;
        Ok(tokio::spawn(async move {
            let r = trx.get(k.as_slice(), false).await.unwrap();
            reply
                .send(FDBChainStoreReply::BlockInfoReply(
                    r.map(|i| Self::decode_block_info(&i)),
                ))
                .expect("send of reply failed in get_block_info()");
        }))
    }

    /// Gets the block info given the hash.
    ///
    /// Expected to be called as part of a larger transaction.
    async fn sub_block_info_by_hash(
        trx: &Transaction,
        hash: &BlockHash,
        h_index_dir: &DirectoryOutput,
        infos_dir: &DirectoryOutput,
    ) -> Result<Option<BlockInfo<<FDBChainStore as ChainStore>::BlockId>>> {
        match Self::get_block_id_from_hash(trx, hash, h_index_dir).await? {
            None => Ok(None),
            Some(id) => {
                let k = Self::get_block_info_key(infos_dir, id)?;
                let r = trx.get(k.as_slice(), false).await.unwrap();
                match r {
                    Some(i) => Ok(Some(Self::decode_block_info(&i))),
                    None => Ok(None),
                }
            }
        }
    }

    /// Handles the actor BlockInfo message.
    async fn get_block_info_by_hash(
        &self,
        hash: BlockHash,
        reply: OneshotSender<FDBChainStoreReply>,
    ) -> Result<JoinHandle<()>> {
        let trx = self.db.create_trx()?;
        let h_index_dir = self.h_index_dir.clone();
        let infos_dir = self.infos_dir.clone();
        Ok(tokio::spawn(async move {
            let r = Self::sub_block_info_by_hash(&trx, &hash, &h_index_dir, &infos_dir)
                .await
                .expect("failure during get block info by hash()");
            reply
                .send(FDBChainStoreReply::BlockInfoReply(r))
                .expect("failed to send reply");
        }))
    }

    // todo: The algorithm used here is to get the block by its block_id, then get the previous by its block id, etc, etc.
    // However, we know that the block ids are always assigned in ascending order and there are comparatively few forks.
    // It may be more efficient to iterate through all block infos, starting with the first and going backwards, and skipping
    // any blocks from forks.
    async fn get_block_infos(
        &self,
        db_id: <FDBChainStore as ChainStore>::BlockId,
        max_blocks: Option<u64>,
        tx: Sender<BlockInfo<<FDBChainStore as ChainStore>::BlockId>>,
        reply: OneshotSender<FDBChainStoreReply>,
    ) -> Result<JoinHandle<()>> {
        let infos_dir = self.infos_dir.clone();
        let mut trx = self.db.create_trx().unwrap();
        let num_blocks = max_blocks.unwrap_or(u64::MAX);
        let mut id = db_id;
        reply
            .send(FDBChainStoreReply::BlockInfosReply)
            .expect("failed to send reply");
        Ok(tokio::spawn(async move {
            let mut x = 0u64;
            loop {
                let k = Self::get_block_info_key(&infos_dir, id).unwrap();
                match trx.get(k.as_slice(), false).await {
                    Err(e) => match e.code() {
                        1007 => {
                            // transaction too old, reset the transaction and continue
                            trx.reset();
                        }
                        _ => {
                            break;
                        }
                    },
                    Ok(r) => {
                        if r.is_none() {
                            break;
                        }
                        let b_info = Self::decode_block_info(&r.unwrap());
                        id = b_info.prev_id;
                        let h = b_info.height;
                        tx.send(b_info).await.unwrap();
                        x += 1;
                        if x >= num_blocks || h == 0 {
                            break;
                        }
                    }
                }
            }
        }))
    }

    /// Implements [ChainStore::store_block_info()].
    async fn store_block_info(
        &self,
        mut block_info: BlockInfo<<FDBChainStore as ChainStore>::BlockId>,
        reply: OneshotSender<FDBChainStoreReply>,
    ) -> Result<JoinHandle<()>> {
        let trx = self.db.create_trx()?;
        let h_index_dir = self.h_index_dir.clone();
        let chain_dir = self.chain_dir.clone();
        let infos_dir = self.infos_dir.clone();
        let next_id_lck = self.next_id_lock.clone();
        Ok(tokio::spawn(async move {
            // lookup id from hash, creating it if it doesn't exist already
            match Self::get_block_id_from_hash(&trx, &block_info.hash, &h_index_dir)
                .await
                .expect("couldnt get block id from hash")           // todo: remove
            {
                None => {
                    let id = Self::get_next_id(&trx, &next_id_lck, &chain_dir)
                        .await
                        .expect("couldnt get next_id");         // todo: remove
                    let k = Self::get_h_index_key(&h_index_dir, &block_info.hash).unwrap();
                    let v = Self::encode_h_index(id);
                    trx.set(&k, &v);
                    block_info.id = id;
                }
                Some(id) => {
                    block_info.id = id;
                }
            }
            let parent = Self::sub_block_info_by_hash(
                &trx,
                &block_info.header.prev_hash,
                &h_index_dir,
                &infos_dir,
            )
            .await
            .expect("failed to get parent by hash"); // todo: remove
            if parent.is_none() {
                panic!("parent not found"); // todo
                                            // return Err(ChainStoreError::ParentNotFound)
            }
            let mut parent = parent.unwrap();
            // check that the child is listed in the parents next_ids
            if !parent.next_ids.contains(&block_info.id) {
                // update the next_ids in the parent and save it
                parent.next_ids.push(block_info.id);
                let k = Self::get_block_info_key(&infos_dir, parent.id).unwrap();
                let v = Self::encode_block_info(&parent);
                trx.set(&k, &v);
            }
            // update total_size & total_tx if possible
            if parent.total_size.is_some() && block_info.size.is_some() {
                block_info.total_size = Some(parent.total_size.unwrap() + block_info.size.unwrap())
            }
            if parent.total_tx.is_some() && block_info.num_tx.is_some() {
                block_info.total_tx = Some(parent.total_tx.unwrap() + block_info.num_tx.unwrap())
            }
            // update height, prev_id, and validity
            block_info.height = parent.height + 1;
            block_info.prev_id = parent.id;
            block_info.validity = match parent.validity {
                BlockValidity::Unknown => BlockValidity::Unknown,
                BlockValidity::Valid => block_info.validity,
                BlockValidity::ValidHeader => {
                    if block_info.validity == BlockValidity::Valid {
                        BlockValidity::ValidHeader
                    } else {
                        block_info.validity
                    }
                }
                BlockValidity::Invalid => BlockValidity::InvalidAncestor,
                BlockValidity::HeaderInvalid => BlockValidity::InvalidAncestor,
                BlockValidity::InvalidAncestor => BlockValidity::InvalidAncestor,
            };
            // save the block info
            let k = Self::get_block_info_key(&infos_dir, block_info.id).unwrap();
            let v = Self::encode_block_info(&block_info);
            trx.set(&k, &v);
            trx.commit().await.expect("couldnt commit transaction"); // todo: remove

            // update the chain state if necessary - IAMHERE

            // send result back
            reply
                .send(FDBChainStoreReply::BlockInfoReply(Option::from(block_info)))
                .expect("send of reply failed in store_block_info()"); // todo: remove
        }))
    }

    /// main actor thread
    async fn run(&mut self) {
        let mut tasks = vec![];
        loop {
            tokio::select! {
                Some((msg, reply)) = self.receiver.recv() => {
                    match msg {
                        FDBChainStoreMessage::ChainState => {
                            let j = self.handle_get_chain_state(reply).await.unwrap();
                            tasks.push(j);
                        },
                        FDBChainStoreMessage::BlockInfo(db_id) => {
                            let j = self.get_block_info(db_id, reply).await.unwrap();
                            tasks.push(j);
                        },
                        FDBChainStoreMessage::BlockInfoByHash(block_hash) => {
                            let j = self.get_block_info_by_hash(block_hash, reply).await.unwrap();
                            tasks.push(j);
                        },
                        FDBChainStoreMessage::BlockInfos(block_id, max_blocks, r_tx) => {
                            let j = self.get_block_infos(block_id, max_blocks, r_tx, reply).await.unwrap();
                            tasks.push(j);
                        },
                        FDBChainStoreMessage::StoreBlockInfo(block_info) => {
                            let j = self.store_block_info(block_info, reply).await.unwrap();
                            tasks.push(j);
                        },
                        FDBChainStoreMessage::Shutdown => {
                            reply.send(FDBChainStoreReply::Done).expect("unexpected failure shutting down");
                            break;
                        }
                    };
                },
                else => {
                    sleep(Duration::from_millis(100)).await;
                }
            }
            // todo: clean up completed handles
            // let mut t2 = vec![];
            // for t in tasks {
            //
            // }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_state_encoding() {
        let s = ChainState {
            most_work_tip: 2,
            active_tips: vec![3, 4],
            dormant_tips: vec![],
            invalid_tips: vec![6, 7],
        };
        let p = FDBChainStoreActor::encode_chain_state(&s);
        let u = FDBChainStoreActor::decode_chain_state(&p);
        assert_eq!(u, s);
    }

    #[test]
    fn block_info_encoding() {
        let b = BlockInfo::genesis_info(BlockchainId::Main);
        let p = FDBChainStoreActor::encode_block_info(&b);
        let v = FDBChainStoreActor::decode_block_info(&p);
        assert_eq!(b, v);
    }

    #[test]
    fn hash_index_encodring() {
        let i = 76265u64;
        let j = FDBChainStoreActor::encode_h_index(i);
        let k = FDBChainStoreActor::decode_h_index(&j);
        assert_eq!(i, k);
    }

    #[test]
    fn tuple_experiments() {
        let t = (1, 2, 3);
        let p = pack(&t);
        let u = unpack::<Vec<Element>>(&p).unwrap();
        println!("{:?}", u);
    }
}
