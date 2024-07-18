use std::borrow::Cow;
use std::collections::BTreeMap;
use async_trait::async_trait;
use bitcoinsv::bitcoin::{BlockchainId, BlockHash, BlockHeader, Encodable};
use foundationdb::directory::{Directory, DirectoryOutput};
use foundationdb::tuple::{Bytes, Element, pack, unpack};
use crate::chain_store::ChainState;
use crate::{BlockId, BlockInfo, BlockValidity, ChainStore, Error, Result};

/// FDBChainStore is an implementation of ChainStore for foundationdb.
///
/// Applications must call foundationdb::boot() before using this struct.
/// See https://docs.rs/foundationdb/latest/foundationdb/fn.boot.html.
///
/// It uses the foundationdb tuple encoding so that the database can be read by multiple
/// languages.
pub struct FDBChainStore {
    db: foundationdb::Database,
    // root directory for chainstore
    chain_dir: DirectoryOutput,
    // BlockInfo directory
    infos_dir: DirectoryOutput,
    // hash index directory
    h_index_dir: DirectoryOutput,
    // hash index cache
    h_index_cache: BTreeMap<BlockHash, BlockId>,
}

impl FDBChainStore {
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
    pub async fn new(root_dir: Vec<String>, chain: BlockchainId) -> Result<Self> {
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
        Self::ensure_db_initialized(&db, &chain_dir, &infos_dir, &h_index_dir, chain).await?;
        Ok(FDBChainStore {
            db, chain_dir, infos_dir, h_index_dir, h_index_cache: BTreeMap::new(),
        })
    }

    // ensure that database is initialized
    async fn ensure_db_initialized(db: &foundationdb::Database, chain_dir: &DirectoryOutput, info_dir: &DirectoryOutput,
                                   h_index_dir: &DirectoryOutput, chain: BlockchainId) -> Result<()> {
        let trx = db.create_trx()?;
        let state_key = Self::get_state_key(chain_dir).unwrap();
        let v = trx.get(&*state_key, false).await?;
        if v.is_none() {
            // initialize database
            // set chain_state
            let v = Self::encode_chain_state(&ChainState { most_work_tip: 0, active_tips: vec![0], dormant_tips: vec![], invalid_tips: vec![]});
            trx.set(&*state_key, &*v);
            // set next_id
            let v = Self::encode_next_id(1);
            let k = Self::get_next_id_key(chain_dir).unwrap();
            trx.set(&*k, &*v);
            // store genesis BlockInfo
            let gbi = BlockInfo::genesis_info(chain);
            let k2 = Self::get_block_info_key(info_dir, 0).unwrap();
            let v2 = Self::encode_block_info(&gbi);
            trx.set(&*k2, &*v2);
            let k3 = Self::get_h_index_key(h_index_dir, &gbi.hash).unwrap();
            let v3 = Self::encode_h_index(0);
            trx.set(&*k3, &*v3);
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
    pub(crate) fn decode_chain_state(v: &Vec<u8>) -> ChainState {
        let (most_work_tip, a, d, i) = unpack::<(u64, Element, Element, Element)>(v).expect("unpack failed in decode_chain_state()");
        let active_tips = a.as_tuple().unwrap().iter().map(|e| e.as_i64().unwrap() as u64).collect();
        let dormant_tips = d.as_tuple().unwrap().iter().map(|e| e.as_i64().unwrap() as u64).collect();
        let invalid_tips = i.as_tuple().unwrap().iter().map(|e| e.as_i64().unwrap() as u64).collect();
        ChainState {
            most_work_tip, active_tips, dormant_tips, invalid_tips
        }
    }

    // encode ChainState to fdb format
    pub(crate) fn encode_chain_state(cs: &ChainState) -> Vec<u8> {
        pack(&(cs.most_work_tip, &cs.active_tips, &cs.dormant_tips, &cs.invalid_tips))
    }

    // get the key for the next_id
    fn get_next_id_key(chain_dir: &DirectoryOutput) -> Result<Vec<u8>> {
        Ok(chain_dir.pack(&Self::NEXT_ID_KEY)?)
    }

    // decode the next_id from fdb
    pub(crate) fn decode_next_id(v: &Vec<u8>) -> BlockId {
        let (i, )  = unpack(v).expect("unpack failed in decode_next_id()");
        return i;
    }

    // encode the next_id into fdb
    pub(crate) fn encode_next_id(v: BlockId) -> Vec<u8> {
        pack(&(v, ))
    }

    // get the key for the BlockInfo
    fn get_block_info_key(info_dir: &DirectoryOutput, block_id: BlockId) -> Result<Vec<u8>> {
        Ok(info_dir.pack(&block_id)?)
    }

    // decode the BlockInfo from fdb
    pub(crate) fn decode_block_info(v: &Vec<u8>) -> BlockInfo {
        // the tuple is too large for the shortcut implementation
        let i = unpack::<Vec<Element>>(v).expect("unpack failed in decode_block_info()");
        let hash = BlockHash::from(i[1].as_bytes().unwrap().to_vec().as_slice());
        let next_ids = i[5].as_tuple().unwrap().iter().map(|j| j.as_i64().unwrap() as u64).collect();
        let chain_work = i[9].as_bytes().map(|j| j.to_vec());
        let miner = i[12].as_str().map(|j| String::from(j));
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
    pub(crate) fn encode_block_info(v: &BlockInfo) -> Vec<u8> {
        let hash = Element::Bytes(Bytes::from(Vec::from(v.hash.hash)));
        let hdr = Element::Bytes(Bytes::from(v.header.to_binary_buf().unwrap()));
        let m = match v.miner.clone() {
            Some(k) => {
                Element::String(Cow::from(k))
            },
            None => Element::Nil
        };
        let n_i = Element::Tuple(v.next_ids.iter().map(|i| Element::Int(*i as i64)).collect());
        let c_w = v.chain_work.clone().map(|j| Element::Bytes(Bytes::from(j))).unwrap_or(Element::Nil);
        let i = vec![Element::Int(v.id as i64), hash, hdr,
                     Element::Int(v.height as i64), Element::Int(v.prev_id as i64), n_i,
                     v.size.map(|j| Element::Int(j as i64)).unwrap_or(Element::Nil),
                     v.num_tx.map(|j| Element::Int(j as i64)).unwrap_or(Element::Nil),
                     v.median_time.map(|j| Element::Int(j as i64)).unwrap_or(Element::Nil),
                     c_w,
                     v.total_tx.map(|j| Element::Int(j as i64)).unwrap_or(Element::Nil),
                     v.total_size.map(|j| Element::Int(j as i64)).unwrap_or(Element::Nil),
                     m,
                     Element::Int(u8::from(v.validity.clone()) as i64)];
        pack(&i)
    }

    // get the key for the hash index
    fn get_h_index_key(h_index_dir: &DirectoryOutput, block_hash: &BlockHash) -> Result<Vec<u8>> {
        Ok(h_index_dir.pack(&block_hash.to_binary_buf().unwrap())?)
    }

    // decode the hash index value from fdb
    pub(crate) fn decode_h_index(v: &Vec<u8>) -> BlockId {
        let (i, )  = unpack(v).expect("unpack failed in decode_h_index()");
        return i;
    }

    // encode the hash index value into fdb
    pub(crate) fn encode_h_index(v: BlockId) -> Vec<u8> {
        pack(&(v, ))
    }

    // get the BlockId from the hash
    pub(crate) async fn get_block_id_from_hash(&mut self, block_hash: &BlockHash) -> Result<Option<BlockId>> {
        match self.h_index_cache.get(block_hash) {
            Some(id) => return Ok(Some(*id)),
            None => {
                let k = Self::get_h_index_key(&self.h_index_dir, block_hash)?;
                let trx = self.db.create_trx()?;
                let v = trx.get(k.as_slice(), false).await?;
                trx.cancel();
                if v.is_none() {
                    return Ok(None);
                }
                let i = Self::decode_h_index(&v.unwrap().to_vec());
                self.h_index_cache.insert(block_hash.clone(), i);
                Ok(Some(i))
            },
        }
    }

    async fn get_next_id(&self) -> Result<BlockId> {
        let trx = self.db.create_trx().unwrap();
        let k = Self::get_next_id_key(&self.chain_dir).unwrap();
        let v= trx.get(&*k, false).await.unwrap().unwrap();
        let id = Self::decode_next_id(&v.to_vec());
        let v2 = Self::encode_next_id(id+1);
        trx.set(&*k, &*v2);
        trx.commit().await.unwrap();
        Ok(id)
    }
}

#[async_trait]
impl ChainStore for FDBChainStore {
    async fn get_chain_state(&mut self) -> Result<ChainState> {
        let k = Self::get_state_key(&self.chain_dir)?;
        let trx = self.db.create_trx()?;
        let v = trx.get(k.as_slice(), false).await?.unwrap();
        trx.cancel();
        Ok(FDBChainStore::decode_chain_state(&v.to_vec()))
    }

    async fn get_block_info(&mut self, db_id: &BlockId) -> Result<Option<BlockInfo>> {
        let k = Self::get_block_info_key(&self.infos_dir, *db_id)?;
        let trx = self.db.create_trx()?;
        let v = trx.get(k.as_slice(), false).await?;
        trx.cancel();
        if v.is_none() {
            Ok(None)
        } else {
            Ok(Some(Self::decode_block_info(&v.unwrap().to_vec())))
        }
    }

    async fn get_block_info_by_hash(&mut self, hash: &BlockHash) -> Result<Option<BlockInfo>> {
        match self.get_block_id_from_hash(hash).await? {
            None => Ok(None),
            Some(id) => self.get_block_info(&id).await
        }
    }

    async fn get_block_infos(&mut self, db_id: &BlockId, max_blocks: Option<u64>) -> Result<Vec<BlockInfo>> {
        todo!()
    }

    async fn store_block_info(&mut self, block_info: BlockInfo) -> Result<BlockInfo> {
        let mut result = block_info.clone();
        match self.get_block_id_from_hash(&result.hash).await? {
            None => {
                let id = self.get_next_id().await?;
                let k = Self::get_h_index_key(&self.h_index_dir, &result.hash).unwrap();
                let v = Self::encode_h_index(id);
                let trx = self.db.create_trx().unwrap();
                trx.set(&*k, &*v);
                trx.commit().await?;
                result.id = id;
            },
            Some(id) => {
                result.id = id;
            }
        }
        let parent = self.get_block_info_by_hash(&result.header.prev_hash).await?;
        if parent.is_none() {
            return Err(Error::ParentNotFound)
        }
        let mut parent = parent.unwrap();
        let trx = self.db.create_trx().unwrap();    // start the update transaction
        // check that the child is listed in the parents next_ids
        if ! parent.next_ids.contains(&result.id) {
            // update the next_ids in the parent and save it
            parent.next_ids.push(result.id);
            let k = Self::get_block_info_key(&self.infos_dir, parent.id).unwrap();
            let v = Self::encode_block_info(&parent);
            trx.set(&*k, &*v);
        }
        // update total_size & total_tx if possible
        if parent.total_size.is_some() && result.size.is_some() {
            result.total_size = Some(parent.total_size.unwrap() + result.size.unwrap())
        }
        if parent.total_tx.is_some() && result.num_tx.is_some() {
            result.total_tx = Some(parent.total_tx.unwrap() + result.num_tx.unwrap())
        }
        // update height, prev_id, and validity
        result.height = parent.height + 1;
        result.prev_id = parent.id;
        result.validity = match parent.validity {
            BlockValidity::Unknown => BlockValidity::Unknown,
            BlockValidity::Valid => result.validity,
            BlockValidity::ValidHeader => {
                if result.validity == BlockValidity::Valid {
                    BlockValidity::ValidHeader
                } else {
                    result.validity
                }
            },
            BlockValidity::Invalid => BlockValidity::InvalidAncestor,
            BlockValidity::HeaderInvalid => BlockValidity::InvalidAncestor,
            BlockValidity::InvalidAncestor => BlockValidity::InvalidAncestor,
        };
        // save the block info
        let k = Self::get_block_info_key(&self.infos_dir, result.id).unwrap();
        let v = Self::encode_block_info(&result);
        trx.set(&*k, &*v);
        trx.commit().await?;
        Ok(result)
    }
}


#[cfg(test)]
mod tests {
    use hex::FromHex;
    use super::*;

    #[test]
    fn chain_state_encoding() {
        let s = ChainState {
            most_work_tip: 2, active_tips: vec![3,4], dormant_tips: vec![], invalid_tips: vec![6,7]
        };
        let p = FDBChainStore::encode_chain_state(&s);
        let u = FDBChainStore::decode_chain_state(&p);
        assert_eq!(u, s);
    }

    #[test]
    fn block_info_encoding() {
        let b = BlockInfo::genesis_info(BlockchainId::Mainnet);
        let p = FDBChainStore::encode_block_info(&b);
        let v = FDBChainStore::decode_block_info(&p);
        assert_eq!(b, v);
    }

    #[test]
    fn hash_index_encodring() {
        let i = 76265u64;
        let j = FDBChainStore::encode_h_index(i);
        let k = FDBChainStore::decode_h_index(&j);
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
