use std::path::PathBuf;
use std::pin::Pin;
use async_trait::async_trait;
use bitcoinsv::bitcoin::{BlockHash, BlockHeader, Encodable};
use tokio::io::AsyncRead;
use crate::{BlockArchive, Error, Result};
use hex::{FromHex, ToHex};
use tokio::fs::File;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReadDirStream;
use crate::block_archive::{BlockHashListStream, BlockHashListStreamFromChannel};

// the absolute maximum number of blocks that will be stored
// this is used to limit the size of the channel used to send block hashes
// at the time of writing, testnet had about 1.2 million blocks
// if this is too small, the background process will wait for the channel to be read
const MAX_BLOCKS: usize = 2_000_000;

/// A simple file-based block archive.
///
/// Blocks are stored in a directory structure based on the block hash. The first level of directories
/// is based on the last two characters of the hex encoded hash, the second level is based on the
/// third and fourth last characters, and the block is stored in a file named after the hash with a
/// "bin" extension.
///
///
/// Example: /31/c5/00000000000000000124a294b9e1e65224f0636ffd4dadac777bed5e709dc531.bin
///
/// This is simplistic to get started. It is not efficient for large numbers of small blocks.
///
/// Example code:
///     let root_dir = std::path::PathBuf::from("/mnt/blockstore/mainnet");
///     let mut archive= SimpleFileBasedBlockArchive::new(root_dir);
///
/// Note that if block files are stored in the wrong location then they are not recognised by the
/// archive.
#[derive(Debug)]
pub struct SimpleFileBasedBlockArchive {
    /// The root of the file store
    pub root_path: PathBuf,
}

impl SimpleFileBasedBlockArchive
{
    /// Create a new block archive with the given root path.
    pub async fn new(root_path: PathBuf) -> Result<SimpleFileBasedBlockArchive> {
        // Check if the root_path is accessible
        match tokio::fs::metadata(&root_path).await {
            Ok(_) => {
                Ok(SimpleFileBasedBlockArchive {
                    root_path,
                })
            },
            Err(e) => {
                Err(e.into()) // Convert the error into your custom error type
            }
        }
    }

    // Get the path for a block.
    fn get_path_from_hash(&self, hash: &BlockHash) -> PathBuf {
        let mut path = self.root_path.clone();
        let s: String = hash.encode_hex();
        path.push(&s[62..]);
        path.push(&s[60..62]);
        path.push(s);
        path.set_extension("bin");
        path
    }

    // Get a list of all blocks in the background, sending results to the channel.
    // Do not return blocks that are stored in the wrong location because these
    // won't be retrievable by get_block().
    async fn block_list_bgrnd(root_path: PathBuf, transmit: tokio::sync::mpsc::Sender<BlockHash>) -> Result<()> {
        let mut stack = Vec::new();
        stack.push(root_path.clone());
        while let Some(path) = stack.pop() {
            let dir = tokio::fs::read_dir(path).await?;
            let mut stream = ReadDirStream::new(dir);
            // it would be fun to spawn a new task for each directory, but that would be a bit daft
            while let Some(entry) = stream.next().await {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    // ignore files which are not .bin files
                    if path.extension().is_none() || path.extension().unwrap() != "bin" {
                        continue;
                    }
                    let f_name = path.file_stem().unwrap().to_str().unwrap();
                    match BlockHash::from_hex(f_name) {
                        Ok(h) => {
                            // ignore files that are not in the correct location
                            let correct_path = root_path.join(&f_name[62..]).join(&f_name[60..62]).join(f_name).with_extension("bin");
                            if path != correct_path {
                                continue;
                            }
                            match transmit.send(h).await {
                                Ok(_) => {}
                                Err(_) => return Ok(())     // this is not an error, the receiver has merely dropped
                            }
                        }
                        // ignore files which are not valid block hashes
                        Err(_) => continue
                    };
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl BlockArchive for SimpleFileBasedBlockArchive
{
    async fn get_block(&self, block_hash: &BlockHash) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
        let path = self.get_path_from_hash(block_hash);
        match File::open(path).await {
            Ok(f) => Ok(Box::new(f)),
            Err(e) => match e.kind() {
                // if the file does not exist, return a BlockNotFound error
                std::io::ErrorKind::NotFound => Err(Error::BlockNotFound),
                _ => Err(e.into())
            }
        }
    }

    /// Check if a block exists in the archive.
    async fn block_exists(&self, block_hash: &BlockHash) -> Result<bool> {
        let path = self.get_path_from_hash(block_hash);
        match tokio::fs::metadata(path).await {
            Ok(_) => Ok(true),
            Err(e) => match e.kind() {
                // if the file does not exist, return false
                std::io::ErrorKind::NotFound => Ok(false),
                _ => Err(e.into())
            }
        }
    }

    async fn store_block(&self, block_hash: &BlockHash, block: &mut Box<dyn AsyncRead + Unpin + Send>) -> Result<()> {
        if self.block_exists(block_hash).await? {
            return Err(Error::BlockExists);
        }
        let path = self.get_path_from_hash(block_hash);
        // create the directory structure if it does not exist
        tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        // store the block in a file
        let mut file = File::create(path).await?;
        tokio::io::copy(block, &mut file).await?;
        Ok(())
    }

    async fn block_size(&self, block_hash: &BlockHash) -> Result<usize> {
        let path = self.get_path_from_hash(block_hash);
        match tokio::fs::metadata(path).await {
            Ok(m) => Ok(m.len() as usize),
            Err(e) => match e.kind() {
                // if the file does not exist, return a BlockNotFound error
                std::io::ErrorKind::NotFound => Err(Error::BlockNotFound),
                _ => Err(e.into())
            }
        }
    }

    async fn block_header(&self, block_hash: &BlockHash) -> Result<BlockHeader> {
        let path = self.get_path_from_hash(block_hash);
        match File::open(path).await {
            Ok(mut file) => Ok(BlockHeader::from_binary(&mut file).await?),
            Err(e) => match e.kind() {
                // if the file does not exist, return a BlockNotFound error
                std::io::ErrorKind::NotFound => Err(Error::BlockNotFound),
                _ => Err(e.into())
            }
        }
    }

    /// Get a list of all the blocks in the archive.
    ///
    /// It returns a stream of block hashes.
    ///
    /// Example code:
    ///     let mut results = archive.block_list().await.unwrap();
    ///     while let Some(block_hash) = results.next().await {
    ///       println!("{}", block_hash);
    ///     }
    ///
    /// This function does not return blocks that are stored in the wrong location because these
    /// won't be retrievable by get_block().
    async fn block_list(&mut self) -> Result<Pin<Box<dyn BlockHashListStream<Item=BlockHash>>>> {
        // make the channel large enough to buffer all hashes, including testnet
        // so that the background task can collect all buffer hashes despite how slow the consumer is
        let (tx, rx) = tokio::sync::mpsc::channel(MAX_BLOCKS);
        let handle = tokio::spawn(Self::block_list_bgrnd(self.root_path.clone(), tx));
        Ok(Box::pin(BlockHashListStreamFromChannel::new(rx, handle)))
    }
}


#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use hex::FromHex;
    use tokio::io::AsyncReadExt;
    use super::*;

    // Test the path generation from a block hash.
    #[tokio::test]
    async fn check_path_from_hash() {
        let s = SimpleFileBasedBlockArchive::new(PathBuf::from("../testdata/blockarchive")).await.unwrap();
        let h = BlockHash::from_hex("00000000000000000124a294b9e1e65224f0636ffd4dadac777bed5e709dc531").unwrap();
        let path = s.get_path_from_hash(&h);
        assert_eq!(path, PathBuf::from("../testdata/blockarchive/31/c5/00000000000000000124a294b9e1e65224f0636ffd4dadac777bed5e709dc531.bin"));
    }

    // Test the block list function.
    // two of the potentially ok block files are stored in the wrong location, so they shouldnt be returned
    #[tokio::test]
    async fn test_block_list() {
        let root = PathBuf::from("../testdata/blockarchive");
        let mut archive = SimpleFileBasedBlockArchive::new(root).await.unwrap();
        let mut results = archive.block_list().await.unwrap();
        let mut count = 0;
        while let Some(_) = results.next().await {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    // Test the block list function with no blocks.
    #[tokio::test]
    async fn test_empty_block_list() {
        // calling a block function from tokio is bad, but this is a test
        let root_dir = Temp::new_dir().unwrap();
        let root = root_dir.to_path_buf();
        let mut archive = SimpleFileBasedBlockArchive::new(root).await.unwrap();
        let mut results = archive.block_list().await.unwrap();
        let mut count = 0;
        while let Some(_) = results.next().await {
            count += 1;
        }
        assert_eq!(count, 0);
    }

    // Test the archive with a non-existent root directory.
    #[tokio::test]
    async fn test_non_existent_root_dir() {
        let root = PathBuf::from("../testdata/nonexistent");
        let archive = SimpleFileBasedBlockArchive::new(root).await;
        assert!(archive.is_err());
    }

    // Test getting a block
    #[tokio::test]
    async fn test_get_block() {
        let root = PathBuf::from("../testdata/blockarchive");
        let archive = SimpleFileBasedBlockArchive::new(root).await.unwrap();
        let h = BlockHash::from_hex("00000000000000a86c0a6d7b3445ff9e64908d6417cd6b256dbc23efd01de26f").unwrap();
        let mut block = archive.get_block(&h).await.unwrap();
        let mut buf = Vec::new();
        block.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), 227);
    }

    // Test unknown block, should return Error:BlockNotFound
    #[tokio::test]
    async fn test_unknown_block() {
        let root = PathBuf::from("../testdata/blockarchive");
        let archive = SimpleFileBasedBlockArchive::new(root).await.unwrap();
        let h = BlockHash::from_hex("0000000000000000094cc2ba6cc08514bcf9cbae26719d0a654a7754f3c75ef1").unwrap();
        let block = archive.get_block(&h).await;
        match block {
            Ok(_) => assert!(false),
            Err(e) => {
                match e {
                    Error::BlockNotFound => assert!(true),
                    _ => assert!(false)
                }
            }
        }
    }

    // Test block exists
    #[tokio::test]
    async fn test_block_exists() {
        let root = PathBuf::from("../testdata/blockarchive");
        let archive = SimpleFileBasedBlockArchive::new(root).await.unwrap();
        let h = BlockHash::from_hex("00000000000000a86c0a6d7b3445ff9e64908d6417cd6b256dbc23efd01de26f").unwrap();
        let exists = archive.block_exists(&h).await.unwrap();
        assert!(exists);
    }

    // Test unknown block does not exist
    #[tokio::test]
    async fn test_unknown_block_exists() {
        let root = PathBuf::from("../testdata/blockarchive");
        let archive = SimpleFileBasedBlockArchive::new(root).await.unwrap();
        let h = BlockHash::from_hex("0000000000000000094cc2ba6cc08514bcf9cbae26719d0a654a7754f3c75ef1").unwrap();
        let exists = archive.block_exists(&h).await.unwrap();
        assert!(!exists);
    }

    // A block that is stored in the wrong location wont exist
    #[tokio::test]
    async fn test_wrong_location_block_exists() {
        let root = PathBuf::from("../testdata/blockarchive");
        let archive = SimpleFileBasedBlockArchive::new(root).await.unwrap();
        let h = BlockHash::from_hex("000000001ee3392a6b6ba0bf2480a0f6bf9cdaaefa331bc0dfb243523af41a44").unwrap();
        let exists = archive.block_exists(&h).await.unwrap();
        assert!(!exists);
    }

    // Test storing a block by storing on in a temporary location and then checking it is stored correctly
    #[tokio::test]
    async fn test_store_block() {
        let root = Temp::new_dir().unwrap();
        let root_path = root.to_path_buf();
        let archive = SimpleFileBasedBlockArchive::new(root_path.clone()).await.unwrap();
        let h = BlockHash::from_hex("00000000000000a86c0a6d7b3445ff9e64908d6417cd6b256dbc23efd01de26f").unwrap();
        let block = "This is a block".as_bytes().to_vec();
        let block_cursor = Box::new(Cursor::new(block.clone()));
        archive.store_block(&h, &mut (block_cursor as Box<dyn AsyncRead + Unpin + Send>)).await.unwrap();
        let exists = archive.block_exists(&h).await.unwrap();
        assert!(exists);
        let mut stored_block = archive.get_block(&h).await.unwrap();
        let mut buf = Vec::new();
        stored_block.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, block);
    }

    // Test storing a block that already exists
    #[tokio::test]
    async fn test_store_existing_block() {
        let root = Temp::new_dir().unwrap();
        let root_path = root.to_path_buf();
        let archive = SimpleFileBasedBlockArchive::new(root_path.clone()).await.unwrap();
        let h = BlockHash::from_hex("00000000000000a86c0a6d7b3445ff9e64908d6417cd6b256dbc23efd01de26f").unwrap();
        let block = "This is a block".as_bytes().to_vec();
        let block_cursor = Box::new(Cursor::new(block.clone()));
        archive.store_block(&h, &mut (block_cursor as Box<dyn AsyncRead + Unpin + Send>)).await.unwrap();
        let exists = archive.block_exists(&h).await.unwrap();
        assert!(exists);
        let block = "This is a new block".as_bytes().to_vec();
        let block_cursor = Box::new(Cursor::new(block.clone()));
        let store = archive.store_block(&h, &mut (block_cursor as Box<dyn AsyncRead + Unpin + Send>)).await;
        match store {
            Ok(_) => assert!(false),
            Err(e) => {
                match e {
                    Error::BlockExists => assert!(true),
                    _ => assert!(false)
                }
            }
        }
    }

    // Test getting the size of a block
    #[tokio::test]
    async fn test_block_size() {
        let root = PathBuf::from("../testdata/blockarchive");
        let archive = SimpleFileBasedBlockArchive::new(root).await.unwrap();
        let h = BlockHash::from_hex("00000000000000a86c0a6d7b3445ff9e64908d6417cd6b256dbc23efd01de26f").unwrap();
        let size = archive.block_size(&h).await.unwrap();
        assert_eq!(size, 227);
    }

    // Test getting the size of an unknown block
    #[tokio::test]
    async fn test_unknown_block_size() {
        let root = PathBuf::from("../testdata/blockarchive");
        let archive = SimpleFileBasedBlockArchive::new(root).await.unwrap();
        let h = BlockHash::from_hex("0000000000000000094cc2ba6cc08514bcf9cbae26719d0a654a7754f3c75ef1").unwrap();
        let size = archive.block_size(&h).await;
        match size {
            Ok(_) => assert!(false),
            Err(e) => {
                match e {
                    Error::BlockNotFound => assert!(true),
                    _ => assert!(false)
                }
            }
        }
    }

    // Testing getting a header
    #[tokio::test]
    async fn test_block_header() {
        let root = PathBuf::from("../testdata/blockarchive");
        let archive = SimpleFileBasedBlockArchive::new(root).await.unwrap();
        let h = BlockHash::from_hex("00000000000000a86c0a6d7b3445ff9e64908d6417cd6b256dbc23efd01de26f").unwrap();
        let header = archive.block_header(&h).await.unwrap();
        assert_eq!(header.version, 2);
        assert_eq!(header.prev_hash, BlockHash::from_hex("0000000000000135aeabf9666fc9f1d5b8573685db070a5f1dfdd78f728a167a").unwrap());
        assert_eq!(header.merkle_root, BlockHash::from_hex("949904a56c861ecde4b43c9fc4ad612b82d10e38bdd164ea820b8cd0e6a39178").unwrap());
    }

    // test getting a header for an unknown block
    #[tokio::test]
    async fn test_unknown_block_header() {
        let root = PathBuf::from("../testdata/blockarchive");
        let archive = SimpleFileBasedBlockArchive::new(root).await.unwrap();
        let h = BlockHash::from_hex("0000000000000000094cc2ba6cc08514bcf9cbae26719d0a654a7754f3c75ef1").unwrap();
        let header = archive.block_header(&h).await;
        match header {
            Ok(_) => assert!(false),
            Err(e) => {
                match e {
                    Error::BlockNotFound => assert!(true),
                    _ => assert!(false)
                }
            }
        }
    }
}
