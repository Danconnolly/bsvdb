use foundationdb::directory::DirectoryError;
use foundationdb::{FdbError, TransactionCommitError};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;

/// Standard Result used in the library
pub type ChainStoreResult<T> = Result<T, ChainStoreError>;

/// Standard error type used in the library
#[derive(Debug)]
pub enum ChainStoreError {
    /// The block was not found.
    BlockNotFound,
    /// The block already exists.
    BlockExists,
    /// The parent of the block was not found
    ParentNotFound,
    /// The method can not be implemented.
    CantImplement,
    /// error sending data
    SendError,
    /// miscellaneous error
    Misc(String),
    IoError(std::io::Error),
    BitcoinSVError(bitcoinsv::Error),
    FdbError(FdbError),
    FdbDirectoryError(DirectoryError),
    FdbTransactionCommitError(TransactionCommitError),
    OneshotRecvError(RecvError)
}

impl std::fmt::Display for ChainStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ChainStoreError::BlockNotFound => write!(f, "Block not found"),
            ChainStoreError::BlockExists => write!(f, "Block exists"),
            ChainStoreError::ParentNotFound => write!(f, "Parent not found"),
            ChainStoreError::CantImplement => write!(f, "Can't implement"),
            ChainStoreError::SendError => write!(f, "send error"),
            ChainStoreError::Misc(err) => write!(f, "misc error {}", err),
            ChainStoreError::IoError(err) => write!(f, "IO error: {}", err),
            ChainStoreError::BitcoinSVError(err) => write!(f, "Bitcoin SV error: {}", err),
            ChainStoreError::FdbError(err) => write!(f, "FDB Error: {}", err),
            ChainStoreError::FdbDirectoryError(err) => write!(f, "FDB Directory Error: {:?}", err),
            ChainStoreError::FdbTransactionCommitError(err) => write!(f, "FBD Transaction Commit Error: {}", err),
            ChainStoreError::OneshotRecvError(err) => write!(f, "Oneshot receiver error: {}", err),
        }
    }
}

impl From<&str> for ChainStoreError {
    fn from(err: &str) -> ChainStoreError {
        ChainStoreError::Misc(String::from(err))
    }
}

impl From<std::io::Error> for ChainStoreError {
    fn from(err: std::io::Error) -> ChainStoreError {
        ChainStoreError::IoError(err)
    }
}

impl From<bitcoinsv::Error> for ChainStoreError {
    fn from(err: bitcoinsv::Error) -> ChainStoreError {
        ChainStoreError::BitcoinSVError(err)
    }
}

impl From<FdbError> for ChainStoreError {
    fn from(err: FdbError) -> ChainStoreError { ChainStoreError::FdbError(err)}
}

impl From<DirectoryError> for ChainStoreError {
    fn from(err: DirectoryError) -> ChainStoreError { ChainStoreError::FdbDirectoryError(err)}
}

impl From<TransactionCommitError> for ChainStoreError {
    fn from(err: TransactionCommitError) -> ChainStoreError { ChainStoreError::FdbTransactionCommitError(err)}
}

impl From<RecvError> for ChainStoreError {
    fn from(err: RecvError) -> ChainStoreError { ChainStoreError::OneshotRecvError(err)}
}
