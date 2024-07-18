use foundationdb::directory::DirectoryError;
use foundationdb::{FdbError, TransactionCommitError};

/// Standard Result used in the library
pub type Result<T> = std::result::Result<T, Error>;

/// Standard error type used in the library
#[derive(Debug)]
pub enum Error {
    /// The block was not found.
    BlockNotFound,
    /// The block already exists.
    BlockExists,
    /// The parent of the block was not found
    ParentNotFound,
    /// The method can not be implemented.
    CantImplement,
    IoError(std::io::Error),
    BitcoinSVError(bitcoinsv::Error),
    FdbError(FdbError),
    FdbDirectoryError(DirectoryError),
    FdbTransactionCommitError(TransactionCommitError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::BlockNotFound => write!(f, "Block not found"),
            Error::BlockExists => write!(f, "Block exists"),
            Error::ParentNotFound => write!(f, "Parent not found"),
            Error::CantImplement => write!(f, "Can't implement"),
            Error::IoError(err) => write!(f, "IO error: {}", err),
            Error::BitcoinSVError(err) => write!(f, "Bitcoin SV error: {}", err),
            Error::FdbError(err) => write!(f, "FDB Error: {}", err),
            Error::FdbDirectoryError(err) => write!(f, "FDB Directory Error: {:?}", err),
            Error::FdbTransactionCommitError(err) => write!(f, "FBD Transaction Commit Error: {}", err),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IoError(err)
    }
}

impl From<bitcoinsv::Error> for Error {
    fn from(err: bitcoinsv::Error) -> Error {
        Error::BitcoinSVError(err)
    }
}

impl From<FdbError> for Error {
    fn from(err: FdbError) -> Error { Error::FdbError(err)}
}

impl From<DirectoryError> for Error {
    fn from(err: DirectoryError) -> Error { Error::FdbDirectoryError(err)}
}

impl From<TransactionCommitError> for Error {
    fn from(err: TransactionCommitError) -> Error { Error::FdbTransactionCommitError(err)}
}
