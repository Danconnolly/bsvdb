use foundationdb::directory::DirectoryError;
use foundationdb::{FdbError, TransactionCommitError};
use tokio::sync::oneshot::error::RecvError;

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
    /// error sending data through a channel
    SendError(String),
    /// miscellaneous error
    Internal(String),
    IoError(std::io::Error),
    BitcoinSVError(bitcoinsv::BsvError),
    FdbError(FdbError),
    FdbDirectoryError(DirectoryError),
    FdbTransactionCommitError(TransactionCommitError),
    OneshotRecvError(RecvError),
    MinactorError(minactor::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::BlockNotFound => write!(f, "Block not found"),
            Error::BlockExists => write!(f, "Block exists"),
            Error::ParentNotFound => write!(f, "Parent not found"),
            Error::CantImplement => write!(f, "Can't implement"),
            Error::SendError(s) => write!(f, "error sending data through channel: {}", s),
            Error::Internal(err) => write!(f, "internal error {}", err),
            Error::IoError(err) => write!(f, "IO error: {}", err),
            Error::BitcoinSVError(err) => write!(f, "Bitcoin SV error: {}", err),
            Error::FdbError(err) => write!(f, "FDB Error: {}", err),
            Error::FdbDirectoryError(err) => write!(f, "FDB Directory Error: {:?}", err),
            Error::FdbTransactionCommitError(err) => {
                write!(f, "FBD Transaction Commit Error: {}", err)
            }
            Error::OneshotRecvError(err) => write!(f, "Oneshot receiver error: {}", err),
            Error::MinactorError(err) => write!(f, "minactor error: {}", err),
        }
    }
}

impl From<&str> for Error {
    fn from(err: &str) -> Error {
        Error::Internal(String::from(err))
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IoError(err)
    }
}

impl From<bitcoinsv::BsvError> for Error {
    fn from(err: bitcoinsv::BsvError) -> Error {
        Error::BitcoinSVError(err)
    }
}

impl From<FdbError> for Error {
    fn from(err: FdbError) -> Error {
        Error::FdbError(err)
    }
}

impl From<DirectoryError> for Error {
    fn from(err: DirectoryError) -> Error {
        Error::FdbDirectoryError(err)
    }
}

impl From<TransactionCommitError> for Error {
    fn from(err: TransactionCommitError) -> Error {
        Error::FdbTransactionCommitError(err)
    }
}

impl From<RecvError> for Error {
    fn from(err: RecvError) -> Error {
        Error::OneshotRecvError(err)
    }
}

impl From<minactor::Error> for Error {
    fn from(value: minactor::Error) -> Self {
        Error::MinactorError(value)
    }
}

/// Internal error type.
#[derive(Clone, Debug)]
pub enum InternalError {}
