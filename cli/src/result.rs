use bsvdb_base::BsvDbBaseError;
use bsvdb_blockarchive::Error as BlockArchiveError;
use bsvdb_chainstore::Error;
use tokio::task::JoinError;

/// Standard Result used in the library
pub type CliResult<T> = std::result::Result<T, CliError>;

/// Standard error type used in the library
#[derive(Debug)]
pub enum CliError {
    BsvDbBase(BsvDbBaseError),
    BlockArchive(BlockArchiveError),
    ChainStore(Error),
    Join(JoinError),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            CliError::BsvDbBase(err) => write!(f, "BSVDB base error: {}", err),
            CliError::BlockArchive(err) => write!(f, "Block Archive error: {}", err),
            CliError::ChainStore(err) => write!(f, "Chain Store error: {}", err),
            CliError::Join(err) => write!(f, "Join error: {}", err),
        }
    }
}

impl From<BsvDbBaseError> for CliError {
    fn from(err: BsvDbBaseError) -> CliError {
        CliError::BsvDbBase(err)
    }
}

impl From<BlockArchiveError> for CliError {
    fn from(err: BlockArchiveError) -> CliError {
        CliError::BlockArchive(err)
    }
}

impl From<Error> for CliError {
    fn from(err: Error) -> CliError {
        CliError::ChainStore(err)
    }
}

impl From<JoinError> for CliError {
    fn from(err: JoinError) -> CliError {
        CliError::Join(err)
    }
}
