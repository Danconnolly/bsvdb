use tokio::task::JoinError;
use bsvdb_base::BsvDbBaseError;
use bsvdb_blockarchive::Error as BlockArchiveError;
use bsvdb_chainstore::ChainStoreError;


/// Standard Result used in the library
pub type CliResult<T> = std::result::Result<T, CliError>;

/// Standard error type used in the library
#[derive(Debug)]
pub enum CliError {
    BsvDbBaseError(BsvDbBaseError),
    BlockArchiveError(BlockArchiveError),
    ChainStoreError(ChainStoreError),
    JoinError(JoinError),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            CliError::BsvDbBaseError(err) => write!(f, "BSVDB base error: {}", err),
            CliError::BlockArchiveError(err) => write!(f, "Block Archive error: {}", err),
            CliError::ChainStoreError(err) => write!(f, "Chain Store error: {}", err),
            CliError::JoinError(err) => write!(f, "Join error: {}", err),
        }
    }
}

impl From<BsvDbBaseError> for CliError {
    fn from(err: BsvDbBaseError) -> CliError {
        CliError::BsvDbBaseError(err)
    }
}

impl From<BlockArchiveError> for CliError {
    fn from(err: BlockArchiveError) -> CliError { CliError::BlockArchiveError(err) }
}

impl From<ChainStoreError> for CliError {
    fn from(err: ChainStoreError) -> CliError { CliError::ChainStoreError(err) }
}

impl From<JoinError> for CliError {
    fn from(err: JoinError) -> CliError { CliError::JoinError(err) }
}
