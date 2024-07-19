use config::ConfigError;

/// Standard Result used in the library
pub type BsvDbBaseResult<T> = std::result::Result<T, BsvDbBaseError>;

/// Standard error type used in the library
#[derive(Debug)]
pub enum BsvDbBaseError {
    /// The blockchain specified is not recognized.
    BlockchainUnknown,
    /// The BlockArchive component is not enabled.
    BlockArchiveNotEnabled,
    /// The ChainStore component is not enabled.
    ChainStoreNotEnabled,
    ConfigError(ConfigError),
}

impl std::fmt::Display for BsvDbBaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            BsvDbBaseError::BlockchainUnknown => write!(f, "Blockchain not recognized."),
            BsvDbBaseError::BlockArchiveNotEnabled => write!(f, "BlockArchive not enabled."),
            BsvDbBaseError::ChainStoreNotEnabled => write!(f, "ChainStore not enabled"),
            BsvDbBaseError::ConfigError(err) => write!(f, "Config error: {}", err),
        }
    }
}

impl From<ConfigError> for BsvDbBaseError {
    fn from(err: ConfigError) -> BsvDbBaseError {
        BsvDbBaseError::ConfigError(err)
    }
}
