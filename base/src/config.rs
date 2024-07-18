use config::{Config, ConfigError, File, FileFormat};
use serde::Deserialize;


#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct BlockArchiveConfig {
    pub root_path: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct ChainStoreConfig {
    pub root_dir: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct BSVDBConfig {
    pub verbose: bool,
    pub blockchain: String,
    pub block_archive: Option<BlockArchiveConfig>,
    pub chain_store: Option<ChainStoreConfig>,
}

impl BSVDBConfig {
    pub fn new(config_path: Option<String>) -> Result<Self, ConfigError> {
        let c_p = config_path.unwrap_or("bsvdb.toml".to_string());
        let s = Config::builder()
            .set_default("verbose", false)?
            .set_default("blockchain", "mainnet")?
            .add_source(File::new(c_p.as_str(), FileFormat::Toml))
            .build()?;
        s.try_deserialize()
    }
}
