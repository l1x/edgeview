use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SiteConfig {
    pub domain: String,
    pub s3_path: String,
    pub s3_region: Option<String>,
    pub sitemap: Option<PathBuf>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub sites: Vec<SiteConfig>,
    pub output_dir: PathBuf,
    pub bots: HashMap<String, String>,
    pub default_s3_region: String,
}

impl Config {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}
