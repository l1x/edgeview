use std::collections::HashMap;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub domain: String,
    pub s3_path: String,
    pub s3_region: String,
    pub sitemap: Option<PathBuf>,
    pub output_dir: PathBuf,
    pub url_rewrites: Option<HashMap<String, String>>,
    pub categories: Option<HashMap<String, String>>,
    pub bots: HashMap<String, String>,
}

impl Config {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}
