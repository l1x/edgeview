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

impl SiteConfig {
    /// Parse `s3_path` into (bucket, prefix). Fails clearly on malformed URLs.
    pub fn s3_bucket_and_prefix(&self) -> anyhow::Result<(String, String)> {
        let url = url::Url::parse(&self.s3_path)
            .map_err(|e| anyhow::anyhow!("Invalid s3_path '{}': {}", self.s3_path, e))?;
        let bucket = url
            .host_str()
            .filter(|h| !h.is_empty())
            .ok_or_else(|| anyhow::anyhow!("s3_path '{}' has no bucket", self.s3_path))?
            .to_string();
        let prefix = url.path().trim_start_matches('/').to_string();
        Ok((bucket, prefix))
    }
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
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Failed to read config file {}: {}", path.display(), e))?;
        let config: Config = toml::from_str(&content).map_err(|e| {
            anyhow::anyhow!("Failed to parse config file {}: {}", path.display(), e)
        })?;
        // Validate all s3_path values at load time
        for site in &config.sites {
            site.s3_bucket_and_prefix()?;
        }
        Ok(config)
    }
}
