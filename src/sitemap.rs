use std::path::Path;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Sitemap {
    #[serde(rename = "url")]
    pub urls: Vec<SitemapUrl>,
}

#[derive(Debug, Deserialize)]
pub struct SitemapUrl {
    pub loc: String,
}

pub fn parse_sitemap(path: &Path) -> anyhow::Result<Vec<String>> {
    let content = std::fs::read_to_string(path)?;
    let sitemap: Sitemap = quick_xml::de::from_str(&content)?;
    Ok(sitemap.urls.into_iter().map(|u| u.loc).collect())
}
