use serde::Deserialize;
use std::path::Path;

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
    parse_sitemap_content(&content)
}

pub fn parse_sitemap_content(content: &str) -> anyhow::Result<Vec<String>> {
    let sitemap: Sitemap = quick_xml::de::from_str(content)?;
    Ok(sitemap.urls.into_iter().map(|u| u.loc).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sitemap_basic() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
                <url>
                    <loc>https://example.com/</loc>
                </url>
                <url>
                    <loc>https://example.com/blog</loc>
                </url>
            </urlset>"#;

        let urls = parse_sitemap_content(xml).unwrap();
        assert_eq!(urls.len(), 2);
        assert_eq!(urls[0], "https://example.com/");
        assert_eq!(urls[1], "https://example.com/blog");
    }
}
