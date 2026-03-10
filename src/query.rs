use std::collections::HashMap;
use std::sync::Arc;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use tracing::info;
use url::Url;
use crate::config::SiteConfig;
use crate::model::*;

/// CloudFront real-time log schema (parquet format, all string columns).
fn cloudfront_schema() -> Schema {
    let fields: Vec<Field> = [
        "DistributionId", "date", "time", "x_edge_location", "sc_bytes",
        "c_ip", "cs_method", "cs_Host", "cs_uri_stem", "sc_status",
        "cs_Referer", "cs_User_Agent", "cs_uri_query", "cs_Cookie",
        "x_edge_result_type", "x_edge_request_id", "x_host_header",
        "cs_protocol", "cs_bytes", "time_taken", "x_forwarded_for",
        "ssl_protocol", "ssl_cipher", "x_edge_response_result_type",
        "cs_protocol_version", "fle_status", "fle_encrypted_fields",
        "c_port", "time_to_first_byte", "x_edge_detailed_result_type",
        "sc_content_type", "sc_content_len", "sc_range_start", "sc_range_end",
        "timestamp_ms", "origin_fbl", "origin_lbl", "asn", "timestamp",
    ].iter().map(|name| Field::new(*name, DataType::Utf8, true)).collect();
    Schema::new(fields)
}

pub struct QueryEngine {
    ctx: SessionContext,
}

impl QueryEngine {
    pub async fn new(site: &SiteConfig, default_region: &str) -> anyhow::Result<Self> {
        let config = SessionConfig::new()
            .set_bool("datafusion.execution.listing_table_ignore_subdirectory", false);
        let ctx = SessionContext::new_with_config(config);

        let s3_url = Url::parse(&site.s3_path)?;
        let region = site.s3_region.as_deref().unwrap_or(default_region);

        let mut builder = AmazonS3Builder::new()
            .with_region(region)
            .with_bucket_name(s3_url.host_str().unwrap_or_default());

        // Use AWS SDK credential chain (supports profiles, SSO, assume-role, etc.)
        let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_string()))
            .load()
            .await;

        if let Some(provider) = aws_config.credentials_provider() {
            use aws_credential_types::provider::ProvideCredentials;
            let creds = provider.provide_credentials().await
                .map_err(|e| anyhow::anyhow!("Failed to resolve AWS credentials: {}", e))?;

            builder = builder
                .with_access_key_id(creds.access_key_id())
                .with_secret_access_key(creds.secret_access_key());

            if let Some(token) = creds.session_token() {
                builder = builder.with_token(token);
            }
        }

        let s3 = builder.build()?;
        ctx.runtime_env().register_object_store(&s3_url, Arc::new(s3));

        Ok(Self { ctx })
    }

    pub async fn load_logs(&self, s3_path: &str, month: &str) -> anyhow::Result<()> {
        let path = format!("{}/{}/", s3_path, month);
        let table_url = ListingTableUrl::parse(&path)?;

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet");

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(Arc::new(cloudfront_schema()));

        let table = ListingTable::try_new(config)?;
        self.ctx.register_table("logs", Arc::new(table))?;
        Ok(())
    }

    pub async fn summary(&self) -> anyhow::Result<(u64, u64)> {
        let df = self.ctx.sql("
            SELECT
                COUNT(*) as hits,
                COUNT(DISTINCT c_ip) as visitors
            FROM logs
            WHERE cs_method = 'GET'
              AND sc_status IN ('200', '304')
        ").await?;

        let batches = df.collect().await?;
        if let Some(batch) = batches.first() {
            let hits = batch.column(0).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast hits column"))?;
            let visitors = batch.column(1).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast visitors column"))?;

            if batch.num_rows() > 0 {
                return Ok((hits.value(0) as u64, visitors.value(0) as u64));
            }
        }
        Ok((0, 0))
    }

    pub async fn daily_traffic(&self) -> anyhow::Result<Vec<DailyTraffic>> {
        let df = self.ctx.sql("
            SELECT
                date,
                COUNT(*) as hits,
                COUNT(DISTINCT c_ip) as visitors
            FROM logs
            WHERE cs_method = 'GET'
              AND sc_status IN ('200', '304')
            GROUP BY date
            ORDER BY date
        ").await?;

        let batches = df.collect().await?;
        let mut results = Vec::new();

        for batch in batches {
            let date_col = batch.column(0).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast date column"))?;
            let hits_col = batch.column(1).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast hits column"))?;
            let visitors_col = batch.column(2).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast visitors column"))?;

            for i in 0..batch.num_rows() {
                results.push(DailyTraffic {
                    date: chrono::NaiveDate::parse_from_str(date_col.value(i), "%Y-%m-%d")?,
                    hits: hits_col.value(i) as u64,
                    visitors: visitors_col.value(i) as u64,
                });
            }
        }

        Ok(results)
    }

    pub async fn bot_hits_by_path(&self, bot_map: &HashMap<String, String>) -> anyhow::Result<HashMap<String, u64>> {
        if bot_map.is_empty() {
            return Ok(HashMap::new());
        }

        let like_clauses: Vec<String> = bot_map.keys()
            .map(|pattern| format!("\"cs_User_Agent\" LIKE '%{}%'", pattern.replace('\'', "''")))
            .collect();
        let where_bots = like_clauses.join(" OR ");

        let sql = format!(
            "SELECT cs_uri_stem, COUNT(*) as bot_hits \
             FROM logs \
             WHERE cs_method = 'GET' \
               AND sc_status IN ('200', '304') \
               AND ({}) \
             GROUP BY cs_uri_stem",
            where_bots
        );

        let df = self.ctx.sql(&sql).await?;
        let batches = df.collect().await?;
        let mut results = HashMap::new();

        for batch in batches {
            let path_col = batch.column(0).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast path column"))?;
            let hits_col = batch.column(1).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast hits column"))?;

            for i in 0..batch.num_rows() {
                results.insert(path_col.value(i).to_string(), hits_col.value(i) as u64);
            }
        }

        info!(paths = results.len(), "Computed bot hits by path");
        Ok(results)
    }

    pub async fn bot_summary(&self, bot_map: &HashMap<String, String>) -> anyhow::Result<(u64, u64)> {
        if bot_map.is_empty() {
            return Ok((0, 0));
        }

        let like_clauses: Vec<String> = bot_map.keys()
            .map(|pattern| format!("\"cs_User_Agent\" LIKE '%{}%'", pattern.replace('\'', "''")))
            .collect();
        let where_bots = like_clauses.join(" OR ");

        let sql = format!(
            "SELECT COUNT(*) as bot_hits, COUNT(DISTINCT c_ip) as bot_visitors \
             FROM logs \
             WHERE cs_method = 'GET' \
               AND sc_status IN ('200', '304') \
               AND ({})",
            where_bots
        );

        let df = self.ctx.sql(&sql).await?;
        let batches = df.collect().await?;
        if let Some(batch) = batches.first() {
            let hits = batch.column(0).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast bot_hits column"))?;
            let visitors = batch.column(1).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast bot_visitors column"))?;
            if batch.num_rows() > 0 {
                return Ok((hits.value(0) as u64, visitors.value(0) as u64));
            }
        }
        Ok((0, 0))
    }

    pub async fn top_pages(&self, bot_hits_by_path: &HashMap<String, u64>) -> anyhow::Result<Vec<PageHits>> {
        let df = self.ctx.sql("
            SELECT
                cs_uri_stem as path,
                COUNT(*) as hits,
                COUNT(DISTINCT c_ip) as visitors
            FROM logs
            WHERE cs_method = 'GET'
              AND sc_status IN ('200', '304')
            GROUP BY cs_uri_stem
            ORDER BY hits DESC
        ").await?;

        let batches = df.collect().await?;
        let mut raw: Vec<(String, u64, u64)> = Vec::new();

        for batch in batches {
            let path_col = batch.column(0).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast path column"))?;
            let hits_col = batch.column(1).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast hits column"))?;
            let visitors_col = batch.column(2).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast visitors column"))?;

            for i in 0..batch.num_rows() {
                raw.push((
                    path_col.value(i).to_string(),
                    hits_col.value(i) as u64,
                    visitors_col.value(i) as u64,
                ));
            }
        }

        // Classify and roll up paths (including bot hits)
        let mut rollup: HashMap<(String, String), (u64, u64, u64)> = HashMap::new();
        for (path, hits, visitors) in &raw {
            let (category, display_path) = classify_path(path);
            let bot_hits = bot_hits_by_path.get(path.as_str()).copied().unwrap_or(0);
            let entry = rollup.entry((category.to_string(), display_path)).or_insert((0, 0, 0));
            entry.0 += hits;
            entry.1 += visitors;
            entry.2 += bot_hits;
        }

        let mut results: Vec<PageHits> = rollup
            .into_iter()
            .map(|((category, path), (hits, visitors, bot_hits))| PageHits {
                path,
                hits,
                visitors,
                bot_hits,
                category,
            })
            .collect();
        results.sort_by_key(|p| std::cmp::Reverse(p.hits));

        Ok(results)
    }

    pub async fn bot_activity(&self, bot_map: &HashMap<String, String>) -> anyhow::Result<Vec<CrawlerStats>> {
        let df = self.ctx.sql("
            SELECT
                \"cs_User_Agent\",
                COUNT(*) as hits,
                MAX(date) as last_crawl
            FROM logs
            WHERE \"cs_User_Agent\" IS NOT NULL
            GROUP BY \"cs_User_Agent\"
        ").await?;

        let batches = df.collect().await?;
        let mut bot_stats: HashMap<String, CrawlerStats> = HashMap::new();

        for batch in batches {
            let ua_col = batch.column(0).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast ua column"))?;
            let hits_col = batch.column(1).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast hits column"))?;
            let last_col = batch.column(2).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast last_crawl column"))?;

            for i in 0..batch.num_rows() {
                let ua = ua_col.value(i);
                if let Some(bot_name) = crate::classify::classify_bot(ua, bot_map) {
                    let hits = hits_col.value(i) as u64;
                    let last_date = chrono::NaiveDate::parse_from_str(last_col.value(i), "%Y-%m-%d")?;
                    let last_crawl = chrono::DateTime::from_naive_utc_and_offset(
                        last_date.and_hms_opt(0, 0, 0).unwrap(),
                        chrono::Utc,
                    );

                    let entry = bot_stats.entry(bot_name.clone()).or_insert(CrawlerStats {
                        bot_name,
                        hits: 0,
                        last_crawl: None,
                    });
                    entry.hits += hits;
                    if entry.last_crawl.map_or(true, |c| last_crawl > c) {
                        entry.last_crawl = Some(last_crawl);
                    }
                }
            }
        }

        let mut results: Vec<CrawlerStats> = bot_stats.into_values().collect();
        results.sort_by_key(|s| std::cmp::Reverse(s.hits));
        info!(count = results.len(), "Classified bot activity");
        Ok(results)
    }

    pub async fn daily_bot_hits_by_path(&self, bot_map: &HashMap<String, String>) -> anyhow::Result<HashMap<String, HashMap<String, u64>>> {
        if bot_map.is_empty() {
            return Ok(HashMap::new());
        }

        let like_clauses: Vec<String> = bot_map.keys()
            .map(|pattern| format!("\"cs_User_Agent\" LIKE '%{}%'", pattern.replace('\'', "''")))
            .collect();
        let where_bots = like_clauses.join(" OR ");

        let sql = format!(
            "SELECT date, cs_uri_stem, COUNT(*) as bot_hits \
             FROM logs \
             WHERE cs_method = 'GET' \
               AND sc_status IN ('200', '304') \
               AND ({}) \
             GROUP BY date, cs_uri_stem",
            where_bots
        );

        let df = self.ctx.sql(&sql).await?;
        let batches = df.collect().await?;
        let mut results: HashMap<String, HashMap<String, u64>> = HashMap::new();

        for batch in batches {
            let date_col = batch.column(0).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast date column"))?;
            let path_col = batch.column(1).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast path column"))?;
            let hits_col = batch.column(2).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast hits column"))?;

            for i in 0..batch.num_rows() {
                results
                    .entry(date_col.value(i).to_string())
                    .or_default()
                    .insert(path_col.value(i).to_string(), hits_col.value(i) as u64);
            }
        }

        info!(dates = results.len(), "Computed daily bot hits by path");
        Ok(results)
    }

    pub async fn daily_top_pages(&self, daily_bot_hits: &HashMap<String, HashMap<String, u64>>) -> anyhow::Result<HashMap<String, Vec<PageHits>>> {
        let df = self.ctx.sql("
            SELECT
                date,
                cs_uri_stem as path,
                COUNT(*) as hits,
                COUNT(DISTINCT c_ip) as visitors
            FROM logs
            WHERE cs_method = 'GET'
              AND sc_status IN ('200', '304')
            GROUP BY date, cs_uri_stem
            ORDER BY date, hits DESC
        ").await?;

        let batches = df.collect().await?;
        // date_str -> (category, display_path) -> (hits, visitors, bot_hits)
        let mut by_date: HashMap<String, HashMap<(String, String), (u64, u64, u64)>> = HashMap::new();

        for batch in batches {
            let date_col = batch.column(0).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast date column"))?;
            let path_col = batch.column(1).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast path column"))?;
            let hits_col = batch.column(2).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast hits column"))?;
            let visitors_col = batch.column(3).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast visitors column"))?;

            for i in 0..batch.num_rows() {
                let date_str = date_col.value(i).to_string();
                let path = path_col.value(i);
                let hits = hits_col.value(i) as u64;
                let visitors = visitors_col.value(i) as u64;

                let (category, display_path) = classify_path(path);
                let bot_hits = daily_bot_hits
                    .get(&date_str)
                    .and_then(|m| m.get(path))
                    .copied()
                    .unwrap_or(0);

                let rollup = by_date.entry(date_str).or_default();
                let entry = rollup.entry((category.to_string(), display_path)).or_insert((0, 0, 0));
                entry.0 += hits;
                entry.1 += visitors;
                entry.2 += bot_hits;
            }
        }

        let mut results: HashMap<String, Vec<PageHits>> = HashMap::new();
        for (date_str, rollup) in by_date {
            let mut pages: Vec<PageHits> = rollup
                .into_iter()
                .map(|((category, path), (hits, visitors, bot_hits))| PageHits {
                    path,
                    hits,
                    visitors,
                    bot_hits,
                    category,
                })
                .collect();
            pages.sort_by_key(|p| std::cmp::Reverse(p.hits));
            results.insert(date_str, pages);
        }

        info!(dates = results.len(), "Computed daily top pages");
        Ok(results)
    }

    pub async fn daily_hourly_traffic(&self) -> anyhow::Result<HashMap<String, Vec<HourlyTraffic>>> {
        let df = self.ctx.sql("
            SELECT
                date,
                LEFT(time, 2) as hour,
                COUNT(*) as hits,
                COUNT(DISTINCT c_ip) as visitors
            FROM logs
            WHERE cs_method = 'GET'
              AND sc_status IN ('200', '304')
            GROUP BY date, hour
            ORDER BY date, hour
        ").await?;

        let batches = df.collect().await?;
        let mut results: HashMap<String, Vec<HourlyTraffic>> = HashMap::new();

        for batch in batches {
            let date_col = batch.column(0).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast date column"))?;
            let hour_col = batch.column(1).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast hour column"))?;
            let hits_col = batch.column(2).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast hits column"))?;
            let visitors_col = batch.column(3).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast visitors column"))?;

            for i in 0..batch.num_rows() {
                let hour: u8 = hour_col.value(i).parse().unwrap_or(0);
                results
                    .entry(date_col.value(i).to_string())
                    .or_default()
                    .push(HourlyTraffic {
                        hour,
                        hits: hits_col.value(i) as u64,
                        visitors: visitors_col.value(i) as u64,
                    });
            }
        }

        // Ensure each day has all 24 hours sorted
        for hours in results.values_mut() {
            hours.sort_by_key(|h| h.hour);
        }

        info!(dates = results.len(), "Computed daily hourly traffic");
        Ok(results)
    }

    pub async fn googlebot_hits(&self) -> anyhow::Result<HashMap<String, u64>> {
        let df = self.ctx.sql("
            SELECT
                cs_uri_stem as path,
                COUNT(*) as hits
            FROM logs
            WHERE \"cs_User_Agent\" LIKE '%Googlebot%'
              AND cs_method = 'GET'
            GROUP BY cs_uri_stem
        ").await?;

        let batches = df.collect().await?;
        let mut results = HashMap::new();

        for batch in batches {
            let path_col = batch.column(0).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast path column"))?;
            let hits_col = batch.column(1).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast hits column"))?;

            for i in 0..batch.num_rows() {
                results.insert(path_col.value(i).to_string(), hits_col.value(i) as u64);
            }
        }

        Ok(results)
    }
}

/// Classify a URL path into a category and a display path (rolled up for static assets).
fn classify_path(path: &str) -> (&'static str, String) {
    if path.starts_with("/articles/") {
        ("article", path.to_string())
    } else if path.starts_with("/static/fonts/") {
        ("static", "/static/fonts/*".to_string())
    } else if path.starts_with("/static/css/") {
        ("static", "/static/css/*".to_string())
    } else if path.starts_with("/static/img/") {
        ("static", "/static/img/*".to_string())
    } else if path.starts_with("/pagefind/") {
        ("static", "/pagefind/*".to_string())
    } else if matches!(path, "/robots.txt" | "/feed.xml" | "/favicon.ico" | "/favicon.svg" | "/sitemap.xml") {
        ("static", path.to_string())
    } else {
        ("page", path.to_string())
    }
}
