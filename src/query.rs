use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use chrono::{Datelike, NaiveDate};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use url::Url;
use crate::config::SiteConfig;
use crate::model::*;

/// The 7 narrow columns we extract from CloudFront logs.
fn narrow_schema() -> Schema {
    Schema::new(vec![
        Field::new("date", DataType::Utf8, true),
        Field::new("time", DataType::Utf8, true),
        Field::new("c_ip", DataType::Utf8, true),
        Field::new("cs_method", DataType::Utf8, true),
        Field::new("cs_uri_stem", DataType::Utf8, true),
        Field::new("sc_status", DataType::Utf8, true),
        Field::new("cs_User_Agent", DataType::Utf8, true),
    ])
}

const MAX_BUCKET_BYTES: usize = 256 * 1024 * 1024;

/// Resolve AWS credentials for S3 access.
async fn resolve_aws_credentials(region: &str) -> anyhow::Result<(String, String, Option<String>)> {
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region.to_string()))
        .load()
        .await;

    if let Some(provider) = aws_config.credentials_provider() {
        use aws_credential_types::provider::ProvideCredentials;
        let creds = provider.provide_credentials().await
            .map_err(|e| anyhow::anyhow!("Failed to resolve AWS credentials: {}", e))?;
        Ok((
            creds.access_key_id().to_string(),
            creds.secret_access_key().to_string(),
            creds.session_token().map(|t| t.to_string()),
        ))
    } else {
        anyhow::bail!("No AWS credentials provider found")
    }
}

/// Sync one day's raw CloudFront logs from S3 to local narrow parquet.
///
/// Downloads all parquet files for the day, extracts 7 columns, writes locally
/// using blake3-based splitting: files assigned to buckets via blake3(filename) mod N
/// where N = max(1, ceil(total_remote_size / 256MB)).
pub async fn sync_day_from_s3(
    site: &SiteConfig,
    default_region: &str,
    date: NaiveDate,
    raw_dir: &Path,
) -> anyhow::Result<()> {
    use futures::TryStreamExt;
    use object_store::ObjectStore;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    let s3_url = Url::parse(&site.s3_path)?;
    let region = site.s3_region.as_deref().unwrap_or(default_region);
    let (ak, sk, token) = resolve_aws_credentials(region).await?;

    let mut builder = AmazonS3Builder::new()
        .with_region(region)
        .with_bucket_name(s3_url.host_str().unwrap_or_default())
        .with_access_key_id(&ak)
        .with_secret_access_key(&sk);
    if let Some(t) = &token {
        builder = builder.with_token(t);
    }
    let s3 = builder.build()?;

    // Build prefix for this day: path/YYYY/MM/DD/
    let s3_prefix = s3_url.path().trim_start_matches('/');
    let day_prefix = object_store::path::Path::from(format!(
        "{}/{}/{:02}/{:02}/",
        s3_prefix, date.year(), date.month(), date.day()
    ));

    // List all parquet files for this day
    let objects: Vec<_> = s3.list(Some(&day_prefix))
        .try_collect()
        .await?;

    if objects.is_empty() {
        tracing::debug!(%date, "No S3 files found for day");
        return Ok(());
    }

    // Determine bucket count from total remote size
    let total_size: usize = objects.iter().map(|o| o.size as usize).sum();
    let n_buckets = std::cmp::max(1, (total_size + MAX_BUCKET_BYTES - 1) / MAX_BUCKET_BYTES);

    tracing::info!(
        %date,
        files = objects.len(),
        total_bytes = total_size,
        buckets = n_buckets,
        "Syncing day from S3"
    );

    // Assign files to buckets via blake3(filename) mod N
    let mut buckets: Vec<Vec<&object_store::ObjectMeta>> = vec![vec![]; n_buckets];
    for obj in &objects {
        let filename = obj.location.filename().unwrap_or_default();
        let hash = blake3::hash(filename.as_bytes());
        let hash_val = u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap());
        let bucket = (hash_val as usize) % n_buckets;
        buckets[bucket].push(obj);
    }

    // Narrow column indices in 39-column CloudFront schema:
    // 1=date, 2=time, 5=c_ip, 6=cs_method, 8=cs_uri_stem, 9=sc_status, 11=cs_User_Agent
    let narrow_indices: Vec<usize> = vec![1, 2, 5, 6, 8, 9, 11];
    let schema = Arc::new(narrow_schema());

    std::fs::create_dir_all(raw_dir)?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();

    for (bucket_idx, bucket_files) in buckets.iter().enumerate() {
        if bucket_files.is_empty() { continue; }

        let output_path = raw_dir.join(format!("{}_{}.parquet", date, bucket_idx));
        let file = std::fs::File::create(&output_path)?;
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props.clone()))?;

        for obj in bucket_files {
            let data = s3.get(&obj.location).await?.bytes().await?;
            let parquet_builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
            let parquet_schema = parquet_builder.parquet_schema().clone();
            let mask = parquet::arrow::ProjectionMask::leaves(
                &parquet_schema,
                narrow_indices.iter().copied(),
            );
            let reader = parquet_builder.with_projection(mask).build()?;
            for batch_result in reader {
                writer.write(&batch_result?)?;
            }
        }

        writer.close()?;
    }

    Ok(())
}

/// Query engine that operates on LOCAL narrow parquet files.
pub struct QueryEngine {
    ctx: SessionContext,
}

impl QueryEngine {
    /// Create a query engine over local narrow parquet files in raw_dir.
    pub fn new_local(raw_dir: &Path) -> anyhow::Result<Self> {
        let config = SessionConfig::new()
            .set_bool("datafusion.execution.listing_table_ignore_subdirectory", false);
        let ctx = SessionContext::new_with_config(config);

        let table_path = format!("{}/", raw_dir.to_string_lossy());
        let table_url = ListingTableUrl::parse(&table_path)?;

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet");

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(Arc::new(narrow_schema()));

        let table = ListingTable::try_new(config)?;
        ctx.register_table("logs", Arc::new(table))?;

        Ok(Self { ctx })
    }

    // --- Per-date query methods (all run against local data) ---

    pub async fn summary_for_date(&self, date: NaiveDate) -> anyhow::Result<(u64, u64)> {
        let sql = format!(
            "SELECT COUNT(*) as hits, COUNT(DISTINCT c_ip) as visitors \
             FROM logs \
             WHERE cs_method = 'GET' AND sc_status IN ('200', '304') AND date = '{}'",
            date
        );
        let df = self.ctx.sql(&sql).await?;
        let batches = df.collect().await?;
        if let Some(batch) = batches.first() {
            if batch.num_rows() > 0 {
                let hits = batch.column(0).as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast hits column"))?;
                let visitors = batch.column(1).as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast visitors column"))?;
                return Ok((hits.value(0) as u64, visitors.value(0) as u64));
            }
        }
        Ok((0, 0))
    }

    pub async fn bot_summary_for_date(&self, date: NaiveDate, bot_map: &HashMap<String, String>) -> anyhow::Result<(u64, u64)> {
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
             WHERE cs_method = 'GET' AND sc_status IN ('200', '304') AND date = '{}' AND ({})",
            date, where_bots
        );
        let df = self.ctx.sql(&sql).await?;
        let batches = df.collect().await?;
        if let Some(batch) = batches.first() {
            if batch.num_rows() > 0 {
                let hits = batch.column(0).as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast bot_hits column"))?;
                let visitors = batch.column(1).as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| anyhow::anyhow!("Failed to downcast bot_visitors column"))?;
                return Ok((hits.value(0) as u64, visitors.value(0) as u64));
            }
        }
        Ok((0, 0))
    }

    pub async fn bot_hits_by_path_for_date(&self, date: NaiveDate, bot_map: &HashMap<String, String>) -> anyhow::Result<HashMap<String, u64>> {
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
             WHERE cs_method = 'GET' AND sc_status IN ('200', '304') AND date = '{}' AND ({}) \
             GROUP BY cs_uri_stem",
            date, where_bots
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
        Ok(results)
    }

    pub async fn top_pages_for_date(&self, date: NaiveDate, bot_hits_by_path: &HashMap<String, u64>) -> anyhow::Result<Vec<PageHits>> {
        let sql = format!(
            "SELECT cs_uri_stem as path, COUNT(*) as hits, COUNT(DISTINCT c_ip) as visitors \
             FROM logs \
             WHERE cs_method = 'GET' AND sc_status IN ('200', '304') AND date = '{}' \
             GROUP BY cs_uri_stem ORDER BY hits DESC",
            date
        );
        let df = self.ctx.sql(&sql).await?;
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
                raw.push((path_col.value(i).to_string(), hits_col.value(i) as u64, visitors_col.value(i) as u64));
            }
        }

        let mut rollup: HashMap<(String, String), (u64, u64, u64)> = HashMap::new();
        for (path, hits, visitors) in &raw {
            let (category, display_path) = classify_path(path);
            let bot_hits = bot_hits_by_path.get(path.as_str()).copied().unwrap_or(0);
            let entry = rollup.entry((category.to_string(), display_path)).or_insert((0, 0, 0));
            entry.0 += hits;
            entry.1 += visitors;
            entry.2 += bot_hits;
        }

        let mut results: Vec<PageHits> = rollup.into_iter()
            .map(|((category, path), (hits, visitors, bot_hits))| PageHits { path, hits, visitors, bot_hits, category })
            .collect();
        results.sort_by_key(|p| std::cmp::Reverse(p.hits));
        Ok(results)
    }

    pub async fn hourly_traffic_for_date(&self, date: NaiveDate) -> anyhow::Result<Vec<HourlyTraffic>> {
        let sql = format!(
            "SELECT LEFT(time, 2) as hour, COUNT(*) as hits, COUNT(DISTINCT c_ip) as visitors \
             FROM logs \
             WHERE cs_method = 'GET' AND sc_status IN ('200', '304') AND date = '{}' \
             GROUP BY hour ORDER BY hour",
            date
        );
        let df = self.ctx.sql(&sql).await?;
        let batches = df.collect().await?;
        let mut hour_map: HashMap<u8, (u64, u64)> = HashMap::new();
        for batch in batches {
            let hour_col = batch.column(0).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast hour column"))?;
            let hits_col = batch.column(1).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast hits column"))?;
            let visitors_col = batch.column(2).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast visitors column"))?;
            for i in 0..batch.num_rows() {
                let hour: u8 = hour_col.value(i).parse().unwrap_or(0);
                hour_map.insert(hour, (hits_col.value(i) as u64, visitors_col.value(i) as u64));
            }
        }
        let results: Vec<HourlyTraffic> = (0..24u8).map(|h| {
            let (hits, visitors) = hour_map.get(&h).copied().unwrap_or((0, 0));
            HourlyTraffic { hour: h, hits, visitors }
        }).collect();
        Ok(results)
    }

    pub async fn bot_activity_for_date(&self, date: NaiveDate, bot_map: &HashMap<String, String>) -> anyhow::Result<Vec<CrawlerStats>> {
        let sql = format!(
            "SELECT \"cs_User_Agent\", COUNT(*) as hits \
             FROM logs \
             WHERE \"cs_User_Agent\" IS NOT NULL AND date = '{}' \
             GROUP BY \"cs_User_Agent\"",
            date
        );
        let df = self.ctx.sql(&sql).await?;
        let batches = df.collect().await?;
        let mut bot_stats: HashMap<String, CrawlerStats> = HashMap::new();
        let last_crawl = chrono::DateTime::from_naive_utc_and_offset(
            date.and_hms_opt(0, 0, 0).unwrap(),
            chrono::Utc,
        );
        for batch in batches {
            let ua_col = batch.column(0).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast ua column"))?;
            let hits_col = batch.column(1).as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast hits column"))?;
            for i in 0..batch.num_rows() {
                let ua = ua_col.value(i);
                if let Some(bot_name) = crate::classify::classify_bot(ua, bot_map) {
                    let entry = bot_stats.entry(bot_name.clone()).or_insert(CrawlerStats {
                        bot_name,
                        hits: 0,
                        last_crawl: None,
                    });
                    entry.hits += hits_col.value(i) as u64;
                    entry.last_crawl = Some(last_crawl);
                }
            }
        }
        let mut results: Vec<CrawlerStats> = bot_stats.into_values().collect();
        results.sort_by_key(|s| std::cmp::Reverse(s.hits));
        Ok(results)
    }

    pub async fn googlebot_hits_for_date(&self, date: NaiveDate) -> anyhow::Result<HashMap<String, u64>> {
        let sql = format!(
            "SELECT cs_uri_stem as path, COUNT(*) as hits \
             FROM logs \
             WHERE \"cs_User_Agent\" LIKE '%Googlebot%' AND cs_method = 'GET' AND date = '{}' \
             GROUP BY cs_uri_stem",
            date
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
        Ok(results)
    }

    pub async fn visitor_ips_for_date(&self, date: NaiveDate, bot_map: &HashMap<String, String>) -> anyhow::Result<Vec<(String, bool)>> {
        let bot_case = if bot_map.is_empty() {
            "false".to_string()
        } else {
            let like_clauses: Vec<String> = bot_map.keys()
                .map(|pattern| format!("\"cs_User_Agent\" LIKE '%{}%'", pattern.replace('\'', "''")))
                .collect();
            format!("CASE WHEN ({}) THEN true ELSE false END", like_clauses.join(" OR "))
        };
        let sql = format!(
            "SELECT DISTINCT c_ip, {} as is_bot \
             FROM logs \
             WHERE cs_method = 'GET' AND sc_status IN ('200', '304') AND date = '{}'",
            bot_case, date
        );
        let df = self.ctx.sql(&sql).await?;
        let batches = df.collect().await?;
        let mut results = Vec::new();
        for batch in batches {
            let ip_col = batch.column(0).as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast c_ip column"))?;
            let bot_col = batch.column(1).as_any().downcast_ref::<datafusion::arrow::array::BooleanArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast is_bot column"))?;
            for i in 0..batch.num_rows() {
                results.push((ip_col.value(i).to_string(), bot_col.value(i)));
            }
        }
        Ok(results)
    }

    /// Query all metrics for a date. Returns (DayCache, visitor_ips).
    pub async fn query_day(&self, date: NaiveDate, bot_map: &HashMap<String, String>) -> anyhow::Result<(DayCache, Vec<(String, bool)>)> {
        let (hits, visitors) = self.summary_for_date(date).await?;
        let (bot_hits, bot_visitors) = self.bot_summary_for_date(date, bot_map).await?;
        let bot_path_hits = self.bot_hits_by_path_for_date(date, bot_map).await?;
        let top_pages = self.top_pages_for_date(date, &bot_path_hits).await?;
        let hourly = self.hourly_traffic_for_date(date).await?;
        let bot_stats = self.bot_activity_for_date(date, bot_map).await?;
        let google_hits = self.googlebot_hits_for_date(date).await?;
        let visitor_ips = self.visitor_ips_for_date(date, bot_map).await?;

        Ok((DayCache {
            date,
            hits,
            visitors,
            bot_hits,
            bot_visitors,
            top_pages,
            hourly,
            bot_stats,
            google_hits,
        }, visitor_ips))
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
