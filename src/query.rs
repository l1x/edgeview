use crate::classify::classify_bot;
use crate::config::SiteConfig;
use crate::model::*;
use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::{Datelike, NaiveDate};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use url::Url;

/// The 8 narrow columns we extract from CloudFront logs.
fn narrow_schema() -> Schema {
    Schema::new(vec![
        Field::new("date", DataType::Utf8, true),
        Field::new("time", DataType::Utf8, true),
        Field::new("c_ip", DataType::Utf8, true),
        Field::new("cs_method", DataType::Utf8, true),
        Field::new("cs_uri_stem", DataType::Utf8, true),
        Field::new("sc_status", DataType::Utf8, true),
        Field::new("cs_Referer", DataType::Utf8, true),
        Field::new("cs_User_Agent", DataType::Utf8, true),
    ])
}

const MAX_BUCKET_BYTES: usize = 256 * 1024 * 1024;

/// Maximum concurrent S3 file downloads (each file makes ~3 range requests).
pub(crate) const S3_CONCURRENCY: usize = 16;

/// Narrow column indices in 39-column CloudFront parquet schema:
/// 1=date, 2=time, 5=c_ip, 6=cs_method, 8=cs_uri_stem, 9=sc_status, 10=cs_Referer, 11=cs_User_Agent
const NARROW_INDICES: [usize; 8] = [1, 2, 5, 6, 8, 9, 10, 11];

// --- S3 range-read parquet reader ---

/// Reads a remote parquet file via S3 range requests.
/// The parquet crate calls `get_bytes` / `get_byte_ranges` only for the
/// column chunks selected by the projection mask — so we download ~18%
/// of the file (7 of 39 columns) instead of the whole thing.
pub(crate) struct S3ParquetReader {
    pub(crate) client: aws_sdk_s3::Client,
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) size: u64,
}

/// Timeout for a single S3 range-read request (send + body collect).
const S3_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Fetch a single byte range from S3 with a timeout.
async fn fetch_range(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    range: std::ops::Range<u64>,
) -> parquet::errors::Result<bytes::Bytes> {
    let range_hdr = format!("bytes={}-{}", range.start, range.end - 1);
    let fut = async {
        let resp = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .range(range_hdr)
            .send()
            .await
            .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
        resp.body
            .collect()
            .await
            .map(|b| b.into_bytes())
            .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))
    };
    tokio::time::timeout(S3_REQUEST_TIMEOUT, fut)
        .await
        .map_err(|_| {
            parquet::errors::ParquetError::General(format!(
                "S3 range read timed out after {}s for {}/{}",
                S3_REQUEST_TIMEOUT.as_secs(),
                bucket,
                key,
            ))
        })?
}

impl parquet::arrow::async_reader::AsyncFileReader for S3ParquetReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> futures::future::BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        Box::pin(async move { fetch_range(&client, &bucket, &key, range).await })
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> futures::future::BoxFuture<'_, parquet::errors::Result<Vec<bytes::Bytes>>> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        Box::pin(async move {
            let futs = ranges
                .into_iter()
                .map(|range| fetch_range(&client, &bucket, &key, range));
            futures::future::try_join_all(futs).await
        })
    }

    fn get_metadata(
        &mut self,
        _options: Option<&parquet::arrow::arrow_reader::ArrowReaderOptions>,
    ) -> futures::future::BoxFuture<
        '_,
        parquet::errors::Result<Arc<parquet::file::metadata::ParquetMetaData>>,
    > {
        let size = self.size;
        Box::pin(async move {
            // Read last 8 bytes: footer_len (4 LE) + magic "PAR1" (4)
            let suffix = self.get_bytes((size - 8)..size).await?;
            if suffix.len() < 8 || &suffix[4..8] != b"PAR1" {
                return Err(parquet::errors::ParquetError::General(
                    "Invalid parquet footer".into(),
                ));
            }
            let footer_len = u32::from_le_bytes(suffix[0..4].try_into().unwrap()) as u64;

            // Read the thrift-encoded footer
            let footer_start = size - 8 - footer_len;
            let footer_bytes = self.get_bytes(footer_start..(size - 8)).await?;
            let metadata =
                parquet::file::metadata::ParquetMetaDataReader::decode_metadata(&footer_bytes)?;
            Ok(Arc::new(metadata))
        })
    }
}

// --- S3 helpers ---

/// List all objects under an S3 prefix, handling pagination.
pub(crate) async fn list_s3_objects(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
) -> anyhow::Result<Vec<aws_sdk_s3::types::Object>> {
    let mut objects = Vec::new();
    let mut continuation_token: Option<String> = None;
    loop {
        let mut req = client.list_objects_v2().bucket(bucket).prefix(prefix);
        if let Some(token) = continuation_token.take() {
            req = req.continuation_token(token);
        }
        let resp = req.send().await?;
        if let Some(contents) = resp.contents {
            objects.extend(contents);
        }
        if resp.is_truncated.unwrap_or(false) {
            continuation_token = resp.next_continuation_token;
        } else {
            break;
        }
    }
    Ok(objects)
}

/// Sync multiple days' CloudFront logs from S3 concurrently.
/// Uses parquet range reads to download only the 7 needed columns (~18% of data).
/// All files across all days are processed in a single concurrent work pool.
pub async fn sync_days_from_s3(
    client: &aws_sdk_s3::Client,
    site: &SiteConfig,
    dates: &[NaiveDate],
    raw_dir: &Path,
) -> anyhow::Result<()> {
    use futures::stream::{self, StreamExt, TryStreamExt};
    use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    let s3_url = Url::parse(&site.s3_path)?;
    let bucket_name = s3_url.host_str().unwrap_or_default().to_string();
    let s3_prefix = s3_url.path().trim_start_matches('/').to_string();
    let client = client.clone();

    std::fs::create_dir_all(raw_dir)?;

    // 1. List all objects for all dates (throttled)
    let day_listings: Vec<(NaiveDate, Vec<aws_sdk_s3::types::Object>)> =
        stream::iter(dates.iter().map(|&date| {
            let client = client.clone();
            let bucket_name = bucket_name.clone();
            let s3_prefix = s3_prefix.clone();
            async move {
                let prefix = format!(
                    "{}/{}/{:02}/{:02}/",
                    s3_prefix,
                    date.year(),
                    date.month(),
                    date.day()
                );
                let objects = list_s3_objects(&client, &bucket_name, &prefix).await?;
                Ok::<_, anyhow::Error>((date, objects))
            }
        }))
        .buffer_unordered(S3_CONCURRENCY)
        .try_collect()
        .await?;

    // 2. Build flat work list: (date, key, size, bucket_idx)
    struct SyncItem {
        date: NaiveDate,
        key: String,
        size: u64,
        bucket_idx: usize,
    }

    let mut work: Vec<SyncItem> = Vec::new();
    let mut total_remote_bytes: u64 = 0;

    for (date, objects) in &day_listings {
        if objects.is_empty() {
            continue;
        }
        let total_size: usize = objects.iter().map(|o| o.size.unwrap_or(0) as usize).sum();
        let n_buckets = std::cmp::max(1, total_size.div_ceil(MAX_BUCKET_BYTES));

        for obj in objects {
            let key = obj.key.as_deref().unwrap_or_default();
            let filename = key.rsplit('/').next().unwrap_or(key);
            let hash = blake3::hash(filename.as_bytes());
            let hash_val = u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap());
            let bucket_idx = (hash_val as usize) % n_buckets;
            let size = obj.size.unwrap_or(0) as u64;
            total_remote_bytes += size;
            work.push(SyncItem {
                date: *date,
                key: key.to_string(),
                size,
                bucket_idx,
            });
        }
    }

    if work.is_empty() {
        return Ok(());
    }

    tracing::info!(
        domain = %site.domain,
        dates = dates.len(),
        files = work.len(),
        total_remote_bytes,
        "Syncing from S3 with range reads (8/39 columns)"
    );

    // 3. Download narrow columns concurrently across all files
    type BatchResult = (NaiveDate, usize, Vec<arrow::array::RecordBatch>);

    let results: Vec<BatchResult> = stream::iter(work.into_iter().map(|item| {
        let client = client.clone();
        let bucket_name = bucket_name.clone();
        async move {
            let reader = S3ParquetReader {
                client,
                bucket: bucket_name,
                key: item.key.clone(),
                size: item.size,
            };
            let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
            let parquet_schema = builder.parquet_schema().clone();
            let mask = parquet::arrow::ProjectionMask::leaves(
                &parquet_schema,
                NARROW_INDICES.iter().copied(),
            );
            let mut stream = builder.with_projection(mask).build()?;
            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                batches.push(batch?);
            }
            Ok::<BatchResult, anyhow::Error>((item.date, item.bucket_idx, batches))
        }
    }))
    .buffer_unordered(S3_CONCURRENCY)
    .try_collect()
    .await?;

    // 4. Group by (date, bucket_idx) and write local ZSTD-compressed parquet
    let mut grouped: HashMap<(NaiveDate, usize), Vec<arrow::array::RecordBatch>> = HashMap::new();
    let mut downloaded_bytes: u64 = 0;
    for (date, bucket_idx, batches) in results {
        for b in &batches {
            downloaded_bytes += b.get_array_memory_size() as u64;
        }
        grouped
            .entry((date, bucket_idx))
            .or_default()
            .extend(batches);
    }

    let schema = Arc::new(narrow_schema());
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();

    for ((date, bucket_idx), batches) in &grouped {
        let output_path = raw_dir.join(format!("{}_{}.parquet", date, bucket_idx));
        let file = std::fs::File::create(&output_path)?;
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props.clone()))?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.close()?;
    }

    tracing::info!(
        domain = %site.domain,
        remote_bytes = total_remote_bytes,
        downloaded_bytes,
        saved_pct = format_args!(
            "{:.0}%",
            if total_remote_bytes > 0 {
                (1.0 - downloaded_bytes as f64 / total_remote_bytes as f64) * 100.0
            } else {
                0.0
            }
        ),
        "S3 sync complete"
    );

    Ok(())
}

// --- Single-pass accumulators ---

#[derive(Default)]
struct PageAccum {
    hits: u64,
    visitor_ips: HashSet<String>,
    bot_hits: u64,
}

struct DayAccumulators {
    hits: u64,
    bot_hits: u64,
    visitor_ips: HashSet<String>,
    bot_visitor_ips: HashSet<String>,
    page_stats: HashMap<(String, String), PageAccum>,
    hourly_hits: [u64; 24],
    hourly_visitors: Vec<HashSet<String>>,
    googlebot_hits: HashMap<String, u64>,
    all_visitor_ips: HashMap<String, bool>,
    bot_activity: HashMap<String, u64>,
    referer_hits: HashMap<String, u64>,
}

impl Default for DayAccumulators {
    fn default() -> Self {
        Self {
            hits: 0,
            bot_hits: 0,
            visitor_ips: HashSet::new(),
            bot_visitor_ips: HashSet::new(),
            page_stats: HashMap::new(),
            hourly_hits: [0; 24],
            hourly_visitors: (0..24).map(|_| HashSet::new()).collect(),
            googlebot_hits: HashMap::new(),
            all_visitor_ips: HashMap::new(),
            bot_activity: HashMap::new(),
            referer_hits: HashMap::new(),
        }
    }
}

impl DayAccumulators {
    fn merge(&mut self, other: Self) {
        self.hits += other.hits;
        self.bot_hits += other.bot_hits;
        self.visitor_ips.extend(other.visitor_ips);
        self.bot_visitor_ips.extend(other.bot_visitor_ips);

        for (key, other_page) in other.page_stats {
            let entry = self.page_stats.entry(key).or_default();
            entry.hits += other_page.hits;
            entry.visitor_ips.extend(other_page.visitor_ips);
            entry.bot_hits += other_page.bot_hits;
        }

        for (i, count) in other.hourly_hits.iter().enumerate() {
            self.hourly_hits[i] += count;
        }
        for (i, visitors) in other.hourly_visitors.into_iter().enumerate() {
            self.hourly_visitors[i].extend(visitors);
        }

        for (path, hits) in other.googlebot_hits {
            *self.googlebot_hits.entry(path).or_default() += hits;
        }

        for (ip, is_bot) in other.all_visitor_ips {
            let entry = self.all_visitor_ips.entry(ip).or_insert(false);
            if is_bot {
                *entry = true;
            }
        }

        for (name, hits) in other.bot_activity {
            *self.bot_activity.entry(name).or_default() += hits;
        }

        for (referer, hits) in other.referer_hits {
            *self.referer_hits.entry(referer).or_default() += hits;
        }
    }

    fn into_results(self, date: NaiveDate) -> (DayCache, Vec<(String, bool)>) {
        let mut top_pages: Vec<PageHits> = self
            .page_stats
            .into_iter()
            .map(|((category, path), accum)| PageHits {
                path,
                hits: accum.hits,
                visitors: accum.visitor_ips.len() as u64,
                bot_hits: accum.bot_hits,
                category,
            })
            .collect();
        top_pages.sort_by_key(|p| std::cmp::Reverse(p.hits));

        let hourly: Vec<HourlyTraffic> = (0..24u8)
            .map(|h| HourlyTraffic {
                hour: h,
                hits: self.hourly_hits[h as usize],
                visitors: self.hourly_visitors[h as usize].len() as u64,
            })
            .collect();

        let last_crawl = chrono::DateTime::from_naive_utc_and_offset(
            date.and_hms_opt(0, 0, 0).unwrap(),
            chrono::Utc,
        );
        let mut bot_stats: Vec<CrawlerStats> = self
            .bot_activity
            .into_iter()
            .map(|(bot_name, hits)| CrawlerStats {
                bot_name,
                hits,
                last_crawl: Some(last_crawl),
            })
            .collect();
        bot_stats.sort_by_key(|s| std::cmp::Reverse(s.hits));

        let mut referer_stats: Vec<RefererStats> = self
            .referer_hits
            .into_iter()
            .map(|(referer, hits)| RefererStats { referer, hits })
            .collect();
        referer_stats.sort_by_key(|r| std::cmp::Reverse(r.hits));

        let visitor_ips: Vec<(String, bool)> = self.all_visitor_ips.into_iter().collect();

        let day_cache = DayCache {
            date,
            hits: self.hits,
            visitors: self.visitor_ips.len() as u64,
            bot_hits: self.bot_hits,
            bot_visitors: self.bot_visitor_ips.len() as u64,
            top_pages,
            hourly,
            bot_stats,
            google_hits: self.googlebot_hits,
            referer_stats,
        };

        (day_cache, visitor_ips)
    }
}

/// Read one parquet file, filter by date, accumulate all metrics in a single pass.
fn scan_file(
    path: &Path,
    target_date: &str,
    bot_map: &HashMap<String, String>,
    site_domain: &str,
) -> anyhow::Result<DayAccumulators> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = std::fs::File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut acc = DayAccumulators::default();

    for batch_result in reader {
        let batch = batch_result?;
        // Columns: date(0), time(1), c_ip(2), cs_method(3), cs_uri_stem(4), sc_status(5), cs_Referer(6), cs_User_Agent(7)
        let date_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let time_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ip_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let method_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let path_col = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let status_col = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let referer_col = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ua_col = batch
            .column(7)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for row in 0..batch.num_rows() {
            if date_col.value(row) != target_date {
                continue;
            }

            let ua = if ua_col.is_null(row) {
                ""
            } else {
                ua_col.value(row)
            };
            let bot_name = if ua.is_empty() {
                None
            } else {
                classify_bot(ua, bot_map)
            };

            // Bot activity: ALL requests (before GET/200/304 filter)
            if let Some(ref name) = bot_name {
                *acc.bot_activity.entry(name.clone()).or_insert(0u64) += 1;
            }

            if method_col.value(row) != "GET" {
                continue;
            }
            let status = status_col.value(row);
            if status != "200" && status != "304" {
                continue;
            }

            let c_ip = ip_col.value(row).to_string();
            let uri = path_col.value(row);
            let is_bot = bot_name.is_some();

            acc.hits += 1;
            acc.visitor_ips.insert(c_ip.clone());

            if is_bot {
                acc.bot_hits += 1;
                acc.bot_visitor_ips.insert(c_ip.clone());
            }

            // Page stats
            let (category, display_path) = classify_path(uri);
            let page = acc
                .page_stats
                .entry((category.to_string(), display_path))
                .or_default();
            page.hits += 1;
            page.visitor_ips.insert(c_ip.clone());
            if is_bot {
                page.bot_hits += 1;
            }

            // Hourly
            let hour: usize = time_col
                .value(row)
                .get(..2)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            acc.hourly_hits[hour] += 1;
            acc.hourly_visitors[hour].insert(c_ip.clone());

            // Googlebot
            if ua.contains("Googlebot") {
                *acc.googlebot_hits.entry(uri.to_string()).or_default() += 1;
            }

            // External referers (skip "-", empty, and same-domain)
            if !referer_col.is_null(row) {
                let referer = referer_col.value(row);
                if referer != "-" && !referer.is_empty() {
                    // Check if it's external by comparing the host
                    let is_external = url::Url::parse(referer)
                        .map(|u| {
                            u.host_str()
                                .is_some_and(|h| !h.eq_ignore_ascii_case(site_domain))
                        })
                        .unwrap_or(false);
                    if is_external {
                        *acc.referer_hits.entry(referer.to_string()).or_default() += 1;
                    }
                }
            }

            // Visitor IP with bot flag
            let entry = acc.all_visitor_ips.entry(c_ip).or_insert(false);
            if is_bot {
                *entry = true;
            }
        }
    }

    Ok(acc)
}

/// Query engine that operates on LOCAL narrow parquet files using direct reads + Rayon.
pub struct QueryEngine {
    raw_dir: PathBuf,
}

impl QueryEngine {
    /// Create a query engine over local narrow parquet files in raw_dir.
    pub fn new_local(raw_dir: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            raw_dir: raw_dir.to_path_buf(),
        })
    }

    /// List parquet files matching a specific date prefix.
    fn files_for_date(&self, date: NaiveDate) -> Vec<PathBuf> {
        let prefix = format!("{}_", date);
        std::fs::read_dir(&self.raw_dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name();
                let name = name.to_string_lossy();
                name.starts_with(prefix.as_str()) && name.ends_with(".parquet")
            })
            .map(|e| e.path())
            .collect()
    }

    /// Query all metrics for multiple dates using flattened Rayon parallelism.
    /// All (date, file) pairs are processed in one flat work pool.
    /// Returns results in the same order as input dates.
    #[allow(clippy::type_complexity)]
    pub fn query_days(
        &self,
        dates: &[NaiveDate],
        bot_map: &HashMap<String, String>,
        site_domain: &str,
    ) -> anyhow::Result<Vec<(DayCache, Vec<(String, bool)>)>> {
        // Flatten all (date, file) pairs
        let work_items: Vec<(NaiveDate, PathBuf)> = dates
            .iter()
            .flat_map(|&date| {
                self.files_for_date(date)
                    .into_iter()
                    .map(move |f| (date, f))
            })
            .collect();

        if work_items.is_empty() {
            return Ok(dates
                .iter()
                .map(|&d| DayAccumulators::default().into_results(d))
                .collect());
        }

        tracing::info!(
            files = work_items.len(),
            dates = dates.len(),
            "Scanning parquet files with Rayon"
        );

        // Process all files in parallel
        let file_results: Vec<(NaiveDate, DayAccumulators)> = work_items
            .par_iter()
            .map(|(date, file)| {
                let acc = scan_file(file, &date.to_string(), bot_map, site_domain)?;
                Ok((*date, acc))
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?;

        // Group by date and merge
        let mut day_accums: HashMap<NaiveDate, DayAccumulators> = HashMap::new();
        for (date, acc) in file_results {
            day_accums.entry(date).or_default().merge(acc);
        }

        // Return in input order
        Ok(dates
            .iter()
            .map(|&date| {
                day_accums
                    .remove(&date)
                    .unwrap_or_default()
                    .into_results(date)
            })
            .collect())
    }
}

/// Classify a URL path into a category and a display path based on file extension.
fn classify_path(path: &str) -> (&'static str, String) {
    let ext = path.rsplit_once('.').map(|(_, e)| e.to_ascii_lowercase());
    match ext.as_deref() {
        Some("css") => ("css", path.to_string()),
        Some("js" | "mjs") => ("js", path.to_string()),
        Some("png" | "jpg" | "jpeg" | "gif" | "webp" | "svg" | "ico" | "avif" | "bmp") => {
            ("image", path.to_string())
        }
        Some("ttf" | "woff" | "woff2" | "eot" | "otf") => ("font", path.to_string()),
        Some("xml" | "json" | "rss" | "atom" | "txt") => ("data", path.to_string()),
        Some("html") | None => ("page", path.to_string()),
        Some(_) => ("page", path.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    /// Write a narrow parquet file with the given rows.
    /// Each row is (date, time, c_ip, cs_method, cs_uri_stem, sc_status, cs_Referer, cs_User_Agent).
    #[allow(clippy::type_complexity)]
    fn write_test_parquet(path: &Path, rows: &[(&str, &str, &str, &str, &str, &str, &str, &str)]) {
        let schema = Arc::new(narrow_schema());
        let file = std::fs::File::create(path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

        let dates: Vec<&str> = rows.iter().map(|r| r.0).collect();
        let times: Vec<&str> = rows.iter().map(|r| r.1).collect();
        let ips: Vec<&str> = rows.iter().map(|r| r.2).collect();
        let methods: Vec<&str> = rows.iter().map(|r| r.3).collect();
        let paths: Vec<&str> = rows.iter().map(|r| r.4).collect();
        let statuses: Vec<&str> = rows.iter().map(|r| r.5).collect();
        let referers: Vec<&str> = rows.iter().map(|r| r.6).collect();
        let uas: Vec<&str> = rows.iter().map(|r| r.7).collect();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(dates)),
                Arc::new(StringArray::from(times)),
                Arc::new(StringArray::from(ips)),
                Arc::new(StringArray::from(methods)),
                Arc::new(StringArray::from(paths)),
                Arc::new(StringArray::from(statuses)),
                Arc::new(StringArray::from(referers)),
                Arc::new(StringArray::from(uas)),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn test_bot_map() -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("Googlebot".to_string(), "Google".to_string());
        m.insert("bingbot".to_string(), "Bing".to_string());
        m
    }

    #[test]
    fn test_scan_file_basic_metrics() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("2026-03-01_0.parquet");
        write_test_parquet(
            &file,
            &[
                (
                    "2026-03-01",
                    "10:30:00",
                    "1.2.3.4",
                    "GET",
                    "/",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
                (
                    "2026-03-01",
                    "10:31:00",
                    "1.2.3.5",
                    "GET",
                    "/about",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
                (
                    "2026-03-01",
                    "10:32:00",
                    "1.2.3.4",
                    "GET",
                    "/",
                    "304",
                    "-",
                    "Mozilla/5.0",
                ),
                // POST should be excluded from hits
                (
                    "2026-03-01",
                    "10:33:00",
                    "1.2.3.6",
                    "POST",
                    "/submit",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
                // 404 should be excluded from hits
                (
                    "2026-03-01",
                    "10:34:00",
                    "1.2.3.7",
                    "GET",
                    "/missing",
                    "404",
                    "-",
                    "Mozilla/5.0",
                ),
                // Different date should be filtered out
                (
                    "2026-03-02",
                    "10:35:00",
                    "1.2.3.8",
                    "GET",
                    "/",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
            ],
        );

        let bots = test_bot_map();
        let acc = scan_file(&file, "2026-03-01", &bots, "example.com").unwrap();
        let (day, visitors) = acc.into_results(NaiveDate::from_ymd_opt(2026, 3, 1).unwrap());

        assert_eq!(day.hits, 3); // 3 GET 200/304
        assert_eq!(day.visitors, 2); // 1.2.3.4 and 1.2.3.5
        assert_eq!(day.bot_hits, 0);
        assert_eq!(day.bot_visitors, 0);
        assert_eq!(visitors.len(), 2);
    }

    #[test]
    fn test_scan_file_bot_detection() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("2026-03-01_0.parquet");
        write_test_parquet(
            &file,
            &[
                (
                    "2026-03-01",
                    "08:00:00",
                    "1.1.1.1",
                    "GET",
                    "/",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
                (
                    "2026-03-01",
                    "08:01:00",
                    "2.2.2.2",
                    "GET",
                    "/articles/rust",
                    "200",
                    "-",
                    "Mozilla/5.0 (compatible; Googlebot/2.1)",
                ),
                (
                    "2026-03-01",
                    "08:02:00",
                    "3.3.3.3",
                    "GET",
                    "/about",
                    "200",
                    "-",
                    "Mozilla/5.0 (compatible; bingbot/2.0)",
                ),
                // Bot doing a POST — counted in bot_activity but not in hits
                (
                    "2026-03-01",
                    "08:03:00",
                    "2.2.2.2",
                    "POST",
                    "/submit",
                    "200",
                    "-",
                    "Mozilla/5.0 (compatible; Googlebot/2.1)",
                ),
            ],
        );

        let bots = test_bot_map();
        let acc = scan_file(&file, "2026-03-01", &bots, "example.com").unwrap();
        let (day, visitors) = acc.into_results(NaiveDate::from_ymd_opt(2026, 3, 1).unwrap());

        assert_eq!(day.hits, 3);
        assert_eq!(day.visitors, 3);
        assert_eq!(day.bot_hits, 2); // Googlebot GET + bingbot GET
        assert_eq!(day.bot_visitors, 2); // 2.2.2.2, 3.3.3.3

        // Bot activity includes POST too
        let google_activity = day
            .bot_stats
            .iter()
            .find(|b| b.bot_name == "Google")
            .unwrap();
        assert_eq!(google_activity.hits, 2); // GET + POST

        // Googlebot hits map (only GET 200/304)
        assert_eq!(*day.google_hits.get("/articles/rust").unwrap(), 1);

        // Visitor IPs: bot IPs should be marked as bot
        let bot_ips: Vec<_> = visitors.iter().filter(|(_, is_bot)| *is_bot).collect();
        assert_eq!(bot_ips.len(), 2);
    }

    #[test]
    fn test_scan_file_hourly_traffic() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("2026-03-01_0.parquet");
        write_test_parquet(
            &file,
            &[
                (
                    "2026-03-01",
                    "09:00:00",
                    "1.1.1.1",
                    "GET",
                    "/",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
                (
                    "2026-03-01",
                    "09:30:00",
                    "1.1.1.2",
                    "GET",
                    "/",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
                (
                    "2026-03-01",
                    "14:00:00",
                    "1.1.1.3",
                    "GET",
                    "/",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
                (
                    "2026-03-01",
                    "23:59:00",
                    "1.1.1.4",
                    "GET",
                    "/",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
            ],
        );

        let bots = test_bot_map();
        let acc = scan_file(&file, "2026-03-01", &bots, "example.com").unwrap();
        let (day, _) = acc.into_results(NaiveDate::from_ymd_opt(2026, 3, 1).unwrap());

        assert_eq!(day.hourly[9].hits, 2); // hour 09
        assert_eq!(day.hourly[9].visitors, 2);
        assert_eq!(day.hourly[14].hits, 1); // hour 14
        assert_eq!(day.hourly[23].hits, 1); // hour 23
        assert_eq!(day.hourly[0].hits, 0); // hour 00
    }

    #[test]
    fn test_scan_file_page_classification() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("2026-03-01_0.parquet");
        write_test_parquet(
            &file,
            &[
                (
                    "2026-03-01",
                    "10:00:00",
                    "1.1.1.1",
                    "GET",
                    "/articles/rust-tips",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
                (
                    "2026-03-01",
                    "10:01:00",
                    "1.1.1.2",
                    "GET",
                    "/articles/rust-tips",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
                (
                    "2026-03-01",
                    "10:02:00",
                    "1.1.1.1",
                    "GET",
                    "/static/css/main.css",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
                (
                    "2026-03-01",
                    "10:03:00",
                    "1.1.1.1",
                    "GET",
                    "/static/css/code.css",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
                (
                    "2026-03-01",
                    "10:04:00",
                    "1.1.1.1",
                    "GET",
                    "/about",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
                (
                    "2026-03-01",
                    "10:05:00",
                    "1.1.1.1",
                    "GET",
                    "/robots.txt",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
            ],
        );

        let bots = test_bot_map();
        let acc = scan_file(&file, "2026-03-01", &bots, "example.com").unwrap();
        let (day, _) = acc.into_results(NaiveDate::from_ymd_opt(2026, 3, 1).unwrap());

        let find_page = |cat: &str, path: &str| -> Option<&PageHits> {
            day.top_pages
                .iter()
                .find(|p| p.category == cat && p.path == path)
        };

        // Article page — no extension → "page"
        let article = find_page("page", "/articles/rust-tips").unwrap();
        assert_eq!(article.hits, 2);
        assert_eq!(article.visitors, 2);

        // CSS files — individual paths, category "css"
        let css1 = find_page("css", "/static/css/main.css").unwrap();
        assert_eq!(css1.hits, 1);
        let css2 = find_page("css", "/static/css/code.css").unwrap();
        assert_eq!(css2.hits, 1);

        // Regular page
        assert!(find_page("page", "/about").is_some());

        // robots.txt → "data" category
        assert!(find_page("data", "/robots.txt").is_some());
    }

    #[test]
    fn test_accumulator_merge() {
        let dir = TempDir::new().unwrap();
        let date = NaiveDate::from_ymd_opt(2026, 3, 1).unwrap();
        let bots = test_bot_map();

        // File 1: human traffic
        let f1 = dir.path().join("2026-03-01_0.parquet");
        write_test_parquet(
            &f1,
            &[
                (
                    "2026-03-01",
                    "10:00:00",
                    "1.1.1.1",
                    "GET",
                    "/",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
                (
                    "2026-03-01",
                    "10:01:00",
                    "1.1.1.2",
                    "GET",
                    "/about",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
            ],
        );

        // File 2: bot traffic + overlapping IP
        let f2 = dir.path().join("2026-03-01_1.parquet");
        write_test_parquet(
            &f2,
            &[
                (
                    "2026-03-01",
                    "11:00:00",
                    "2.2.2.2",
                    "GET",
                    "/",
                    "200",
                    "-",
                    "Mozilla/5.0 (compatible; Googlebot/2.1)",
                ),
                // Same IP as file 1 — should not double-count visitors
                (
                    "2026-03-01",
                    "11:01:00",
                    "1.1.1.1",
                    "GET",
                    "/contact",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
            ],
        );

        let mut acc1 = scan_file(&f1, "2026-03-01", &bots, "example.com").unwrap();
        let acc2 = scan_file(&f2, "2026-03-01", &bots, "example.com").unwrap();
        acc1.merge(acc2);
        let (day, visitors) = acc1.into_results(date);

        assert_eq!(day.hits, 4);
        assert_eq!(day.visitors, 3); // 1.1.1.1, 1.1.1.2, 2.2.2.2 (deduped)
        assert_eq!(day.bot_hits, 1);
        assert_eq!(day.bot_visitors, 1);
        assert_eq!(visitors.len(), 3);
    }

    #[test]
    fn test_query_engine_files_for_date() {
        let dir = TempDir::new().unwrap();

        // Create files for different dates
        write_test_parquet(
            &dir.path().join("2026-03-01_0.parquet"),
            &[(
                "2026-03-01",
                "10:00:00",
                "1.1.1.1",
                "GET",
                "/",
                "200",
                "-",
                "Mozilla/5.0",
            )],
        );
        write_test_parquet(
            &dir.path().join("2026-03-01_1.parquet"),
            &[(
                "2026-03-01",
                "10:00:00",
                "1.1.1.1",
                "GET",
                "/",
                "200",
                "-",
                "Mozilla/5.0",
            )],
        );
        write_test_parquet(
            &dir.path().join("2026-03-02_0.parquet"),
            &[(
                "2026-03-02",
                "10:00:00",
                "1.1.1.1",
                "GET",
                "/",
                "200",
                "-",
                "Mozilla/5.0",
            )],
        );
        // Non-matching file
        std::fs::write(dir.path().join("readme.txt"), "not a parquet").unwrap();

        let engine = QueryEngine::new_local(dir.path()).unwrap();

        let mar01 = NaiveDate::from_ymd_opt(2026, 3, 1).unwrap();
        let mar02 = NaiveDate::from_ymd_opt(2026, 3, 2).unwrap();
        let mar03 = NaiveDate::from_ymd_opt(2026, 3, 3).unwrap();

        assert_eq!(engine.files_for_date(mar01).len(), 2);
        assert_eq!(engine.files_for_date(mar02).len(), 1);
        assert_eq!(engine.files_for_date(mar03).len(), 0);
    }

    #[test]
    fn test_query_days_flattened_parallelism() {
        let dir = TempDir::new().unwrap();
        let bots = test_bot_map();

        // Day 1: 2 files
        write_test_parquet(
            &dir.path().join("2026-03-01_0.parquet"),
            &[
                (
                    "2026-03-01",
                    "10:00:00",
                    "1.1.1.1",
                    "GET",
                    "/",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
                (
                    "2026-03-01",
                    "10:01:00",
                    "1.1.1.2",
                    "GET",
                    "/about",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
            ],
        );
        write_test_parquet(
            &dir.path().join("2026-03-01_1.parquet"),
            &[(
                "2026-03-01",
                "11:00:00",
                "1.1.1.3",
                "GET",
                "/",
                "200",
                "-",
                "Mozilla/5.0",
            )],
        );

        // Day 2: 1 file with bot
        write_test_parquet(
            &dir.path().join("2026-03-02_0.parquet"),
            &[(
                "2026-03-02",
                "14:00:00",
                "2.2.2.2",
                "GET",
                "/",
                "200",
                "-",
                "Mozilla/5.0 (compatible; Googlebot/2.1)",
            )],
        );

        let engine = QueryEngine::new_local(dir.path()).unwrap();
        let dates = vec![
            NaiveDate::from_ymd_opt(2026, 3, 1).unwrap(),
            NaiveDate::from_ymd_opt(2026, 3, 2).unwrap(),
        ];
        let results = engine.query_days(&dates, &bots, "example.com").unwrap();

        assert_eq!(results.len(), 2);

        // Day 1: 3 hits from 3 visitors, no bots
        let (day1, _) = &results[0];
        assert_eq!(day1.date, dates[0]);
        assert_eq!(day1.hits, 3);
        assert_eq!(day1.visitors, 3);
        assert_eq!(day1.bot_hits, 0);

        // Day 2: 1 bot hit
        let (day2, _) = &results[1];
        assert_eq!(day2.date, dates[1]);
        assert_eq!(day2.hits, 1);
        assert_eq!(day2.bot_hits, 1);
        assert_eq!(day2.bot_visitors, 1);
    }

    #[test]
    fn test_external_referer_extraction() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("2026-03-01_0.parquet");
        write_test_parquet(
            &file,
            &[
                // External referer — should be counted
                (
                    "2026-03-01",
                    "10:00:00",
                    "1.1.1.1",
                    "GET",
                    "/",
                    "200",
                    "https://www.google.com/search?q=test",
                    "Mozilla/5.0",
                ),
                // Same external referer — should increment count
                (
                    "2026-03-01",
                    "10:01:00",
                    "1.1.1.2",
                    "GET",
                    "/about",
                    "200",
                    "https://www.google.com/search?q=test",
                    "Mozilla/5.0",
                ),
                // Different external referer
                (
                    "2026-03-01",
                    "10:02:00",
                    "1.1.1.3",
                    "GET",
                    "/",
                    "200",
                    "https://news.ycombinator.com/item?id=123",
                    "Mozilla/5.0",
                ),
                // Internal referer — should be filtered out
                (
                    "2026-03-01",
                    "10:03:00",
                    "1.1.1.4",
                    "GET",
                    "/about",
                    "200",
                    "https://dev.l1x.be/articles/rust",
                    "Mozilla/5.0",
                ),
                // No referer ("-") — should be filtered out
                (
                    "2026-03-01",
                    "10:04:00",
                    "1.1.1.5",
                    "GET",
                    "/",
                    "200",
                    "-",
                    "Mozilla/5.0",
                ),
                // Empty referer — should be filtered out
                (
                    "2026-03-01",
                    "10:05:00",
                    "1.1.1.6",
                    "GET",
                    "/",
                    "200",
                    "",
                    "Mozilla/5.0",
                ),
            ],
        );

        let bots = test_bot_map();
        let acc = scan_file(&file, "2026-03-01", &bots, "dev.l1x.be").unwrap();
        let (day, _) = acc.into_results(NaiveDate::from_ymd_opt(2026, 3, 1).unwrap());

        // Should have 2 unique external referers
        assert_eq!(day.referer_stats.len(), 2);

        // Google referer should have 2 hits (top)
        assert_eq!(
            day.referer_stats[0].referer,
            "https://www.google.com/search?q=test"
        );
        assert_eq!(day.referer_stats[0].hits, 2);

        // HN referer should have 1 hit
        assert_eq!(
            day.referer_stats[1].referer,
            "https://news.ycombinator.com/item?id=123"
        );
        assert_eq!(day.referer_stats[1].hits, 1);
    }

    #[test]
    fn test_query_days_empty() {
        let dir = TempDir::new().unwrap();
        let bots = test_bot_map();
        let engine = QueryEngine::new_local(dir.path()).unwrap();

        let dates = vec![NaiveDate::from_ymd_opt(2026, 3, 1).unwrap()];
        let results = engine.query_days(&dates, &bots, "example.com").unwrap();

        assert_eq!(results.len(), 1);
        let (day, visitors) = &results[0];
        assert_eq!(day.hits, 0);
        assert_eq!(day.visitors, 0);
        assert!(visitors.is_empty());
    }
}
