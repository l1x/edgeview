use crate::classify::classify_bot;
use crate::config::SiteConfig;
use crate::model::*;
use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use time::Date;

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
            if size < 12 {
                return Err(parquet::errors::ParquetError::General(format!(
                    "File too small ({} bytes) to be valid parquet",
                    size
                )));
            }
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
/// Reads from compacted daily parquet files (`<domain>/compact/YYYY/MM/DD.parquet`),
/// using byte-range requests to download only the 8 needed columns (~18% of data).
pub async fn sync_days_from_s3(
    client: &aws_sdk_s3::Client,
    site: &SiteConfig,
    dates: &[Date],
    raw_dir: &Path,
) -> anyhow::Result<()> {
    use futures::stream::{self, StreamExt, TryStreamExt};
    use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    let (bucket_name, s3_prefix) = site.s3_bucket_and_prefix()?;
    let compact_prefix = crate::compact::compact_s3_prefix(&s3_prefix);
    let client = client.clone();

    std::fs::create_dir_all(raw_dir)?;

    // 1. HEAD each compact file to get size (and confirm existence)
    struct CompactFile {
        date: Date,
        key: String,
        size: u64,
    }

    let heads: Vec<Option<CompactFile>> = stream::iter(dates.iter().map(|&date| {
        let client = client.clone();
        let bucket_name = bucket_name.clone();
        let key = crate::compact::compact_key(&compact_prefix, date);
        async move {
            match client
                .head_object()
                .bucket(&bucket_name)
                .key(&key)
                .send()
                .await
            {
                Ok(resp) => {
                    let size = resp.content_length.unwrap_or(0) as u64;
                    if size < 12 {
                        tracing::warn!(date = %date, size, "Compact file too small, skipping");
                        return Ok(None);
                    }
                    Ok::<_, anyhow::Error>(Some(CompactFile { date, key, size }))
                }
                Err(e) => {
                    let svc = e.into_service_error();
                    if svc.is_not_found() {
                        tracing::debug!(date = %date, "No compact file, skipping");
                        Ok(None)
                    } else {
                        Err(anyhow::anyhow!(
                            "HEAD compact file failed for {}: {}",
                            date,
                            svc
                        ))
                    }
                }
            }
        }
    }))
    .buffer_unordered(S3_CONCURRENCY)
    .try_collect()
    .await?;

    let work: Vec<CompactFile> = heads.into_iter().flatten().collect();

    if work.is_empty() {
        return Ok(());
    }

    let total_remote_bytes: u64 = work.iter().map(|f| f.size).sum();

    tracing::info!(
        domain = %site.domain,
        dates = dates.len(),
        compact_files = work.len(),
        total_remote_bytes,
        "Syncing from S3 compact files with range reads (8/39 columns)"
    );

    // 2. Download narrow columns from each compact file concurrently
    type BatchResult = (Date, Vec<arrow::array::RecordBatch>);

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
            Ok::<BatchResult, anyhow::Error>((item.date, batches))
        }
    }))
    .buffer_unordered(S3_CONCURRENCY)
    .try_collect()
    .await?;

    // 3. Write one local narrow parquet file per date
    let schema = Arc::new(narrow_schema());
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();

    let mut local_bytes: u64 = 0;
    for (date, batches) in &results {
        let output_path = raw_dir.join(format!("{}_0.parquet", date));
        let file = std::fs::File::create(&output_path)?;
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props.clone()))?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.close()?;
        local_bytes += std::fs::metadata(&output_path)?.len();
    }

    tracing::info!(
        domain = %site.domain,
        compact_files = results.len(),
        remote_bytes = total_remote_bytes,
        local_bytes,
        "S3 sync complete (8/39 columns via range reads)"
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

    fn into_results(self, date: Date) -> (DayCache, Vec<(String, bool)>) {
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

        let last_crawl = date.with_time(time::Time::MIDNIGHT).assume_utc();
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

        let total_visitors = self.all_visitor_ips.len() as u64;
        let bot_visitors = self.all_visitor_ips.values().filter(|&&b| b).count() as u64;
        let visitor_ips: Vec<(String, bool)> = self.all_visitor_ips.into_iter().collect();

        let day_cache = DayCache {
            date,
            hits: self.hits,
            visitors: total_visitors,
            bot_hits: self.bot_hits,
            bot_visitors,
            top_pages,
            hourly,
            bot_stats,
            google_hits: self.googlebot_hits,
            referer_stats,
        };

        (day_cache, visitor_ips)
    }
}

/// Downcast an arrow column to StringArray with a clear error.
fn col_as_str(batch: &arrow::array::RecordBatch, idx: usize) -> anyhow::Result<&StringArray> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("Column {} is not Utf8", batch.schema().field(idx).name()))
}

/// Extract the host from a URL string without full parsing.
fn extract_referer_host(referer: &str) -> Option<&str> {
    let after_scheme = referer
        .strip_prefix("https://")
        .or_else(|| referer.strip_prefix("http://"))?;
    Some(after_scheme.split('/').next().unwrap_or(after_scheme))
}

/// Read one parquet file, filter by date, accumulate all metrics in a single pass.
fn scan_file(
    path: &Path,
    target_date: &str,
    bot_map: &[(String, String)],
    site_domain: &str,
) -> anyhow::Result<DayAccumulators> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = std::fs::File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut acc = DayAccumulators::default();

    for batch_result in reader {
        let batch = batch_result?;
        // Columns: date(0), time(1), c_ip(2), cs_method(3), cs_uri_stem(4), sc_status(5), cs_Referer(6), cs_User_Agent(7)
        let date_col = col_as_str(&batch, 0)?;
        let time_col = col_as_str(&batch, 1)?;
        let ip_col = col_as_str(&batch, 2)?;
        let method_col = col_as_str(&batch, 3)?;
        let path_col = col_as_str(&batch, 4)?;
        let status_col = col_as_str(&batch, 5)?;
        let referer_col = col_as_str(&batch, 6)?;
        let ua_col = col_as_str(&batch, 7)?;

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

            if is_bot {
                acc.bot_hits += 1;
            }

            // Page stats
            let (category, display_path) = classify_path(uri);
            let page = acc
                .page_stats
                .entry((category.to_string(), display_path.to_string()))
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
                    let is_external = extract_referer_host(referer)
                        .is_some_and(|h| !h.eq_ignore_ascii_case(site_domain));
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
    fn files_for_date(&self, date: Date) -> Vec<PathBuf> {
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
        dates: &[Date],
        bot_map: &[(String, String)],
        site_domain: &str,
    ) -> anyhow::Result<Vec<(DayCache, Vec<(String, bool)>)>> {
        // Flatten all (date, file) pairs
        let work_items: Vec<(Date, PathBuf)> = dates
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
        let file_results: Vec<(Date, DayAccumulators)> = work_items
            .par_iter()
            .map(|(date, file)| {
                let acc = scan_file(file, &date.to_string(), bot_map, site_domain)?;
                Ok((*date, acc))
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?;

        // Group by date and merge
        let mut day_accums: HashMap<Date, DayAccumulators> = HashMap::new();
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

/// Classify a URL path into a category based on file extension.
fn classify_path(path: &str) -> (&'static str, &str) {
    let ext = path.rsplit_once('.').map(|(_, e)| e);
    let category = match ext {
        Some(e) if e.eq_ignore_ascii_case("css") => "css",
        Some(e) if e.eq_ignore_ascii_case("js") || e.eq_ignore_ascii_case("mjs") => "js",
        Some(e)
            if [
                "png", "jpg", "jpeg", "gif", "webp", "svg", "ico", "avif", "bmp",
            ]
            .iter()
            .any(|x| e.eq_ignore_ascii_case(x)) =>
        {
            "image"
        }
        Some(e)
            if ["ttf", "woff", "woff2", "eot", "otf"]
                .iter()
                .any(|x| e.eq_ignore_ascii_case(x)) =>
        {
            "font"
        }
        Some(e)
            if ["xml", "json", "rss", "atom", "txt"]
                .iter()
                .any(|x| e.eq_ignore_ascii_case(x)) =>
        {
            "data"
        }
        _ => "page",
    };
    (category, path)
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

    fn test_bot_map() -> Vec<(String, String)> {
        vec![
            ("Googlebot".to_string(), "Google".to_string()),
            ("bingbot".to_string(), "Bing".to_string()),
        ]
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
        let (day, visitors) = acc.into_results(time::macros::date!(2026 - 03 - 01));

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
        let (day, visitors) = acc.into_results(time::macros::date!(2026 - 03 - 01));

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
        let (day, _) = acc.into_results(time::macros::date!(2026 - 03 - 01));

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
        let (day, _) = acc.into_results(time::macros::date!(2026 - 03 - 01));

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
        let date = time::macros::date!(2026 - 03 - 01);
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

        let mar01 = time::macros::date!(2026 - 03 - 01);
        let mar02 = time::macros::date!(2026 - 03 - 02);
        let mar03 = time::macros::date!(2026 - 03 - 03);

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
            time::macros::date!(2026 - 03 - 01),
            time::macros::date!(2026 - 03 - 02),
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
        let (day, _) = acc.into_results(time::macros::date!(2026 - 03 - 01));

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

        let dates = vec![time::macros::date!(2026 - 03 - 01)];
        let results = engine.query_days(&dates, &bots, "example.com").unwrap();

        assert_eq!(results.len(), 1);
        let (day, visitors) = &results[0];
        assert_eq!(day.hits, 0);
        assert_eq!(day.visitors, 0);
        assert!(visitors.is_empty());
    }
}
