use crate::config::SiteConfig;
use crate::pipeline::Timings;
use crate::query::{list_s3_objects, S3_CONCURRENCY};
use arrow::array::RecordBatch;
use arrow::compute::{concat_batches, lexsort_to_indices, take, SortColumn, SortOptions};
use futures::stream::{self, StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use time::{Date, OffsetDateTime};
use tracing::{info, warn};

/// Timeout for downloading a full S3 object (all 39 columns).
const FULL_DOWNLOAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Maximum retries for a single S3 download.
const MAX_RETRIES: u32 = 3;

/// Sort columns for optimal parquet compression (low cardinality first).
const SORT_COLUMNS: &[&str] = &[
    "date",
    "x_edge_location",
    "sc_status",
    "cs_method",
    "cs_uri_stem",
    "time",
];

pub struct CompactConfig {
    pub dry_run: bool,
    pub force: bool,
}

struct CompactDayResult {
    raw_files: usize,
    raw_bytes: u64,
    compact_bytes: u64,
    rows: u64,
    skipped: bool,
}

pub(crate) fn compact_s3_prefix(raw_s3_path: &str) -> String {
    let trimmed = raw_s3_path.trim_end_matches('/');
    if let Some(base) = trimmed.strip_suffix("/raw") {
        format!("{}/compact", base)
    } else {
        format!("{}/compact", trimmed)
    }
}

pub(crate) fn compact_key(compact_prefix: &str, date: Date) -> String {
    format!(
        "{}/{}/{:02}/{:02}.parquet",
        compact_prefix,
        date.year(),
        u8::from(date.month()),
        date.day()
    )
}

async fn compact_exists(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> anyhow::Result<bool> {
    match client.head_object().bucket(bucket).key(key).send().await {
        Ok(_) => Ok(true),
        Err(e) => {
            let svc = e.into_service_error();
            if svc.is_not_found() {
                Ok(false)
            } else {
                Err(anyhow::anyhow!("HEAD {} failed: {}", key, svc))
            }
        }
    }
}

/// Download all columns from all raw files for a single day.
async fn download_full_day(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    s3_prefix: &str,
    date: Date,
) -> anyhow::Result<(Vec<RecordBatch>, usize, u64)> {
    let prefix = format!(
        "{}/{}/{:02}/{:02}/",
        s3_prefix,
        date.year(),
        u8::from(date.month()),
        date.day()
    );
    let objects = list_s3_objects(client, bucket, &prefix).await?;
    let file_count = objects.len();
    let total_bytes: u64 = objects.iter().map(|o| o.size.unwrap_or(0) as u64).sum();

    if objects.is_empty() {
        return Ok((Vec::new(), 0, 0));
    }

    // Download full objects with retry — avoids range-read timeout issues
    let batches: Vec<Vec<RecordBatch>> = stream::iter(objects.into_iter().map(|obj| {
        let client = client.clone();
        let bucket = bucket.to_string();
        async move {
            let key = obj.key.as_deref().unwrap_or_default().to_string();
            let mut last_err = None;
            for attempt in 0..MAX_RETRIES {
                if attempt > 0 {
                    let delay = std::time::Duration::from_secs(2u64.pow(attempt));
                    warn!(key = %key, attempt, delay_s = delay.as_secs(), "Retrying S3 download");
                    tokio::time::sleep(delay).await;
                }
                let fut = async {
                    let resp = client.get_object().bucket(&bucket).key(&key).send().await?;
                    let bytes = resp.body.collect().await?.into_bytes();
                    Ok::<_, anyhow::Error>(bytes)
                };
                match tokio::time::timeout(FULL_DOWNLOAD_TIMEOUT, fut).await {
                    Ok(Ok(bytes)) => {
                        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
                        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
                        return Ok::<Vec<RecordBatch>, anyhow::Error>(batches);
                    }
                    Ok(Err(e)) => last_err = Some(e),
                    Err(_) => {
                        last_err = Some(anyhow::anyhow!(
                            "S3 download timed out after {}s for {}/{}",
                            FULL_DOWNLOAD_TIMEOUT.as_secs(),
                            bucket,
                            key
                        ))
                    }
                }
            }
            Err(last_err.unwrap())
        }
    }))
    .buffer_unordered(S3_CONCURRENCY)
    .try_collect()
    .await?;

    let all_batches: Vec<RecordBatch> = batches.into_iter().flatten().collect();
    Ok((all_batches, file_count, total_bytes))
}

/// Concatenate and sort batches by the predefined sort columns.
fn sort_batches(batches: Vec<RecordBatch>) -> anyhow::Result<RecordBatch> {
    let schema = batches[0].schema();
    let combined = concat_batches(&schema, &batches)?;

    let sort_cols: Vec<SortColumn> = SORT_COLUMNS
        .iter()
        .filter_map(|&name| {
            combined.schema().index_of(name).ok().map(|idx| SortColumn {
                values: combined.column(idx).clone(),
                options: Some(SortOptions::default()),
            })
        })
        .collect();

    if sort_cols.is_empty() {
        return Ok(combined);
    }

    let indices = lexsort_to_indices(&sort_cols, None)?;
    let sorted_columns: Vec<Arc<dyn arrow::array::Array>> = (0..combined.num_columns())
        .map(|i| take(combined.column(i), &indices, None))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_new(schema, sorted_columns)?)
}

/// Write sorted data to a temp file with optimal compression settings.
fn write_compact_parquet(batch: &RecordBatch, path: &std::path::Path) -> anyhow::Result<u64> {
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(parquet::basic::ZstdLevel::try_new(3)?))
        .set_max_row_group_size(1_048_576)
        .set_data_page_size_limit(1_048_576)
        .set_dictionary_enabled(true)
        .build();

    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;

    Ok(std::fs::metadata(path)?.len())
}

/// Upload a local file to S3.
async fn upload_to_s3(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    path: &std::path::Path,
) -> anyhow::Result<()> {
    let body = aws_sdk_s3::primitives::ByteStream::from_path(path).await?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await?;
    Ok(())
}

/// Compact a single day: download all columns, sort, write, upload.
async fn compact_day(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    s3_prefix: &str,
    compact_prefix: &str,
    date: Date,
    config: &CompactConfig,
) -> anyhow::Result<CompactDayResult> {
    let key = compact_key(compact_prefix, date);

    // Idempotency check
    if !config.force && compact_exists(client, bucket, &key).await? {
        info!(date = %date, key = %key, "Already compacted, skipping");
        return Ok(CompactDayResult {
            raw_files: 0,
            raw_bytes: 0,
            compact_bytes: 0,
            rows: 0,
            skipped: true,
        });
    }

    let (batches, file_count, raw_bytes) =
        download_full_day(client, bucket, s3_prefix, date).await?;

    if batches.is_empty() {
        info!(date = %date, "No raw files, skipping");
        return Ok(CompactDayResult {
            raw_files: 0,
            raw_bytes: 0,
            compact_bytes: 0,
            rows: 0,
            skipped: true,
        });
    }

    let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

    if config.dry_run {
        info!(
            date = %date,
            files = file_count,
            raw_bytes,
            rows = total_rows,
            "Would compact (dry run)"
        );
        return Ok(CompactDayResult {
            raw_files: file_count,
            raw_bytes,
            compact_bytes: 0,
            rows: total_rows,
            skipped: false,
        });
    }

    // Sort
    let sorted = tokio::task::spawn_blocking(move || sort_batches(batches)).await??;

    // Write to temp file
    let tmp_dir = tempfile::tempdir()?;
    let tmp_path = tmp_dir.path().join("compact.parquet");
    let compact_bytes = {
        let tmp = tmp_path.clone();
        tokio::task::spawn_blocking(move || write_compact_parquet(&sorted, &tmp)).await??
    };

    // Upload
    upload_to_s3(client, bucket, &key, &tmp_path).await?;

    info!(
        date = %date,
        files = file_count,
        rows = total_rows,
        raw_bytes,
        compact_bytes,
        ratio = format_args!("{:.1}x", raw_bytes as f64 / compact_bytes.max(1) as f64),
        key = %key,
        "Compacted"
    );

    Ok(CompactDayResult {
        raw_files: file_count,
        raw_bytes,
        compact_bytes,
        rows: total_rows,
        skipped: false,
    })
}

/// Compact all dates for a site within a date range.
pub async fn compact_site(
    s3_client: &aws_sdk_s3::Client,
    site: &SiteConfig,
    dates: Vec<Date>,
    config: &CompactConfig,
    timings: &mut Timings,
) -> anyhow::Result<()> {
    let (bucket, s3_prefix) = site.s3_bucket_and_prefix()?;
    let compact_prefix = compact_s3_prefix(&s3_prefix);

    let today = OffsetDateTime::now_utc().date();
    let dates: Vec<Date> = dates.into_iter().filter(|&d| d < today).collect();

    if dates.is_empty() {
        warn!(domain = %site.domain, "No dates to compact (today is excluded)");
        return Ok(());
    }

    info!(
        domain = %site.domain,
        dates = dates.len(),
        compact_prefix = %compact_prefix,
        dry_run = config.dry_run,
        "Starting compaction"
    );

    let t = std::time::Instant::now();
    let mut total_raw_bytes: u64 = 0;
    let mut total_compact_bytes: u64 = 0;
    let mut total_files: usize = 0;
    let mut total_rows: u64 = 0;
    let mut compacted_days: usize = 0;
    let mut skipped_days: usize = 0;

    // Process days sequentially to limit memory usage
    for date in &dates {
        let result = compact_day(
            s3_client,
            &bucket,
            &s3_prefix,
            &compact_prefix,
            *date,
            config,
        )
        .await?;

        if result.skipped {
            skipped_days += 1;
        } else {
            compacted_days += 1;
            total_raw_bytes += result.raw_bytes;
            total_compact_bytes += result.compact_bytes;
            total_files += result.raw_files;
            total_rows += result.rows;
        }
    }

    let label = if config.dry_run {
        "Compact (dry run)".to_string()
    } else {
        "Compact".to_string()
    };
    timings.record(&site.domain, &label, t.elapsed());

    info!(
        domain = %site.domain,
        compacted_days,
        skipped_days,
        total_files,
        total_rows,
        raw_bytes = total_raw_bytes,
        compact_bytes = total_compact_bytes,
        "Compaction complete"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compact_s3_prefix_strips_raw() {
        assert_eq!(compact_s3_prefix("dev.l1x.be/raw"), "dev.l1x.be/compact");
        assert_eq!(compact_s3_prefix("dev.l1x.be/raw/"), "dev.l1x.be/compact");
    }

    #[test]
    fn test_compact_s3_prefix_appends_compact() {
        assert_eq!(
            compact_s3_prefix("dev.l1x.be/logs"),
            "dev.l1x.be/logs/compact"
        );
    }

    #[test]
    fn test_compact_key_format() {
        let date = time::macros::date!(2026 - 03 - 01);
        assert_eq!(
            compact_key("dev.l1x.be/compact", date),
            "dev.l1x.be/compact/2026/03/01.parquet"
        );
    }

    #[test]
    fn test_compact_key_round_trip() {
        let date = time::macros::date!(2026 - 12 - 25);
        let prefix = compact_s3_prefix("dev.l1x.be/raw");
        assert_eq!(
            compact_key(&prefix, date),
            "dev.l1x.be/compact/2026/12/25.parquet"
        );
    }
}
