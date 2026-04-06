use crate::config::SiteConfig;
use crate::model::{
    human_bot_pct, last_day_of_month, DayCache, Kpi, MonthReport, YearReport, TOP_PAGES_LIMIT,
};
use crate::query::QueryEngine;
use crate::svg::theme::GREY_ORANGE;
use crate::svg::SvgDoc;
use chrono::{Datelike, NaiveDate, Utc};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

/// Collects wall-clock timing measurements for a summary table.
pub struct Timings {
    entries: Vec<TimingEntry>,
}

struct TimingEntry {
    domain: String,
    phase: String,
    duration_ms: u128,
}

impl Timings {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn record(&mut self, domain: &str, phase: &str, duration: std::time::Duration) {
        self.entries.push(TimingEntry {
            domain: domain.to_string(),
            phase: phase.to_string(),
            duration_ms: duration.as_millis(),
        });
    }

    pub fn print_summary(&self) {
        if self.entries.is_empty() {
            return;
        }
        // Column widths
        let w_domain = self
            .entries
            .iter()
            .map(|e| e.domain.len())
            .max()
            .unwrap_or(6)
            .max(6);
        let w_phase = self
            .entries
            .iter()
            .map(|e| e.phase.len())
            .max()
            .unwrap_or(5)
            .max(5);

        let header = format!(
            "  {:<w_domain$}  {:<w_phase$}  {:>10}",
            "Domain", "Phase", "Duration"
        );
        let separator = format!("  {:-<w_domain$}  {:-<w_phase$}  {:->10}", "", "", "");

        let mut lines = vec![
            String::new(),
            "  Timing Summary".to_string(),
            separator.clone(),
            header,
            separator.clone(),
        ];

        for entry in &self.entries {
            let duration_str = if entry.duration_ms >= 1000 {
                format!("{:.1}s", entry.duration_ms as f64 / 1000.0)
            } else {
                format!("{}ms", entry.duration_ms)
            };
            lines.push(format!(
                "  {:<w_domain$}  {:<w_phase$}  {:>10}",
                entry.domain, entry.phase, duration_str
            ));
        }

        // Total
        let total_ms: u128 = self.entries.iter().map(|e| e.duration_ms).sum();
        let total_str = if total_ms >= 1000 {
            format!("{:.1}s", total_ms as f64 / 1000.0)
        } else {
            format!("{}ms", total_ms)
        };
        lines.push(separator);
        lines.push(format!(
            "  {:<w_domain$}  {:<w_phase$}  {:>10}",
            "", "Total", total_str
        ));
        lines.push(String::new());

        info!("{}", lines.join("\n"));
    }
}

/// All dates in a month up to today.
fn dates_in_month(month: &str) -> anyhow::Result<Vec<NaiveDate>> {
    let parts: Vec<&str> = month.split('-').collect();
    if parts.len() != 2 {
        anyhow::bail!("Invalid month format '{}', expected YYYY-MM", month);
    }
    let year: i32 = parts[0].parse()?;
    let month_num: u32 = parts[1].parse()?;
    let first_day = NaiveDate::from_ymd_opt(year, month_num, 1)
        .ok_or_else(|| anyhow::anyhow!("Invalid month: {}", month))?;
    let last = last_day_of_month(year, month_num);
    let today = Utc::now().date_naive();
    let end_date = if last < today { last } else { today };

    let mut dates = Vec::new();
    let mut date = first_day;
    while date <= end_date {
        dates.push(date);
        date = date.succ_opt().unwrap();
    }
    Ok(dates)
}

/// Scan the directory once and return all dates that have local parquet files.
fn cached_parquet_dates(raw_dir: &Path) -> std::collections::HashSet<NaiveDate> {
    std::fs::read_dir(raw_dir)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            if !name.ends_with(".parquet") {
                return None;
            }
            // Filenames are "YYYY-MM-DD_N.parquet"
            name.split('_').next()?.parse::<NaiveDate>().ok()
        })
        .collect()
}

/// Delete local parquet files for a given date.
fn delete_local_parquet(raw_dir: &Path, date: NaiveDate) {
    let prefix = format!("{}_", date);
    if let Ok(entries) = std::fs::read_dir(raw_dir) {
        for entry in entries.filter_map(|e| e.ok()) {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with(prefix.as_str()) && name_str.ends_with(".parquet") {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

/// Write visitor IPs to a Parquet file: columns (date Utf8, c_ip Utf8, is_bot Boolean).
fn write_visitor_parquet(
    path: &std::path::Path,
    date: &NaiveDate,
    visitors: &[(String, bool)],
) -> anyhow::Result<()> {
    use arrow::array::{BooleanArray, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    let date_str = date.to_string();
    let dates: Vec<&str> = visitors.iter().map(|_| date_str.as_str()).collect();
    let ips: Vec<&str> = visitors.iter().map(|(ip, _)| ip.as_str()).collect();
    let bots: Vec<bool> = visitors.iter().map(|(_, b)| *b).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("date", DataType::Utf8, false),
        Field::new("c_ip", DataType::Utf8, false),
        Field::new("is_bot", DataType::Boolean, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(dates)),
            Arc::new(StringArray::from(ips)),
            Arc::new(BooleanArray::from(bots)),
        ],
    )?;

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = std::fs::File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

/// Count unique visitors from visitor Parquet files in the given date range.
/// Returns (total_unique_visitors, bot_unique_visitors).
async fn count_unique_visitors(
    visitor_dir: &std::path::Path,
    date_from: NaiveDate,
    date_to: NaiveDate,
) -> anyhow::Result<(u64, u64)> {
    let visitor_dir = visitor_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        count_unique_visitors_sync(&visitor_dir, date_from, date_to)
    })
    .await?
}

fn count_unique_visitors_sync(
    visitor_dir: &std::path::Path,
    date_from: NaiveDate,
    date_to: NaiveDate,
) -> anyhow::Result<(u64, u64)> {
    use arrow::array::{BooleanArray, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use rayon::prelude::*;

    if !visitor_dir.exists() {
        return Ok((0, 0));
    }

    let files: Vec<PathBuf> = std::fs::read_dir(visitor_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            if !name.ends_with(".parquet") {
                return false;
            }
            let date_str = &name[..name.len() - 8];
            date_str
                .parse::<NaiveDate>()
                .map(|d| d >= date_from && d <= date_to)
                .unwrap_or(false)
        })
        .map(|e| e.path())
        .collect();

    let file_results: Vec<HashMap<String, bool>> = files
        .par_iter()
        .map(|path| {
            let file = std::fs::File::open(path)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
            let mut ips: HashMap<String, bool> = HashMap::new();
            for batch_result in reader {
                let batch = batch_result?;
                let ip_col = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let bot_col = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    let ip = ip_col.value(i).to_string();
                    let is_bot = bot_col.value(i);
                    let entry = ips.entry(ip).or_insert(false);
                    if is_bot {
                        *entry = true;
                    }
                }
            }
            Ok(ips)
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;

    let mut all_ips: HashMap<String, bool> = HashMap::new();
    for map in file_results {
        for (ip, is_bot) in map {
            let entry = all_ips.entry(ip).or_insert(false);
            if is_bot {
                *entry = true;
            }
        }
    }

    let total = all_ips.len() as u64;
    let bot = all_ips.values().filter(|&&b| b).count() as u64;
    Ok((total, bot))
}

/// Compare sitemap URLs against Googlebot-crawled URLs to find gaps.
fn find_sitemap_gaps(site: &SiteConfig, google_hits: &HashMap<String, u64>) -> Vec<String> {
    let Some(sitemap_path) = &site.sitemap else {
        return Vec::new();
    };
    match crate::sitemap::parse_sitemap(sitemap_path) {
        Ok(sitemap_urls) => sitemap_urls
            .into_iter()
            .filter(|url| {
                let path = url::Url::parse(url)
                    .map(|u| u.path().to_string())
                    .unwrap_or_else(|_| url.clone());
                !google_hits.contains_key(&path)
            })
            .collect(),
        Err(e) => {
            warn!(domain = %site.domain, error = %e, "Failed to parse sitemap");
            Vec::new()
        }
    }
}

/// Build the month SVG content (shared between --month and --year modes).
fn build_month_svg(
    site: &SiteConfig,
    month: &str,
    report: &MonthReport,
    missing_urls: &[String],
) -> String {
    let content_pages: Vec<_> = report
        .top_pages
        .iter()
        .filter(|p| p.category == "page")
        .take(TOP_PAGES_LIMIT)
        .cloned()
        .collect();
    let static_assets: Vec<_> = report
        .top_pages
        .iter()
        .filter(|p| p.category != "page")
        .cloned()
        .collect();

    let mut doc = SvgDoc::new(800.0, GREY_ORANGE);
    doc.add_section_title(&format!("{} / {}", site.domain, month));

    let (hits_human_pct, hits_bot_pct) = human_bot_pct(report.total_hits, report.total_bot_hits);
    let (vis_human_pct, vis_bot_pct) =
        human_bot_pct(report.total_visitors, report.total_bot_visitors);

    doc.add_kpi_cards(&[
        Kpi {
            label: "Total Hits".to_string(),
            value: report.total_hits.to_string(),
            change: Some(format!("{}% human · {}% bot", hits_human_pct, hits_bot_pct)),
        },
        Kpi {
            label: "Unique Visitors".to_string(),
            value: report.total_visitors.to_string(),
            change: Some(format!("{}% human · {}% bot", vis_human_pct, vis_bot_pct)),
        },
        Kpi {
            label: "Active Bots".to_string(),
            value: report.bot_stats.len().to_string(),
            change: None,
        },
    ]);

    doc.add_daily_traffic_section(&report.daily);
    doc.add_top_content_pages(&content_pages);
    doc.add_static_assets(&static_assets);
    doc.add_bot_activity_section(&report.bot_stats);

    if site.sitemap.is_some() {
        doc.add_crawl_gap_section(missing_urls);
    }

    doc.finalize()
}

/// Sync from S3 where needed, then scan all local parquet.
/// Today is always re-fetched. Past days are cached locally.
async fn sync_and_query_month(
    s3_client: &aws_sdk_s3::Client,
    site: &SiteConfig,
    month: &str,
    bots: &HashMap<String, String>,
    cache_dir: &Path,
    no_cache: bool,
    timings: &mut Timings,
) -> anyhow::Result<Vec<DayCache>> {
    let raw_dir = cache_dir.join("raw");
    let visitor_dir = cache_dir.join("visitors");
    let today = Utc::now().date_naive();
    let all_dates = dates_in_month(month)?;

    if all_dates.is_empty() {
        return Ok(Vec::new());
    }

    // Today's data is still being written — always re-fetch.
    // With --no-cache, re-fetch everything.
    let mut cached = cached_parquet_dates(&raw_dir);
    for &date in &all_dates {
        if date == today || no_cache {
            delete_local_parquet(&raw_dir, date);
            cached.remove(&date);
        }
    }

    // Sync dates that have no local parquet
    let dates_to_sync: Vec<NaiveDate> = all_dates
        .iter()
        .copied()
        .filter(|date| !cached.contains(date))
        .collect();

    info!(
        domain = %site.domain,
        month,
        total = all_dates.len(),
        to_sync = dates_to_sync.len(),
        cached = all_dates.len() - dates_to_sync.len(),
        "Month status"
    );

    if !dates_to_sync.is_empty() {
        let t = Instant::now();
        crate::query::sync_days_from_s3(s3_client, site, &dates_to_sync, &raw_dir).await?;
        let label = format!("S3 sync {}", month);
        timings.record(&site.domain, &label, t.elapsed());
    }

    // Scan all dates from local parquet
    let t = Instant::now();
    let engine = QueryEngine::new_local(&raw_dir)?;
    let bots_owned = bots.clone();
    let domain_owned = site.domain.clone();

    let results = tokio::task::spawn_blocking(move || {
        engine.query_days(&all_dates, &bots_owned, &domain_owned)
    })
    .await??;
    let label = format!("Parquet scan {}", month);
    timings.record(&site.domain, &label, t.elapsed());

    // Write visitor parquet only for newly synced dates
    let t = Instant::now();
    let synced: std::collections::HashSet<NaiveDate> = dates_to_sync.into_iter().collect();
    let mut all_days: Vec<DayCache> = Vec::new();
    for (day_cache, visitor_ips) in results {
        if synced.contains(&day_cache.date) {
            let parquet_file = visitor_dir.join(format!("{}.parquet", day_cache.date));
            write_visitor_parquet(&parquet_file, &day_cache.date, &visitor_ips)?;
        }
        all_days.push(day_cache);
    }
    let label = format!("Visitor write {}", month);
    timings.record(&site.domain, &label, t.elapsed());

    Ok(all_days)
}

pub async fn process_site(
    s3_client: &aws_sdk_s3::Client,
    site: &SiteConfig,
    month: &str,
    no_cache: bool,
    bots: &HashMap<String, String>,
    output_dir: &PathBuf,
    timings: &mut Timings,
    all_domains: &[String],
) -> anyhow::Result<()> {
    let cache_dir = PathBuf::from(".edgeview_cache").join(&site.domain);
    std::fs::create_dir_all(&cache_dir)?;

    let all_days =
        sync_and_query_month(s3_client, site, month, bots, &cache_dir, no_cache, timings).await?;

    // Compute exact visitor counts from Parquet
    let t = Instant::now();
    let visitor_dir = cache_dir.join("visitors");
    let parts: Vec<&str> = month.split('-').collect();
    let year: i32 = parts[0].parse()?;
    let month_num: u32 = parts[1].parse()?;
    let first_day = NaiveDate::from_ymd_opt(year, month_num, 1).unwrap();
    let last = last_day_of_month(year, month_num);
    let today = Utc::now().date_naive();
    let end_date = if last < today { last } else { today };

    let exact_visitors = if visitor_dir.exists() {
        let (v, bv) = count_unique_visitors(&visitor_dir, first_day, end_date).await?;
        info!(domain = %site.domain, month, visitors = v, bot_visitors = bv, "Exact visitor counts from Parquet");
        Some((v, bv))
    } else {
        None
    };
    timings.record(&site.domain, "Visitor count", t.elapsed());

    let report = MonthReport::from_day_caches(all_days, exact_visitors);

    // Optional Sitemap Analysis
    let missing_urls = find_sitemap_gaps(site, &report.google_hits);

    // Generate SVG + HTML
    let t = Instant::now();
    let output_file = output_dir.join(format!("{}-{}.svg", site.domain, month));
    info!(domain = %site.domain, path = %output_file.display(), "Generating SVG report");
    let svg_content = build_month_svg(site, month, &report, &missing_urls);
    std::fs::create_dir_all(output_dir)?;
    std::fs::write(&output_file, &svg_content)?;

    let html_file = output_dir.join(format!("{}-{}.html", site.domain, month));
    info!(domain = %site.domain, path = %html_file.display(), "Generating HTML report");
    let html_content = crate::html::generate_report(
        &site.domain,
        month,
        &svg_content,
        &report.daily_pages,
        &report.daily_hourly,
        &report.bot_stats,
        all_domains,
    );
    std::fs::write(&html_file, html_content)?;
    timings.record(&site.domain, "Reports", t.elapsed());

    Ok(())
}

/// Build year SVG report content.
fn build_year_svg(domain: &str, year: &str, year_report: &YearReport) -> String {
    let mut doc = SvgDoc::new(800.0, GREY_ORANGE);
    doc.add_section_title(&format!("{} / {}", domain, year));

    let (hits_human_pct, hits_bot_pct) =
        human_bot_pct(year_report.total_hits, year_report.total_bot_hits);
    let (vis_human_pct, vis_bot_pct) =
        human_bot_pct(year_report.total_visitors, year_report.total_bot_visitors);

    doc.add_kpi_cards(&[
        Kpi {
            label: "Total Hits".to_string(),
            value: year_report.total_hits.to_string(),
            change: Some(format!("{}% human · {}% bot", hits_human_pct, hits_bot_pct)),
        },
        Kpi {
            label: "Unique Visitors".to_string(),
            value: year_report.total_visitors.to_string(),
            change: Some(format!("{}% human · {}% bot", vis_human_pct, vis_bot_pct)),
        },
        Kpi {
            label: "Active Bots".to_string(),
            value: year_report.bot_stats.len().to_string(),
            change: None,
        },
    ]);

    doc.add_monthly_traffic_section(&year_report.monthly);

    let content_pages: Vec<_> = year_report
        .top_pages
        .iter()
        .filter(|p| p.category == "page")
        .take(TOP_PAGES_LIMIT)
        .cloned()
        .collect();
    let static_assets: Vec<_> = year_report
        .top_pages
        .iter()
        .filter(|p| p.category != "page")
        .cloned()
        .collect();

    doc.add_top_content_pages(&content_pages);
    doc.add_static_assets(&static_assets);
    doc.add_bot_activity_section(&year_report.bot_stats);

    doc.finalize()
}

/// Build the MonthHtmlData for the year HTML dashboard from a MonthReport.
fn build_month_html_data(month_str: &str, report: &MonthReport) -> crate::html::MonthHtmlData {
    let summary = crate::model::MonthSummary::from_month_report(report, month_str);

    let mut dates: Vec<&String> = report.daily_pages.keys().collect();
    dates.sort();
    let day_data: Vec<crate::html::DayHtmlData> = dates
        .into_iter()
        .map(|date_str| {
            let pages = report.daily_pages[date_str].clone();
            let hourly = report
                .daily_hourly
                .get(date_str.as_str())
                .cloned()
                .unwrap_or_default();
            crate::html::DayHtmlData {
                date: date_str.clone(),
                hits: pages.iter().map(|p| p.hits).sum(),
                visitors: pages.iter().map(|p| p.visitors).sum(),
                bot_hits: pages.iter().map(|p| p.bot_hits).sum(),
                pages,
                hourly,
                bot_stats: report.bot_stats.clone(),
                referer_stats: Vec::new(),
            }
        })
        .collect();

    crate::html::MonthHtmlData {
        month: month_str.to_string(),
        summary,
        days: day_data,
    }
}

/// Scan output directory for existing year report HTML files for this domain.
/// Returns a sorted list of years (always includes the current year being generated).
fn discover_available_years(output_dir: &Path, domain: &str, current_year: i32) -> Vec<i32> {
    let prefix = format!("{}-", domain);
    let mut years: std::collections::BTreeSet<i32> = std::collections::BTreeSet::new();
    years.insert(current_year);
    if let Ok(entries) = std::fs::read_dir(output_dir) {
        for entry in entries.filter_map(|e| e.ok()) {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(rest) = name.strip_prefix(&prefix) {
                // Match "{domain}-YYYY.html" but not "{domain}-YYYY-MM.html"
                if let Some(year_str) = rest.strip_suffix(".html") {
                    if year_str.len() == 4 {
                        if let Ok(y) = year_str.parse::<i32>() {
                            years.insert(y);
                        }
                    }
                }
            }
        }
    }
    years.into_iter().collect()
}

pub async fn process_site_year(
    s3_client: &aws_sdk_s3::Client,
    site: &SiteConfig,
    year: &str,
    no_cache: bool,
    bots: &HashMap<String, String>,
    output_dir: &PathBuf,
    timings: &mut Timings,
    all_domains: &[String],
) -> anyhow::Result<()> {
    let cache_dir = PathBuf::from(".edgeview_cache").join(&site.domain);
    std::fs::create_dir_all(&cache_dir)?;

    let year_num: i32 = year.parse()?;
    let today = Utc::now().date_naive();
    let max_month = if year_num == today.year() {
        today.month()
    } else {
        12
    };

    let mut month_data: Vec<(String, MonthReport)> = Vec::new();
    let mut month_html_data: Vec<crate::html::MonthHtmlData> = Vec::new();
    let visitor_dir = cache_dir.join("visitors");

    std::fs::create_dir_all(output_dir)?;

    for m in 1..=max_month {
        let month_str = format!("{}-{:02}", year, m);
        let all_days = sync_and_query_month(
            s3_client, site, &month_str, bots, &cache_dir, no_cache, timings,
        )
        .await?;

        if all_days.is_empty() {
            warn!(domain = %site.domain, month = %month_str, "No data for month, skipping");
            continue;
        }

        // Compute exact visitors for this month from Parquet
        let t = Instant::now();
        let first_day = NaiveDate::from_ymd_opt(year_num, m, 1).unwrap();
        let last = last_day_of_month(year_num, m);
        let end_date = if last < today { last } else { today };

        let exact_visitors = if visitor_dir.exists() {
            let (v, bv) = count_unique_visitors(&visitor_dir, first_day, end_date).await?;
            Some((v, bv))
        } else {
            None
        };
        let label = format!("Visitor count {}", month_str);
        timings.record(&site.domain, &label, t.elapsed());

        let report = MonthReport::from_day_caches(all_days, exact_visitors);
        let missing_urls = find_sitemap_gaps(site, &report.google_hits);

        // Generate month SVG + HTML
        let t = Instant::now();
        let svg_content = build_month_svg(site, &month_str, &report, &missing_urls);
        std::fs::write(
            output_dir.join(format!("{}-{}.svg", site.domain, month_str)),
            &svg_content,
        )?;

        let html_file = output_dir.join(format!("{}-{}.html", site.domain, month_str));
        info!(domain = %site.domain, month = %month_str, path = %html_file.display(), "Generating month HTML report");
        let html_content = crate::html::generate_report(
            &site.domain,
            &month_str,
            &svg_content,
            &report.daily_pages,
            &report.daily_hourly,
            &report.bot_stats,
            all_domains,
        );
        std::fs::write(&html_file, html_content)?;

        let label = format!("Reports {}", month_str);
        timings.record(&site.domain, &label, t.elapsed());

        month_html_data.push(build_month_html_data(&month_str, &report));
        month_data.push((month_str, report));
    }

    if month_data.is_empty() {
        warn!(domain = %site.domain, year, "No data for any month in year");
        return Ok(());
    }

    // Compute exact year-level visitors from Parquet
    let t = Instant::now();
    let first_day = NaiveDate::from_ymd_opt(year_num, 1, 1).unwrap();
    let last_day = NaiveDate::from_ymd_opt(year_num, 12, 31).unwrap();
    let end_date = if last_day < today { last_day } else { today };

    let (year_visitors, year_bot_visitors) = if visitor_dir.exists() {
        count_unique_visitors(&visitor_dir, first_day, end_date).await?
    } else {
        let v: u64 = month_data.iter().map(|(_, r)| r.total_visitors).sum();
        let bv: u64 = month_data.iter().map(|(_, r)| r.total_bot_visitors).sum();
        (v, bv)
    };
    info!(domain = %site.domain, year, visitors = year_visitors, bot_visitors = year_bot_visitors, "Year-level visitor counts");
    timings.record(&site.domain, "Visitor count year", t.elapsed());

    let year_report =
        YearReport::from_month_data(year, month_data, (year_visitors, year_bot_visitors));

    // Generate year SVG + HTML
    let t = Instant::now();
    let year_svg = build_year_svg(&site.domain, year, &year_report);
    std::fs::write(
        output_dir.join(format!("{}-{}.svg", site.domain, year)),
        &year_svg,
    )?;

    let html_file = output_dir.join(format!("{}-{}.html", site.domain, year));
    info!(domain = %site.domain, year, path = %html_file.display(), "Generating year HTML report");
    let available_years = discover_available_years(output_dir, &site.domain, year_num);
    let html_content = crate::html::generate_year_report(
        &site.domain,
        year,
        &year_report,
        &month_html_data,
        all_domains,
        &available_years,
    );
    std::fs::write(&html_file, html_content)?;
    timings.record(&site.domain, "Reports year", t.elapsed());

    Ok(())
}
