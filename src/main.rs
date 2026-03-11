mod config;
mod model;
mod query;
mod svg;
mod classify;
mod html;
mod sitemap;

use std::path::PathBuf;
use std::sync::Arc;
use std::collections::HashMap;
use chrono::{Datelike, NaiveDate, Utc};
use clap::Parser;
use tracing::{info, warn, error};
use crate::config::{Config, SiteConfig};
use crate::query::QueryEngine;
use crate::svg::SvgDoc;
use crate::svg::theme::GREY_ORANGE;
use crate::model::{DayCache, Kpi, MonthReport, YearReport};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config: Option<PathBuf>,

    #[arg(short, long)]
    month: Option<String>,

    #[arg(short, long)]
    year: Option<String>,

    #[arg(long, default_value_t = false)]
    no_cache: bool,
}

/// Determine which dates in the given month need querying.
/// Past days with existing cache files are skipped. Today is always re-queried.
fn dates_to_query(month: &str, cache_dir: &PathBuf, no_cache: bool) -> anyhow::Result<(Vec<NaiveDate>, Vec<NaiveDate>)> {
    let parts: Vec<&str> = month.split('-').collect();
    if parts.len() != 2 {
        anyhow::bail!("Invalid month format '{}', expected YYYY-MM", month);
    }
    let year: i32 = parts[0].parse()?;
    let month_num: u32 = parts[1].parse()?;

    let first_day = NaiveDate::from_ymd_opt(year, month_num, 1)
        .ok_or_else(|| anyhow::anyhow!("Invalid month: {}", month))?;
    let last_day_of_month = if month_num == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap().pred_opt().unwrap()
    } else {
        NaiveDate::from_ymd_opt(year, month_num + 1, 1).unwrap().pred_opt().unwrap()
    };
    let today = Utc::now().date_naive();
    let end_date = if last_day_of_month < today { last_day_of_month } else { today };

    let mut to_query = Vec::new();
    let mut cached = Vec::new();

    let mut date = first_day;
    while date <= end_date {
        let cache_file = cache_dir.join(format!("{}.json", date));
        if no_cache || date == today || !cache_file.exists() {
            to_query.push(date);
        } else {
            cached.push(date);
        }
        date = date.succ_opt().unwrap();
    }

    Ok((to_query, cached))
}

/// Write visitor IPs to a Parquet file: columns (date Utf8, c_ip Utf8, is_bot Boolean).
fn write_visitor_parquet(path: &std::path::Path, date: &NaiveDate, visitors: &[(String, bool)]) -> anyhow::Result<()> {
    use datafusion::arrow::array::{BooleanArray, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
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
async fn count_unique_visitors(visitor_dir: &std::path::Path, date_from: NaiveDate, date_to: NaiveDate) -> anyhow::Result<(u64, u64)> {
    use datafusion::prelude::*;
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::array::Int64Array;

    if !visitor_dir.exists() {
        return Ok((0, 0));
    }

    let ctx = SessionContext::new();
    let abs_path = std::fs::canonicalize(visitor_dir)?;
    let table_path = format!("{}/", abs_path.to_string_lossy());
    let table_url = ListingTableUrl::parse(&table_path)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("date", DataType::Utf8, false),
        Field::new("c_ip", DataType::Utf8, false),
        Field::new("is_bot", DataType::Boolean, false),
    ]));

    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension(".parquet");

    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        .with_schema(schema);

    let table = ListingTable::try_new(config)?;
    ctx.register_table("visitors", Arc::new(table))?;

    let sql = format!(
        "SELECT COUNT(DISTINCT c_ip) as total_visitors \
         FROM visitors \
         WHERE date >= '{}' AND date <= '{}'",
        date_from, date_to
    );
    let df = ctx.sql(&sql).await?;
    let batches = df.collect().await?;
    let total = if let Some(batch) = batches.first() {
        if batch.num_rows() > 0 {
            batch.column(0).as_any().downcast_ref::<Int64Array>()
                .map(|a| a.value(0) as u64)
                .unwrap_or(0)
        } else { 0 }
    } else { 0 };

    let sql_bot = format!(
        "SELECT COUNT(DISTINCT c_ip) as bot_visitors \
         FROM visitors \
         WHERE date >= '{}' AND date <= '{}' AND is_bot = true",
        date_from, date_to
    );
    let df = ctx.sql(&sql_bot).await?;
    let batches = df.collect().await?;
    let bot = if let Some(batch) = batches.first() {
        if batch.num_rows() > 0 {
            batch.column(0).as_any().downcast_ref::<Int64Array>()
                .map(|a| a.value(0) as u64)
                .unwrap_or(0)
        } else { 0 }
    } else { 0 };

    Ok((total, bot))
}

/// Build the month SVG content (shared between --month and --year modes).
fn build_month_svg(site: &SiteConfig, month: &str, report: &MonthReport, missing_urls: &[String]) -> String {
    let content_pages: Vec<_> = report.top_pages.iter()
        .filter(|p| p.category == "article" || p.category == "page")
        .take(15)
        .cloned()
        .collect();
    let static_assets: Vec<_> = report.top_pages.iter()
        .filter(|p| p.category == "static")
        .cloned()
        .collect();

    let mut doc = SvgDoc::new(800.0, GREY_ORANGE);
    doc.add_section_title(&format!("{} / {}", site.domain, month));

    let hits_bot_pct = if report.total_hits > 0 { (report.total_bot_hits * 100) / report.total_hits } else { 0 };
    let hits_human_pct = 100 - hits_bot_pct;
    let vis_bot_pct = if report.total_visitors > 0 { (report.total_bot_visitors * 100) / report.total_visitors } else { 0 };
    let vis_human_pct = 100 - vis_bot_pct;

    doc.add_kpi_cards(&[
        Kpi { label: "Total Hits".to_string(), value: report.total_hits.to_string(), change: Some(format!("{}% human · {}% bot", hits_human_pct, hits_bot_pct)) },
        Kpi { label: "Unique Visitors".to_string(), value: report.total_visitors.to_string(), change: Some(format!("{}% human · {}% bot", vis_human_pct, vis_bot_pct)) },
        Kpi { label: "Active Bots".to_string(), value: report.bot_stats.len().to_string(), change: None },
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

/// Sync uncached days from S3 to local narrow parquet, then query locally.
/// Returns all DayCaches for the month.
async fn sync_and_query_month(
    site: &SiteConfig,
    month: &str,
    bots: &HashMap<String, String>,
    default_region: &str,
    cache_dir: &PathBuf,
    no_cache: bool,
) -> anyhow::Result<Vec<DayCache>> {
    let raw_dir = cache_dir.join("raw");
    let visitor_dir = cache_dir.join("visitors");
    let (to_query, cached_dates) = dates_to_query(month, cache_dir, no_cache)?;

    info!(
        domain = %site.domain,
        month,
        to_query = to_query.len(),
        cached = cached_dates.len(),
        "Per-day cache status"
    );

    // Load cached DayCaches
    let mut all_days: Vec<DayCache> = Vec::new();
    for date in &cached_dates {
        let cache_file = cache_dir.join(format!("{}.json", date));
        let content = std::fs::read_to_string(&cache_file)?;
        let day_cache: DayCache = serde_json::from_str(&content)?;
        all_days.push(day_cache);
    }

    if to_query.is_empty() {
        return Ok(all_days);
    }

    // Phase 1: Sync — download narrow parquet from S3 (1 GET per file)
    for date in &to_query {
        info!(domain = %site.domain, %date, "Syncing raw logs from S3");
        crate::query::sync_day_from_s3(site, default_region, *date, &raw_dir).await?;
    }

    // Phase 2: Query — all queries run against local parquet
    let engine = QueryEngine::new_local(&raw_dir)?;

    for date in &to_query {
        info!(domain = %site.domain, %date, "Querying day (local)");
        let (day_cache, visitor_ips) = engine.query_day(*date, bots).await?;

        // Write DayCache JSON
        let cache_file = cache_dir.join(format!("{}.json", date));
        let json = serde_json::to_string_pretty(&day_cache)?;
        std::fs::write(&cache_file, json)?;

        // Write visitor Parquet
        let parquet_file = visitor_dir.join(format!("{}.parquet", date));
        write_visitor_parquet(&parquet_file, date, &visitor_ips)?;
        info!(domain = %site.domain, %date, visitors = visitor_ips.len(), "Wrote visitor parquet");

        all_days.push(day_cache);
    }

    Ok(all_days)
}

async fn process_site(
    site: &SiteConfig,
    month: &str,
    no_cache: bool,
    bots: &HashMap<String, String>,
    default_region: &str,
    output_dir: &PathBuf,
) -> anyhow::Result<()> {
    let cache_dir = PathBuf::from(".edgeview_cache").join(&site.domain);
    std::fs::create_dir_all(&cache_dir)?;

    // Migration: remove old month-level cache file if present
    let old_cache_file = cache_dir.join(format!("{}.json", month));
    if old_cache_file.exists() {
        if let Ok(content) = std::fs::read_to_string(&old_cache_file) {
            if content.contains("\"total_hits\"") {
                warn!(domain = %site.domain, "Removing old month-level cache file: {}", old_cache_file.display());
                let _ = std::fs::remove_file(&old_cache_file);
            }
        }
    }

    let all_days = sync_and_query_month(site, month, bots, default_region, &cache_dir, no_cache).await?;

    // Compute exact visitor counts from Parquet
    let visitor_dir = cache_dir.join("visitors");
    let parts: Vec<&str> = month.split('-').collect();
    let year: i32 = parts[0].parse()?;
    let month_num: u32 = parts[1].parse()?;
    let first_day = NaiveDate::from_ymd_opt(year, month_num, 1).unwrap();
    let last_day = if month_num == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap().pred_opt().unwrap()
    } else {
        NaiveDate::from_ymd_opt(year, month_num + 1, 1).unwrap().pred_opt().unwrap()
    };
    let today = Utc::now().date_naive();
    let end_date = if last_day < today { last_day } else { today };

    let exact_visitors = if visitor_dir.exists() {
        let (v, bv) = count_unique_visitors(&visitor_dir, first_day, end_date).await?;
        info!(domain = %site.domain, month, visitors = v, bot_visitors = bv, "Exact visitor counts from Parquet");
        Some((v, bv))
    } else {
        None
    };

    let report = MonthReport::from_day_caches(all_days, exact_visitors);

    // Optional Sitemap Analysis
    let mut missing_urls: Vec<String> = Vec::new();
    if let Some(sitemap_path) = &site.sitemap {
        info!(domain = %site.domain, "Parsing sitemap and analyzing indexing gaps");
        let sitemap_urls = crate::sitemap::parse_sitemap(sitemap_path)?;
        for url in sitemap_urls {
            let path = url::Url::parse(&url).map(|u| u.path().to_string()).unwrap_or(url.clone());
            if !report.google_hits.contains_key(&path) {
                missing_urls.push(url.clone());
            }
        }
    }

    // Generate SVG
    let output_file = output_dir.join(format!("{}-{}.svg", site.domain, month));
    info!(domain = %site.domain, path = %output_file.display(), "Generating SVG report");
    let svg_content = build_month_svg(site, month, &report, &missing_urls);
    std::fs::create_dir_all(output_dir)?;
    std::fs::write(&output_file, &svg_content)?;

    // Generate HTML report with month + daily tabs
    let html_file = output_dir.join(format!("{}-{}.html", site.domain, month));
    info!(domain = %site.domain, path = %html_file.display(), "Generating HTML report");
    let html_content = crate::html::generate_report(
        &site.domain,
        month,
        &svg_content,
        &report.daily_pages,
        &report.daily_hourly,
        &report.bot_stats,
    );
    std::fs::write(&html_file, html_content)?;

    Ok(())
}

async fn process_site_year(
    site: &SiteConfig,
    year: &str,
    no_cache: bool,
    bots: &HashMap<String, String>,
    default_region: &str,
    output_dir: &PathBuf,
) -> anyhow::Result<()> {
    let cache_dir = PathBuf::from(".edgeview_cache").join(&site.domain);
    std::fs::create_dir_all(&cache_dir)?;

    let year_num: i32 = year.parse()?;
    let today = Utc::now().date_naive();
    let max_month = if year_num == today.year() { today.month() } else { 12 };

    let mut all_months: Vec<String> = Vec::new();
    for m in 1..=max_month {
        all_months.push(format!("{}-{:02}", year, m));
    }

    // Build MonthReport + SVG + HTML for each month
    let mut month_data: Vec<(String, MonthReport)> = Vec::new();
    let mut month_svgs: Vec<(String, String)> = Vec::new();
    let visitor_dir = cache_dir.join("visitors");

    for month_str in &all_months {
        let all_days = sync_and_query_month(site, month_str, bots, default_region, &cache_dir, no_cache).await?;

        if all_days.is_empty() {
            continue;
        }

        // Compute exact visitors for this month from Parquet
        let parts: Vec<&str> = month_str.split('-').collect();
        let y: i32 = parts[0].parse()?;
        let mn: u32 = parts[1].parse()?;
        let first_day = NaiveDate::from_ymd_opt(y, mn, 1).unwrap();
        let last_day = if mn == 12 {
            NaiveDate::from_ymd_opt(y + 1, 1, 1).unwrap().pred_opt().unwrap()
        } else {
            NaiveDate::from_ymd_opt(y, mn + 1, 1).unwrap().pred_opt().unwrap()
        };
        let end_date = if last_day < today { last_day } else { today };

        let exact_visitors = if visitor_dir.exists() {
            let (v, bv) = count_unique_visitors(&visitor_dir, first_day, end_date).await?;
            Some((v, bv))
        } else {
            None
        };

        let report = MonthReport::from_day_caches(all_days, exact_visitors);

        // Optional Sitemap Analysis
        let mut missing_urls: Vec<String> = Vec::new();
        if let Some(sitemap_path) = &site.sitemap {
            let sitemap_urls = crate::sitemap::parse_sitemap(sitemap_path)?;
            for url in sitemap_urls {
                let path = url::Url::parse(&url).map(|u| u.path().to_string()).unwrap_or(url.clone());
                if !report.google_hits.contains_key(&path) {
                    missing_urls.push(url.clone());
                }
            }
        }

        // Generate month SVG + HTML
        let svg_content = build_month_svg(site, month_str, &report, &missing_urls);

        std::fs::create_dir_all(output_dir)?;
        let svg_file = output_dir.join(format!("{}-{}.svg", site.domain, month_str));
        std::fs::write(&svg_file, &svg_content)?;

        let html_file = output_dir.join(format!("{}-{}.html", site.domain, month_str));
        info!(domain = %site.domain, month = %month_str, path = %html_file.display(), "Generating month HTML report");
        let html_content = crate::html::generate_report(
            &site.domain,
            month_str,
            &svg_content,
            &report.daily_pages,
            &report.daily_hourly,
            &report.bot_stats,
        );
        std::fs::write(&html_file, html_content)?;

        // Build month summary SVG for year tabs
        let summary = crate::model::MonthSummary::from_month_report(&report, month_str);
        let summary_svg = crate::html::build_month_summary_svg(&site.domain, month_str, &summary);
        month_svgs.push((month_str.clone(), summary_svg));

        month_data.push((month_str.clone(), report));
    }

    if month_data.is_empty() {
        warn!(domain = %site.domain, year, "No data for any month in year");
        return Ok(());
    }

    // Compute exact year-level visitors from Parquet
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

    let year_report = YearReport::from_month_data(year, month_data, (year_visitors, year_bot_visitors));

    // Generate year SVG
    let mut doc = SvgDoc::new(800.0, GREY_ORANGE);
    doc.add_section_title(&format!("{} / {}", site.domain, year));

    let hits_bot_pct = if year_report.total_hits > 0 { (year_report.total_bot_hits * 100) / year_report.total_hits } else { 0 };
    let hits_human_pct = 100 - hits_bot_pct;
    let vis_bot_pct = if year_report.total_visitors > 0 { (year_report.total_bot_visitors * 100) / year_report.total_visitors } else { 0 };
    let vis_human_pct = 100 - vis_bot_pct;

    doc.add_kpi_cards(&[
        Kpi { label: "Total Hits".to_string(), value: year_report.total_hits.to_string(), change: Some(format!("{}% human · {}% bot", hits_human_pct, hits_bot_pct)) },
        Kpi { label: "Unique Visitors".to_string(), value: year_report.total_visitors.to_string(), change: Some(format!("{}% human · {}% bot", vis_human_pct, vis_bot_pct)) },
        Kpi { label: "Active Bots".to_string(), value: year_report.bot_stats.len().to_string(), change: None },
    ]);

    doc.add_monthly_traffic_section(&year_report.monthly);

    let content_pages: Vec<_> = year_report.top_pages.iter()
        .filter(|p| p.category == "article" || p.category == "page")
        .take(15)
        .cloned()
        .collect();
    let static_assets: Vec<_> = year_report.top_pages.iter()
        .filter(|p| p.category == "static")
        .cloned()
        .collect();

    doc.add_top_content_pages(&content_pages);
    doc.add_static_assets(&static_assets);
    doc.add_bot_activity_section(&year_report.bot_stats);

    let year_svg = doc.finalize();

    let svg_file = output_dir.join(format!("{}-{}.svg", site.domain, year));
    std::fs::write(&svg_file, &year_svg)?;

    let html_file = output_dir.join(format!("{}-{}.html", site.domain, year));
    info!(domain = %site.domain, year, path = %html_file.display(), "Generating year HTML report");
    let html_content = crate::html::generate_year_report(
        &site.domain,
        year,
        &year_svg,
        &month_svgs,
    );
    std::fs::write(&html_file, html_content)?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    if args.month.is_none() && args.year.is_none() {
        anyhow::bail!("At least one of --month or --year is required");
    }

    let config_path = args.config.unwrap_or_else(|| PathBuf::from("edgeview.toml"));
    if !config_path.exists() {
        anyhow::bail!("Configuration file not found: {}. Please create one or specify with --config.", config_path.display());
    }

    let config = Config::load(&config_path)?;

    for site in &config.sites {
        if let Some(year) = &args.year {
            if let Err(e) = process_site_year(site, year, args.no_cache, &config.bots, &config.default_s3_region, &config.output_dir).await {
                error!(domain = %site.domain, error = %e, "Failed to process site year");
                return Err(e.context(format!("Failed to process site {} for year {}", site.domain, year)));
            }
        }

        if let Some(month) = &args.month {
            if let Err(e) = process_site(site, month, args.no_cache, &config.bots, &config.default_s3_region, &config.output_dir).await {
                error!(domain = %site.domain, error = %e, "Failed to process site");
                return Err(e.context(format!("Failed to process site {}", site.domain)));
            }
        }
    }

    info!("Finished processing all sites");
    Ok(())
}
