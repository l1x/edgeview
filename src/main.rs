mod config;
mod model;
mod query;
mod svg;
mod classify;
mod sitemap;

use std::path::PathBuf;
use std::collections::HashMap;
use clap::Parser;
use tracing::{info, error};
use crate::config::{Config, SiteConfig};
use crate::query::QueryEngine;
use crate::svg::SvgDoc;
use crate::svg::theme::GREY_ORANGE;
use crate::model::{Kpi, MonthReport};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config: Option<PathBuf>,

    #[arg(short, long)]
    month: String,

    #[arg(long, default_value_t = false)]
    no_cache: bool,
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
    let cache_file = cache_dir.join(format!("{}.json", month));

    let report = if !no_cache && cache_file.exists() {
        info!(domain = %site.domain, "Loading data from local cache");
        let content = std::fs::read_to_string(&cache_file)?;
        serde_json::from_str::<MonthReport>(&content)?
    } else {
        info!(domain = %site.domain, month, "Initializing DataFusion engine");
        let s3_month = month.replace("-", "/");
        let engine = QueryEngine::new(site, default_region).await?;
        engine.load_logs(&site.s3_path, &s3_month).await?;

        info!(domain = %site.domain, "Analyzing traffic from S3");
        let (total_hits, total_visitors) = engine.summary().await?;
        let daily = engine.daily_traffic().await?;
        let bot_path_hits = engine.bot_hits_by_path(bots).await?;
        let (total_bot_hits, total_bot_visitors) = engine.bot_summary(bots).await?;
        let top_pages = engine.top_pages(&bot_path_hits).await?;
        let bot_stats = engine.bot_activity(bots).await?;
        let google_hits = engine.googlebot_hits().await?;

        let report = MonthReport {
            total_hits,
            total_visitors,
            total_bot_hits,
            total_bot_visitors,
            daily,
            top_pages,
            bot_stats,
            google_hits,
        };

        std::fs::create_dir_all(&cache_dir)?;
        let json = serde_json::to_string_pretty(&report)?;
        std::fs::write(&cache_file, json)?;
        info!(domain = %site.domain, "Cached report to {}", cache_file.display());

        report
    };

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

    // Split pages by category
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
    doc.add_section_title(&format!("Traffic Report: {} ({})", site.domain, month));

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
        doc.add_crawl_gap_section(&missing_urls);
    }

    let svg_content = doc.finalize();
    std::fs::create_dir_all(output_dir)?;
    std::fs::write(&output_file, svg_content)?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let config_path = args.config.unwrap_or_else(|| PathBuf::from("edgeview.toml"));
    if !config_path.exists() {
        anyhow::bail!("Configuration file not found: {}. Please create one or specify with --config.", config_path.display());
    }

    let config = Config::load(&config_path)?;

    for site in &config.sites {
        if let Err(e) = process_site(&site, &args.month, args.no_cache, &config.bots, &config.default_s3_region, &config.output_dir).await {
            error!(domain = %site.domain, error = %e, "Failed to process site");
            return Err(e.context(format!("Failed to process site {}", site.domain)));
        }
    }

    info!("Finished processing all sites");
    Ok(())
}
