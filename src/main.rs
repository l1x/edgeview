mod config;
mod model;
mod query;
mod svg;
mod classify;
mod sitemap;

use std::path::PathBuf;
use clap::Parser;
use crate::config::Config;
use crate::query::QueryEngine;
use crate::svg::SvgDoc;
use crate::svg::theme::SCANDINAVIAN;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config: Option<PathBuf>,

    #[arg(short, long)]
    s3_path: Option<String>,

    #[arg(short, long)]
    month: String,

    #[arg(short, long)]
    domain: Option<String>,

    #[arg(short, long)]
    sitemap: Option<PathBuf>,

    #[arg(short, long)]
    output: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Load configuration
    let config = if let Some(path) = args.config {
        Config::load(&path)?
    } else {
        // Fallback to defaults or partial config from args
        Config {
            domain: args.domain.unwrap_or_default(),
            s3_path: args.s3_path.unwrap_or_default(),
            s3_region: "eu-west-1".to_string(),
            sitemap: args.sitemap,
            output_dir: args.output.parent().unwrap_or_else(|| std::path::Path::new(".")).to_path_buf(),
            url_rewrites: None,
            categories: None,
            bots: std::collections::HashMap::new(),
        }
    };

    println!("Initializing DataFusion engine for month {}...", args.month);
    let engine = QueryEngine::new(&config).await?;
    engine.load_logs(&config.s3_path, &args.month).await?;

    // Perform queries
    println!("Analyzing traffic...");
    // let daily = engine.daily_traffic().await?;
    
    // Generate SVG
    println!("Generating SVG report to {}...", args.output.display());
    let mut doc = SvgDoc::new(800.0, SCANDINAVIAN);
    doc.add_section_title(&format!("Traffic Report: {} ({})", config.domain, args.month));
    
    // Placeholder sections
    doc.add_bar_chart("Daily Hits (Top 5)", &[
        ("2026-03-01".to_string(), 120u64),
        ("2026-03-02".to_string(), 150u64),
        ("2026-03-03".to_string(), 90u64),
    ]);

    let svg_content = doc.finalize();
    std::fs::write(&args.output, svg_content)?;

    println!("Report generated successfully.");
    Ok(())
}
