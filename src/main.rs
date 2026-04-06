mod classify;
mod compact;
mod config;
mod html;
mod model;
mod pipeline;
mod query;
mod sitemap;
mod svg;

use std::path::PathBuf;
use tracing::{error, info};

/// CloudFront log analytics → SVG/HTML reports
#[derive(argh::FromArgs)]
struct Args {
    /// config file path
    #[argh(option, short = 'c')]
    config: Option<PathBuf>,

    /// print version and exit
    #[argh(switch, short = 'v')]
    version: bool,

    #[argh(subcommand)]
    command: Option<Command>,
}

#[derive(argh::FromArgs)]
#[argh(subcommand)]
enum Command {
    Report(ReportArgs),
    Compact(CompactArgs),
}

/// generate SVG/HTML reports from CloudFront logs
#[derive(argh::FromArgs)]
#[argh(subcommand, name = "report")]
struct ReportArgs {
    /// month to process (YYYY-MM)
    #[argh(option, short = 'm')]
    month: Option<String>,

    /// year to process (YYYY)
    #[argh(option, short = 'y')]
    year: Option<String>,

    /// re-fetch all data from S3
    #[argh(switch)]
    no_cache: bool,

    /// only process this site domain
    #[argh(option, short = 's')]
    site: Option<String>,
}

/// compact raw S3 parquet files into sorted daily files
#[derive(argh::FromArgs)]
#[argh(subcommand, name = "compact")]
struct CompactArgs {
    /// month to compact (YYYY-MM)
    #[argh(option, short = 'm')]
    month: Option<String>,

    /// year to compact (YYYY)
    #[argh(option, short = 'y')]
    year: Option<String>,

    /// only process this site domain
    #[argh(option, short = 's')]
    site: Option<String>,

    /// show what would be done without writing
    #[argh(switch)]
    dry_run: bool,

    /// force re-compact even if compact file exists
    #[argh(switch)]
    force: bool,
}

#[tokio::main]
async fn main() {
    use tracing_subscriber::EnvFilter;
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("edgeview=info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args: Args = argh::from_env();
    if args.version {
        println!("edgeview {}", env!("CARGO_PKG_VERSION"));
        return;
    }

    if let Err(e) = run(args).await {
        error!("{e:#}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> anyhow::Result<()> {
    let command = args.command.ok_or_else(|| {
        anyhow::anyhow!("No command specified. Use `edgeview report` or `edgeview compact`.")
    })?;

    let config_path = args
        .config
        .unwrap_or_else(|| PathBuf::from("edgeview.toml"));
    if !config_path.exists() {
        anyhow::bail!(
            "Configuration file not found: {}. Please create one or specify with --config.",
            config_path.display()
        );
    }
    let config = config::Config::load(&config_path)?;

    match command {
        Command::Report(cmd) => run_report(cmd, &config).await,
        Command::Compact(cmd) => run_compact(cmd, &config).await,
    }
}

fn filter_sites<'a>(
    config: &'a config::Config,
    site_filter: &Option<String>,
) -> anyhow::Result<Vec<&'a config::SiteConfig>> {
    if let Some(ref filter) = site_filter {
        let matched: Vec<_> = config
            .sites
            .iter()
            .filter(|s| s.domain == *filter)
            .collect();
        if matched.is_empty() {
            let available: Vec<_> = config.sites.iter().map(|s| s.domain.as_str()).collect();
            anyhow::bail!(
                "Site '{}' not found in config. Available: {}",
                filter,
                available.join(", ")
            );
        }
        Ok(matched)
    } else {
        Ok(config.sites.iter().collect())
    }
}

async fn make_s3_client(
    config: &config::Config,
    sites: &[&config::SiteConfig],
) -> anyhow::Result<aws_sdk_s3::Client> {
    let aws_config = aws_config::load_from_env().await;

    let s3_region = aws_config
        .region()
        .map(|r| r.to_string())
        .unwrap_or_else(|| config.default_s3_region.clone());

    let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
        .region(aws_config::Region::new(s3_region))
        .stalled_stream_protection(aws_sdk_s3::config::StalledStreamProtectionConfig::disabled())
        .build();
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

    // Validate credentials
    let s3_url = url::Url::parse(&sites[0].s3_path)
        .map_err(|e| anyhow::anyhow!("Invalid s3_path '{}': {}", sites[0].s3_path, e))?;
    let bucket = s3_url.host_str().unwrap();
    let prefix = s3_url.path().trim_start_matches('/');
    s3_client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .max_keys(1)
        .send()
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "AWS credentials not working: {e:?}\n\n  aws sso login --profile $AWS_PROFILE"
            )
        })?;

    Ok(s3_client)
}

async fn run_report(cmd: ReportArgs, config: &config::Config) -> anyhow::Result<()> {
    if cmd.month.is_none() && cmd.year.is_none() {
        anyhow::bail!("At least one of --month or --year is required");
    }

    let sites = filter_sites(config, &cmd.site)?;
    let s3_client = make_s3_client(config, &sites).await?;
    let mut timings = pipeline::Timings::new();
    let all_domains: Vec<String> = sites.iter().map(|s| s.domain.clone()).collect();

    info!("Starting report generation");

    for site in sites {
        if let Some(year) = &cmd.year {
            pipeline::process_site_year(
                &s3_client,
                site,
                year,
                cmd.no_cache,
                &config.bots,
                &config.output_dir,
                &mut timings,
                &all_domains,
            )
            .await
            .map_err(|e| {
                e.context(format!(
                    "Failed to process {} for year {}",
                    site.domain, year
                ))
            })?;
        }

        if let Some(month) = &cmd.month {
            pipeline::process_site(
                &s3_client,
                site,
                month,
                cmd.no_cache,
                &config.bots,
                &config.output_dir,
                &mut timings,
                &all_domains,
            )
            .await
            .map_err(|e| e.context(format!("Failed to process {}", site.domain)))?;
        }
    }

    timings.print_summary();
    Ok(())
}

async fn run_compact(cmd: CompactArgs, config: &config::Config) -> anyhow::Result<()> {
    if cmd.month.is_none() && cmd.year.is_none() {
        anyhow::bail!("At least one of --month or --year is required");
    }

    let sites = filter_sites(config, &cmd.site)?;
    let s3_client = make_s3_client(config, &sites).await?;
    let mut timings = pipeline::Timings::new();

    let compact_config = compact::CompactConfig {
        dry_run: cmd.dry_run,
        force: cmd.force,
    };

    info!("Starting compaction");

    for site in sites {
        let dates = if let Some(year) = &cmd.year {
            compact::dates_in_year(year)?
        } else if let Some(month) = &cmd.month {
            compact::dates_in_month(month)?
        } else {
            unreachable!()
        };

        compact::compact_site(&s3_client, site, dates, &compact_config, &mut timings)
            .await
            .map_err(|e| e.context(format!("Compaction failed for {}", site.domain)))?;
    }

    timings.print_summary();
    Ok(())
}
