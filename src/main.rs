mod classify;
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

#[tokio::main]
async fn main() {
    use tracing_subscriber::EnvFilter;
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("edgeview=info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    if let Err(e) = run().await {
        error!("{e:#}");
        std::process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    info!("Starting up");

    let args: Args = argh::from_env();
    if args.month.is_none() && args.year.is_none() {
        anyhow::bail!("At least one of --month or --year is required");
    }

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

    let sites: Vec<&config::SiteConfig> = if let Some(ref filter) = args.site {
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
        matched
    } else {
        config.sites.iter().collect()
    };

    // AWS setup
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

    // Validate credentials by listing a single object from the first site's prefix
    let s3_url = url::Url::parse(&sites[0].s3_path)
        .map_err(|e| anyhow::anyhow!("Invalid s3_path '{}': {}", sites[0].s3_path, e))?;
    let bucket = s3_url.host_str().unwrap();
    let prefix = s3_url.path().trim_start_matches('/');
    if let Err(e) = s3_client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .max_keys(1)
        .send()
        .await
    {
        error!("AWS credentials not working: {e:?}\n\n  aws sso login --profile $AWS_PROFILE");
        std::process::exit(1);
    }

    let mut timings = pipeline::Timings::new();
    let all_domains: Vec<String> = sites.iter().map(|s| s.domain.clone()).collect();

    for site in sites {
        if let Some(year) = &args.year {
            if let Err(e) = pipeline::process_site_year(
                &s3_client,
                site,
                year,
                args.no_cache,
                &config.bots,
                &config.output_dir,
                &mut timings,
                &all_domains,
            )
            .await
            {
                return Err(e.context(format!(
                    "Failed to process site {} for year {}",
                    site.domain, year
                )));
            }
        }

        if let Some(month) = &args.month {
            if let Err(e) = pipeline::process_site(
                &s3_client,
                site,
                month,
                args.no_cache,
                &config.bots,
                &config.output_dir,
                &mut timings,
                &all_domains,
            )
            .await
            {
                return Err(e.context(format!("Failed to process site {}", site.domain)));
            }
        }
    }

    timings.print_summary();
    Ok(())
}
