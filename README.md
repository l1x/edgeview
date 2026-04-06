# edgeview

CloudFront log analytics tool. Processes access logs from S3, classifies traffic (human vs bot), and generates HTML/SVG reports with hourly charts, daily breakdowns, and visitor metrics.

## Install

```sh
cargo install --path .
```

## Usage

```sh
# Generate report with default config
edgeview -c edgeview.toml

# Compact Parquet files
edgeview -c edgeview.toml compact
```

## License

MIT
