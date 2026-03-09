# PRD: edgeview — CloudFront Log Analyzer & SVG Report Generator

## 1. Overview
`edgeview` is a Rust-based CLI tool designed for technical founders to gain high-fidelity insights into their web traffic and search engine indexing coverage. It processes CloudFront logs stored in S3 using DataFusion, caches daily aggregates locally, and generates beautiful, Scandinavian-style SVG reports.

## 2. Problem Statement
CloudFront logs are voluminous and stored in raw Parquet format, making them inaccessible for quick daily checks. Standard analytics (Google Analytics) are often blocked by privacy tools or provide "black box" data. Founders need a way to see raw request data, specifically focusing on bot behavior and crawl gaps, without the overhead of a full ELK stack or expensive SaaS tools.

## 3. Users & Value (Personas)
### Persona: The Technical Founder
- **Pain:** Doesn't know if Googlebot is actually crawling new articles or just hitting the homepage.
- **Key questions:** "Is my sitemap fully indexed?", "Which bots are hitting me hardest?", "Did my recent deployment break traffic in specific regions?"
- **Success signal:** A single SVG file that can be opened in any browser to see the health of the site's "edge" traffic.

## 4. Goals
- **G-1 (Efficiency):** Reduce report generation time by caching daily aggregates locally (avoiding repeated S3 scans).
- **G-2 (Clarity):** Provide a visual "Scandinavian" design that highlights KPIs (Page Views, Unique Visitors, Crawl Gaps) with high legibility.
- **G-3 (Actionability):** Explicitly identify "Indexing Gaps" by comparing sitemap.xml against Googlebot logs.
- **G-4 (Portability):** Generate self-contained SVG files that are easy to archive or embed in future web dashboards.

## 5. Job Stories
- **JS-1:** As a founder, I can run `edgeview --month 2026-03`, so that I can see my monthly growth and bot activity.
- **JS-2:** As a founder, I can configure multiple domains, so that I can manage my entire portfolio of sites from one tool.
- **JS-3:** As a developer, I can tweak the SVG design tokens in a config file, so that the reports match my brand aesthetic.

## 6. Assumptions
- AWS credentials are available in the local environment via the standard `credential_chain`.
- CloudFront logs are partitioned by `YYYY/MM/DD/HH` as is standard for S3 delivery.
- Users have a valid `sitemap.xml` for crawl gap analysis.

## 7. Functional Requirements

### FR-1: Multi-Domain Query Engine
- **Requirement:** Support a configuration that defines multiple S3 buckets and domains.
- **Acceptance:** The tool iterates through all configured sites and generates a separate SVG for each.

### FR-2: Local Daily Aggregation (Caching)
- **Requirement:** Query results must be stored locally by date.
- **Acceptance:** If a report is run for "March", and "March 01-08" data is already cached, only "March 09" is fetched from S3.

### FR-3: Bot & Crawler Classification
- **Requirement:** Map User-Agent strings to known bots (Google, Bing, Claude, GPT).
- **Acceptance:** The report includes a "Bot Activity" section with hits per bot name.

### FR-4: Crawl Gap Detection
- **Requirement:** Cross-reference `sitemap.xml` URLs with `logs` where `User-Agent` contains "Googlebot".
- **Acceptance:** The report lists "Missing from Index" URLs (pages in sitemap but never visited by Googlebot).

### FR-5: SVG Report Generation
- **Requirement:** Generate a Scandinavian-themed SVG with a dynamic height based on content.
- **Acceptance:** Output is a single `.svg` file containing: KPI Cards, Daily Traffic Chart, Top Pages, and Crawl Gap Checklist.

## 8. Non-functional Requirements

### NFR-1: Performance
- **Requirement:** Cold start (no cache) for 1 month of logs should complete under 30 seconds for a typical site.
- **Acceptance:** Benchmarked against a 1GB Parquet log set on S3.

### NFR-2: Visual Design
- **Requirement:** Adhere to the "Scandinavian" design system (#fafafa background, system-ui fonts, pastel accents).
- **Acceptance:** Visual review of generated SVG in Chrome and Firefox.

## 9. Technical Constraints
- **Language:** Rust (Stable).
- **Query Engine:** DataFusion (with `object-store` for S3).
- **CLI Framework:** `clap` (v4).
- **Visualization:** Raw SVG string generation (no heavy JS libraries).
- **Environment:** Must run on macOS/Linux.
- **Prohibitions:** No external database (Postgres/Redis); all state must be in local files.

## 10. Non-Goals
- Real-time "live" streaming of logs (the tool is for batch analysis).
- Log "cleaning" or mutation (it is read-only).
- Web-based UI (it is a CLI that outputs a file).

## 11. Success Metrics
- **Metric 1:** Time saved vs. manual DuckDB/SQL queries (Target: < 5 seconds for cached runs).
- **Metric 2:** Identification of at least 1 "hidden" crawl gap per site run.
