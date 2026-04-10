use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::HashMap;
use time::{Date, Month, OffsetDateTime};

/// Maximum number of content/static pages shown in tables.
pub const TOP_PAGES_LIMIT: usize = 15;

/// Maximum number of bots shown in SVG reports.
pub const TOP_BOTS_SVG_LIMIT: usize = 5;

/// Maximum number of bots shown in HTML year/month tables.
pub const TOP_BOTS_HTML_LIMIT: usize = 10;

/// Maximum number of missing sitemap URLs shown.
pub const MAX_SITEMAP_GAPS: usize = 15;

/// Maximum number of referers shown in reports.
pub const TOP_REFERERS_LIMIT: usize = 15;

/// Split pages into (content_pages limited to TOP_PAGES_LIMIT, static_assets).
pub fn split_pages(pages: &[PageHits]) -> (Vec<PageHits>, Vec<PageHits>) {
    let content: Vec<PageHits> = pages
        .iter()
        .filter(|p| p.category == "page")
        .take(TOP_PAGES_LIMIT)
        .cloned()
        .collect();
    let statics: Vec<PageHits> = pages
        .iter()
        .filter(|p| p.category != "page")
        .cloned()
        .collect();
    (content, statics)
}

/// Parse an ISO 8601 date string ("YYYY-MM-DD") into a `time::Date`.
pub fn parse_date(s: &str) -> Option<Date> {
    Date::parse(s, time::macros::format_description!("[year]-[month]-[day]")).ok()
}

/// Compute (human_pct, bot_pct) with proper f64 rounding.
pub fn human_bot_pct(hits: u64, bot_hits: u64) -> (u64, u64) {
    if hits == 0 {
        return (100, 0);
    }
    let bot_pct = ((bot_hits.min(hits) as f64 / hits as f64) * 100.0).round() as u64;
    (100u64.saturating_sub(bot_pct), bot_pct)
}

/// Return the last day of a given month.
pub fn last_day_of_month(year: i32, month: u32) -> Date {
    let next = if month == 12 {
        Date::from_calendar_date(year + 1, Month::January, 1).unwrap()
    } else {
        Date::from_calendar_date(year, Month::try_from(month as u8 + 1).unwrap(), 1).unwrap()
    };
    next.previous_day().unwrap()
}

/// Parse a "YYYY-MM" string into (year, month_number, first_day).
pub fn parse_month(month: &str) -> anyhow::Result<(i32, u32, Date)> {
    let parts: Vec<&str> = month.split('-').collect();
    if parts.len() != 2 {
        anyhow::bail!("Invalid month format '{}', expected YYYY-MM", month);
    }
    let year: i32 = parts[0].parse()?;
    let month_num: u32 = parts[1].parse()?;
    let first_day = Date::from_calendar_date(year, Month::try_from(month_num as u8)?, 1)
        .map_err(|e| anyhow::anyhow!("Invalid month '{}': {}", month, e))?;
    Ok((year, month_num, first_day))
}

/// All dates in a month up to today.
pub fn dates_in_month(month: &str) -> anyhow::Result<Vec<Date>> {
    let (year, month_num, first) = parse_month(month)?;
    let last = last_day_of_month(year, month_num);
    let today = OffsetDateTime::now_utc().date();
    let end = last.min(today);

    let mut dates = Vec::new();
    let mut d = first;
    while d <= end {
        dates.push(d);
        d = d.next_day().unwrap();
    }
    Ok(dates)
}

/// All dates in a year up to today.
pub fn dates_in_year(year: &str) -> anyhow::Result<Vec<Date>> {
    let year_num: i32 = year.parse()?;
    let today = OffsetDateTime::now_utc().date();
    let max_month: u32 = if year_num == today.year() {
        u8::from(today.month()) as u32
    } else {
        12
    };

    let mut dates = Vec::new();
    for m in 1..=max_month {
        let month_str = format!("{}-{:02}", year, m);
        dates.extend(dates_in_month(&month_str)?);
    }
    Ok(dates)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyTraffic {
    pub date: Date,
    pub hits: u64,
    pub visitors: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageHits {
    pub path: String,
    pub hits: u64,
    pub visitors: u64,
    pub bot_hits: u64,
    pub category: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HourlyTraffic {
    pub hour: u8,
    pub hits: u64,
    pub visitors: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrawlerStats {
    pub bot_name: String,
    pub hits: u64,
    pub last_crawl: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefererStats {
    pub referer: String,
    pub hits: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MonthReport {
    pub total_hits: u64,
    pub total_visitors: u64,
    pub total_bot_hits: u64,
    pub total_bot_visitors: u64,
    pub daily: Vec<DailyTraffic>,
    pub top_pages: Vec<PageHits>,
    pub bot_stats: Vec<CrawlerStats>,
    pub google_hits: HashMap<String, u64>,
    #[serde(default)]
    pub daily_pages: HashMap<String, Vec<PageHits>>,
    #[serde(default)]
    pub daily_hourly: HashMap<String, Vec<HourlyTraffic>>,
    #[serde(default)]
    pub referer_stats: Vec<RefererStats>,
}

#[derive(Debug)]
pub struct DayCache {
    pub date: Date,
    pub hits: u64,
    pub visitors: u64,
    pub bot_hits: u64,
    pub bot_visitors: u64,
    pub top_pages: Vec<PageHits>,
    pub hourly: Vec<HourlyTraffic>,
    pub bot_stats: Vec<CrawlerStats>,
    pub google_hits: HashMap<String, u64>,
    pub referer_stats: Vec<RefererStats>,
}

fn merge_page_rollup<'a>(sources: impl Iterator<Item = &'a Vec<PageHits>>) -> Vec<PageHits> {
    let mut rollup: HashMap<(String, String), (u64, u64, u64)> = HashMap::new();
    for pages in sources {
        for p in pages {
            let entry = rollup
                .entry((p.category.clone(), p.path.clone()))
                .or_default();
            entry.0 += p.hits;
            entry.1 += p.visitors;
            entry.2 += p.bot_hits;
        }
    }
    let mut result: Vec<PageHits> = rollup
        .into_iter()
        .map(|((category, path), (hits, visitors, bot_hits))| PageHits {
            path,
            hits,
            visitors,
            bot_hits,
            category,
        })
        .collect();
    result.sort_by_key(|p| Reverse(p.hits));
    result
}

fn merge_bot_rollup<'a>(sources: impl Iterator<Item = &'a Vec<CrawlerStats>>) -> Vec<CrawlerStats> {
    let mut rollup: HashMap<String, (u64, Option<OffsetDateTime>)> = HashMap::new();
    for stats in sources {
        for b in stats {
            let entry = rollup.entry(b.bot_name.clone()).or_insert((0, None));
            entry.0 += b.hits;
            if b.last_crawl > entry.1 {
                entry.1 = b.last_crawl;
            }
        }
    }
    let mut result: Vec<CrawlerStats> = rollup
        .into_iter()
        .map(|(bot_name, (hits, last_crawl))| CrawlerStats {
            bot_name,
            hits,
            last_crawl,
        })
        .collect();
    result.sort_by_key(|s| Reverse(s.hits));
    result
}

fn merge_google_hits<'a>(
    sources: impl Iterator<Item = &'a HashMap<String, u64>>,
) -> HashMap<String, u64> {
    let mut merged: HashMap<String, u64> = HashMap::new();
    for map in sources {
        for (path, hits) in map {
            *merged.entry(path.clone()).or_default() += hits;
        }
    }
    merged
}

fn merge_referer_rollup<'a>(
    sources: impl Iterator<Item = &'a Vec<RefererStats>>,
) -> Vec<RefererStats> {
    let mut rollup: HashMap<String, u64> = HashMap::new();
    for stats in sources {
        for r in stats {
            *rollup.entry(r.referer.clone()).or_default() += r.hits;
        }
    }
    let mut result: Vec<RefererStats> = rollup
        .into_iter()
        .map(|(referer, hits)| RefererStats { referer, hits })
        .collect();
    result.sort_by_key(|r| Reverse(r.hits));
    result
}

impl MonthReport {
    /// Build a MonthReport from DayCaches.
    /// If exact_visitors is Some, use Parquet-derived counts; otherwise fall back to summing daily.
    pub fn from_day_caches(mut days: Vec<DayCache>, exact_visitors: Option<(u64, u64)>) -> Self {
        days.sort_by_key(|d| d.date);

        let total_hits: u64 = days.iter().map(|d| d.hits).sum();
        let total_bot_hits: u64 = days.iter().map(|d| d.bot_hits).sum();
        let (total_visitors, total_bot_visitors) = exact_visitors.unwrap_or_else(|| {
            let v: u64 = days.iter().map(|d| d.visitors).sum();
            let bv: u64 = days.iter().map(|d| d.bot_visitors).sum();
            (v, bv)
        });

        let daily: Vec<DailyTraffic> = days
            .iter()
            .map(|d| DailyTraffic {
                date: d.date,
                hits: d.hits,
                visitors: d.visitors,
            })
            .collect();

        let top_pages = merge_page_rollup(days.iter().map(|d| &d.top_pages));
        let bot_stats = merge_bot_rollup(days.iter().map(|d| &d.bot_stats));
        let google_hits = merge_google_hits(days.iter().map(|d| &d.google_hits));
        let referer_stats = merge_referer_rollup(days.iter().map(|d| &d.referer_stats));

        let mut daily_pages: HashMap<String, Vec<PageHits>> = HashMap::new();
        let mut daily_hourly: HashMap<String, Vec<HourlyTraffic>> = HashMap::new();
        for day in &days {
            let date_str = day.date.to_string();
            daily_pages.insert(date_str.clone(), day.top_pages.clone());
            daily_hourly.insert(date_str, day.hourly.clone());
        }

        MonthReport {
            total_hits,
            total_visitors,
            total_bot_hits,
            total_bot_visitors,
            daily,
            top_pages,
            bot_stats,
            google_hits,
            daily_pages,
            daily_hourly,
            referer_stats,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Kpi {
    pub label: String,
    pub value: String,
    pub change: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MonthlyTraffic {
    pub month: String, // "2026-03"
    pub hits: u64,
    pub visitors: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct YearReport {
    pub year: String,
    pub total_hits: u64,
    pub total_visitors: u64,
    pub total_bot_hits: u64,
    pub total_bot_visitors: u64,
    pub monthly: Vec<MonthlyTraffic>,
    pub top_pages: Vec<PageHits>,
    pub bot_stats: Vec<CrawlerStats>,
    pub google_hits: HashMap<String, u64>,
    pub referer_stats: Vec<RefererStats>,
}

impl YearReport {
    pub fn from_month_data(
        year: &str,
        months: Vec<(String, MonthReport)>,
        exact_visitors: (u64, u64),
    ) -> Self {
        let total_hits: u64 = months.iter().map(|(_, r)| r.total_hits).sum();
        let total_bot_hits: u64 = months.iter().map(|(_, r)| r.total_bot_hits).sum();
        let (total_visitors, total_bot_visitors) = exact_visitors;

        let monthly: Vec<MonthlyTraffic> = months
            .iter()
            .map(|(m, r)| MonthlyTraffic {
                month: m.clone(),
                hits: r.total_hits,
                visitors: r.total_visitors,
            })
            .collect();

        let top_pages = merge_page_rollup(months.iter().map(|(_, r)| &r.top_pages));
        let bot_stats = merge_bot_rollup(months.iter().map(|(_, r)| &r.bot_stats));
        let google_hits = merge_google_hits(months.iter().map(|(_, r)| &r.google_hits));
        let referer_stats = merge_referer_rollup(months.iter().map(|(_, r)| &r.referer_stats));

        YearReport {
            year: year.to_string(),
            total_hits,
            total_visitors,
            total_bot_hits,
            total_bot_visitors,
            monthly,
            top_pages,
            bot_stats,
            google_hits,
            referer_stats,
        }
    }
}
