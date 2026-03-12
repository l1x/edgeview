use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct DailyTraffic {
    pub date: NaiveDate,
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
    pub last_crawl: Option<DateTime<Utc>>,
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
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DayCache {
    pub date: NaiveDate,
    pub hits: u64,
    pub visitors: u64,
    pub bot_hits: u64,
    pub bot_visitors: u64,
    pub top_pages: Vec<PageHits>,
    pub hourly: Vec<HourlyTraffic>,
    pub bot_stats: Vec<CrawlerStats>,
    pub google_hits: HashMap<String, u64>,
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

        // Merge top_pages by (category, path), summing hits/visitors/bot_hits
        let mut page_rollup: HashMap<(String, String), (u64, u64, u64)> = HashMap::new();
        for day in &days {
            for p in &day.top_pages {
                let entry = page_rollup
                    .entry((p.category.clone(), p.path.clone()))
                    .or_default();
                entry.0 += p.hits;
                entry.1 += p.visitors;
                entry.2 += p.bot_hits;
            }
        }
        let mut top_pages: Vec<PageHits> = page_rollup
            .into_iter()
            .map(|((category, path), (hits, visitors, bot_hits))| PageHits {
                path,
                hits,
                visitors,
                bot_hits,
                category,
            })
            .collect();
        top_pages.sort_by_key(|p| Reverse(p.hits));

        // Merge bot_stats by bot_name, sum hits, max last_crawl
        let mut bot_rollup: HashMap<String, CrawlerStats> = HashMap::new();
        for day in &days {
            for b in &day.bot_stats {
                let entry = bot_rollup
                    .entry(b.bot_name.clone())
                    .or_insert(CrawlerStats {
                        bot_name: b.bot_name.clone(),
                        hits: 0,
                        last_crawl: None,
                    });
                entry.hits += b.hits;
                if b.last_crawl > entry.last_crawl {
                    entry.last_crawl = b.last_crawl;
                }
            }
        }
        let mut bot_stats: Vec<CrawlerStats> = bot_rollup.into_values().collect();
        bot_stats.sort_by_key(|s| Reverse(s.hits));

        // Merge google_hits by path
        let mut google_hits: HashMap<String, u64> = HashMap::new();
        for day in &days {
            for (path, hits) in &day.google_hits {
                *google_hits.entry(path.clone()).or_default() += hits;
            }
        }

        // Build daily_pages and daily_hourly from each DayCache
        let mut daily_pages: HashMap<String, Vec<PageHits>> = HashMap::new();
        let mut daily_hourly: HashMap<String, Vec<HourlyTraffic>> = HashMap::new();
        for day in &days {
            let date_str = day.date.format("%Y-%m-%d").to_string();
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
pub struct MonthSummary {
    pub month: String,
    pub total_hits: u64,
    pub total_visitors: u64,
    pub total_bot_hits: u64,
    pub total_bot_visitors: u64,
    pub daily: Vec<DailyTraffic>,
    pub top_pages: Vec<PageHits>,
    pub bot_stats: Vec<CrawlerStats>,
    pub google_hits: HashMap<String, u64>,
}

impl MonthSummary {
    pub fn from_month_report(report: &MonthReport, month: &str) -> Self {
        MonthSummary {
            month: month.to_string(),
            total_hits: report.total_hits,
            total_visitors: report.total_visitors,
            total_bot_hits: report.total_bot_hits,
            total_bot_visitors: report.total_bot_visitors,
            daily: report
                .daily
                .iter()
                .map(|d| DailyTraffic {
                    date: d.date,
                    hits: d.hits,
                    visitors: d.visitors,
                })
                .collect(),
            top_pages: report.top_pages.clone(),
            bot_stats: report
                .bot_stats
                .iter()
                .map(|b| CrawlerStats {
                    bot_name: b.bot_name.clone(),
                    hits: b.hits,
                    last_crawl: b.last_crawl,
                })
                .collect(),
            google_hits: report.google_hits.clone(),
        }
    }
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
    pub month_summaries: Vec<MonthSummary>,
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

        // Merge top_pages across months
        let mut page_rollup: HashMap<(String, String), (u64, u64, u64)> = HashMap::new();
        for (_, r) in &months {
            for p in &r.top_pages {
                let entry = page_rollup
                    .entry((p.category.clone(), p.path.clone()))
                    .or_default();
                entry.0 += p.hits;
                entry.1 += p.visitors;
                entry.2 += p.bot_hits;
            }
        }
        let mut top_pages: Vec<PageHits> = page_rollup
            .into_iter()
            .map(|((category, path), (hits, visitors, bot_hits))| PageHits {
                path,
                hits,
                visitors,
                bot_hits,
                category,
            })
            .collect();
        top_pages.sort_by_key(|p| Reverse(p.hits));

        // Merge bot_stats across months
        let mut bot_rollup: HashMap<String, CrawlerStats> = HashMap::new();
        for (_, r) in &months {
            for b in &r.bot_stats {
                let entry = bot_rollup
                    .entry(b.bot_name.clone())
                    .or_insert(CrawlerStats {
                        bot_name: b.bot_name.clone(),
                        hits: 0,
                        last_crawl: None,
                    });
                entry.hits += b.hits;
                if b.last_crawl > entry.last_crawl {
                    entry.last_crawl = b.last_crawl;
                }
            }
        }
        let mut bot_stats: Vec<CrawlerStats> = bot_rollup.into_values().collect();
        bot_stats.sort_by_key(|s| Reverse(s.hits));

        // Merge google_hits across months
        let mut google_hits: HashMap<String, u64> = HashMap::new();
        for (_, r) in &months {
            for (path, hits) in &r.google_hits {
                *google_hits.entry(path.clone()).or_default() += hits;
            }
        }

        // Build month summaries
        let month_summaries: Vec<MonthSummary> = months
            .iter()
            .map(|(m, r)| MonthSummary::from_month_report(r, m))
            .collect();

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
            month_summaries,
        }
    }
}
