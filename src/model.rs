use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Serialize, Deserialize)]
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
    pub google_hits: std::collections::HashMap<String, u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Kpi {
    pub label: String,
    pub value: String,
    pub change: Option<String>,
}
