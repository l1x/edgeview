use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DailyTraffic {
    pub date: NaiveDate,
    pub hits: u64,
    pub visitors: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PageHits {
    pub path: String,
    pub hits: u64,
    pub visitors: u64,
    pub category: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Referrer {
    pub domain: String,
    pub hits: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CrawlerStats {
    pub bot_name: String,
    pub hits: u64,
    pub last_crawl: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CrawlRecord {
    pub path: String,
    pub hits: u64,
    pub last_date: NaiveDate,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EdgeStats {
    pub location: String,
    pub hits: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Kpi {
    pub label: String,
    pub value: String,
    pub change: Option<String>,
}
