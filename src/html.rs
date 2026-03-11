use std::collections::HashMap;
use crate::model::*;
use crate::svg::SvgDoc;
use crate::svg::theme::GREY_ORANGE;

/// Build a daily SVG matching the month view layout.
fn build_daily_svg(
    domain: &str,
    date: &str,
    pages: &[PageHits],
    hourly: &[HourlyTraffic],
    bot_stats: &[CrawlerStats],
) -> String {
    let total_hits: u64 = pages.iter().map(|p| p.hits).sum();
    let total_visitors: u64 = pages.iter().map(|p| p.visitors).sum();
    let total_bot_hits: u64 = pages.iter().map(|p| p.bot_hits).sum();

    let mut doc = SvgDoc::new(800.0, GREY_ORANGE);
    doc.add_section_title(&format!("{} / {}", domain, date));

    let hits_bot_pct = if total_hits > 0 { (total_bot_hits * 100) / total_hits } else { 0 };
    let hits_human_pct = 100 - hits_bot_pct;

    let bot_visitors = if total_hits > 0 { (total_visitors as f64 * total_bot_hits as f64 / total_hits as f64) as u64 } else { 0 };
    let vis_bot_pct = if total_visitors > 0 { (bot_visitors * 100) / total_visitors } else { 0 };
    let vis_human_pct = 100 - vis_bot_pct;

    doc.add_kpi_cards(&[
        Kpi {
            label: "Total Hits".to_string(),
            value: total_hits.to_string(),
            change: Some(format!("{}% human · {}% bot", hits_human_pct, hits_bot_pct)),
        },
        Kpi {
            label: "Unique Visitors".to_string(),
            value: total_visitors.to_string(),
            change: Some(format!("{}% human · {}% bot", vis_human_pct, vis_bot_pct)),
        },
        Kpi {
            label: "Bot Hits".to_string(),
            value: total_bot_hits.to_string(),
            change: None,
        },
    ]);

    doc.add_hourly_traffic_section(hourly);

    let content_pages: Vec<_> = pages.iter()
        .filter(|p| p.category == "article" || p.category == "page")
        .take(15)
        .cloned()
        .collect();
    let static_assets: Vec<_> = pages.iter()
        .filter(|p| p.category == "static")
        .cloned()
        .collect();

    doc.add_top_content_pages(&content_pages);
    doc.add_static_assets(&static_assets);
    doc.add_bot_activity_section(bot_stats);

    doc.finalize()
}

/// Build a compact month summary SVG for embedding in year report tabs.
pub fn build_month_summary_svg(domain: &str, month: &str, summary: &MonthSummary) -> String {
    let mut doc = SvgDoc::new(800.0, GREY_ORANGE);
    doc.add_section_title(&format!("{} / {}", domain, month));

    let hits_bot_pct = if summary.total_hits > 0 { (summary.total_bot_hits * 100) / summary.total_hits } else { 0 };
    let hits_human_pct = 100 - hits_bot_pct;
    let vis_bot_pct = if summary.total_visitors > 0 { (summary.total_bot_visitors * 100) / summary.total_visitors } else { 0 };
    let vis_human_pct = 100 - vis_bot_pct;

    doc.add_kpi_cards(&[
        Kpi {
            label: "Total Hits".to_string(),
            value: summary.total_hits.to_string(),
            change: Some(format!("{}% human · {}% bot", hits_human_pct, hits_bot_pct)),
        },
        Kpi {
            label: "Unique Visitors".to_string(),
            value: summary.total_visitors.to_string(),
            change: Some(format!("{}% human · {}% bot", vis_human_pct, vis_bot_pct)),
        },
        Kpi {
            label: "Active Bots".to_string(),
            value: summary.bot_stats.len().to_string(),
            change: None,
        },
    ]);

    doc.add_daily_traffic_section(&summary.daily);

    let content_pages: Vec<_> = summary.top_pages.iter()
        .filter(|p| p.category == "article" || p.category == "page")
        .take(5)
        .cloned()
        .collect();
    doc.add_top_content_pages(&content_pages);

    let top_bots: Vec<_> = summary.bot_stats.iter().take(3).cloned().collect();
    doc.add_bot_activity_section(&top_bots);

    doc.finalize()
}

/// Format a date string like "2026-03-01" into a short tab label like "Mar 1".
fn date_tab_label(date_str: &str) -> String {
    if let Ok(d) = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
        d.format("%b %-d").to_string()
    } else {
        date_str.to_string()
    }
}

/// Generate a self-contained HTML report with CSS-only tabs.
pub fn generate_report(
    domain: &str,
    month: &str,
    monthly_svg: &str,
    daily_pages: &HashMap<String, Vec<PageHits>>,
    daily_hourly: &HashMap<String, Vec<HourlyTraffic>>,
    bot_stats: &[CrawlerStats],
) -> String {
    let mut dates: Vec<&String> = daily_pages.keys().collect();
    dates.sort();

    let mut tab_inputs = String::new();
    let mut tab_labels = String::new();
    let mut tab_panels = String::new();
    let mut css_rules = String::new();

    // Month tab (default checked)
    tab_inputs.push_str(r#"<input type="radio" name="tabs" id="tab-month" checked>"#);
    tab_inputs.push('\n');
    tab_labels.push_str(r#"<label for="tab-month">Month</label>"#);
    tab_labels.push('\n');
    tab_panels.push_str(&format!(
        r#"<div class="panel" id="panel-month">{}</div>"#,
        monthly_svg
    ));
    tab_panels.push('\n');
    css_rules.push_str("#tab-month:checked ~ .panels #panel-month { display: block; }\n");
    css_rules.push_str("#tab-month:checked ~ .tabs label[for='tab-month'] { background: #f97316; color: #fff; }\n");

    // Day tabs
    let empty_hourly = Vec::new();
    for (i, date_str) in dates.iter().enumerate() {
        let id = format!("tab-d{}", i);
        let panel_id = format!("panel-d{}", i);
        let label = date_tab_label(date_str);

        tab_inputs.push_str(&format!(
            r#"<input type="radio" name="tabs" id="{}">"#,
            id
        ));
        tab_inputs.push('\n');

        tab_labels.push_str(&format!(
            r#"<label for="{}">{}</label>"#,
            id, label
        ));
        tab_labels.push('\n');

        let pages = &daily_pages[date_str.as_str()];
        let hourly = daily_hourly.get(date_str.as_str()).unwrap_or(&empty_hourly);
        let daily_svg = build_daily_svg(domain, date_str, pages, hourly, bot_stats);
        tab_panels.push_str(&format!(
            r#"<div class="panel" id="{}">{}</div>"#,
            panel_id, daily_svg
        ));
        tab_panels.push('\n');

        css_rules.push_str(&format!(
            "#{id}:checked ~ .panels #{panel_id} {{ display: block; }}\n"
        ));
        css_rules.push_str(&format!(
            "#{id}:checked ~ .tabs label[for='{id}'] {{ background: #f97316; color: #fff; }}\n"
        ));
    }

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>EdgeView Report: {domain} ({month})</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: system-ui, -apple-system, sans-serif; background: #f5f5f5; color: #2d2d2d; padding: 24px; }}
h1 {{ font-size: 20px; font-weight: 600; margin-bottom: 16px; text-align: center; }}
input[type="radio"] {{ display: none; }}
.tabs {{ display: flex; flex-wrap: wrap; gap: 4px; margin-bottom: 16px; justify-content: center; }}
.tabs label {{ padding: 8px 16px; background: #e5e5e5; border-radius: 6px; cursor: pointer; font-size: 14px; font-weight: 500; color: #2d2d2d; transition: background 0.15s, color 0.15s; }}
.tabs label:hover {{ background: #d4d4d4; }}
.panel {{ display: none; }}
.panel svg {{ max-width: 800px; width: 100%; height: auto; display: block; margin: 0 auto; }}
{css_rules}
</style>
</head>
<body>
{tab_inputs}
<div class="tabs">
{tab_labels}
</div>
<div class="panels">
{tab_panels}
</div>
</body>
</html>"#,
        domain = domain,
        month = month,
        css_rules = css_rules,
        tab_inputs = tab_inputs,
        tab_labels = tab_labels,
        tab_panels = tab_panels,
    )
}

/// Generate a self-contained year HTML report with Year + month tabs.
pub fn generate_year_report(
    domain: &str,
    year: &str,
    year_svg: &str,
    month_svgs: &[(String, String)], // (month like "2026-01", svg_content)
) -> String {
    let month_labels = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];

    let mut tab_inputs = String::new();
    let mut tab_labels = String::new();
    let mut tab_panels = String::new();
    let mut css_rules = String::new();

    // Year tab (default checked)
    tab_inputs.push_str(r#"<input type="radio" name="tabs" id="tab-year" checked>"#);
    tab_inputs.push('\n');
    tab_labels.push_str(r#"<label for="tab-year">Year</label>"#);
    tab_labels.push('\n');
    tab_panels.push_str(&format!(
        r#"<div class="panel" id="panel-year">{}</div>"#,
        year_svg
    ));
    tab_panels.push('\n');
    css_rules.push_str("#tab-year:checked ~ .panels #panel-year { display: block; }\n");
    css_rules.push_str("#tab-year:checked ~ .tabs label[for='tab-year'] { background: #f97316; color: #fff; }\n");

    // Month tabs
    for (i, (month, svg)) in month_svgs.iter().enumerate() {
        let id = format!("tab-m{}", i);
        let panel_id = format!("panel-m{}", i);
        // Extract month number from "YYYY-MM"
        let month_num: usize = month.split('-').nth(1)
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1);
        let label = month_labels.get(month_num.wrapping_sub(1)).unwrap_or(&"?");

        tab_inputs.push_str(&format!(
            r#"<input type="radio" name="tabs" id="{}">"#, id
        ));
        tab_inputs.push('\n');

        tab_labels.push_str(&format!(
            r#"<label for="{}">{}</label>"#, id, label
        ));
        tab_labels.push('\n');

        let month_html_link = format!("{}-{}.html", domain, month);
        tab_panels.push_str(&format!(
            r#"<div class="panel" id="{panel_id}">{svg}<p style="text-align:center;margin-top:12px"><a href="{link}">Full month report →</a></p></div>"#,
            panel_id = panel_id,
            svg = svg,
            link = month_html_link,
        ));
        tab_panels.push('\n');

        css_rules.push_str(&format!(
            "#{id}:checked ~ .panels #{panel_id} {{ display: block; }}\n"
        ));
        css_rules.push_str(&format!(
            "#{id}:checked ~ .tabs label[for='{id}'] {{ background: #f97316; color: #fff; }}\n"
        ));
    }

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>EdgeView Report: {domain} ({year})</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: system-ui, -apple-system, sans-serif; background: #f5f5f5; color: #2d2d2d; padding: 24px; }}
h1 {{ font-size: 20px; font-weight: 600; margin-bottom: 16px; text-align: center; }}
input[type="radio"] {{ display: none; }}
.tabs {{ display: flex; flex-wrap: wrap; gap: 4px; margin-bottom: 16px; justify-content: center; }}
.tabs label {{ padding: 8px 16px; background: #e5e5e5; border-radius: 6px; cursor: pointer; font-size: 14px; font-weight: 500; color: #2d2d2d; transition: background 0.15s, color 0.15s; }}
.tabs label:hover {{ background: #d4d4d4; }}
.panel {{ display: none; }}
.panel svg {{ max-width: 800px; width: 100%; height: auto; display: block; margin: 0 auto; }}
a {{ color: #f97316; text-decoration: none; font-weight: 500; }}
a:hover {{ text-decoration: underline; }}
{css_rules}
</style>
</head>
<body>
{tab_inputs}
<div class="tabs">
{tab_labels}
</div>
<div class="panels">
{tab_panels}
</div>
</body>
</html>"#,
        domain = domain,
        year = year,
        css_rules = css_rules,
        tab_inputs = tab_inputs,
        tab_labels = tab_labels,
        tab_panels = tab_panels,
    )
}
