use crate::model::*;
use crate::svg::theme::GREY_ORANGE;
use crate::svg::SvgDoc;
use chrono::{Datelike, NaiveDate};
use std::collections::HashMap;

/// Build a daily SVG matching the month view layout.
pub fn build_daily_svg(
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

    let hits_bot_pct = if total_hits > 0 {
        (total_bot_hits * 100) / total_hits
    } else {
        0
    };
    let hits_human_pct = 100 - hits_bot_pct;

    let bot_visitors = if total_hits > 0 {
        (total_visitors as f64 * total_bot_hits as f64 / total_hits as f64) as u64
    } else {
        0
    };
    let vis_bot_pct = if total_visitors > 0 {
        (bot_visitors * 100) / total_visitors
    } else {
        0
    };
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

    let content_pages: Vec<_> = pages
        .iter()
        .filter(|p| p.category == "article" || p.category == "page")
        .take(15)
        .cloned()
        .collect();
    let static_assets: Vec<_> = pages
        .iter()
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

    let hits_bot_pct = if summary.total_hits > 0 {
        (summary.total_bot_hits * 100) / summary.total_hits
    } else {
        0
    };
    let hits_human_pct = 100 - hits_bot_pct;
    let vis_bot_pct = if summary.total_visitors > 0 {
        (summary.total_bot_visitors * 100) / summary.total_visitors
    } else {
        0
    };
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

    let content_pages: Vec<_> = summary
        .top_pages
        .iter()
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
    css_rules.push_str(
        "#tab-month:checked ~ .tabs label[for='tab-month'] { background: #f97316; color: #fff; }\n",
    );

    // Day tabs
    let empty_hourly = Vec::new();
    for (i, date_str) in dates.iter().enumerate() {
        let id = format!("tab-d{}", i);
        let panel_id = format!("panel-d{}", i);
        let label = date_tab_label(date_str);

        tab_inputs.push_str(&format!(r#"<input type="radio" name="tabs" id="{}">"#, id));
        tab_inputs.push('\n');

        tab_labels.push_str(&format!(r#"<label for="{}">{}</label>"#, id, label));
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

// ─── Year report: data structures ───

/// Data for one day in the year report.
pub struct DayHtmlData {
    pub date: String, // "2026-03-01"
    pub hits: u64,
    pub visitors: u64,
    pub bot_hits: u64,
    pub pages: Vec<PageHits>,
    pub hourly: Vec<HourlyTraffic>,
    pub bot_stats: Vec<CrawlerStats>,
}

/// Data for one month in the year report.
pub struct MonthHtmlData {
    pub month: String, // "2026-03"
    pub summary: MonthSummary,
    pub days: Vec<DayHtmlData>,
}

// ─── Year report: HTML rendering helpers ───

fn fmt_num(n: u64) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(b as char);
    }
    result
}

fn human_bot_pct(hits: u64, bot_hits: u64) -> (u64, u64) {
    if hits == 0 {
        return (100, 0);
    }
    let bot_pct = (bot_hits.min(hits) * 100) / hits;
    (100 - bot_pct, bot_pct)
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn render_kpi_cards(kpis: &[(&str, u64, Option<String>)]) -> String {
    let mut h = String::from("<div class=\"kpi-row\">");
    for (label, value, sub) in kpis {
        h.push_str("<div class=\"kpi-card\"><div class=\"kpi-label\">");
        h.push_str(label);
        h.push_str("</div><div class=\"kpi-value\">");
        h.push_str(&fmt_num(*value));
        h.push_str("</div>");
        if let Some(s) = sub {
            h.push_str("<div class=\"kpi-sub\">");
            h.push_str(s);
            h.push_str("</div>");
        }
        h.push_str("</div>");
    }
    h.push_str("</div>");
    h
}

fn render_bar_chart(bars: &[(String, u64)]) -> String {
    let max_val = bars.iter().map(|(_, v)| *v).max().unwrap_or(1).max(1);
    let mut h = String::from("<div class=\"bar-chart\">");
    for (label, value) in bars {
        let pct = (*value as f64 / max_val as f64) * 100.0;
        h.push_str("<div class=\"bar-group\"><div class=\"bar\" style=\"height:");
        h.push_str(&format!("{pct:.1}"));
        h.push_str("%\" title=\"");
        h.push_str(&fmt_num(*value));
        h.push_str(" hits\"></div><div class=\"bar-label\">");
        h.push_str(label);
        h.push_str("</div></div>");
    }
    h.push_str("</div>");
    h
}

fn render_page_table(pages: &[PageHits], limit: usize) -> String {
    let mut h = String::from(concat!(
        "<table class=\"data-table\"><thead><tr>",
        "<th class=\"col-path\">Path</th><th>Hits</th>",
        "<th>Visitors</th><th>Human</th><th>Bot</th>",
        "</tr></thead><tbody>"
    ));
    for page in pages.iter().take(limit) {
        let (hp, bp) = human_bot_pct(page.hits, page.bot_hits);
        h.push_str("<tr><td class=\"path-cell\">");
        h.push_str(&html_escape(&page.path));
        h.push_str("</td><td>");
        h.push_str(&fmt_num(page.hits));
        h.push_str("</td><td>");
        h.push_str(&fmt_num(page.visitors));
        h.push_str("</td><td>");
        h.push_str(&hp.to_string());
        h.push_str("%</td><td>");
        h.push_str(&bp.to_string());
        h.push_str("%</td></tr>");
    }
    h.push_str("</tbody></table>");
    h
}

fn render_bot_table(bots: &[CrawlerStats], limit: usize) -> String {
    let mut h = String::from(concat!(
        "<table class=\"data-table\"><thead><tr>",
        "<th>Bot</th><th>Hits</th>",
        "</tr></thead><tbody>"
    ));
    for bot in bots.iter().take(limit) {
        h.push_str("<tr><td>");
        h.push_str(&html_escape(&bot.bot_name));
        h.push_str("</td><td>");
        h.push_str(&fmt_num(bot.hits));
        h.push_str("</td></tr>");
    }
    h.push_str("</tbody></table>");
    h
}

fn card(title: &str, content: &str) -> String {
    let mut h = String::from("<div class=\"card\"><h3 class=\"card-title\">");
    h.push_str(title);
    h.push_str("</h3>");
    h.push_str(content);
    h.push_str("</div>");
    h
}

// ─── Year report: main generator ───

/// Generate a self-contained year HTML dashboard with left sidebar navigation.
pub fn generate_year_report(
    domain: &str,
    year: &str,
    year_report: &YearReport,
    months: &[MonthHtmlData],
) -> String {
    let month_labels = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];

    let mut html = String::with_capacity(2 * 1024 * 1024);

    // ── Head ──
    html.push_str("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n");
    html.push_str("<meta charset=\"utf-8\">\n");
    html.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
    html.push_str(&format!(
        "<title>EdgeView — {} ({})</title>\n",
        domain, year
    ));
    html.push_str("<style>\n");
    html.push_str(
        r#"*{margin:0;padding:0;box-sizing:border-box}
body{font-family:system-ui,-apple-system,sans-serif;background:#f0f0f0;color:#1a1a1a}
.layout{display:grid;grid-template-columns:260px 1fr;min-height:100vh}
.sidebar{position:sticky;top:0;height:100vh;overflow-y:auto;padding:24px 16px;background:#fff;border-right:1px solid #e5e7eb}
.content{padding:32px;overflow-y:auto}
.brand{font-size:22px;font-weight:700;color:#f97316;margin-bottom:4px;letter-spacing:-0.5px}
.site-label{font-size:13px;color:#6b7280;margin-bottom:24px;word-break:break-all}
.year-btn{width:100%;margin-bottom:16px;font-size:15px;padding:10px}
.month-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin-bottom:16px}
.cal-wrap{border-top:1px solid #e5e7eb;padding-top:16px;display:none}
.cal-header{text-align:center;font-weight:600;font-size:14px;margin-bottom:8px;color:#1a1a1a}
.cal-grid{display:grid;grid-template-columns:repeat(7,1fr);gap:2px}
.cal-hdr{text-align:center;font-size:11px;font-weight:600;color:#9ca3af;padding:4px 0}
.cal-spacer{}
.cal-grid .nav-btn{padding:6px 2px;font-size:12px}
.nav-btn{padding:8px 12px;background:#f5f5f5;border:none;border-radius:8px;cursor:pointer;font-size:13px;font-weight:500;color:#1a1a1a;transition:all .15s;font-family:inherit}
.nav-btn:hover{background:#e5e5e5}
.nav-btn.active{background:#f97316;color:#fff}
.panel{display:none}
.panel.active{display:block}
.kpi-row{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-bottom:24px}
.kpi-card{background:#fff;border-radius:12px;padding:20px 24px;box-shadow:0 1px 3px rgba(0,0,0,.08)}
.kpi-label{font-size:13px;color:#6b7280;font-weight:500;margin-bottom:8px}
.kpi-value{font-size:28px;font-weight:700;color:#f97316;margin-bottom:4px}
.kpi-sub{font-size:12px;color:#9ca3af}
.card{background:#fff;border-radius:12px;padding:24px;box-shadow:0 1px 3px rgba(0,0,0,.08);margin-bottom:20px}
.card-title{font-size:15px;font-weight:600;margin-bottom:16px;color:#1a1a1a}
.bar-chart{display:flex;align-items:flex-end;gap:3px;height:200px;padding-bottom:28px;position:relative;justify-content:flex-start}
.bar-group{flex:0 0 auto;width:32px;display:flex;flex-direction:column;align-items:center;height:100%;justify-content:flex-end}
.bar{width:100%;background:#f97316;border-radius:3px 3px 0 0;min-height:2px;transition:opacity .15s;cursor:default}
.bar:hover{opacity:.75}
.bar-label{font-size:11px;color:#6b7280;margin-top:6px;white-space:nowrap}
.data-table{width:100%;border-collapse:collapse;font-size:13px}
.data-table th{text-align:left;padding:8px 12px;color:#6b7280;font-weight:500;border-bottom:1px solid #e5e7eb;font-size:12px;text-transform:uppercase;letter-spacing:.5px}
.data-table td{padding:10px 12px;border-bottom:1px solid #f3f4f6}
.data-table tr:last-child td{border-bottom:none}
.data-table tbody tr:hover{background:#f9fafb}
.path-cell{max-width:420px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-family:ui-monospace,SFMono-Regular,monospace;font-size:12px}
.month-link{display:block;text-align:center;margin-top:16px;font-size:13px}
a{color:#f97316;text-decoration:none;font-weight:500}
a:hover{text-decoration:underline}
@media(max-width:768px){
  .layout{grid-template-columns:1fr}
  .sidebar{position:static;height:auto;border-right:none;border-bottom:1px solid #e5e7eb}
  .kpi-row{grid-template-columns:1fr}
}
"#,
    );
    html.push_str("</style>\n</head>\n<body>\n");

    // ── Layout ──
    html.push_str("<div class=\"layout\">\n");

    // ── Sidebar ──
    html.push_str("<nav class=\"sidebar\">\n");
    html.push_str("<div class=\"brand\">EdgeView</div>\n");
    html.push_str("<div class=\"site-label\">");
    html.push_str(domain);
    html.push_str("</div>\n");
    html.push_str(&format!(
        "<button class=\"nav-btn year-btn active\" data-level=\"year\" data-panel=\"panel-year\">{}</button>\n",
        year
    ));

    // Month grid (3×4)
    html.push_str("<div class=\"month-grid\" id=\"month-grid\">\n");
    for (i, md) in months.iter().enumerate() {
        let month_num: usize = md
            .month
            .split('-')
            .nth(1)
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1);
        let label = month_labels.get(month_num.wrapping_sub(1)).unwrap_or(&"?");
        html.push_str(&format!(
            "<button class=\"nav-btn\" data-level=\"month\" data-month=\"{i}\" data-panel=\"panel-m{i}\">{label}</button>\n",
            i = i, label = label,
        ));
    }
    html.push_str("</div>\n");

    // Calendar wrap
    html.push_str("<div class=\"cal-wrap\" id=\"cal-wrap\">\n");
    html.push_str("<div class=\"cal-header\" id=\"cal-header\"></div>\n");
    html.push_str("<div class=\"cal-grid\">");
    html.push_str("<span class=\"cal-hdr\">Mo</span><span class=\"cal-hdr\">Tu</span>");
    html.push_str("<span class=\"cal-hdr\">We</span><span class=\"cal-hdr\">Th</span>");
    html.push_str("<span class=\"cal-hdr\">Fr</span><span class=\"cal-hdr\">Sa</span>");
    html.push_str("<span class=\"cal-hdr\">Su</span></div>\n");

    // Day groups (one per month)
    for (i, md) in months.iter().enumerate() {
        html.push_str(&format!(
            "<div class=\"day-group cal-grid\" id=\"days-{}\" style=\"display:none\">",
            i
        ));
        let first_date_str = format!("{}-01", md.month);
        let offset = NaiveDate::parse_from_str(&first_date_str, "%Y-%m-%d")
            .map(|d| d.weekday().num_days_from_monday() as usize)
            .unwrap_or(0);
        for _ in 0..offset {
            html.push_str("<span class=\"cal-spacer\"></span>");
        }
        for (j, day) in md.days.iter().enumerate() {
            let day_num = day
                .date
                .split('-')
                .nth(2)
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(0);
            html.push_str(&format!(
                "<button class=\"nav-btn\" data-level=\"day\" data-panel=\"panel-m{i}-d{j}\">{day}</button>",
                i = i, j = j, day = day_num,
            ));
        }
        html.push_str("</div>\n");
    }
    html.push_str("</div>\n"); // cal-wrap
    html.push_str("</nav>\n"); // sidebar

    // ── Content panels ──
    html.push_str("<main class=\"content\">\n");

    // Year panel
    {
        html.push_str("<div class=\"panel active\" id=\"panel-year\">\n");
        let (hp, bp) = human_bot_pct(year_report.total_hits, year_report.total_bot_hits);
        let (vhp, vbp) = human_bot_pct(year_report.total_visitors, year_report.total_bot_visitors);
        html.push_str(&render_kpi_cards(&[
            (
                "Total Hits",
                year_report.total_hits,
                Some(format!("{hp}% human · {bp}% bot")),
            ),
            (
                "Unique Visitors",
                year_report.total_visitors,
                Some(format!("{vhp}% human · {vbp}% bot")),
            ),
            ("Active Bots", year_report.bot_stats.len() as u64, None),
        ]));

        let bars: Vec<(String, u64)> = year_report
            .monthly
            .iter()
            .enumerate()
            .map(|(i, m)| (month_labels[i.min(11)].to_string(), m.hits))
            .collect();
        html.push_str(&card("Monthly Traffic", &render_bar_chart(&bars)));

        let content_pages: Vec<PageHits> = year_report
            .top_pages
            .iter()
            .filter(|p| p.category == "article" || p.category == "page")
            .take(15)
            .cloned()
            .collect();
        if !content_pages.is_empty() {
            html.push_str(&card("Top Content", &render_page_table(&content_pages, 15)));
        }

        let static_pages: Vec<PageHits> = year_report
            .top_pages
            .iter()
            .filter(|p| p.category == "static")
            .cloned()
            .collect();
        if !static_pages.is_empty() {
            html.push_str(&card(
                "Static Assets",
                &render_page_table(&static_pages, 15),
            ));
        }

        html.push_str(&card(
            "Bot Activity",
            &render_bot_table(&year_report.bot_stats, 10),
        ));
        html.push_str("</div>\n");
    }

    // Month panels
    for (i, md) in months.iter().enumerate() {
        html.push_str(&format!("<div class=\"panel\" id=\"panel-m{}\">\n", i));
        let s = &md.summary;
        let (hp, bp) = human_bot_pct(s.total_hits, s.total_bot_hits);
        let (vhp, vbp) = human_bot_pct(s.total_visitors, s.total_bot_visitors);
        html.push_str(&render_kpi_cards(&[
            (
                "Total Hits",
                s.total_hits,
                Some(format!("{hp}% human · {bp}% bot")),
            ),
            (
                "Unique Visitors",
                s.total_visitors,
                Some(format!("{vhp}% human · {vbp}% bot")),
            ),
            ("Active Bots", s.bot_stats.len() as u64, None),
        ]));

        let bars: Vec<(String, u64)> = s
            .daily
            .iter()
            .map(|d| (d.date.format("%-d").to_string(), d.hits))
            .collect();
        html.push_str(&card("Daily Traffic", &render_bar_chart(&bars)));

        let content_pages: Vec<PageHits> = s
            .top_pages
            .iter()
            .filter(|p| p.category == "article" || p.category == "page")
            .take(10)
            .cloned()
            .collect();
        if !content_pages.is_empty() {
            html.push_str(&card("Top Content", &render_page_table(&content_pages, 10)));
        }

        html.push_str(&card("Bot Activity", &render_bot_table(&s.bot_stats, 5)));

        let month_html_link = format!("{}-{}.html", domain, md.month);
        html.push_str(&format!(
            "<a class=\"month-link\" href=\"{}\">Full month report &rarr;</a>\n",
            month_html_link
        ));
        html.push_str("</div>\n");
    }

    // Day panels
    for (i, md) in months.iter().enumerate() {
        for (j, day) in md.days.iter().enumerate() {
            html.push_str(&format!(
                "<div class=\"panel\" id=\"panel-m{}-d{}\">\n",
                i, j
            ));
            let (hp, bp) = human_bot_pct(day.hits, day.bot_hits);
            let bot_visitors = if day.hits > 0 {
                (day.visitors as f64 * day.bot_hits as f64 / day.hits as f64) as u64
            } else {
                0
            };
            let (vhp, vbp) = human_bot_pct(day.visitors, bot_visitors);
            html.push_str(&render_kpi_cards(&[
                (
                    "Total Hits",
                    day.hits,
                    Some(format!("{hp}% human · {bp}% bot")),
                ),
                (
                    "Unique Visitors",
                    day.visitors,
                    Some(format!("{vhp}% human · {vbp}% bot")),
                ),
                ("Bot Hits", day.bot_hits, None),
            ]));

            let bars: Vec<(String, u64)> = day
                .hourly
                .iter()
                .map(|t| (t.hour.to_string(), t.hits))
                .collect();
            html.push_str(&card("Hourly Traffic (UTC)", &render_bar_chart(&bars)));

            let content_pages: Vec<PageHits> = day
                .pages
                .iter()
                .filter(|p| p.category == "article" || p.category == "page")
                .take(15)
                .cloned()
                .collect();
            if !content_pages.is_empty() {
                html.push_str(&card("Top Content", &render_page_table(&content_pages, 15)));
            }

            let static_pages: Vec<PageHits> = day
                .pages
                .iter()
                .filter(|p| p.category == "static")
                .cloned()
                .collect();
            if !static_pages.is_empty() {
                html.push_str(&card(
                    "Static Assets",
                    &render_page_table(&static_pages, 15),
                ));
            }

            html.push_str(&card("Bot Activity", &render_bot_table(&day.bot_stats, 5)));
            html.push_str("</div>\n");
        }
    }

    html.push_str("</main>\n"); // content
    html.push_str("</div>\n"); // layout

    // ── JavaScript ──
    html.push_str("<script>\n");
    html.push_str(
        r#"document.querySelector('.sidebar').addEventListener('click', function(e) {
  var btn = e.target.closest('.nav-btn');
  if (!btn) return;
  var level = btn.dataset.level;
  var panel = btn.dataset.panel;
  document.querySelectorAll('.panel').forEach(function(p) { p.classList.remove('active'); });
  document.getElementById(panel).classList.add('active');
  if (level === 'year') {
    document.getElementById('month-grid').querySelectorAll('.nav-btn').forEach(function(b) { b.classList.remove('active'); });
    document.getElementById('cal-wrap').style.display = 'none';
    btn.classList.add('active');
  } else if (level === 'month') {
    document.getElementById('month-grid').querySelectorAll('.nav-btn').forEach(function(b) { b.classList.remove('active'); });
    btn.classList.add('active');
    document.getElementById('cal-header').textContent = btn.textContent;
    var calWrap = document.getElementById('cal-wrap');
    calWrap.style.display = 'block';
    calWrap.querySelectorAll('.day-group').forEach(function(g) { g.style.display = 'none'; });
    calWrap.querySelectorAll('.nav-btn').forEach(function(b) { b.classList.remove('active'); });
    var group = document.getElementById('days-' + btn.dataset.month);
    if (group) group.style.display = 'grid';
  } else if (level === 'day') {
    btn.closest('.day-group').querySelectorAll('.nav-btn').forEach(function(b) { b.classList.remove('active'); });
    btn.classList.add('active');
  }
});
"#,
    );
    html.push_str("</script>\n");
    html.push_str("</body>\n</html>");

    html
}
