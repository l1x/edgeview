use crate::model::{human_bot_pct, *};
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

    let (hits_human_pct, hits_bot_pct) = human_bot_pct(total_hits, total_bot_hits);

    let bot_visitors = if total_hits > 0 {
        (total_visitors as f64 * total_bot_hits as f64 / total_hits as f64) as u64
    } else {
        0
    };
    let (vis_human_pct, vis_bot_pct) = human_bot_pct(total_visitors, bot_visitors);

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
        .filter(|p| p.category == "page")
        .take(TOP_PAGES_LIMIT)
        .cloned()
        .collect();
    let static_assets: Vec<_> = pages
        .iter()
        .filter(|p| p.category != "page")
        .cloned()
        .collect();

    doc.add_top_content_pages(&content_pages);
    doc.add_static_assets(&static_assets);
    doc.add_bot_activity_section(bot_stats);

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
    all_domains: &[String],
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

    let site_nav = if all_domains.len() > 1 {
        let mut nav = String::from("<div class=\"site-nav\">\n");
        for d in all_domains {
            if d == domain {
                nav.push_str(&format!(
                    "<button class=\"active\">{}</button>\n",
                    html_escape(d)
                ));
            } else {
                nav.push_str(&format!(
                    "<a href=\"{}-{}.html\">{}</a>\n",
                    d,
                    month,
                    html_escape(d)
                ));
            }
        }
        nav.push_str("</div>\n");
        nav
    } else {
        String::new()
    };

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
.site-nav {{ display: flex; flex-wrap: wrap; gap: 4px; margin-bottom: 16px; justify-content: center; }}
.site-nav a, .site-nav button {{ padding: 8px 16px; background: #e5e5e5; border: none; border-radius: 6px; cursor: pointer; font-size: 14px; font-weight: 500; color: #2d2d2d; transition: background 0.15s, color 0.15s; text-decoration: none; font-family: inherit; }}
.site-nav a:hover, .site-nav button:hover {{ background: #d4d4d4; }}
.site-nav button.active {{ background: #f97316; color: #fff; }}
.tabs {{ display: flex; flex-wrap: wrap; gap: 4px; margin-bottom: 16px; justify-content: center; }}
.tabs label {{ padding: 8px 16px; background: #e5e5e5; border-radius: 6px; cursor: pointer; font-size: 14px; font-weight: 500; color: #2d2d2d; transition: background 0.15s, color 0.15s; }}
.tabs label:hover {{ background: #d4d4d4; }}
.panel {{ display: none; }}
.panel svg {{ max-width: 800px; width: 100%; height: auto; display: block; margin: 0 auto; }}
{css_rules}
</style>
</head>
<body>
{site_nav}{tab_inputs}
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
        site_nav = site_nav,
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
    pub referer_stats: Vec<RefererStats>,
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

fn render_referer_table(referers: &[RefererStats], limit: usize) -> String {
    let mut h = String::from(concat!(
        "<table class=\"data-table\"><thead><tr>",
        "<th class=\"col-path\">Referer</th><th>Hits</th>",
        "</tr></thead><tbody>"
    ));
    for r in referers.iter().take(limit) {
        h.push_str("<tr><td class=\"path-cell\">");
        h.push_str(&html_escape(&r.referer));
        h.push_str("</td><td>");
        h.push_str(&fmt_num(r.hits));
        h.push_str("</td></tr>");
    }
    h.push_str("</tbody></table>");
    h
}

/// Render static assets grouped by category (CSS, JS, Images, Fonts, Data) as separate cards.
fn render_grouped_static_cards(pages: &[PageHits]) -> String {
    let groups: &[(&str, &str)] = &[
        ("css", "CSS"),
        ("js", "JavaScript"),
        ("image", "Images"),
        ("font", "Fonts"),
        ("data", "Data"),
    ];
    let mut html = String::new();
    for &(cat, label) in groups {
        let items: Vec<&PageHits> = pages.iter().filter(|p| p.category == cat).collect();
        if items.is_empty() {
            continue;
        }
        let title = format!("Static — {}", label);
        let table = render_page_table_ref(&items, items.len());
        html.push_str(&card(&title, &table));
    }
    html
}

/// Like render_page_table but takes &[&PageHits].
fn render_page_table_ref(pages: &[&PageHits], limit: usize) -> String {
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
    all_domains: &[String],
    available_years: &[i32],
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
.site-nav{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:16px}
.year-nav{display:flex;gap:6px;margin-bottom:16px}
.year-btn{flex:1;font-size:15px;padding:10px;text-align:center;text-decoration:none}
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

    // Site selector (only when multiple sites)
    if all_domains.len() > 1 {
        html.push_str("<div class=\"site-nav\">\n");
        for d in all_domains {
            if d == domain {
                html.push_str(&format!(
                    "<button class=\"nav-btn active\">{}</button>\n",
                    html_escape(d)
                ));
            } else {
                html.push_str(&format!(
                    "<a class=\"nav-btn\" href=\"{}-{}.html\">{}</a>\n",
                    d,
                    year,
                    html_escape(d)
                ));
            }
        }
        html.push_str("</div>\n");
    }

    html.push_str("<div class=\"site-label\">");
    html.push_str(domain);
    html.push_str("</div>\n");

    // Year selector
    let year_num: i32 = year.parse().unwrap_or(2026);
    html.push_str("<div class=\"year-nav\">\n");
    for &y in available_years {
        if y == year_num {
            html.push_str(&format!(
                "<button class=\"nav-btn year-btn active\" data-level=\"year\" data-panel=\"panel-year\">{}</button>\n",
                y
            ));
        } else {
            html.push_str(&format!(
                "<a class=\"nav-btn year-btn\" href=\"{}-{}.html\">{}</a>\n",
                domain, y, y
            ));
        }
    }
    html.push_str("</div>\n");

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
            .filter(|p| p.category == "page")
            .take(TOP_PAGES_LIMIT)
            .cloned()
            .collect();
        if !content_pages.is_empty() {
            html.push_str(&card(
                "Top Content",
                &render_page_table(&content_pages, TOP_PAGES_LIMIT),
            ));
        }

        let static_pages: Vec<PageHits> = year_report
            .top_pages
            .iter()
            .filter(|p| p.category != "page")
            .cloned()
            .collect();
        if !static_pages.is_empty() {
            html.push_str(&render_grouped_static_cards(&static_pages));
        }

        html.push_str(&card(
            "Bot Activity",
            &render_bot_table(&year_report.bot_stats, TOP_BOTS_HTML_LIMIT),
        ));
        if !year_report.referer_stats.is_empty() {
            html.push_str(&card(
                "Top Referers",
                &render_referer_table(&year_report.referer_stats, TOP_REFERERS_LIMIT),
            ));
        }
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
            .filter(|p| p.category == "page")
            .take(TOP_PAGES_LIMIT)
            .cloned()
            .collect();
        if !content_pages.is_empty() {
            html.push_str(&card(
                "Top Content",
                &render_page_table(&content_pages, TOP_PAGES_LIMIT),
            ));
        }

        let static_pages: Vec<PageHits> = s
            .top_pages
            .iter()
            .filter(|p| p.category != "page")
            .cloned()
            .collect();
        if !static_pages.is_empty() {
            html.push_str(&render_grouped_static_cards(&static_pages));
        }

        html.push_str(&card(
            "Bot Activity",
            &render_bot_table(&s.bot_stats, TOP_BOTS_SVG_LIMIT),
        ));
        if !s.referer_stats.is_empty() {
            html.push_str(&card(
                "Top Referers",
                &render_referer_table(&s.referer_stats, TOP_REFERERS_LIMIT),
            ));
        }

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
                .filter(|p| p.category == "page")
                .take(TOP_PAGES_LIMIT)
                .cloned()
                .collect();
            if !content_pages.is_empty() {
                html.push_str(&card(
                    "Top Content",
                    &render_page_table(&content_pages, TOP_PAGES_LIMIT),
                ));
            }

            let static_pages: Vec<PageHits> = day
                .pages
                .iter()
                .filter(|p| p.category != "page")
                .cloned()
                .collect();
            if !static_pages.is_empty() {
                html.push_str(&render_grouped_static_cards(&static_pages));
            }

            html.push_str(&card(
                "Bot Activity",
                &render_bot_table(&day.bot_stats, TOP_BOTS_SVG_LIMIT),
            ));
            if !day.referer_stats.is_empty() {
                html.push_str(&card(
                    "Top Referers",
                    &render_referer_table(&day.referer_stats, TOP_REFERERS_LIMIT),
                ));
            }
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
