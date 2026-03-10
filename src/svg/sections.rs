use crate::svg::SvgDoc;
use crate::model::*;

fn human_bot_pct(hits: u64, bot_hits: u64) -> (u64, u64) {
    if hits == 0 {
        return (100, 0);
    }
    let bot_hits = bot_hits.min(hits);
    let bot_pct = (bot_hits * 100) / hits;
    let human_pct = 100 - bot_pct;
    (human_pct, bot_pct)
}

impl SvgDoc {
    pub fn add_kpi_cards(&mut self, kpis: &[Kpi]) {
        let card_width = (self.width - self.theme.spacing * (kpis.len() as f64 + 1.0)) / kpis.len() as f64;
        let mut x_offset = self.theme.spacing;
        
        for kpi in kpis {
            self.content.push_str(&format!(
                r#"<rect x="{x}" y="{y}" width="{w}" height="100" rx="8" fill="{bg}" stroke="{border}" />
                   <text x="{tx}" y="{ty}" class="text-mono">{label}</text>
                   <text x="{tx}" y="{vy}" class="accent">{value}</text>"#,
                x = x_offset,
                y = self.y_cursor,
                w = card_width,
                bg = self.theme.card_bg,
                border = self.theme.border,
                tx = x_offset + 15.0,
                ty = self.y_cursor + 30.0,
                label = kpi.label,
                vy = self.y_cursor + 60.0,
                value = kpi.value
            ));
            if let Some(change) = &kpi.change {
                self.content.push_str(&format!(
                    r#"<text x="{tx}" y="{cy}" class="text-muted">{change}</text>"#,
                    tx = x_offset + 15.0,
                    cy = self.y_cursor + 85.0,
                    change = change,
                ));
            }
            x_offset += card_width + self.theme.spacing;
        }
        self.y_cursor += 120.0;
    }

    pub fn add_daily_traffic_section(&mut self, traffic: &[DailyTraffic]) {
        self.add_section_title("Daily Traffic");
        if traffic.is_empty() { return; }

        let max_hits = traffic.iter().map(|t| t.hits).max().unwrap_or(1) as f64;
        let chart_height = 200.0;
        let chart_width = self.width - self.theme.spacing * 2.0;
        let bar_width = (chart_width / traffic.len() as f64) * 0.8;
        let gap = (chart_width / traffic.len() as f64) * 0.2;

        for (i, t) in traffic.iter().enumerate() {
            let h = (t.hits as f64 / max_hits) * chart_height;
            let x = self.theme.spacing + i as f64 * (bar_width + gap);
            let y = self.y_cursor + chart_height - h;

            self.content.push_str(&format!(
                r#"<rect x="{x}" y="{y}" width="{w}" height="{h}" fill="{fill}" rx="2" />"#,
                x = x,
                y = y,
                w = bar_width,
                h = h,
                fill = self.theme.bar_pastel
            ));
        }
        self.y_cursor += chart_height + 40.0;
    }

    pub fn add_top_content_pages(&mut self, pages: &[PageHits]) {
        self.add_section_title("Top Content");
        for page in pages {
            let (human_pct, bot_pct) = human_bot_pct(page.hits, page.bot_hits);
            self.content.push_str(&format!(
                r#"<text x="{x}" y="{y}" class="text">{path}</text>
                   <text x="{rx}" y="{y}" class="text-mono" text-anchor="end">{hits} hits · {visitors} visitors · {human_pct}% human · {bot_pct}% bot</text>"#,
                x = self.theme.spacing,
                y = self.y_cursor + 20.0,
                path = page.path,
                rx = self.width - self.theme.spacing,
                hits = page.hits,
                visitors = page.visitors,
                human_pct = human_pct,
                bot_pct = bot_pct,
            ));
            self.y_cursor += 30.0;
        }
        self.y_cursor += self.theme.spacing;
    }

    pub fn add_static_assets(&mut self, pages: &[PageHits]) {
        self.add_section_title("Static Assets");
        for page in pages {
            let (human_pct, bot_pct) = human_bot_pct(page.hits, page.bot_hits);
            self.content.push_str(&format!(
                r#"<text x="{x}" y="{y}" class="text">{path}</text>
                   <text x="{rx}" y="{y}" class="text-mono" text-anchor="end">{hits} hits · {visitors} visitors · {human_pct}% human · {bot_pct}% bot</text>"#,
                x = self.theme.spacing,
                y = self.y_cursor + 20.0,
                path = page.path,
                rx = self.width - self.theme.spacing,
                hits = page.hits,
                visitors = page.visitors,
                human_pct = human_pct,
                bot_pct = bot_pct,
            ));
            self.y_cursor += 30.0;
        }
        self.y_cursor += self.theme.spacing;
    }

    pub fn add_bot_activity_section(&mut self, bots: &[CrawlerStats]) {
        self.add_section_title("Bot Activity");
        for bot in bots.iter().take(5) {
            self.content.push_str(&format!(
                r#"<text x="{x}" y="{y}" class="text">{name}</text>
                   <text x="{rx}" y="{y}" class="text-mono" text-anchor="end">{hits} hits</text>"#,
                x = self.theme.spacing,
                y = self.y_cursor + 20.0,
                name = bot.bot_name,
                rx = self.width - self.theme.spacing,
                hits = bot.hits
            ));
            self.y_cursor += 30.0;
        }
        self.y_cursor += self.theme.spacing;
    }

    pub fn add_crawl_gap_section(&mut self, missing_urls: &[String]) {
        self.add_section_title("Indexing Gaps (Sitemap vs Googlebot)");
        if missing_urls.is_empty() {
            self.content.push_str(&format!(
                r#"<text x="{x}" y="{y}" class="text" fill="{{#}}22863a">✓ All sitemap URLs have been crawled by Googlebot.</text>"#,
                x = self.theme.spacing,
                y = self.y_cursor + 20.0
            ));
            self.y_cursor += 40.0;
        } else {
            for url in missing_urls.iter().take(15) {
                self.content.push_str(&format!(
                    r#"<text x="{x}" y="{y}" class="text" fill="{{#}}d73a49">○ {url}</text>"#,
                    x = self.theme.spacing,
                    y = self.y_cursor + 20.0,
                    url = url
                ));
                self.y_cursor += 25.0;
            }
            if missing_urls.len() > 15 {
                self.content.push_str(&format!(
                    r#"<text x="{x}" y="{y}" class="text-muted">... and {more} more</text>"#,
                    x = self.theme.spacing + 20.0,
                    y = self.y_cursor + 20.0,
                    more = missing_urls.len() - 15
                ));
                self.y_cursor += 30.0;
            }
        }
        self.y_cursor += self.theme.spacing;
    }
}
