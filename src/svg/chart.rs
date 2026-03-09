use crate::svg::SvgDoc;

impl SvgDoc {
    pub fn add_bar_chart(&mut self, title: &str, bars: &[(String, u64)]) {
        self.add_section_title(title);
        // Basic bar chart implementation
        for (label, value) in bars {
            self.content.push_str(&format!(
                r#"<rect x="{x}" y="{y}" width="{width}" height="20" fill="{fill}" />
                   <text x="{tx}" y="{ty}" class="text">{label} ({value})</text>"#,
                x = self.theme.spacing,
                y = self.y_cursor,
                width = (*value as f64).min(self.width - self.theme.spacing * 2.0),
                fill = self.theme.bar_pastel,
                tx = self.theme.spacing,
                ty = self.y_cursor + 15.0,
                label = label,
                value = value
            ));
            self.y_cursor += 30.0;
        }
        self.y_cursor += self.theme.spacing;
    }
}
