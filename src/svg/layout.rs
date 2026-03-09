use crate::svg::SvgDoc;

impl SvgDoc {
    pub fn add_space(&mut self, height: f64) {
        self.y_cursor += height;
    }

    pub fn add_section_title(&mut self, title: &str) {
        self.content.push_str(&format!(
            r#"<text x="{x}" y="{y}" class="title">{title}</text>"#,
            x = self.theme.spacing,
            y = self.y_cursor + 30.0,
            title = title
        ));
        self.y_cursor += 50.0;
    }
}
