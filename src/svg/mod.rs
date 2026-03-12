pub mod layout;
pub mod sections;
pub mod theme;

use crate::svg::theme::Theme;

pub struct SvgDoc {
    pub width: f64,
    pub y_cursor: f64,
    pub content: String,
    pub theme: Theme,
}

impl SvgDoc {
    pub fn new(width: f64, theme: Theme) -> Self {
        Self {
            width,
            y_cursor: theme.spacing,
            content: String::new(),
            theme,
        }
    }

    pub fn finalize(self) -> String {
        format!(
            r#"<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">
    <rect width="{width}" height="{height}" fill="{bg}" />
    <style>
        .text {{ font-family: {font}; font-size: 14px; fill: {text_main}; }}
        .text-mono {{ font-family: {font_mono}; font-size: 12px; fill: {text_muted}; }}
        .title {{ font-family: {font}; font-size: 24px; font-weight: 600; fill: {text_main}; }}
        .accent {{ font-family: {font}; font-size: 24px; font-weight: 600; fill: {accent}; }}
    </style>
    {content}
</svg>"#,
            width = self.width,
            height = self.y_cursor + self.theme.spacing,
            bg = self.theme.canvas_bg,
            font = self.theme.font_family,
            font_mono = self.theme.font_mono,
            text_main = self.theme.text_main,
            text_muted = self.theme.text_muted,
            accent = self.theme.accent,
            content = self.content
        )
    }
}
