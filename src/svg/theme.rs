pub struct Theme {
    pub canvas_bg: &'static str,
    pub card_bg: &'static str,
    pub accent_primary: &'static str,
    pub text_main: &'static str,
    pub text_muted: &'static str,
    pub border: &'static str,
    pub bar_pastel: &'static str,
    pub font_family: &'static str,
    pub font_mono: &'static str,
    pub spacing: f64,
}

pub const SCANDINAVIAN: Theme = Theme {
    canvas_bg: "#fafafa",
    card_bg: "#ffffff",
    accent_primary: "#333333",
    text_main: "#1a1a1a",
    text_muted: "#666666",
    border: "#e5e5e5",
    bar_pastel: "#f0f4f8",
    font_family: "system-ui, -apple-system, sans-serif",
    font_mono: "'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace",
    spacing: 24.0,
};
