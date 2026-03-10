pub struct Theme {
    pub canvas_bg: &'static str,
    pub card_bg: &'static str,
    pub text_main: &'static str,
    pub text_muted: &'static str,
    pub border: &'static str,
    pub bar_pastel: &'static str,
    pub accent: &'static str,
    pub font_family: &'static str,
    pub font_mono: &'static str,
    pub spacing: f64,
}

pub const GREY_ORANGE: Theme = Theme {
    canvas_bg: "#f5f5f5",
    card_bg: "#ffffff",
    text_main: "#2d2d2d",
    text_muted: "#737373",
    border: "#d4d4d4",
    bar_pastel: "#f97316",
    accent: "#f97316",
    font_family: "system-ui, -apple-system, sans-serif",
    font_mono: "'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace",
    spacing: 24.0,
};
