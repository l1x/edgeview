pub fn classify_bot(user_agent: &str, bot_map: &[(String, String)]) -> Option<String> {
    for (pattern, name) in bot_map {
        if user_agent.contains(pattern) {
            return Some(name.clone());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_bot_map() -> Vec<(String, String)> {
        vec![("Googlebot".to_string(), "Google".to_string())]
    }

    #[test]
    fn test_classify_googlebot() {
        let bots = test_bot_map();
        let ua = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
        assert_eq!(classify_bot(ua, &bots), Some("Google".to_string()));
    }

    #[test]
    fn test_classify_unknown() {
        let bots = test_bot_map();
        let ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)";
        assert_eq!(classify_bot(ua, &bots), None);
    }
}
