pub fn classify_bot(
    user_agent: &str,
    bot_map: &std::collections::HashMap<String, String>,
) -> Option<String> {
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
    use std::collections::HashMap;

    #[test]
    fn test_classify_googlebot() {
        let mut bot_map = HashMap::new();
        bot_map.insert("Googlebot".to_string(), "Google".to_string());

        let ua = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
        assert_eq!(classify_bot(ua, &bot_map), Some("Google".to_string()));
    }

    #[test]
    fn test_classify_unknown() {
        let mut bot_map = HashMap::new();
        bot_map.insert("Googlebot".to_string(), "Google".to_string());

        let ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)";
        assert_eq!(classify_bot(ua, &bot_map), None);
    }
}
