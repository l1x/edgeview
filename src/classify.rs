pub fn classify_bot(user_agent: &str, bot_map: &std::collections::HashMap<String, String>) -> Option<String> {
    for (pattern, name) in bot_map {
        if user_agent.contains(pattern) {
            return Some(name.clone());
        }
    }
    None
}
