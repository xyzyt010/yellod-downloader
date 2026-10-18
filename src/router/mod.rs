fn is_direct_media_url(url: &str) -> bool {
    let lower = url.to_lowercase();
    lower.ends_with(".mp4") || lower.ends_with(".mp3") ||
    lower.ends_with(".mkv") || lower.ends_with(".webm") ||
    lower.ends_with(".m4a") || lower.ends_with(".flac") ||
    lower.ends_with(".ogg") || lower.ends_with(".wav")
}

pub fn detect_tool(_url: &str, _cobalt_enabled: bool) -> String {
    "ytdlp".into()
}

pub fn detect_tool_full(url: &str, cobalt_enabled: bool) -> String {
    if is_direct_media_url(url) {
        return "aria2".into();
    }
    detect_tool(url, cobalt_enabled)
}
