//! Ghost-text completion.
//!
//! The index tier reads paths, identifiers, commands, and earlier prompts.
//! The model tier runs only when nothing is in flight and a small model is
//! configured. A busy chat returns no model completion.

pub const COMMANDS: &[&str] = &[
    "/help",
    "/channels",
    "/use",
    "/thread",
    "/threads",
    "/model",
    "/models",
    "/history",
    "/alias",
    "/suggest",
];

/// A model completion is allowed only for a long prefix, a configured model,
/// and a daemon that is not already answering.
pub fn allow_llm(prefix: &str, model: &str, inflight: u32) -> bool {
    prefix.trim().len() >= 10 && !model.trim().is_empty() && inflight == 0
}

/// Suffix to append, from data already on the node. `None` means try the
/// model or return empty.
pub fn index_suffix(
    prefix: &str,
    models: &[String],
    paths: &[String],
    symbols: &[String],
    prompts: &[String],
) -> Option<String> {
    let prefix = prefix.trim_end_matches(['\n', '\r']);
    if prefix.starts_with('/') {
        return command_suffix(prefix, models);
    }
    let tail = trailing_token(prefix);
    if tail.len() >= 3 {
        if let Some(suffix) = path_suffix(tail, paths) {
            return Some(suffix);
        }
    }
    if tail.len() >= 4 && looks_like_ident(tail) {
        if let Some(suffix) = symbol_suffix(tail, symbols) {
            return Some(suffix);
        }
    }
    if prefix.trim().len() >= 6 {
        return prompt_suffix(prefix.trim(), prompts);
    }
    None
}

pub fn identifier_tokens(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if is_ident_start(bytes[i]) {
            let start = i;
            i += 1;
            while i < bytes.len() && is_ident_cont(bytes[i]) {
                i += 1;
            }
            if i - start >= 4 {
                if let Ok(token) = std::str::from_utf8(&bytes[start..i]) {
                    out.push(token.to_string());
                }
            }
        } else {
            i += 1;
        }
    }
    out
}

/// Keep one short continuation. Drop a copy of the prefix when the model
/// repeats it.
pub fn llm_suffix(prefix: &str, raw: &str) -> String {
    let mut text = raw.trim();
    if let Some(rest) = text.strip_prefix(prefix.trim()) {
        text = rest.trim_start();
    }
    let line = text.lines().next().unwrap_or("").trim().trim_matches('"');
    let words: Vec<&str> = line.split_whitespace().take(15).collect();
    words.join(" ")
}

fn command_suffix(prefix: &str, models: &[String]) -> Option<String> {
    let rest = prefix.trim_start_matches('/');
    if let Some(model_prefix) = rest.strip_prefix("model ") {
        let want = model_prefix.trim();
        if want.is_empty() {
            return None;
        }
        let hit = models.iter().find(|id| {
            id.to_ascii_lowercase()
                .starts_with(&want.to_ascii_lowercase())
                && id.len() > want.len()
        })?;
        return Some(hit[want.len()..].to_string());
    }
    if prefix.contains(' ') {
        return None;
    }
    let hit = COMMANDS
        .iter()
        .filter(|cmd| cmd.starts_with(prefix) && cmd.len() > prefix.len())
        .min_by_key(|cmd| cmd.len())?;
    Some((*hit)[prefix.len()..].to_string())
}

fn trailing_token(prefix: &str) -> &str {
    let token = prefix.split_whitespace().last().unwrap_or(prefix);
    token.trim_start_matches('@')
}

fn path_suffix(tail: &str, paths: &[String]) -> Option<String> {
    let want = tail.to_ascii_lowercase();
    let mut hits: Vec<&String> = paths
        .iter()
        .filter(|path| {
            path.to_ascii_lowercase().starts_with(&want) && path.len() > tail.len()
        })
        .collect();
    if hits.is_empty() {
        return None;
    }
    hits.sort_by_key(|path| (path.len(), path.as_str()));
    let hit = hits[0];
    Some(hit[tail.len()..].to_string())
}

fn looks_like_ident(tail: &str) -> bool {
    tail.contains('_') || camel_hump(tail)
}

fn camel_hump(tail: &str) -> bool {
    let mut chars = tail.chars();
    let Some(mut prev) = chars.next() else {
        return false;
    };
    for ch in chars {
        if prev.is_ascii_lowercase() && ch.is_ascii_uppercase() {
            return true;
        }
        prev = ch;
    }
    false
}

fn symbol_suffix(tail: &str, symbols: &[String]) -> Option<String> {
    let want = tail.to_ascii_lowercase();
    let mut counts: Vec<(String, usize)> = Vec::new();
    for symbol in symbols {
        if symbol.len() <= tail.len() {
            continue;
        }
        if !symbol.to_ascii_lowercase().starts_with(&want) {
            continue;
        }
        if let Some(row) = counts.iter_mut().find(|(name, _)| name == symbol) {
            row.1 += 1;
        } else {
            counts.push((symbol.clone(), 1));
        }
    }
    counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.len().cmp(&b.0.len())));
    let (hit, _) = counts.first()?;
    Some(hit[tail.len()..].to_string())
}

fn prompt_suffix(prefix: &str, prompts: &[String]) -> Option<String> {
    let want = prefix.to_ascii_lowercase();
    let hit = prompts.iter().find(|prompt| {
        let text = prompt.trim();
        text.to_ascii_lowercase().starts_with(&want) && text.len() > prefix.len()
    })?;
    let text = hit.trim();
    Some(text[prefix.len()..].to_string())
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_cont(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_prefix_returns_only_the_suffix() {
        let suffix = index_suffix(
            "open src/exec",
            &[],
            &["src/executor.rs".into(), "src/lib.rs".into()],
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(suffix, "utor.rs");
    }

    #[test]
    fn the_most_common_identifier_wins() {
        let symbols = vec![
            "verify_task".into(),
            "verify_task_sig".into(),
            "verify_task_sig".into(),
        ];
        let suffix = index_suffix("call verify_task", &[], &[], &symbols, &[]).unwrap();
        assert_eq!(suffix, "_sig");
    }

    #[test]
    fn a_slash_completes_the_shortest_command_and_a_model_id() {
        assert_eq!(index_suffix("/th", &[], &[], &[], &[]).as_deref(), Some("read"));
        let models = vec!["huge/mesh-coder-7b".into()];
        assert_eq!(
            index_suffix("/model huge/me", &models, &[], &[], &[]).as_deref(),
            Some("sh-coder-7b")
        );
    }

    #[test]
    fn a_recent_prompt_completes_when_the_tree_does_not() {
        let suffix = index_suffix(
            "how does the parser",
            &[],
            &[],
            &[],
            &["how does the parser accept empty input".into()],
        )
        .unwrap();
        assert_eq!(suffix, " accept empty input");
    }

    #[test]
    fn a_busy_daemon_does_not_call_the_model() {
        assert!(!allow_llm("how does the parser", "local/tiny", 1));
        assert!(!allow_llm("short", "local/tiny", 0));
        assert!(!allow_llm("how does the parser", "", 0));
        assert!(allow_llm("how does the parser", "local/tiny", 0));
    }

    #[test]
    fn the_index_scan_stays_small() {
        let paths: Vec<String> = (0..2000).map(|i| format!("src/file{i}.rs")).collect();
        let started = std::time::Instant::now();
        let suffix = index_suffix("see src/file19", &[], &paths, &[], &[]).unwrap();
        assert!(started.elapsed().as_millis() < 50);
        assert_eq!(suffix, ".rs");
    }

    #[test]
    fn a_repeated_prefix_is_stripped_from_the_model_text() {
        let suffix = llm_suffix(
            "how does verify_",
            "how does verify_task_sig check the payload\nmore",
        );
        assert_eq!(suffix, "task_sig check the payload");
    }
}
