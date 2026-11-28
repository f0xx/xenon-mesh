//! Page fetch and web search used as model tools.
//!
//! Nodes ask the hub to perform the HTTP call. The lookup itself is a plain
//! client: DuckDuckGo's HTML results for a query, and a stripped page for a
//! URL. A model asks by ending its reply with a `web` or `fetch` fence.

use anyhow::{bail, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lookup {
    pub kind: String,
    pub query: String,
}

pub fn urls_in(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < text.len() {
        if !text.is_char_boundary(i) {
            i += 1;
            continue;
        }
        let rest = &text[i..];
        let matched = if rest.starts_with("https://") || rest.starts_with("http://") {
            true
        } else {
            false
        };
        if matched {
            let end = rest
                .find(|c: char| c.is_whitespace() || "<>\"')".contains(c))
                .map(|n| i + n)
                .unwrap_or(text.len());
            let url = text[i..end].trim_end_matches('.').to_string();
            if url.len() > 8 && !out.contains(&url) {
                out.push(url);
            }
            if out.len() >= 2 {
                break;
            }
            i = end.max(i + 1);
        } else {
            i += rest.chars().next().map(|c| c.len_utf8()).unwrap_or(1);
        }
    }
    out
}

/// A question that needs the tree, not a general chat reply.
pub fn wants_code(question: &str) -> bool {
    let q = question.to_ascii_lowercase();
    [
        "patch", "diff", "implement", "fix", "review", "refactor", "bug", "function",
        "where is", "how does", "source", "crate", "module", "compile", "build",
    ]
    .iter()
    .any(|word| q.contains(word))
        || question.contains('/')
        || question.contains(".rs")
        || question.contains(".c")
        || question.contains(".h")
        || question.contains(".py")
        || question.contains(".go")
}

/// When a coding reply never asks for a file and never offers a diff, open
/// the tree once. A path in the question is a read. Otherwise the longest
/// identifier is a grep.
pub fn forced_lookup(question: &str, reply: &str) -> Option<Lookup> {
    if !wants_code(question) || reply.contains("```diff") {
        return None;
    }
    let (_, asked) = take_requests(reply);
    if !asked.is_empty() {
        return None;
    }
    for raw in question.split_whitespace() {
        let token = raw.trim_matches(|c: char| "`,.;:()[]<>\"'".contains(c));
        if token.contains("://") || token.len() < 4 {
            continue;
        }
        let looks_like_path = token.contains('/')
            || token.ends_with(".rs")
            || token.ends_with(".c")
            || token.ends_with(".h")
            || token.ends_with(".py")
            || token.ends_with(".go")
            || token.ends_with(".toml");
        if looks_like_path {
            return Some(Lookup {
                kind: "read".into(),
                query: token.chars().take(300).collect(),
            });
        }
    }
    let mut best = String::new();
    for token in crate::retrieve::tokens(question) {
        if token.len() >= 5 && token.len() > best.len() {
            best = token;
        }
    }
    if best.is_empty() {
        return None;
    }
    Some(Lookup {
        kind: "grep".into(),
        query: best,
    })
}

/// Split lookup fences out of a reply. The fences are not part of the text
/// that is shown after the lookup runs.
pub fn take_requests(text: &str) -> (String, Vec<Lookup>) {
    let mut kept = String::new();
    let mut tools = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("```") {
        kept.push_str(&rest[..start]);
        let after = &rest[start + 3..];
        let lang_end = after.find('\n').unwrap_or(after.len());
        let lang = after[..lang_end].trim();
        let body_from = if after[..lang_end].contains('\n') {
            lang_end + 1
        } else {
            lang_end
        };
        let body = &after[body_from..];
        let close = body.find("```");
        let (inside, next) = match close {
            Some(pos) => (&body[..pos], &body[pos + 3..]),
            None => (body, ""),
        };
        match lang {
            "web" | "search" => {
                let query = inside.trim();
                if !query.is_empty() {
                    tools.push(Lookup {
                        kind: "web".into(),
                        query: query.chars().take(300).collect(),
                    });
                }
            }
            "fetch" | "url" => {
                let query = inside.trim();
                if query.starts_with("http://") || query.starts_with("https://") {
                    tools.push(Lookup {
                        kind: "fetch".into(),
                        query: query.chars().take(500).collect(),
                    });
                } else {
                    kept.push_str("```");
                    kept.push_str(lang);
                    kept.push('\n');
                    kept.push_str(inside);
                    kept.push_str("```");
                }
            }
            "read" | "file" => {
                for line in inside.lines().map(str::trim).filter(|l| !l.is_empty()).take(3) {
                    tools.push(Lookup {
                        kind: "read".into(),
                        query: line.chars().take(300).collect(),
                    });
                }
            }
            "agent" | "delegate" => {
                let query = inside.trim();
                if crate::orchestrate::parse_agent_task(query).is_some() {
                    tools.push(Lookup {
                        kind: "agent".into(),
                        query: query.chars().take(2_200).collect(),
                    });
                } else {
                    kept.push_str("```");
                    kept.push_str(lang);
                    kept.push('\n');
                    kept.push_str(inside);
                    if close.is_some() {
                        kept.push_str("```");
                    }
                }
            }
            "grep" | "find" => {
                let query = inside.trim();
                if !query.is_empty() {
                    tools.push(Lookup {
                        kind: "grep".into(),
                        query: query.chars().take(200).collect(),
                    });
                }
            }
            _ => {
                kept.push_str("```");
                kept.push_str(lang);
                if body_from > lang_end {
                    kept.push('\n');
                }
                kept.push_str(inside);
                if close.is_some() {
                    kept.push_str("```");
                }
            }
        }
        rest = next;
    }
    kept.push_str(rest);
    (kept.trim().to_string(), tools)
}

/// Tool fences buried in a reasoning trace. The visible reply often omits them.
pub fn tool_fence_text(text: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(start) = rest.find("```") {
        let after = &rest[start + 3..];
        let lang_end = after.find('\n').unwrap_or(after.len());
        let lang = after[..lang_end].trim().to_ascii_lowercase();
        let keep = matches!(
            lang.as_str(),
            "read" | "file" | "grep" | "find" | "web" | "search" | "fetch" | "url" | "agent"
                | "delegate"
        );
        let body_from = if after[..lang_end].contains('\n') {
            lang_end + 1
        } else {
            lang_end
        };
        let body = &after[body_from..];
        let Some(close) = body.find("```") else {
            break;
        };
        let next = &body[close + 3..];
        if keep {
            let block_end = rest.len() - next.len();
            out.push_str(&rest[start..block_end]);
            if !out.ends_with('\n') {
                out.push('\n');
            }
        }
        rest = next;
    }
    out
}

pub async fn search(query: &str, searx_url: &str) -> Result<String> {
    let base = searx_url.trim().trim_end_matches('/');
    if !base.is_empty() {
        let url = format!(
            "{base}/search?q={}&format=json&categories=general",
            urlencoding_min(query)
        );
        if let Ok(body) = http_get(&url).await {
            if let Some(text) = searx_snippets(&body) {
                return Ok(text);
            }
        }
    }
    let url = format!(
        "https://html.duckduckgo.com/html/?q={}",
        urlencoding_min(query)
    );
    let html = http_get(&url).await?;
    Ok(snippets(&html))
}

fn searx_snippets(body: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    let results = v.get("results")?.as_array()?;
    let mut out = String::new();
    for item in results.iter().take(5) {
        let title = item.get("title").and_then(|t| t.as_str()).unwrap_or("");
        let url = item.get("url").and_then(|t| t.as_str()).unwrap_or("");
        let content = item.get("content").and_then(|t| t.as_str()).unwrap_or("");
        if title.is_empty() && url.is_empty() {
            continue;
        }
        out.push_str(&format!("- {title}\n  {url}\n  {content}\n"));
    }
    if out.is_empty() {
        None
    } else {
        Some(clip(&out, 4_000))
    }
}

pub async fn fetch_page(url: &str) -> Result<String> {
    if !(url.starts_with("https://") || url.starts_with("http://")) {
        bail!("only http(s) pages can be fetched");
    }
    let html = http_get(url).await?;
    let text = strip_html(&html);
    if text.trim().is_empty() {
        bail!("page had no readable text");
    }
    Ok(clip(&text, 8_000))
}

/// A repository read request. `..`, absolute paths, and backslashes are refused.
pub fn parse_read_spec(spec: &str) -> Result<(String, Option<(usize, usize)>)> {
    let trimmed = spec.trim().trim_start_matches("./");
    if trimmed.is_empty() {
        bail!("empty file path");
    }
    let (path, range) = if let Some((path, range)) = trimmed.rsplit_once(':') {
        if let Some((a, b)) = range.split_once('-') {
            if let (Ok(start), Ok(end)) = (a.trim().parse::<usize>(), b.trim().parse::<usize>()) {
                let start = start.max(1);
                let end = end.max(start);
                (path.trim(), Some((start, end.min(start + 400))))
            } else {
                (trimmed, None)
            }
        } else if let Ok(line) = range.trim().parse::<usize>() {
            let line = line.max(1);
            let start = line.saturating_sub(30).max(1);
            (path.trim(), Some((start, line + 60)))
        } else {
            (trimmed, None)
        }
    } else {
        (trimmed, None)
    };
    if path.is_empty()
        || path.starts_with('/')
        || path.contains('\\')
        || path.split('/').any(|part| part.is_empty() || part == "." || part == "..")
    {
        bail!("refused path");
    }
    Ok((path.to_string(), range))
}

/// Numbered lines. `range` is 1-based and inclusive. Output stops at `max_chars`.
pub fn format_line_slice(text: &str, range: Option<(usize, usize)>, max_chars: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let total = lines.len();
    if total == 0 {
        return "(empty file)\n".into();
    }
    let (start, end) = match range {
        Some((s, e)) => {
            let end = e.min(total);
            let start = s.min(end).max(1);
            (start, end)
        }
        None => (1, total),
    };
    if start > total {
        return format!("file has {total} lines\n");
    }
    let mut out = String::new();
    for idx in start..=end {
        let Some(line) = lines.get(idx - 1) else { break };
        let row = format!("{idx:>5}|{line}\n");
        if out.len() + row.len() > max_chars && !out.is_empty() {
            out.push_str(&format!("…[truncated at line {idx} of {total}]\n"));
            break;
        }
        out.push_str(&row);
    }
    if end < total && !out.contains("…[truncated") {
        out.push_str(&format!("…[showing lines {start}..{end} of {total}]\n"));
    }
    out
}

pub async fn run(lookup: &Lookup, searx_url: &str) -> Result<String> {
    match lookup.kind.as_str() {
        "fetch" => fetch_page(&lookup.query).await,
        "agent" => bail!("agent tasks are run by the scheduler, not the browser"),
        _ => search(&lookup.query, searx_url).await,
    }
}

async fn http_get(url: &str) -> Result<String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::limited(4))
        .build()?;
    let resp = client
        .get(url)
        .header(
            "user-agent",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        )
        .header("accept-language", "en")
        .send()
        .await?;
    if !resp.status().is_success() {
        bail!("status {}", resp.status());
    }
    let bytes = resp.bytes().await?;
    Ok(String::from_utf8_lossy(&bytes).to_string())
}

fn snippets(html: &str) -> String {
    let text = strip_html(html);
    let mut lines: Vec<String> = text
        .lines()
        .map(str::trim)
        .filter(|ln| ln.len() > 40)
        .filter(|ln| {
            let low = ln.to_ascii_lowercase();
            !low.contains("duckduckgo") && !low.contains("javascript")
        })
        .take(12)
        .map(|ln| clip(ln, 280))
        .collect();
    if lines.is_empty() {
        lines.push("The search page had no readable results.".into());
    }
    lines.join("\n")
}

fn strip_html(html: &str) -> String {
    let mut out = String::new();
    let mut in_tag = false;
    let mut skip = 0;
    let lower = html.to_ascii_lowercase();
    let bytes = html.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if skip > 0 {
            skip -= 1;
            i += 1;
            continue;
        }
        let rest = &lower[i..];
        if rest.starts_with("<script") || rest.starts_with("<style") {
            let end = if rest.starts_with("<script") {
                "</script>"
            } else {
                "</style>"
            };
            if let Some(pos) = rest.find(end) {
                i += pos + end.len();
                continue;
            }
        }
        let c = html[i..].chars().next().unwrap_or(' ');
        if c == '<' {
            in_tag = true;
        } else if c == '>' {
            in_tag = false;
            out.push(' ');
        } else if !in_tag {
            out.push(c);
        }
        i += c.len_utf8();
    }
    let mut collapsed = String::new();
    let mut space = false;
    for c in out.chars() {
        if c.is_whitespace() {
            if !space {
                collapsed.push('\n');
            }
            space = true;
        } else {
            collapsed.push(c);
            space = false;
        }
    }
    collapsed
}

fn clip(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        text.to_string()
    } else {
        format!("{}…", text.chars().take(max).collect::<String>())
    }
}

fn urlencoding_min(text: &str) -> String {
    let mut out = String::new();
    for b in text.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fences_become_lookups_and_leave_the_reply() {
        let (text, tools) = take_requests("See the docs.\n```web\nrust ownership\n```\n");
        assert_eq!(text, "See the docs.");
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].kind, "web");
        assert_eq!(tools[0].query, "rust ownership");

        let (text2, tools2) = take_requests("Checking storage.\n```read\nsrc/storage.rs:10-50\n```\n```grep\nappend_public\n```");
        assert_eq!(text2, "Checking storage.");
        assert_eq!(tools2.len(), 2);
        assert_eq!(tools2[0].kind, "read");
        assert_eq!(tools2[1].kind, "grep");
    }

    #[test]
    fn read_specs_keep_a_relative_path_and_refuse_escapes() {
        let (path, range) = parse_read_spec("src/storage.rs:10-50").unwrap();
        assert_eq!(path, "src/storage.rs");
        assert_eq!(range, Some((10, 50)));
        let (path, range) = parse_read_spec("./src/main.rs:80").unwrap();
        assert_eq!(path, "src/main.rs");
        assert_eq!(range, Some((50, 140)));
        assert!(parse_read_spec("../etc/passwd").is_err());
        assert!(parse_read_spec("/etc/passwd").is_err());
        assert!(parse_read_spec("src//secret.rs").is_err());
    }

    #[test]
    fn line_slices_use_real_line_numbers_and_stop_at_the_budget() {
        let text = "alpha\nbeta\ngamma\n";
        let slice = format_line_slice(text, Some((2, 3)), 10_000);
        assert!(slice.contains("    2|beta"), "{slice}");
        assert!(slice.contains("    3|gamma"), "{slice}");
        assert!(!slice.contains("alpha"));
        let short = format_line_slice(text, None, 12);
        assert!(short.contains("truncated") || short.contains("showing"), "{short}");
    }

    #[test]
    fn urls_step_over_multibyte_characters() {
        let text = "давай http://example.com/a и ещё";
        assert_eq!(urls_in(text), vec!["http://example.com/a".to_string()]);
        assert!(urls_in("привет, без ссылки").is_empty());
    }

    #[test]
    fn reasoning_can_hide_a_read_fence() {
        let hidden = "I should look.\n```read\nsrc/lib.rs:1-20\n```\nthen answer";
        let lifted = tool_fence_text(hidden);
        assert!(lifted.contains("```read"));
        assert!(lifted.contains("src/lib.rs:1-20"));
        assert!(tool_fence_text("```cpp\nint x;\n```").is_empty());
    }

    #[test]
    fn an_agent_fence_is_a_tool_and_rust_is_not() {
        let (text, tools) = take_requests("answer\n```agent\nhuge/qwen\ncheck the parser\n```");
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].kind, "agent");
        assert!(!text.contains("check the parser"));
        let (kept, none) = take_requests("```rust\nfn main() {}\n```");
        assert!(none.is_empty());
        assert!(kept.contains("fn main"));
    }

    #[test]
    fn a_code_fence_stays_in_the_reply() {
        let (text, tools) = take_requests("```rust\nfn main() {}\n```");
        assert!(tools.is_empty());
        assert!(text.contains("fn main"));
    }

    #[test]
    fn a_patch_request_without_a_diff_opens_the_named_file() {
        let lookup = forced_lookup("patch src/parser.rs to accept empty input", "I would change the parser.").unwrap();
        assert_eq!(lookup.kind, "read");
        assert_eq!(lookup.query, "src/parser.rs");
        assert!(forced_lookup("hello there", "fine").is_none());
        assert!(forced_lookup("fix the parser", "```diff\n+ok\n```").is_none());
        let grep = forced_lookup("where is verify_task_sig used", "I am not sure").unwrap();
        assert_eq!(grep.kind, "grep");
        assert_eq!(grep.query, "verify");
    }
}
