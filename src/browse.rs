//! Page fetch and web search used as model tools.
//!
//! Nodes ask the hub to perform the HTTP call. The lookup itself is a plain
//! client: DuckDuckGo's HTML results for a query, and a stripped page for a
//! URL. A model asks by ending its reply with a `web` or `fetch` fence.

use anyhow::{bail, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lookup {
    pub kind: &'static str,
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
                        kind: "web",
                        query: query.chars().take(300).collect(),
                    });
                }
            }
            "fetch" | "url" => {
                let query = inside.trim();
                if query.starts_with("http://") || query.starts_with("https://") {
                    tools.push(Lookup {
                        kind: "fetch",
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

pub async fn search(query: &str) -> Result<String> {
    let url = format!(
        "https://html.duckduckgo.com/html/?q={}",
        urlencoding_min(query)
    );
    let html = http_get(&url).await?;
    Ok(snippets(&html))
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

pub async fn run(lookup: &Lookup) -> Result<String> {
    match lookup.kind {
        "fetch" => fetch_page(&lookup.query).await,
        _ => search(&lookup.query).await,
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
    }

    #[test]
    fn urls_step_over_multibyte_characters() {
        let text = "давай http://example.com/a и ещё";
        assert_eq!(urls_in(text), vec!["http://example.com/a".to_string()]);
        assert!(urls_in("привет, без ссылки").is_empty());
    }

    #[test]
    fn a_code_fence_stays_in_the_reply() {
        let (text, tools) = take_requests("```rust\nfn main() {}\n```");
        assert!(tools.is_empty());
        assert!(text.contains("fn main"));
    }
}
