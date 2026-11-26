//! Web search as a model.
//!
//! The chat is reduced to a short query. A shell script performs the HTTP
//! call so the request shape can change without rebuilding the daemon.
//! This provider does not receive repository source.

use super::{ChatTurn, StreamEvent};
use anyhow::{bail, Context, Result};
use std::time::Duration;

pub fn search_query(turns: &[ChatTurn]) -> String {
    let question = turns
        .iter()
        .rev()
        .find(|turn| turn.role == "user" || turn.role == "human")
        .map(|turn| turn.text.trim().to_string())
        .unwrap_or_default();
    let mut prior = Vec::new();
    for turn in turns.iter().rev().skip(1).take(6) {
        if turn.role == "system" {
            continue;
        }
        let text: String = turn.text.chars().take(180).collect();
        if text.trim().is_empty() {
            continue;
        }
        prior.push(format!("{}: {text}", turn.role));
    }
    prior.reverse();
    let question: String = question.chars().take(500).collect();
    if prior.is_empty() {
        question
    } else {
        format!("{question}\n\nRecent chat:\n{}", prior.join("\n"))
    }
}

pub async fn ask(
    command: &[String],
    user_agent: &str,
    search_url: &str,
    turns: &[ChatTurn],
    timeout: Duration,
    tx: Option<&tokio::sync::mpsc::Sender<StreamEvent>>,
) -> Result<String> {
    let program = command
        .first()
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .context("google provider has no command")?;
    let query = search_query(turns);
    if query.trim().is_empty() {
        bail!("empty search query");
    }
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(command.iter().skip(1))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .env("XMESH_UA", user_agent)
        .env("XMESH_SEARCH_URL", search_url)
        .kill_on_drop(true);
    let mut child = cmd.spawn().with_context(|| format!("could not run {program}"))?;
    if let Some(mut stdin) = child.stdin.take() {
        use tokio::io::AsyncWriteExt;
        let _ = stdin.write_all(query.as_bytes()).await;
    }
    let out = tokio::time::timeout(timeout, child.wait_with_output())
        .await
        .context("web search timed out")?
        .context("web search failed")?;
    let mut text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if text.is_empty() {
        let err = String::from_utf8_lossy(&out.stderr);
        bail!(
            "web search returned nothing (exit {}): {}",
            out.status.code().unwrap_or(-1),
            err.chars().take(300).collect::<String>()
        );
    }
    if text.chars().count() > 12_000 {
        text = text.chars().take(12_000).collect();
        text.push_str("\n…[truncated]");
    }
    if let Some(tx) = tx {
        let _ = tx.send(StreamEvent::Delta { text: text.clone() }).await;
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_keeps_the_question_and_a_short_history() {
        let turns = [
            ChatTurn {
                role: "user".into(),
                text: "earlier note".into(),
                attachments: vec![],
            },
            ChatTurn {
                role: "assistant".into(),
                text: "ack".into(),
                attachments: vec![],
            },
            ChatTurn {
                role: "user".into(),
                text: "what changed in rust this year?".into(),
                attachments: vec![],
            },
        ];
        let q = search_query(&turns);
        assert!(q.starts_with("what changed in rust this year?"));
        assert!(q.contains("earlier note"));
        assert!(!q.contains("system"));
    }
}
