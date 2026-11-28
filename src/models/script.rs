//! A provider whose HTTP shape lives in a script.
//!
//! The daemon writes one JSON object to the script's stdin and reads stdout.
//! Lines that start with `XMESH_DELTA ` are streamed. Any other stdout is the
//! whole reply. The API key is the `XMESH_API_KEY` environment variable and
//! is not written into the JSON.

use super::{ChatTurn, StreamEvent};
use anyhow::{bail, Context, Result};
use std::time::Duration;

pub fn reply_from_stdout(stdout: &str) -> (String, Vec<String>) {
    let mut whole = String::new();
    let mut deltas = Vec::new();
    let mut plain = String::new();
    for line in stdout.split_inclusive('\n') {
        if let Some(rest) = line.trim_end_matches(['\n', '\r']).strip_prefix("XMESH_DELTA ") {
            deltas.push(rest.to_string());
            whole.push_str(rest);
        } else {
            plain.push_str(line);
        }
    }
    if deltas.is_empty() {
        (plain.trim().to_string(), Vec::new())
    } else {
        (whole, deltas)
    }
}

pub async fn run(
    command: &[String],
    base_url: &str,
    api_key: &str,
    model: &str,
    system: &str,
    turns: &[ChatTurn],
    timeout: Duration,
    tx: Option<&tokio::sync::mpsc::Sender<StreamEvent>>,
) -> Result<String> {
    let program = command
        .first()
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .context("script provider has no command")?;
    let body = serde_json::json!({
        "model": model,
        "system": system,
        "messages": turns.iter().map(|turn| serde_json::json!({
            "role": turn.role,
            "content": turn.text
        })).collect::<Vec<_>>()
    });
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(command.iter().skip(1))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .env("XMESH_BASE_URL", base_url)
        .env("XMESH_MODEL", model)
        .env("XMESH_API_KEY", api_key)
        .kill_on_drop(true);
    let mut child = cmd
        .spawn()
        .with_context(|| format!("could not run {program}"))?;
    if let Some(mut stdin) = child.stdin.take() {
        use tokio::io::AsyncWriteExt;
        let _ = stdin.write_all(body.to_string().as_bytes()).await;
    }
    let out = tokio::time::timeout(timeout, child.wait_with_output())
        .await
        .context("provider script timed out")?
        .context("provider script failed")?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        bail!(
            "provider script exited {}: {}",
            out.status.code().unwrap_or(-1),
            err.chars().take(300).collect::<String>()
        );
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let (reply, deltas) = reply_from_stdout(&text);
    if reply.trim().is_empty() {
        bail!("provider script returned an empty reply");
    }
    if let Some(tx) = tx {
        if deltas.is_empty() {
            let _ = tx
                .send(StreamEvent::Delta {
                    text: reply.clone(),
                    model: String::new(),
                })
                .await;
        } else {
            for delta in deltas {
                let _ = tx
                    .send(StreamEvent::Delta {
                        text: delta,
                        model: String::new(),
                    })
                    .await;
            }
        }
    }
    Ok(reply)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_lines_are_the_reply_and_plain_stdout_is_kept() {
        let (text, deltas) = reply_from_stdout("XMESH_DELTA hello\nXMESH_DELTA world\n");
        assert_eq!(text, "helloworld");
        assert_eq!(deltas.len(), 2);
        let (plain, none) = reply_from_stdout("just text\n");
        assert_eq!(plain, "just text");
        assert!(none.is_empty());
    }

    #[tokio::test]
    async fn a_script_reply_is_the_child_stdout() {
        let text = run(
            &[
                "/bin/sh".into(),
                "-c".into(),
                "cat >/dev/null; printf 'pong'".into(),
            ],
            "",
            "",
            "demo",
            "",
            &[],
            Duration::from_secs(5),
            None,
        )
        .await
        .unwrap();
        assert_eq!(text, "pong");
    }
}
