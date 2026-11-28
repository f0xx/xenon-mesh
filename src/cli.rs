//! Talks to an already running daemon. Does not bind a port.

use anyhow::{bail, Context, Result};
use serde_json::Value;
use std::io::{BufRead, Write};

pub async fn run(args: &[String]) -> Result<()> {
    let url = match args.iter().position(|a| a == "--url") {
        Some(i) => args
            .get(i + 1)
            .context("xmeshd cli --url needs an address")?
            .trim_end_matches('/')
            .to_string(),
        None => default_url(),
    };
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(7200))
        .build()?;
    let mut channel = "general".to_string();
    let mut thread = "main".to_string();
    let mut model = String::new();
    println!("xmeshd cli  {url}");
    println!("channel #{channel} / {thread}    /help lists commands");
    let stdin = std::io::stdin();
    let mut lines = stdin.lock().lines();
    loop {
        print!("#{channel}/{thread}> ");
        let _ = std::io::stdout().flush();
        let Some(line) = lines.next() else { break };
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix('/') {
            let mut parts = rest.splitn(2, char::is_whitespace);
            let cmd = parts.next().unwrap_or("");
            let arg = parts.next().unwrap_or("").trim();
            match cmd {
                "help" => print_help(),
                "quit" | "exit" => break,
                "channels" => print_json(client.get(format!("{url}/api/channels")).send().await?).await?,
                "use" => {
                    if arg.is_empty() {
                        println!("usage: /use <channel>");
                    } else {
                        channel = arg.to_string();
                        thread = "main".to_string();
                        println!("#{channel} / {thread}");
                    }
                }
                "thread" => {
                    if arg.is_empty() {
                        println!("{thread}");
                    } else {
                        thread = arg.to_string();
                        println!("#{channel} / {thread}");
                    }
                }
                "threads" => {
                    print_json(
                        client
                            .get(format!(
                                "{url}/api/threads?channel={}",
                                urlencoding(&channel)
                            ))
                            .send()
                            .await?,
                    )
                    .await?;
                }
                "models" => print_models(&client, &url).await?,
                "model" => {
                    if arg.is_empty() {
                        println!("{}", if model.is_empty() { "(default)" } else { &model });
                    } else {
                        model = arg.to_string();
                        println!("{model}");
                    }
                }
                "history" => {
                    let n: usize = arg.parse().unwrap_or(20);
                    show_history(&client, &url, &channel, &thread, n).await?;
                }
                "alias" => {
                    if arg.is_empty() {
                        print_json(client.get(format!("{url}/api/alias")).send().await?).await?;
                    } else {
                        let body = serde_json::json!({ "alias": arg });
                        print_json(
                            client
                                .post(format!("{url}/api/alias"))
                                .json(&body)
                                .send()
                                .await?,
                        )
                        .await?;
                    }
                }
                "suggest" => suggest_cmd(&client, &url, &channel, &thread, arg).await?,
                "send" => {
                    if arg.is_empty() {
                        println!("usage: /send <text>");
                    } else {
                        chat(&client, &url, &channel, &thread, &model, arg).await?;
                    }
                }
                other => println!("unknown command /{other}    /help"),
            }
        } else {
            chat(&client, &url, &channel, &thread, &model, trimmed).await?;
        }
    }
    Ok(())
}

async fn suggest_cmd(
    client: &reqwest::Client,
    url: &str,
    channel: &str,
    thread: &str,
    arg: &str,
) -> Result<()> {
    if arg.is_empty() || arg == "on" || arg == "off" {
        if arg == "on" || arg == "off" {
            let body = serde_json::json!({ "channel": channel, "enabled": arg == "on" });
            let resp = client
                .post(format!("{url}/api/suggest-pref"))
                .json(&body)
                .send()
                .await?;
            if !resp.status().is_success() {
                bail!("{} {}", resp.status(), resp.text().await?);
            }
        }
        let value: Value = client
            .get(format!(
                "{url}/api/suggest-pref?channel={}",
                urlencoding(channel)
            ))
            .send()
            .await?
            .json()
            .await?;
        let on = value.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true);
        println!("{}", if on { "on" } else { "off" });
        return Ok(());
    }
    let body = serde_json::json!({ "channel": channel, "thread": thread, "prefix": arg });
    let value: Value = client
        .post(format!("{url}/api/suggest"))
        .json(&body)
        .send()
        .await?
        .json()
        .await?;
    let suffix = value
        .get("completion")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if suffix.is_empty() {
        println!("{arg}");
    } else {
        println!("{arg}{suffix}");
    }
    Ok(())
}

fn print_help() {
    println!(
        "\
/help
/channels
/use <channel>
/thread [name]
/threads
/model [provider/model]
/models
/history [count]
/alias [name]
/suggest [on|off|text]
/send <text>
/quit
A line without a slash is sent as a chat message.
/suggest with no argument prints on or off for this channel.
/suggest on and /suggest off remember that choice.
/suggest followed by text asks for a completion and prints it."
    );
}

fn default_url() -> String {
    let local = "/etc/xmeshd/local.yaml";
    let cluster = "/etc/xmeshd/cluster.yaml";
    match crate::config::AppConfig::load(cluster, local) {
        Ok(cfg) => rewrite_loopback(&cfg.node.http_addr),
        Err(_) => "http://127.0.0.1:8080".into(),
    }
}

fn rewrite_loopback(addr: &str) -> String {
    let (host, port) = addr.rsplit_once(':').unwrap_or(("127.0.0.1", "8080"));
    let host = match host {
        "0.0.0.0" | "[::]" | "*" => "127.0.0.1",
        other => other,
    };
    format!("http://{host}:{port}")
}

fn urlencoding(text: &str) -> String {
    let mut out = String::new();
    for b in text.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

async fn print_json(resp: reqwest::Response) -> Result<()> {
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        bail!("{status} {text}");
    }
    match serde_json::from_str::<Value>(&text) {
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value)?),
        Err(_) => println!("{text}"),
    }
    Ok(())
}

async fn print_models(client: &reqwest::Client, url: &str) -> Result<()> {
    let value: Value = client.get(format!("{url}/api/config")).send().await?.json().await?;
    if let Some(models) = value.get("models").and_then(|m| m.as_array()) {
        for model in models {
            if let Some(id) = model.as_str() {
                println!("{id}");
            } else if let Some(id) = model.get("id").and_then(|v| v.as_str()) {
                println!("{id}");
            }
        }
    } else {
        println!("{}", serde_json::to_string_pretty(&value)?);
    }
    Ok(())
}

async fn show_history(
    client: &reqwest::Client,
    url: &str,
    channel: &str,
    thread: &str,
    n: usize,
) -> Result<()> {
    let value: Value = client
        .get(format!(
            "{url}/api/history?channel={}&thread={}",
            urlencoding(channel),
            urlencoding(thread)
        ))
        .send()
        .await?
        .json()
        .await?;
    let Some(rows) = value.as_array() else {
        println!("{value}");
        return Ok(());
    };
    let start = rows.len().saturating_sub(n);
    for row in &rows[start..] {
        let role = row.get("role").and_then(|v| v.as_str()).unwrap_or("?");
        let text = row.get("text").and_then(|v| v.as_str()).unwrap_or("");
        let seq = row.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);
        println!("--- {seq} [{role}]");
        println!("{text}");
        if let (Some(a), Some(b)) = (
            row.get("started_at_ms").and_then(|v| v.as_i64()),
            row.get("finished_at_ms").and_then(|v| v.as_i64()),
        ) {
            if b >= a && a > 0 {
                println!("generation took {}", format_took(b - a));
            }
        }
    }
    Ok(())
}

async fn chat(
    client: &reqwest::Client,
    url: &str,
    channel: &str,
    thread: &str,
    model: &str,
    prompt: &str,
) -> Result<()> {
    let body = serde_json::json!({
        "channel": channel,
        "thread": thread,
        "prompt": prompt,
        "model": model,
        "attachments": [],
        "rerun": false
    });
    let mut resp = client.post(format!("{url}/api/chat")).json(&body).send().await?;
    if !resp.status().is_success() {
        bail!("{} {}", resp.status(), resp.text().await.unwrap_or_default());
    }
    let mut buf = String::new();
    let mut opened = String::new();
    while let Some(chunk) = resp.chunk().await? {
        buf.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(idx) = buf.find("\n\n") {
            let block = buf[..idx].to_string();
            buf = buf[idx + 2..].to_string();
            let Some(data) = block.trim().strip_prefix("data:") else {
                continue;
            };
            let Ok(ev) = serde_json::from_str::<Value>(data.trim()) else {
                continue;
            };
            match ev.get("type").and_then(|v| v.as_str()).unwrap_or("") {
                "status" => println!("… {}", ev.get("text").and_then(|v| v.as_str()).unwrap_or("")),
                "thought" => print!("{}", ev.get("text").and_then(|v| v.as_str()).unwrap_or("")),
                "delta" => print!("{}", ev.get("text").and_then(|v| v.as_str()).unwrap_or("")),
                "saved" => {
                    if let Some(name) = ev.get("thread").and_then(|v| v.as_str()) {
                        if !name.is_empty() {
                            opened = name.to_string();
                        }
                    }
                }
                "done" => {
                    println!();
                    if let Some(used) = ev.get("model_used").and_then(|v| v.as_str()) {
                        println!("[{used}]");
                    }
                }
                _ => {}
            }
            let _ = std::io::stdout().flush();
        }
    }
    if !opened.is_empty() {
        println!("thread {opened}");
    }
    Ok(())
}

fn format_took(ms: i64) -> String {
    let mut s = (ms / 1000).max(0);
    let h = s / 3600;
    s %= 3600;
    let m = s / 60;
    s %= 60;
    let mut bits = Vec::new();
    if h > 0 {
        bits.push(format!("{h}hr"));
    }
    if m > 0 {
        bits.push(format!("{m}mins"));
    }
    bits.push(format!("{s}secs"));
    bits.join(" ")
}
