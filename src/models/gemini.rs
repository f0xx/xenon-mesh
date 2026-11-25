use super::{map_role, ChatTurn, ConcreteKind, StreamEvent};
use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;

fn endpoint(model: &str, action: &str) -> String {
    format!("https://generativelanguage.googleapis.com/v1beta/models/{model}:{action}")
}

fn contents(turns: &[ChatTurn]) -> Vec<Value> {
    turns
        .iter()
        .filter(|t| t.role != "system")
        .map(|t| {
            let mut parts = vec![json!({ "text": t.text })];
            for att in &t.attachments {
                parts.push(json!({
                    "inlineData": { "mimeType": att.mime_type, "data": att.data_base64 }
                }));
            }
            json!({
                "role": map_role(ConcreteKind::Gemini, &t.role),
                "parts": parts
            })
        })
        .collect()
}

fn body(system_ctx: &str, turns: &[ChatTurn], thinking: bool) -> Value {
    let mut extra = String::new();
    for t in turns.iter().filter(|t| t.role == "system") {
        extra.push('\n');
        extra.push_str(&t.text);
    }
    let system = format!("{system_ctx}{extra}");
    let mut body = if thinking {
        json!({
            "contents": contents(turns),
            "generationConfig": { "temperature": 0.2, "thinkingConfig": { "includeThoughts": true } }
        })
    } else {
        json!({
            "contents": contents(turns),
            "generationConfig": { "temperature": 0.2 }
        })
    };
    if !system.trim().is_empty() {
        body["systemInstruction"] = json!({ "parts": [{ "text": system }] });
    }
    body
}

pub async fn embed(
    http: &Client,
    api_key: &str,
    model: &str,
    text: &str,
    timeout: Duration,
) -> Result<Vec<f32>> {
    if api_key.is_empty() {
        bail!("gemini API key is empty");
    }
    let res: Value = http
        .post(endpoint(model, "embedContent"))
        .timeout(timeout)
        .header("x-goog-api-key", api_key)
        .json(&json!({
            "model": format!("models/{model}"),
            "content": { "parts": [{ "text": text }] }
        }))
        .send()
        .await?
        .json()
        .await?;
    let vals = res["embedding"]["values"]
        .as_array()
        .context("missing embedding values")?;
    Ok(vals
        .iter()
        .filter_map(|v| v.as_f64().map(|f| f as f32))
        .collect())
}

pub async fn generate(
    http: &Client,
    api_key: &str,
    model: &str,
    system_ctx: &str,
    turns: &[ChatTurn],
    timeout: Duration,
) -> Result<String> {
    if api_key.is_empty() {
        bail!("gemini API key is empty");
    }
    let res: Value = http
        .post(endpoint(model, "generateContent"))
        .timeout(timeout)
        .header("x-goog-api-key", api_key)
        .json(&body(system_ctx, turns, false))
        .send()
        .await?
        .json()
        .await?;
    if let Some(txt) = res["candidates"]
        .get(0)
        .and_then(|c| c["content"]["parts"].get(0))
        .and_then(|p| p["text"].as_str())
    {
        return Ok(txt.to_string());
    }
    let err = res["error"]["message"]
        .as_str()
        .unwrap_or("empty gemini response");
    bail!("status=502 {err}")
}

pub async fn stream(
    http: &Client,
    api_key: &str,
    model: &str,
    system_ctx: &str,
    turns: &[ChatTurn],
    timeout: Duration,
    tx: &tokio::sync::mpsc::Sender<StreamEvent>,
) -> Result<String> {
    if api_key.is_empty() {
        bail!("gemini API key is empty");
    }
    let url = format!("{}?alt=sse", endpoint(model, "streamGenerateContent"));
    let mut resp = http
        .post(url)
        .timeout(timeout)
        .header("x-goog-api-key", api_key)
        .json(&body(system_ctx, turns, true))
        .send()
        .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let err_txt = resp.text().await.unwrap_or_default();
        let msg = super::remote::api_error_message(&err_txt);
        bail!("status={} {msg}", status.as_u16());
    }

    let mut full = String::new();
    let mut buf = String::new();
    while let Some(chunk) = resp.chunk().await? {
        let mut pending = Vec::new();
        super::remote::feed_sse(&mut buf, &String::from_utf8_lossy(&chunk), |data| {
            if let Ok(v) = serde_json::from_str::<Value>(data) {
                if let Some(parts) = v["candidates"]
                    .get(0)
                    .and_then(|c| c["content"]["parts"].as_array())
                {
                    for p in parts {
                        let txt = p["text"].as_str().unwrap_or("");
                        if txt.is_empty() {
                            continue;
                        }
                        if p["thought"].as_bool().unwrap_or(false) {
                            pending.push(StreamEvent::Thought {
                                text: txt.to_string(),
                            });
                        } else {
                            full.push_str(txt);
                            pending.push(StreamEvent::Delta {
                                text: txt.to_string(),
                            });
                        }
                    }
                }
            }
        });
        for ev in pending {
            let _ = tx.send(ev).await;
        }
    }
    Ok(full)
}
