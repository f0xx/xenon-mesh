use super::{map_role, ChatTurn, ConcreteKind, StreamEvent};
use crate::config::Sampling;
use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;

fn endpoint(model: &str, action: &str) -> String {
    format!("https://generativelanguage.googleapis.com/v1beta/models/{model}:{action}")
}

fn contents(turns: &[ChatTurn]) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::new();
    for t in turns.iter().filter(|t| t.role != "system") {
        let role = map_role(ConcreteKind::Gemini, &t.role);
        let label = if t.role.trim().is_empty()
            || t.role == "user"
            || t.role == "assistant"
            || t.role == "model"
        {
            String::new()
        } else {
            format!("[{}]:\n", t.role)
        };
        let mut parts = vec![json!({ "text": format!("{label}{}", t.text) })];
        for att in &t.attachments {
            parts.push(json!({
                "inlineData": { "mimeType": att.mime_type, "data": att.data_base64 }
            }));
        }
        if let Some(last) = out.last_mut() {
            if last["role"].as_str() == Some(role) {
                if let Some(arr) = last["parts"].as_array_mut() {
                    arr.extend(parts);
                    continue;
                }
            }
        }
        out.push(json!({
            "role": role,
            "parts": parts
        }));
    }
    out
}

fn body(system_ctx: &str, turns: &[ChatTurn], thinking: bool, sampling: Sampling) -> Value {
    let mut extra = String::new();
    for t in turns.iter().filter(|t| t.role == "system") {
        extra.push('\n');
        extra.push_str(&t.text);
    }
    let system = format!("{system_ctx}{extra}");
    let mut generation = json!({ "temperature": sampling.temperature.unwrap_or(0.2) });
    if let Some(top_p) = sampling.top_p {
        generation["topP"] = json!(top_p);
    }
    if let Some(max_tokens) = sampling.max_tokens {
        generation["maxOutputTokens"] = json!(max_tokens);
    }
    if thinking {
        generation["thinkingConfig"] = json!({ "includeThoughts": true });
    }
    let mut body = json!({
        "contents": contents(turns),
        "generationConfig": generation
    });
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

fn reply_text(res: &Value) -> String {
    let mut out = String::new();
    let Some(parts) = res["candidates"]
        .get(0)
        .and_then(|c| c["content"]["parts"].as_array())
    else {
        return out;
    };
    for part in parts {
        if part["thought"].as_bool().unwrap_or(false) {
            continue;
        }
        if let Some(text) = part["text"].as_str() {
            out.push_str(text);
        }
    }
    out
}

/// These Gemini models advertise `generateContent` and not
/// `streamGenerateContent`. The stream endpoint hangs or returns 503.
/// A 503 is retried; a 429 is the project's quota and is returned as-is.
async fn generate_value(
    http: &Client,
    api_key: &str,
    model: &str,
    system_ctx: &str,
    turns: &[ChatTurn],
    timeout: Duration,
    sampling: Sampling,
) -> Result<Value> {
    let mut delay = 1u64;
    let mut last = String::new();
    let thinking = model.starts_with("gemini-4");
    let mut sampling = sampling;
    if thinking && sampling.max_tokens.is_none() {
        // Thinking tokens come out of this budget. Leave room for the answer.
        sampling.max_tokens = Some(8192);
    }
    for attempt in 0..3 {
        let resp = http
            .post(endpoint(model, "generateContent"))
            .timeout(timeout)
            .header("x-goog-api-key", api_key)
            .json(&body(system_ctx, turns, thinking, sampling))
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if status.as_u16() == 503 && attempt + 1 < 3 {
            last = super::remote::api_error_message(&text);
            tokio::time::sleep(Duration::from_secs(delay)).await;
            delay *= 2;
            continue;
        }
        if !status.is_success() {
            let msg = super::remote::api_error_message(&text);
            bail!(
                "status={} {msg} (generateContent {})",
                status.as_u16(),
                model
            );
        }
        return serde_json::from_str(&text).with_context(|| {
            format!(
                "gemini response was not JSON: {}",
                text.chars().take(180).collect::<String>()
            )
        });
    }
    bail!("status=503 {last} (generateContent {model}, retried)")
}

pub async fn generate(
    http: &Client,
    api_key: &str,
    model: &str,
    system_ctx: &str,
    turns: &[ChatTurn],
    timeout: Duration,
    sampling: Sampling,
) -> Result<String> {
    if api_key.is_empty() {
        bail!("gemini API key is empty");
    }
    let res = generate_value(http, api_key, model, system_ctx, turns, timeout, sampling).await?;
    let text = reply_text(&res);
    if text.is_empty() {
        bail!("empty gemini response");
    }
    Ok(text)
}

pub async fn stream(
    http: &Client,
    api_key: &str,
    model: &str,
    system_ctx: &str,
    turns: &[ChatTurn],
    timeout: Duration,
    sampling: Sampling,
    tx: &tokio::sync::mpsc::Sender<StreamEvent>,
) -> Result<String> {
    let text = generate(http, api_key, model, system_ctx, turns, timeout, sampling).await?;
    let _ = tx
        .send(StreamEvent::Delta {
            text: text.clone(),
            model: String::new(),
        })
        .await;
    Ok(text)
}
