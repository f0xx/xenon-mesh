use super::{map_role, ChatTurn, ConcreteKind, StreamEvent};
use anyhow::{bail, Result};
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;

pub fn openai_root(base: &str) -> String {
    let b = base.trim().trim_end_matches('/');
    if b.ends_with("/v1") {
        b.to_string()
    } else {
        format!("{b}/v1")
    }
}

pub fn service_root(base: &str) -> String {
    let b = base.trim().trim_end_matches('/');
    b.strip_suffix("/v1").unwrap_or(b).to_string()
}

pub fn api_error_message(body: &str) -> String {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| {
            v["error"]["message"]
                .as_str()
                .or_else(|| v["error"].as_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| body.chars().take(240).collect())
}

pub fn feed_sse(buf: &mut String, chunk: &str, mut on_data: impl FnMut(&str)) {
    buf.push_str(chunk);
    while let Some(pos) = buf.find('\n') {
        let line = buf[..pos].trim().to_string();
        buf.drain(..=pos);
        if let Some(data) = line.strip_prefix("data:") {
            let data = data.trim();
            if !data.is_empty() {
                on_data(data);
            }
        }
    }
}

pub fn openai_delta_parts(v: &Value) -> (String, String) {
    let delta = &v["choices"][0]["delta"];
    let content = delta["content"].as_str().unwrap_or("").to_string();
    let reasoning = delta["reasoning"]
        .as_str()
        .or_else(|| delta["reasoning_content"].as_str())
        .unwrap_or("")
        .to_string();
    (content, reasoning)
}

pub fn openai_completion_text(v: &Value) -> Option<String> {
    v["choices"][0]["message"]["content"]
        .as_str()
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
}

pub fn openai_embedding(v: &Value) -> Option<Vec<f32>> {
    let arr = v["data"][0]["embedding"].as_array()?;
    Some(
        arr.iter()
            .filter_map(|x| x.as_f64().map(|f| f as f32))
            .collect(),
    )
}

pub fn ollama_embedding(v: &Value) -> Option<Vec<f32>> {
    let arr = v["embeddings"]
        .get(0)
        .and_then(|e| e.as_array())
        .or_else(|| v["embedding"].as_array())?;
    Some(
        arr.iter()
            .filter_map(|x| x.as_f64().map(|f| f as f32))
            .collect(),
    )
}

pub fn openai_model_ids(v: &Value) -> Vec<String> {
    v["data"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|m| m["id"].as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

pub fn ollama_model_ids(v: &Value) -> Vec<String> {
    v["models"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|m| m["name"].as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

pub fn ollama_parts(v: &Value) -> (String, String, bool) {
    let content = v["message"]["content"].as_str().unwrap_or("").to_string();
    let thinking = v["message"]["thinking"].as_str().unwrap_or("").to_string();
    let done = v["done"].as_bool().unwrap_or(false);
    (content, thinking, done)
}

pub fn kind_from_openapi(v: &Value) -> Option<ConcreteKind> {
    let paths = v.get("paths")?.as_object()?;
    let mut openai = false;
    let mut ollama = false;
    for p in paths.keys() {
        if p.contains("chat/completions") {
            openai = true;
        }
        if p.contains("/api/chat") {
            ollama = true;
        }
    }
    if openai {
        Some(ConcreteKind::OpenAi)
    } else if ollama {
        Some(ConcreteKind::Ollama)
    } else {
        None
    }
}

fn messages(system_ctx: &str, turns: &[ChatTurn], kind: ConcreteKind) -> Vec<Value> {
    let mut out = Vec::new();
    if !system_ctx.trim().is_empty() {
        out.push(json!({"role": "system", "content": system_ctx}));
    }
    for t in turns {
        if t.role == "system" {
            out.push(json!({"role": "system", "content": t.text}));
            continue;
        }
        let role = map_role(kind, &t.role);
        let images: Vec<_> = t
            .attachments
            .iter()
            .filter(|a| a.mime_type.starts_with("image/"))
            .collect();
        let mut text = t.text.clone();
        for att in t
            .attachments
            .iter()
            .filter(|a| !a.mime_type.starts_with("image/"))
        {
            match base64_decode(&att.data_base64)
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
            {
                Some(s) => text.push_str(&format!("\n\n[file: {}]\n{s}", att.name)),
                None => text.push_str(&format!("\n\n[file: {} attached]", att.name)),
            }
        }
        if images.is_empty() {
            out.push(json!({"role": role, "content": text}));
        } else {
            let mut parts = vec![json!({"type": "text", "text": text})];
            for img in images {
                parts.push(json!({
                    "type": "image_url",
                    "image_url": { "url": format!("data:{};base64,{}", img.mime_type, img.data_base64) }
                }));
            }
            out.push(json!({"role": role, "content": parts}));
        }
    }
    out
}

fn b64_val(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'Z' => Some(c - b'A'),
        b'a'..=b'z' => Some(c - b'a' + 26),
        b'0'..=b'9' => Some(c - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

fn base64_decode(data: &str) -> Result<Vec<u8>, ()> {
    let bytes: Vec<u8> = data.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes.len() - i < 2 {
            return Err(());
        }
        let a = b64_val(bytes[i]).ok_or(())?;
        let b = b64_val(bytes[i + 1]).ok_or(())?;
        let c_pad = bytes.get(i + 2).copied().unwrap_or(b'=') == b'=';
        let d_pad = bytes.get(i + 3).copied().unwrap_or(b'=') == b'=';
        let c = if c_pad {
            0
        } else {
            b64_val(bytes[i + 2]).ok_or(())?
        };
        let d = if d_pad {
            0
        } else {
            b64_val(*bytes.get(i + 3).ok_or(())?).ok_or(())?
        };
        out.push((a << 2) | (b >> 4));
        if !c_pad {
            out.push((b << 4) | (c >> 2));
        }
        if !d_pad {
            out.push((c << 6) | d);
        }
        i += 4;
    }
    Ok(out)
}

fn auth(req: reqwest::RequestBuilder, api_key: &str) -> reqwest::RequestBuilder {
    if api_key.is_empty() {
        req
    } else {
        req.bearer_auth(api_key)
    }
}

async fn error_from(resp: reqwest::Response) -> anyhow::Error {
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    anyhow::anyhow!("status={} {}", status.as_u16(), api_error_message(&body))
}

pub async fn get_json(http: &Client, url: &str) -> Result<Value> {
    let resp = http.get(url).send().await?;
    if !resp.status().is_success() {
        return Err(error_from(resp).await);
    }
    Ok(resp.json().await?)
}

pub async fn openai_list_models(http: &Client, base: &str, api_key: &str) -> Result<Vec<String>> {
    let resp = auth(http.get(format!("{}/models", openai_root(base))), api_key)
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(error_from(resp).await);
    }
    Ok(openai_model_ids(&resp.json().await?))
}

pub async fn ollama_list_models(http: &Client, base: &str) -> Result<Vec<String>> {
    let resp = http
        .get(format!("{}/api/tags", service_root(base)))
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(error_from(resp).await);
    }
    Ok(ollama_model_ids(&resp.json().await?))
}

pub async fn openai_embed(
    http: &Client,
    base: &str,
    api_key: &str,
    model: &str,
    text: &str,
    timeout: Duration,
) -> Result<Vec<f32>> {
    let resp = auth(
        http.post(format!("{}/embeddings", openai_root(base)))
            .timeout(timeout),
        api_key,
    )
    .json(&json!({"model": model, "input": text}))
    .send()
    .await?;
    if !resp.status().is_success() {
        return Err(error_from(resp).await);
    }
    openai_embedding(&resp.json().await?).ok_or_else(|| anyhow::anyhow!("missing embedding"))
}

pub async fn ollama_embed(
    http: &Client,
    base: &str,
    model: &str,
    text: &str,
    timeout: Duration,
) -> Result<Vec<f32>> {
    let resp = http
        .post(format!("{}/api/embed", service_root(base)))
        .timeout(timeout)
        .json(&json!({"model": model, "input": text}))
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(error_from(resp).await);
    }
    ollama_embedding(&resp.json().await?).ok_or_else(|| anyhow::anyhow!("missing embedding"))
}

pub async fn openai_generate(
    http: &Client,
    base: &str,
    api_key: &str,
    model: &str,
    system_ctx: &str,
    turns: &[ChatTurn],
    timeout: Duration,
) -> Result<String> {
    let resp = auth(
        http.post(format!("{}/chat/completions", openai_root(base)))
            .timeout(timeout),
        api_key,
    )
    .json(&json!({
        "model": model,
        "temperature": 0.2,
        "messages": messages(system_ctx, turns, ConcreteKind::OpenAi)
    }))
    .send()
    .await?;
    if !resp.status().is_success() {
        return Err(error_from(resp).await);
    }
    let v: Value = resp.json().await?;
    openai_completion_text(&v).ok_or_else(|| anyhow::anyhow!("empty completion"))
}

pub async fn ollama_generate(
    http: &Client,
    base: &str,
    model: &str,
    system_ctx: &str,
    turns: &[ChatTurn],
    timeout: Duration,
) -> Result<String> {
    let resp = http
        .post(format!("{}/api/chat", service_root(base)))
        .timeout(timeout)
        .json(&json!({
            "model": model,
            "stream": false,
            "messages": messages(system_ctx, turns, ConcreteKind::Ollama)
        }))
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(error_from(resp).await);
    }
    let v: Value = resp.json().await?;
    v["message"]["content"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("empty completion"))
}

pub async fn openai_stream(
    http: &Client,
    base: &str,
    api_key: &str,
    model: &str,
    system_ctx: &str,
    turns: &[ChatTurn],
    timeout: Duration,
    tx: &tokio::sync::mpsc::Sender<StreamEvent>,
) -> Result<String> {
    let resp = auth(
        http.post(format!("{}/chat/completions", openai_root(base)))
            .timeout(timeout),
        api_key,
    )
    .json(&json!({
        "model": model,
        "temperature": 0.2,
        "stream": true,
        "messages": messages(system_ctx, turns, ConcreteKind::OpenAi)
    }))
    .send()
    .await?;
    if !resp.status().is_success() {
        return Err(error_from(resp).await);
    }
    read_openai_sse(resp, tx).await
}

pub async fn ollama_stream(
    http: &Client,
    base: &str,
    model: &str,
    system_ctx: &str,
    turns: &[ChatTurn],
    timeout: Duration,
    tx: &tokio::sync::mpsc::Sender<StreamEvent>,
) -> Result<String> {
    let resp = http
        .post(format!("{}/api/chat", service_root(base)))
        .timeout(timeout)
        .json(&json!({
            "model": model,
            "stream": true,
            "messages": messages(system_ctx, turns, ConcreteKind::Ollama)
        }))
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(error_from(resp).await);
    }
    let mut full = String::new();
    let mut buf = String::new();
    let mut resp = resp;
    while let Some(chunk) = resp.chunk().await? {
        buf.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(pos) = buf.find('\n') {
            let line = buf[..pos].trim().to_string();
            buf.drain(..=pos);
            if line.is_empty() {
                continue;
            }
            if let Ok(v) = serde_json::from_str::<Value>(&line) {
                let (content, thinking, _done) = ollama_parts(&v);
                if !thinking.is_empty() {
                    emit(tx, StreamEvent::Thought { text: thinking }).await;
                }
                if !content.is_empty() {
                    full.push_str(&content);
                    emit(tx, StreamEvent::Delta { text: content }).await;
                }
            }
        }
    }
    Ok(full)
}

async fn emit(tx: &tokio::sync::mpsc::Sender<StreamEvent>, ev: StreamEvent) {
    let _ = tx.send(ev).await;
}

fn apply_openai_data(data: &str, full: &mut String, pending: &mut Vec<StreamEvent>) {
    if data == "[DONE]" {
        return;
    }
    if let Ok(v) = serde_json::from_str::<Value>(data) {
        let (content, reasoning) = openai_delta_parts(&v);
        if !reasoning.is_empty() {
            pending.push(StreamEvent::Thought { text: reasoning });
        }
        if !content.is_empty() {
            full.push_str(&content);
            pending.push(StreamEvent::Delta { text: content });
        }
    }
}

async fn read_openai_sse(
    mut resp: reqwest::Response,
    tx: &tokio::sync::mpsc::Sender<StreamEvent>,
) -> Result<String> {
    let mut full = String::new();
    let mut buf = String::new();
    while let Some(chunk) = resp.chunk().await? {
        let mut pending = Vec::new();
        feed_sse(&mut buf, &String::from_utf8_lossy(&chunk), |data| {
            apply_openai_data(data, &mut full, &mut pending);
        });
        for ev in pending {
            emit(tx, ev).await;
        }
    }
    if let Some(rest) = buf.trim().strip_prefix("data:") {
        let mut pending = Vec::new();
        apply_openai_data(rest.trim(), &mut full, &mut pending);
        for ev in pending {
            emit(tx, ev).await;
        }
    }
    Ok(full)
}

pub async fn detect_kind(http: &Client, base: &str, openapi_url: &str) -> Result<ConcreteKind> {
    if !openapi_url.trim().is_empty() {
        if let Ok(v) = get_json(http, openapi_url.trim()).await {
            if let Some(k) = kind_from_openapi(&v) {
                return Ok(k);
            }
        }
    }
    let root = service_root(base);
    for path in ["/openapi.json", "/swagger.json", "/v1/openapi.json"] {
        if let Ok(v) = get_json(http, &format!("{root}{path}")).await {
            if let Some(k) = kind_from_openapi(&v) {
                return Ok(k);
            }
        }
    }
    if openai_list_models(http, base, "").await.is_ok() {
        return Ok(ConcreteKind::OpenAi);
    }
    if ollama_list_models(http, base).await.is_ok() {
        return Ok(ConcreteKind::Ollama);
    }
    bail!("no OpenAI-compatible or Ollama chat API at {base}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn roots_and_openapi_detection() {
        assert_eq!(
            openai_root("http://models.internal:11434"),
            "http://models.internal:11434/v1"
        );
        assert_eq!(
            openai_root("http://models.internal:11434/v1/"),
            "http://models.internal:11434/v1"
        );
        assert_eq!(
            service_root("http://models.internal:11434/v1"),
            "http://models.internal:11434"
        );
        let spec = json!({"paths": {"/v1/chat/completions": {}, "/v1/embeddings": {}}});
        assert_eq!(kind_from_openapi(&spec), Some(ConcreteKind::OpenAi));
        let ollama = json!({"paths": {"/api/chat": {}, "/api/tags": {}}});
        assert_eq!(kind_from_openapi(&ollama), Some(ConcreteKind::Ollama));
        assert_eq!(kind_from_openapi(&json!({"paths": {"/health": {}}})), None);
    }

    #[test]
    fn parses_openai_stream_chunks_including_reasoning() {
        let v = json!({"choices":[{"delta":{"content":"pong","reasoning":"short"}}]});
        assert_eq!(openai_delta_parts(&v), ("pong".into(), "short".into()));
        let done = json!({"choices":[{"message":{"content":"pong"}}]});
        assert_eq!(openai_completion_text(&done).as_deref(), Some("pong"));
        let emb = json!({"data":[{"embedding":[0.5, 1.5]}]});
        assert_eq!(openai_embedding(&emb), Some(vec![0.5, 1.5]));
        let mut buf = String::new();
        let mut got = Vec::new();
        feed_sse(&mut buf, "data: {\"a\":1}\n\ndata: [DONE]\n", |d| {
            got.push(d.to_string())
        });
        assert_eq!(got, vec!["{\"a\":1}".to_string(), "[DONE]".to_string()]);
        assert_eq!(base64_decode("aGk=").unwrap(), b"hi");
    }

    #[test]
    fn parses_ollama_tags_and_chat_line() {
        let tags = json!({"models":[{"name":"coder:7b"},{"name":"general:7b"}]});
        assert_eq!(
            ollama_model_ids(&tags),
            vec!["coder:7b".to_string(), "general:7b".to_string()]
        );
        let line = json!({"message":{"content":"hi","thinking":"hmm"},"done":false});
        assert_eq!(ollama_parts(&line), ("hi".into(), "hmm".into(), false));
    }

    #[tokio::test]
    async fn openai_stream_against_local_server() {
        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(|| async {
                let body = concat!(
                    "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"po\"}}]}\n\n",
                    "data: {\"choices\":[{\"delta\":{\"content\":\"ng\",\"reasoning\":\"ok\"}}]}\n\n",
                    "data: [DONE]\n\n"
                );
                axum::response::Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(axum::body::Body::from(body))
                    .unwrap()
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let http = Client::new();
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let text = openai_stream(
            &http,
            &format!("http://{addr}"),
            "",
            "demo",
            "system",
            &[ChatTurn {
                role: "user".into(),
                text: "ping".into(),
                attachments: vec![],
            }],
            Duration::from_secs(5),
            &tx,
        )
        .await
        .unwrap();
        assert_eq!(text, "pong");
        let mut saw_thought = false;
        while let Ok(ev) = rx.try_recv() {
            if matches!(ev, StreamEvent::Thought { .. }) {
                saw_thought = true;
            }
        }
        assert!(saw_thought);
    }
}
