//! Model Context Protocol (MCP) stdio server for xmeshd.
//!
//! Exposes channel context, pinned/learned memory, and thread messaging over
//! JSON-RPC 2.0 on stdin/stdout so IDE agents (Cursor, Zed, Claude Code) can
//! query and update the local or mesh knowledge base without external runtimes.

use crate::retrieve::{assemble_context, map_entries, ENTRY_LEARNED, ENTRY_PINNED};
use crate::storage::{now_ms, pb, validate_channel_name, Storage};
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::debug;

const PROTOCOL_VERSION: &str = "2024-11-05";

/// HTTP origin of a running daemon. `0.0.0.0` is this machine.
pub fn http_base(addr: &str) -> String {
    let (host, port) = addr.rsplit_once(':').unwrap_or(("127.0.0.1", "8080"));
    let host = match host {
        "0.0.0.0" | "[::]" | "*" => "127.0.0.1",
        other => other,
    };
    format!("http://{host}:{port}")
}

/// Speak MCP on stdin/stdout by forwarding each frame to a daemon that is
/// already serving `/api/mcp`. This does not open the database and does not
/// stop the web UI or a running chat.
pub async fn run_stdio_bridge(base_url: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()?;
    let url = format!("{}/api/mcp", base_url.trim_end_matches('/'));
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut lines = BufReader::new(stdin).lines();
    while let Some(line) = lines.next_line().await? {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match client
            .post(&url)
            .header("content-type", "application/json")
            .body(trimmed.to_string())
            .send()
            .await
        {
            Ok(resp) if resp.status() == reqwest::StatusCode::NO_CONTENT => {}
            Ok(resp) => {
                let text = resp.text().await.unwrap_or_default();
                let mut out = text.trim().to_string();
                if out.is_empty() {
                    continue;
                }
                out.push('\n');
                stdout.write_all(out.as_bytes()).await?;
                stdout.flush().await?;
            }
            Err(err) => {
                let id = serde_json::from_str::<Value>(trimmed)
                    .ok()
                    .and_then(|v| v.get("id").cloned())
                    .unwrap_or(Value::Null);
                let resp = json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "code": -32000, "message": format!("daemon unreachable: {err}") }
                });
                write_frame(&mut stdout, &resp).await?;
            }
        }
    }
    Ok(())
}

/// Run the MCP server loop over standard input/output.
/// Ensure tracing/logging is directed to stderr before calling this function,
/// because stdout is strictly reserved for newline-delimited JSON-RPC frames.
pub async fn run_stdio(storage: Storage, node_id: String) -> Result<()> {
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut lines = BufReader::new(stdin).lines();

    while let Some(line) = lines.next_line().await? {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let req: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(err) => {
                let resp = json!({
                    "jsonrpc": "2.0",
                    "id": Value::Null,
                    "error": { "code": -32700, "message": format!("Parse error: {err}") }
                });
                write_frame(&mut stdout, &resp).await?;
                continue;
            }
        };

        if let Some(resp) = handle_request(&storage, &node_id, &req).await {
            write_frame(&mut stdout, &resp).await?;
        }
    }

    Ok(())
}

async fn write_frame(stdout: &mut tokio::io::Stdout, payload: &Value) -> Result<()> {
    let mut encoded = serde_json::to_string(payload)?;
    encoded.push('\n');
    stdout.write_all(encoded.as_bytes()).await?;
    stdout.flush().await?;
    Ok(())
}

pub async fn handle_request(storage: &Storage, node_id: &str, req: &Value) -> Option<Value> {
    let id = req.get("id").cloned();
    let method = req.get("method").and_then(Value::as_str).unwrap_or("");
    let params = req.get("params").cloned().unwrap_or_else(|| json!({}));

    debug!(method, "MCP request received");

    // Notifications do not carry an `id` and expect no response.
    if id.is_none() || method.starts_with("notifications/") {
        return None;
    }
    let id = id.unwrap();

    let outcome = match method {
        "initialize" => Ok(json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {
                "tools": { "listChanged": false },
                "resources": { "subscribe": false, "listChanged": false }
            },
            "serverInfo": {
                "name": "xmeshd",
                "version": env!("CARGO_PKG_VERSION")
            }
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(tools_manifest()),
        "tools/call" => dispatch_tool(storage, node_id, &params).await,
        "resources/list" => list_resources(storage).await,
        "resources/read" => read_resource(storage, &params).await,
        _ => Err(anyhow::anyhow!("Method not found: {method}")),
    };

    Some(match outcome {
        Ok(result) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result
        }),
        Err(err) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32603,
                "message": err.to_string()
            }
        }),
    })
}

fn tools_manifest() -> Value {
    json!({
        "tools": [
            {
                "name": "xmesh_search_context",
                "description": "Search project source files, pinned invariants, and learned notes inside an xmeshd channel.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "channel": { "type": "string", "description": "Channel/project name (e.g. 'general' or 'xenon-mesh')" },
                        "query": { "type": "string", "description": "Search query or symbol name" },
                        "top_k": { "type": "integer", "description": "Max source files to include (default 4)" }
                    },
                    "required": ["channel", "query"]
                }
            },
            {
                "name": "xmesh_save_memory",
                "description": "Store a pinned invariant or learned engineering rule in the channel knowledge base.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "channel": { "type": "string", "description": "Target channel/namespace" },
                        "kind": { "type": "string", "enum": ["pin", "learn"], "description": "'pin' for permanent invariants, 'learn' for ranked notes" },
                        "topic": { "type": "string", "description": "Short path-like key (e.g. 'memory/musl-build')" },
                        "content": { "type": "string", "description": "Concise technical rule or invariant" }
                    },
                    "required": ["channel", "kind", "topic", "content"]
                }
            },
            {
                "name": "xmesh_read_thread",
                "description": "Read recent messages and model council debates from an xmeshd channel thread.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "channel": { "type": "string", "description": "Channel name" },
                        "thread": { "type": "string", "description": "Thread ID (default 'main')" },
                        "limit": { "type": "integer", "description": "Max messages to fetch (default 30)" }
                    },
                    "required": ["channel"]
                }
            },
            {
                "name": "xmesh_post_message",
                "description": "Post a message, diff proposal, or handoff note into an xmeshd channel thread.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "channel": { "type": "string", "description": "Channel name" },
                        "thread": { "type": "string", "description": "Thread ID (default 'main')" },
                        "content": { "type": "string", "description": "Markdown message body" }
                    },
                    "required": ["channel", "content"]
                }
            }
        ]
    })
}

async fn dispatch_tool(storage: &Storage, node_id: &str, params: &Value) -> Result<Value> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .context("Missing tool name")?;
    let args = params.get("arguments").cloned().unwrap_or_else(|| json!({}));

    let text = match name {
        "xmesh_search_context" => tool_search_context(storage, &args).await?,
        "xmesh_save_memory" => tool_save_memory(storage, node_id, &args).await?,
        "xmesh_read_thread" => tool_read_thread(storage, &args).await?,
        "xmesh_post_message" => tool_post_message(storage, node_id, &args).await?,
        _ => bail!("Unknown tool: {name}"),
    };

    Ok(json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "isError": false
    }))
}

async fn tool_search_context(storage: &Storage, args: &Value) -> Result<String> {
    let channel = args
        .get("channel")
        .and_then(Value::as_str)
        .context("Missing 'channel'")?;
    validate_channel_name(channel)?;
    let query = args
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or("");
    let top_k = args
        .get("top_k")
        .and_then(Value::as_u64)
        .unwrap_or(4)
        .clamp(1, 12) as usize;

    let namespaces = match storage.get_channel(channel).await? {
        Some(ch) => ch.context_namespaces,
        None => vec![channel.to_string()],
    };

    let mut raw_entries = Vec::new();
    for ns in &namespaces {
        raw_entries.extend(storage.load_entries(Some(ns)).await?);
    }
    if raw_entries.is_empty() {
        return Ok(format!("No indexed entries found for channel `{channel}`."));
    }

    let mapped = map_entries(&raw_entries);
    let assembled = assemble_context(&mapped, query, &[], top_k, 24_000, 6_000, &mut Vec::new());
    if assembled.is_empty() {
        Ok(format!("No matching context blocks in `{channel}` for query `{query}`."))
    } else {
        Ok(assembled)
    }
}

async fn tool_save_memory(storage: &Storage, node_id: &str, args: &Value) -> Result<String> {
    let channel = args
        .get("channel")
        .and_then(Value::as_str)
        .context("Missing 'channel'")?;
    validate_channel_name(channel)?;
    let kind_str = args
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("learn");
    let entry_type = match kind_str {
        "pin" => ENTRY_PINNED,
        "learn" => ENTRY_LEARNED,
        _ => bail!("kind must be 'pin' or 'learn'"),
    };
    let topic = args
        .get("topic")
        .and_then(Value::as_str)
        .context("Missing 'topic'")?
        .trim();
    if topic.is_empty() {
        bail!("topic cannot be empty");
    }
    let content = args
        .get("content")
        .and_then(Value::as_str)
        .context("Missing 'content'")?
        .trim();
    if content.is_empty() {
        bail!("content cannot be empty");
    }

    let id = blake3::hash(format!("{channel}:{entry_type}:{topic}").as_bytes())
        .to_hex()
        .to_string();
    let content_hash = blake3::hash(content.as_bytes()).to_hex().to_string();

    let entry = pb::ContextEntry {
        id: id.clone(),
        namespace: channel.to_string(),
        entry_type,
        source_path: topic.to_string(),
        content: content.to_string(),
        content_hash,
        embedding: Vec::new(),
        updated_at_ms: now_ms(),
        origin_node: node_id.to_string(),
        meta: Default::default(),
    };

    storage.upsert_proto(&entry, true).await?;
    storage
        .record_audit(node_id, "mcp.save_memory", channel, "main", topic)
        .await?;

    Ok(format!(
        "Saved {kind_str} entry `{topic}` in channel `{channel}` (id: {id})."
    ))
}

async fn tool_read_thread(storage: &Storage, args: &Value) -> Result<String> {
    let channel = args
        .get("channel")
        .and_then(Value::as_str)
        .context("Missing 'channel'")?;
    validate_channel_name(channel)?;
    let thread = args
        .get("thread")
        .and_then(Value::as_str)
        .unwrap_or("main");
    let limit = args
        .get("limit")
        .and_then(Value::as_i64)
        .unwrap_or(30)
        .clamp(1, 200);

    let messages = storage.load_thread(channel, thread, limit).await?;
    if messages.is_empty() {
        return Ok(format!("Thread `{channel}/{thread}` is empty."));
    }

    let mut out = String::new();
    for m in messages {
        out.push_str(&format!("[#{} {}]: {}\n\n", m.seq, m.author, m.content));
    }
    Ok(out)
}

async fn tool_post_message(storage: &Storage, node_id: &str, args: &Value) -> Result<String> {
    let channel = args
        .get("channel")
        .and_then(Value::as_str)
        .context("Missing 'channel'")?;
    validate_channel_name(channel)?;
    let thread = args
        .get("thread")
        .and_then(Value::as_str)
        .unwrap_or("main");
    let content = args
        .get("content")
        .and_then(Value::as_str)
        .context("Missing 'content'")?;
    if content.trim().is_empty() {
        bail!("content cannot be empty");
    }

    storage.ensure_channel(channel, "private", node_id).await?;
    let seq = storage.next_seq(channel, thread).await?;
    let msg = pb::SessionMessage {
        channel_id: channel.to_string(),
        thread_id: thread.to_string(),
        seq,
        author: format!("{node_id} (mcp)"),
        content: content.to_string(),
        created_at_ms: now_ms(),
    };
    storage.append_message(&msg).await?;
    storage
        .record_audit(node_id, "mcp.post_message", channel, thread, &format!("seq={seq}"))
        .await?;

    Ok(format!("Posted message #{seq} to `{channel}/{thread}`."))
}

async fn list_resources(storage: &Storage) -> Result<Value> {
    let channels = storage.list_channel_rows().await?;
    let resources: Vec<Value> = channels
        .into_iter()
        .map(|c| {
            json!({
                "uri": format!("xmesh://channel/{}/memory", c.name),
                "name": format!("{} ({})", c.name, c.visibility),
                "description": format!("Pinned invariants and learned notes for channel {}", c.name),
                "mimeType": "text/markdown"
            })
        })
        .collect();
    Ok(json!({ "resources": resources }))
}

async fn read_resource(storage: &Storage, params: &Value) -> Result<Value> {
    let uri = params
        .get("uri")
        .and_then(Value::as_str)
        .context("Missing 'uri'")?;
    let rest = uri
        .strip_prefix("xmesh://channel/")
        .context("Unsupported URI scheme")?;
    let channel = rest
        .strip_suffix("/memory")
        .context("Unsupported resource path")?;
    validate_channel_name(channel)?;

    let items = storage.list_channel_memory(channel).await?;
    let mut md = format!("# Memory for channel `{channel}`\n\n");
    if items.is_empty() {
        md.push_str("_No pinned or learned entries yet._\n");
    } else {
        for item in items {
            md.push_str(&format!(
                "## [{}] `{}`\n{}\n\n",
                item.kind, item.source_path, item.content
            ));
        }
    }

    Ok(json!({
        "contents": [
            {
                "uri": uri,
                "mimeType": "text/markdown",
                "text": md
            }
        ]
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn temp_storage() -> Storage {
        let path = std::env::temp_dir().join(format!(
            "xmesh-mcp-{}-{}.db",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_file(&path);
        Storage::connect(None, path.to_str().unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn mcp_roundtrip_saves_memory_and_reads_resource() {
        let st = temp_storage().await;

        let init = handle_request(
            &st,
            "node-test",
            &json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {} }),
        )
        .await
        .unwrap();
        assert_eq!(init["result"]["protocolVersion"], PROTOCOL_VERSION);

        st.ensure_channel("demo", "private", "node-test")
            .await
            .unwrap();

        let save = handle_request(
            &st,
            "node-test",
            &json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "xmesh_save_memory",
                    "arguments": {
                        "channel": "demo",
                        "kind": "pin",
                        "topic": "memory/musl",
                        "content": "Link libgit2 statically"
                    }
                }
            }),
        )
        .await
        .unwrap();
        assert!(save["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("Saved pin"));

        let res = handle_request(
            &st,
            "node-test",
            &json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "resources/read",
                "params": { "uri": "xmesh://channel/demo/memory" }
            }),
        )
        .await
        .unwrap();
        let text = res["result"]["contents"][0]["text"].as_str().unwrap();
        assert!(text.contains("Link libgit2 statically"));
    }

    #[tokio::test]
    async fn a_notification_has_no_response() {
        let st = temp_storage().await;
        let none = handle_request(
            &st,
            "node-test",
            &json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
        )
        .await;
        assert!(none.is_none());
        assert_eq!(http_base("0.0.0.0:8091"), "http://127.0.0.1:8091");
    }

    #[tokio::test]
    async fn the_demo_script_drives_the_http_route() {
        use axum::extract::State;
        use axum::response::IntoResponse;
        use axum::routing::post;
        use axum::Json;
        use std::sync::Arc;

        let st = Arc::new(temp_storage().await);
        let app = axum::Router::new()
            .route(
                "/api/mcp",
                post(
                    |State(st): State<Arc<Storage>>, Json(req): Json<Value>| async move {
                        match handle_request(&st, "node-test", &req).await {
                            Some(resp) => Json(resp).into_response(),
                            None => axum::http::StatusCode::NO_CONTENT.into_response(),
                        }
                    },
                ),
            )
            .with_state(st);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/mcp-demo.sh");
        let out = tokio::process::Command::new("sh")
            .arg(&script)
            .arg(format!("http://127.0.0.1:{port}"))
            .output()
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&out.stdout);
        assert!(
            out.status.success(),
            "{} {}",
            text,
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(text.contains("xmesh_search_context"), "{text}");
        assert!(text.contains("Saved pin"), "{text}");
        assert!(text.contains("Posted message"), "{text}");
    }
}
