use crate::config::{AppConfig, NodeMode};
use crate::git_sync::{discover_bare_repos, scan_bare_repo, scan_workdir};
use crate::hub;
use crate::models::{Attachment, ChatTurn, ModelHub, StreamEvent};
use crate::retrieve::{assemble_context, coding_preamble, map_entries};
use crate::storage::pb::cluster_service_client::ClusterServiceClient;
use crate::storage::pb::{ContextEntry, EntryType, NodeTaskRequest, SessionMessage, SyncEnvelope};
use crate::storage::{now_ms, ChannelRow, Storage};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Html,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::warn;

pub struct WebState {
    pub cfg: Arc<AppConfig>,
    pub db: Arc<Storage>,
    pub models: Arc<ModelHub>,
    pub hmac_key: [u8; 32],
    pub status: Arc<RwLock<IndexStatus>>,
}

#[derive(Clone, Serialize, Default)]
pub struct IndexStatus {
    pub busy: bool,
    pub phase: String,
    pub current_repo: String,
    pub done_repos: usize,
    pub total_repos: usize,
    pub indexed_files: usize,
    pub mode: String,
    pub embeddings: bool,
}

#[derive(Deserialize)]
pub struct ChatReq {
    pub channel: String,
    pub thread: String,
    pub prompt: String,
    pub model: Option<String>,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
}

#[derive(Deserialize)]
pub struct ForkThreadReq {
    pub channel: String,
    pub src_thread: String,
    pub dst_thread: String,
    pub keep_head: usize,
    pub keep_tail: usize,
}

#[derive(Deserialize)]
pub struct HistoryQuery {
    pub channel: String,
    pub thread: String,
}
#[derive(Deserialize)]
pub struct ThreadsQuery {
    pub channel: String,
}
#[derive(Deserialize)]
pub struct ReindexQuery {
    pub namespace: String,
}
#[derive(Deserialize)]
pub struct MemListQuery {
    pub channel: String,
}
#[derive(Deserialize)]
pub struct MemDelQuery {
    pub id: String,
}
#[derive(Deserialize)]
pub struct LearnReq {
    pub channel: String,
    pub title: String,
    pub text: String,
    pub pin: bool,
}
#[derive(Deserialize)]
pub struct ApproveReq {
    pub node_grpc_url: String,
    pub target_repo: String,
    pub unified_diff: String,
    pub deploy_pipeline: String,
}
#[derive(Deserialize)]
pub struct OaiChatReq {
    pub model: String,
    pub messages: Vec<OaiMsg>,
}
#[derive(Deserialize, Serialize)]
pub struct OaiMsg {
    pub role: String,
    pub content: String,
}
#[derive(Deserialize)]
pub struct ChannelUpsert {
    pub name: String,
    #[serde(default)]
    pub visibility: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub context_namespaces: Vec<String>,
}

pub fn router(state: Arc<WebState>) -> Router {
    Router::new()
        .route("/", get(|| async { Html(include_str!("../assets/index.html")) }))
        .route("/api/health", get(handle_health))
        .route("/api/chat", post(handle_persistent_chat))
        .route("/api/history", get(handle_history))
        .route("/api/threads", get(handle_list_threads))
        .route("/api/memory/learn", post(handle_learn))
        .route("/api/task/approve", post(handle_approve))
        .route("/api/internal/reindex", post(handle_reindex))
        .route("/api/config", get(handle_config))
        .route("/api/threads/fork", post(handle_fork_thread))
        .route("/api/memory", get(handle_list_memory).delete(handle_delete_memory))
        .route("/api/channels", get(handle_list_channels).post(handle_upsert_channel))
        .route("/api/status", get(|State(st): State<Arc<WebState>>| async move {
            Json(st.status.read().await.clone())
        }))
        .route("/v1/chat/completions", post(handle_oai_compat))
        .layer(axum::extract::DefaultBodyLimit::max(25 * 1024 * 1024))
        .with_state(state)
}

fn mode_name(mode: &NodeMode) -> &'static str {
    match mode {
        NodeMode::Master => "master",
        NodeMode::Node => "node",
    }
}

fn master_url(cfg: &AppConfig) -> Option<&str> {
    match cfg.node.mode {
        NodeMode::Node => cfg.node.master_url.as_deref().filter(|s| !s.is_empty()),
        NodeMode::Master => None,
    }
}

async fn handle_health(State(st): State<Arc<WebState>>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "ok": true,
        "mode": mode_name(&st.cfg.node.mode),
        "embeddings": st.models.embeddings_configured().await
    }))
}

async fn handle_config(State(st): State<Arc<WebState>>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "default_model": st.models.default_model().await,
        "models": st.models.model_choices().await,
        "mode": mode_name(&st.cfg.node.mode),
        "node_id": st.cfg.node.id,
        "embeddings": st.models.embeddings_configured().await
    }))
}

async fn handle_list_channels(
    State(st): State<Arc<WebState>>,
) -> Result<Json<Vec<ChannelRow>>, StatusCode> {
    let mut rows = st
        .db
        .list_channel_rows()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if let Some(url) = master_url(&st.cfg) {
        if let Ok(remote) = hub::list_public(url).await {
            for ch in remote {
                let row = ChannelRow::from_proto(&ch);
                if !rows.iter().any(|r| r.name == row.name) {
                    rows.push(row);
                }
            }
        }
    }
    if !rows.iter().any(|r| r.name == "general") {
        rows.insert(
            0,
            ChannelRow {
                name: "general".into(),
                visibility: "public".into(),
                namespace: "general".into(),
                origin_node: st.cfg.node.id.clone(),
                context_namespaces: vec!["general".into()],
                updated_at_ms: 0,
            },
        );
    }
    rows.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(rows))
}

async fn handle_upsert_channel(
    State(st): State<Arc<WebState>>,
    Json(req): Json<ChannelUpsert>,
) -> Result<Json<ChannelRow>, StatusCode> {
    let namespace = if req.namespace.trim().is_empty() {
        req.name.clone()
    } else {
        req.namespace
    };
    let mut contexts = req.context_namespaces;
    if contexts.is_empty() {
        contexts.push(namespace.clone());
    }
    let row = ChannelRow {
        name: req.name,
        visibility: if req.visibility.trim().is_empty() {
            "private".into()
        } else {
            req.visibility
        },
        namespace,
        origin_node: st.cfg.node.id.clone(),
        context_namespaces: contexts,
        updated_at_ms: now_ms(),
    };
    st.db
        .upsert_channel(&row)
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let stored = st
        .db
        .get_channel(&row.name)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .unwrap_or(row);
    if stored.visibility == "public" {
        if let Some(url) = master_url(&st.cfg) {
            let env = SyncEnvelope {
                schema_version: 2,
                source_node: st.cfg.node.id.clone(),
                timestamp_ms: now_ms(),
                channels: vec![stored.to_proto()],
                ..Default::default()
            };
            if let Err(err) = hub::push_envelope(url, env).await {
                warn!("public channel was saved locally but not published: {err}");
            }
        }
    }
    Ok(Json(stored))
}

async fn channel_is_public(st: &WebState, name: &str) -> bool {
    match st.db.get_channel(name).await.ok().flatten() {
        Some(ch) => ch.visibility == "public",
        None => name == "general",
    }
}

async fn handle_history(
    State(st): State<Arc<WebState>>,
    Query(q): Query<HistoryQuery>,
) -> Result<Json<Vec<ChatTurn>>, StatusCode> {
    if channel_is_public(&st, &q.channel).await
        && st
            .db
            .thread_visibility(&q.channel, &q.thread)
            .await
            .unwrap_or_default()
            != "private"
    {
        if let Some(url) = master_url(&st.cfg) {
            if let Ok(env) = hub::query_namespace(url, &q.channel).await {
                for msg in env.messages.into_iter().filter(|m| m.thread_id == q.thread) {
                    let _ = st.db.append_message(&msg).await;
                }
            }
        }
    }
    let msgs = st
        .db
        .load_thread(&q.channel, &q.thread, 200)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(
        msgs.into_iter()
            .map(|m| ChatTurn {
                role: m.author,
                text: m.content,
                attachments: vec![],
            })
            .collect(),
    ))
}

async fn handle_list_threads(
    State(st): State<Arc<WebState>>,
    Query(q): Query<ThreadsQuery>,
) -> Result<Json<Vec<String>>, StatusCode> {
    st.db
        .list_threads(&q.channel)
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn handle_learn(
    State(st): State<Arc<WebState>>,
    Json(req): Json<LearnReq>,
) -> Result<StatusCode, StatusCode> {
    let summary = if req.pin {
        req.text
    } else {
        st.models
            .distill(&req.text)
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?
    };
    let id = blake3::hash(format!("mem:{}:{}", req.channel, req.title).as_bytes())
        .to_hex()
        .to_string();
    let hash = blake3::hash(summary.as_bytes()).to_hex().to_string();
    let entry = ContextEntry {
        id,
        namespace: req.channel.clone(),
        entry_type: if req.pin {
            EntryType::Pinned as i32
        } else {
            EntryType::Learned as i32
        },
        source_path: format!("memory/{}", req.title),
        content: summary,
        content_hash: hash,
        origin_node: st.cfg.node.id.clone(),
        updated_at_ms: now_ms(),
        ..Default::default()
    };
    st.db
        .upsert_proto(&entry, true)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if channel_is_public(&st, &req.channel).await {
        if let Some(url) = master_url(&st.cfg) {
            let env = SyncEnvelope {
                schema_version: 2,
                source_node: st.cfg.node.id.clone(),
                timestamp_ms: now_ms(),
                entries: vec![entry],
                ..Default::default()
            };
            let _ = hub::push_envelope(url, env).await;
        }
    }
    Ok(StatusCode::OK)
}

async fn handle_approve(
    State(st): State<Arc<WebState>>,
    Json(req): Json<ApproveReq>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let task_id = blake3::hash(req.unified_diff.as_bytes()).to_hex()[..16].to_string();
    let exp = now_ms() + 30_000;
    let payload = format!(
        "{}:{}:{}:{}:{}",
        task_id, req.target_repo, req.unified_diff, req.deploy_pipeline, exp
    );
    let sig = blake3::keyed_hash(&st.hmac_key, payload.as_bytes())
        .as_bytes()
        .to_vec();
    let mut client = ClusterServiceClient::connect(req.node_grpc_url)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let res = client
        .execute_task(NodeTaskRequest {
            task_id,
            target_repo: req.target_repo,
            unified_diff: req.unified_diff,
            deploy_pipeline: req.deploy_pipeline,
            master_hmac_sig: sig,
            expires_at_ms: exp,
        })
        .await
        .map_err(|_| StatusCode::FORBIDDEN)?
        .into_inner();
    Ok(Json(
        serde_json::json!({ "success": res.success, "code": res.exit_code, "out": res.stdout_log, "err": res.stderr_log }),
    ))
}

async fn handle_list_memory(
    State(st): State<Arc<WebState>>,
    Query(q): Query<MemListQuery>,
) -> Result<Json<Vec<crate::storage::MemoryItem>>, StatusCode> {
    st.db
        .list_channel_memory(&q.channel)
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn handle_delete_memory(
    State(st): State<Arc<WebState>>,
    Query(q): Query<MemDelQuery>,
) -> Result<StatusCode, StatusCode> {
    st.db
        .delete_entry(&q.id)
        .await
        .map(|_| StatusCode::OK)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn handle_fork_thread(
    State(st): State<Arc<WebState>>,
    Json(req): Json<ForkThreadReq>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if channel_is_public(&st, &req.channel).await {
        if let Some(url) = master_url(&st.cfg) {
            if let Ok(env) = hub::query_namespace(url, &req.channel).await {
                for msg in env
                    .messages
                    .into_iter()
                    .filter(|m| m.thread_id == req.src_thread)
                {
                    let _ = st.db.append_message(&msg).await;
                }
            }
        }
    }
    let copied = st
        .db
        .fork_thread(
            &req.channel,
            &req.src_thread,
            &req.dst_thread,
            req.keep_head,
            req.keep_tail,
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(
        serde_json::json!({ "copied": copied, "thread": req.dst_thread, "visibility": "private" }),
    ))
}

async fn handle_reindex(
    State(st): State<Arc<WebState>>,
    Query(q): Query<ReindexQuery>,
) -> Result<StatusCode, StatusCode> {
    let Some(root) = &st.cfg.git.bare_repos_root else {
        return Ok(StatusCode::NO_CONTENT);
    };
    let repos = discover_bare_repos(Path::new(root));
    let Some((_, repo_path)) = repos.into_iter().find(|(ns, _)| ns == &q.namespace) else {
        return Err(StatusCode::NOT_FOUND);
    };
    if let Ok(entries) = scan_bare_repo(&repo_path, &q.namespace) {
        for mut e in entries {
            if let Ok(emb) = st.models.embed_text(&e.content).await {
                e.embedding = emb;
            }
            let _ = st.db.upsert_proto(&e, false).await;
        }
    }
    Ok(StatusCode::OK)
}

async fn handle_oai_compat(
    State(st): State<Arc<WebState>>,
    Json(req): Json<OaiChatReq>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let channel = req.model.strip_prefix("channel:").unwrap_or(&req.model);
    let last_user = req
        .messages
        .iter()
        .rev()
        .find(|m| m.role == "user")
        .map(|m| m.content.as_str())
        .unwrap_or("");
    let ctx = prompt_for(&st, channel, "main", last_user)
        .await
        .unwrap_or_default();
    let turns: Vec<ChatTurn> = req
        .messages
        .into_iter()
        .map(|m| ChatTurn {
            role: m.role,
            text: m.content,
            attachments: vec![],
        })
        .collect();
    let reply = st
        .models
        .generate("", &ctx, &turns)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(serde_json::json!({
        "id": "chatcmpl-xmesh",
        "object": "chat.completion",
        "choices": [{ "index": 0, "message": { "role": "assistant", "content": reply }, "finish_reason": "stop" }]
    })))
}

async fn prompt_for(
    st: &WebState,
    channel: &str,
    _thread: &str,
    query: &str,
) -> Result<String, StatusCode> {
    let channel_row = st.db.get_channel(channel).await.ok().flatten();
    let namespaces = channel_row
        .as_ref()
        .map(|c| c.context_namespaces.clone())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| vec![channel.to_string()]);
    let mut merged: HashMap<String, ContextEntry> = HashMap::new();
    for ns in &namespaces {
        if let Ok(entries) = st.db.load_entries(Some(ns)).await {
            for e in entries {
                merged.insert(e.id.clone(), e);
            }
        }
        if channel_is_public(st, channel).await {
            if let Some(url) = master_url(&st.cfg) {
                if let Ok(env) = hub::query_namespace(url, ns).await {
                    for e in env.entries {
                        merged.entry(e.id.clone()).or_insert(e);
                    }
                }
            }
        }
        if let Some(repo) = st.cfg.git.watched_repos.iter().find(|r| &r.namespace == ns) {
            let path = repo.local_path.clone();
            let ns = ns.clone();
            let origin = st.cfg.node.id.clone();
            if let Ok(Ok(files)) =
                tokio::task::spawn_blocking(move || scan_workdir(Path::new(&path), &ns, &origin))
                    .await
            {
                for e in files {
                    merged.entry(e.id.clone()).or_insert(e);
                }
            }
        }
    }
    let entries: Vec<_> = merged.into_values().collect();
    let q_emb = st.models.embed_text(query).await.unwrap_or_default();
    let retrieved = assemble_context(&map_entries(&entries), query, &q_emb, 8, 28_000);
    Ok(format!("{}\n\n{retrieved}", coding_preamble(channel)))
}

async fn store_message(st: &WebState, mut msg: SessionMessage, sync_hub: bool) -> SessionMessage {
    if sync_hub {
        if let Some(url) = master_url(&st.cfg) {
            match hub::append_public(url, msg.clone()).await {
                Ok(stored) => {
                    let _ = st.db.append_message(&stored).await;
                    return stored;
                }
                Err(err) => warn!("hub append failed, keeping the message locally: {err}"),
            }
        } else if let Ok(stored) = st.db.append_public_message(&msg).await {
            return stored;
        }
    }
    if msg.seq == 0 {
        msg.seq = st
            .db
            .next_seq(&msg.channel_id, &msg.thread_id)
            .await
            .unwrap_or(1);
    }
    let _ = st.db.append_message(&msg).await;
    msg
}

async fn handle_persistent_chat(
    State(st): State<Arc<WebState>>,
    Json(req): Json<ChatReq>,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    let past = st
        .db
        .load_thread(&req.channel, &req.thread, 100)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let public = channel_is_public(&st, &req.channel).await;
    let private_thread = st
        .db
        .thread_visibility(&req.channel, &req.thread)
        .await
        .unwrap_or_default()
        == "private";
    let sync_hub = public && !private_thread;

    let att_note = if req.attachments.is_empty() {
        String::new()
    } else {
        format!(
            "\n[Attached: {}]",
            req.attachments
                .iter()
                .map(|a| a.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let user_msg = SessionMessage {
        channel_id: req.channel.clone(),
        thread_id: req.thread.clone(),
        seq: 0,
        author: "user".into(),
        content: format!("{}{}", req.prompt, att_note),
        created_at_ms: 0,
    };
    let _stored_user = store_message(&st, user_msg, sync_hub).await;

    let mut turns: Vec<ChatTurn> = past
        .into_iter()
        .map(|m| ChatTurn {
            role: m.author,
            text: m.content,
            attachments: vec![],
        })
        .collect();
    turns.push(ChatTurn {
        role: "user".into(),
        text: req.prompt.clone(),
        attachments: req.attachments,
    });

    let (tx, rx) = tokio::sync::mpsc::channel::<StreamEvent>(64);
    let st_bg = st.clone();
    let model = req.model.filter(|s| !s.is_empty()).unwrap_or_default();
    tokio::spawn(async move {
        let _ = tx
            .send(StreamEvent::Status {
                text: "Building context...".into(),
            })
            .await;
        let ctx = prompt_for(&st_bg, &req.channel, &req.thread, &req.prompt)
            .await
            .unwrap_or_default();
        let (author, final_text) = match st_bg
            .models
            .stream_chat(&model, &ctx, &turns, tx.clone())
            .await
        {
            Ok((reply, used)) => (used, reply),
            Err(err) => {
                let err_txt = format!("[ERROR] {err}");
                let _ = tx
                    .send(StreamEvent::Delta {
                        text: err_txt.clone(),
                    })
                    .await;
                ("error".into(), err_txt)
            }
        };
        let author = if author.is_empty() { model } else { author };
        let _ = store_message(
            &st_bg,
            SessionMessage {
                channel_id: req.channel,
                thread_id: req.thread,
                seq: 0,
                author,
                content: final_text,
                created_at_ms: 0,
            },
            sync_hub,
        )
        .await;
    });

    let stream = ReceiverStream::new(rx)
        .map(|ev| Ok(Event::default().data(serde_json::to_string(&ev).unwrap_or_default())));
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
