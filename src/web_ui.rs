use crate::config::{AppConfig, NodeMode};
use crate::git_sync::{discover_bare_repos, scan_bare_repo, scan_workdir};
use crate::hub;
use crate::models::{Attachment, ChatTurn, ModelChoice, ModelHub, StreamEvent};
use crate::retrieve::{assemble_context, coding_preamble, map_entries};
use crate::storage::pb::cluster_service_client::ClusterServiceClient;
use crate::storage::pb::{ContextEntry, EntryType, NodeTaskRequest, SessionMessage, SyncEnvelope};
use crate::storage::{
    channel_group, cmp_sidebar, now_ms, AuditEvent, ChannelGroup, ChannelPref, ChannelRow,
    SidebarKey, Storage,
};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
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

#[derive(Clone)]
pub struct Presence {
    pub channel: String,
    pub viewing: bool,
}

pub struct WebState {
    pub cfg: Arc<AppConfig>,
    pub db: Arc<Storage>,
    pub models: Arc<ModelHub>,
    pub hmac_key: [u8; 32],
    pub node_key: Option<[u8; 32]>,
    pub peer_keys: HashMap<String, [u8; 32]>,
    pub status: Arc<RwLock<IndexStatus>>,
    pub presence: Arc<std::sync::Mutex<Presence>>,
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
        .route("/api/internal/complete", post(handle_complete))
        .route("/api/config", get(handle_config))
        .route("/api/threads/fork", post(handle_fork_thread))
        .route("/api/memory", get(handle_list_memory).delete(handle_delete_memory))
        .route("/api/channels", get(handle_list_channels).post(handle_upsert_channel))
        .route("/api/channel-state", post(handle_channel_state))
        .route("/api/presence", post(handle_presence))
        .route("/api/share", post(handle_share))
        .route("/api/join", post(handle_join))
        .route("/api/inspect", post(handle_inspect))
        .route("/api/audit", get(handle_audit).post(handle_audit_post))
        .route("/api/mesh/channels", get(handle_mesh_channels))
        .route("/api/mesh/activity", get(handle_mesh_activity))
        .route("/api/mesh/share", post(handle_mesh_share))
        .route("/api/mesh/join", post(handle_mesh_join))
        .route("/api/mesh/history", get(handle_mesh_history))
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

fn master_http(cfg: &AppConfig) -> Option<&str> {
    match cfg.node.mode {
        NodeMode::Node => cfg
            .node
            .master_http
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty()),
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
    let mut models = st.models.model_choices().await;
    let mut default_model = st.models.default_model().await;
    if let Some(base) = master_http(&st.cfg) {
        if let Some(remote) = fetch_hub_config(base).await {
            if default_model.is_empty() {
                default_model = remote.default_model;
            }
            for choice in remote.models {
                if !models.iter().any(|m| m.id == choice.id) {
                    models.push(choice);
                }
            }
        }
    }
    if master_http(&st.cfg).is_none() {
        for choice in models.iter_mut() {
            if choice.kind == "gemini" {
                choice.host = st.cfg.node.id.clone();
                choice.label = format!("{} : {}", choice.model, choice.host);
                choice.proxied = true;
            }
        }
    }
    Json(serde_json::json!({
        "default_model": default_model,
        "models": models,
        "mode": mode_name(&st.cfg.node.mode),
        "node_id": st.cfg.node.id,
        "embeddings": st.models.embeddings_configured().await
    }))
}

#[derive(Deserialize)]
struct RemoteCatalog {
    #[serde(default)]
    default_model: String,
    #[serde(default)]
    models: Vec<ModelChoice>,
}

async fn fetch_hub_config(base: &str) -> Option<RemoteCatalog> {
    let url = format!("{}/api/config", base.trim_end_matches('/'));
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .ok()?;
    client.get(url).send().await.ok()?.json().await.ok()
}

async fn handle_list_channels(
    State(st): State<Arc<WebState>>,
) -> Result<Json<Vec<SidebarChannel>>, StatusCode> {
    let mut rows = st
        .db
        .list_channel_rows()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut remote_seq = std::collections::HashMap::new();
    if let Some(base) = master_http(&st.cfg) {
        if let Some(mesh) = fetch_mesh(base, &st.cfg.node.id).await {
            remote_seq = mesh.activity;
            for ch in mesh.channels {
                if !rows.iter().any(|r| r.name == ch.name) {
                    rows.push(ch);
                }
            }
        }
    } else if let Some(url) = master_url(&st.cfg) {
        if let Ok(remote) = hub::list_public(url).await {
            for ch in remote {
                let row = ChannelRow::from_proto(&ch);
                if !rows.iter().any(|r| r.name == row.name) {
                    rows.push(row);
                }
            }
        }
    }
    if matches!(st.cfg.node.mode, NodeMode::Master) {
        if let Ok(local_seq) = st.db.activity().await {
            for (name, seq) in local_seq {
                remote_seq.insert(name, seq);
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
    let mut sidebar = Vec::with_capacity(rows.len());
    for row in rows {
        let pref = st
            .db
            .get_pref(&row.name)
            .await
            .unwrap_or_else(|_| ChannelPref::bare(&row.name));
        let local_max = st.db.max_seq(&row.name).await.unwrap_or(0);
        let max_seq = local_max.max(*remote_seq.get(&row.name).unwrap_or(&0));
        let unread = (max_seq - pref.last_read_seq).max(0);
        let members = st.db.list_members(&row.name).await.unwrap_or_default();
        sidebar.push(SidebarChannel {
            group: channel_group(&row.name, &row.visibility),
            pinned: pref.pinned,
            muted: pref.muted,
            archived: pref.archived && row.name != "general",
            closed: pref.closed && row.name != "general",
            read_count: pref.last_read_seq.max(0),
            unread_count: unread,
            use_count: pref.use_count,
            members,
            name: row.name,
            visibility: row.visibility,
            namespace: row.namespace,
            origin_node: row.origin_node,
        });
    }
    sidebar.sort_by(|a, b| {
        cmp_sidebar(
            &SidebarKey {
                group: a.group,
                pinned: a.pinned,
                use_count: a.use_count,
                name: a.name.clone(),
            },
            &SidebarKey {
                group: b.group,
                pinned: b.pinned,
                use_count: b.use_count,
                name: b.name.clone(),
            },
        )
    });
    Ok(Json(sidebar))
}

#[derive(Clone, Serialize)]
struct SidebarChannel {
    name: String,
    visibility: String,
    namespace: String,
    origin_node: String,
    group: ChannelGroup,
    pinned: bool,
    muted: bool,
    archived: bool,
    closed: bool,
    read_count: i64,
    unread_count: i64,
    use_count: i64,
    members: Vec<String>,
}

#[derive(Deserialize)]
struct MeshView {
    #[serde(default)]
    channels: Vec<ChannelRow>,
    #[serde(default)]
    activity: std::collections::HashMap<String, i64>,
}

async fn fetch_mesh(base: &str, node_id: &str) -> Option<MeshView> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .ok()?;
    let root = base.trim_end_matches('/');
    let channels: Vec<ChannelRow> = client
        .get(format!("{root}/api/mesh/channels"))
        .query(&[("node_id", node_id)])
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    let activity: std::collections::HashMap<String, i64> = client
        .get(format!("{root}/api/mesh/activity"))
        .query(&[("node_id", node_id)])
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    Some(MeshView { channels, activity })
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

async fn channel_visibility(st: &WebState, name: &str) -> String {
    match st.db.get_channel(name).await.ok().flatten() {
        Some(ch) => ch.visibility,
        None if name == "general" => "public".into(),
        None => "private".into(),
    }
}

async fn channel_is_public(st: &WebState, name: &str) -> bool {
    channel_visibility(st, name).await == "public"
}

async fn handle_history(
    State(st): State<Arc<WebState>>,
    Query(q): Query<HistoryQuery>,
) -> Result<Json<Vec<HistoryItem>>, StatusCode> {
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
    if channel_visibility(&st, &q.channel).await == "shared" {
        if let Some(base) = master_http(&st.cfg) {
            if let Some(msgs) =
                fetch_shared_history(base, &st.cfg.node.id, &q.channel, &q.thread).await
            {
                for msg in msgs {
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
            .map(|m| HistoryItem {
                role: m.author,
                text: m.content,
                created_at_ms: m.created_at_ms,
            })
            .collect(),
    ))
}

#[derive(Serialize)]
struct HistoryItem {
    role: String,
    text: String,
    created_at_ms: i64,
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
    let pipeline_name = req.deploy_pipeline.clone();
    let repo_name = req.target_repo.clone();
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
    let mut action = if pipeline_name.contains("--force") || pipeline_name.contains("force-with-lease")
    {
        "git.force_push"
    } else if pipeline_name.contains("push") {
        "git.push"
    } else {
        "task.execute"
    };
    let mut detail = serde_json::json!({"pipeline": pipeline_name, "exit": res.exit_code});
    if let Some(line) = res.stdout_log.lines().find(|line| line.starts_with("XMESH_GIT ")) {
        if let Ok(git) = serde_json::from_str::<serde_json::Value>(line.trim_start_matches("XMESH_GIT ")) {
            action = if git.get("force").and_then(|v| v.as_bool()).unwrap_or(false) {
                "git.force_push"
            } else {
                "git.push"
            };
            detail = serde_json::json!({
                "pipeline": pipeline_name,
                "exit": res.exit_code,
                "force": git.get("force").cloned().unwrap_or(serde_json::Value::Bool(false)),
                "ref": git.get("ref").cloned().unwrap_or(serde_json::Value::String(String::new())),
                "old": git.get("old").cloned().unwrap_or(serde_json::Value::String(String::new())),
                "new": git.get("new").cloned().unwrap_or(serde_json::Value::String(String::new()))
            });
        }
    }
    publish_audit(&st, action, &repo_name, "", &detail.to_string()).await;
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
        if let Some(dir) = project_dir(&st.cfg, ns) {
            let origin = st.cfg.node.id.clone();
            let ns = ns.clone();
            if let Ok(Ok(files)) =
                tokio::task::spawn_blocking(move || scan_workdir(&dir, &ns, &origin)).await
            {
                for e in files {
                    merged.entry(e.id.clone()).or_insert(e);
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

async fn store_message(st: &WebState, msg: SessionMessage, sync_hub: bool) -> SessionMessage {
    let stored = if sync_hub {
        if let Some(url) = master_url(&st.cfg) {
            match hub::append_public(url, msg.clone(), &st.cfg.node.id, st.node_key.as_ref()).await {
                Ok(stored) => {
                    let _ = st.db.append_message(&stored).await;
                    stored
                }
                Err(err) => {
                    warn!("hub append failed, keeping the message locally: {err}");
                    local_append(st, msg).await
                }
            }
        } else if let Ok(stored) = st.db.append_public_message(&msg).await {
            let action = if stored.author == "user" {
                "chat.request"
            } else {
                "chat.reply"
            };
            let _ = st
                .db
                .record_audit(
                    &st.cfg.node.id,
                    action,
                    &stored.channel_id,
                    &stored.thread_id,
                    &stored.content,
                )
                .await;
            stored
        } else {
            local_append(st, msg).await
        }
    } else {
        local_append(st, msg).await
    };
    let viewing = st
        .presence
        .lock()
        .ok()
        .map(|p| p.viewing && p.channel == stored.channel_id)
        .unwrap_or(false);
    if stored.author == "user" || viewing {
        let _ = st.db.mark_read(&stored.channel_id, stored.seq as i64).await;
    }
    stored
}

async fn local_append(st: &WebState, mut msg: SessionMessage) -> SessionMessage {
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

fn project_dir(cfg: &AppConfig, ns: &str) -> Option<std::path::PathBuf> {
    cfg.git.project_roots.iter().find_map(|root| {
        let path = std::path::PathBuf::from(&root.path).join(ns);
        path.is_dir().then_some(path)
    })
}

#[derive(Deserialize)]
struct CompleteReq {
    #[serde(default)]
    model: String,
    #[serde(default)]
    system: String,
    #[serde(default)]
    turns: Vec<ChatTurn>,
    #[serde(default)]
    audit: Option<AuditHint>,
}

#[derive(Clone, Deserialize)]
struct AuditHint {
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    channel: String,
    #[serde(default)]
    thread: String,
}

async fn handle_complete(
    State(st): State<Arc<WebState>>,
    Json(req): Json<CompleteReq>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    if let Some(hint) = req.audit.clone() {
        let preview = req
            .turns
            .iter()
            .rev()
            .find(|t| t.role == "user")
            .map(|t| t.text.chars().take(400).collect::<String>())
            .unwrap_or_default();
        let detail = serde_json::json!({
            "model": req.model,
            "preview": preview
        })
        .to_string();
        let _ = st
            .db
            .record_audit(
                if hint.node_id.is_empty() {
                    &st.cfg.node.id
                } else {
                    &hint.node_id
                },
                "model.request",
                &hint.channel,
                &hint.thread,
                &detail,
            )
            .await;
    }
    let (tx, rx) = tokio::sync::mpsc::channel::<StreamEvent>(64);
    tokio::spawn(async move {
        if let Err(err) = st
            .models
            .stream_chat(&req.model, &req.system, &req.turns, tx.clone())
            .await
        {
            let _ = tx
                .send(StreamEvent::Delta {
                    text: format!("[ERROR] {err}"),
                })
                .await;
            let _ = tx
                .send(StreamEvent::Done {
                    model_used: "error".into(),
                })
                .await;
        }
    });
    let stream = ReceiverStream::new(rx)
        .map(|ev| Ok(Event::default().data(serde_json::to_string(&ev).unwrap_or_default())));
    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn dispatch_chat(
    st: &WebState,
    model: &str,
    system: &str,
    turns: &[ChatTurn],
    tx: tokio::sync::mpsc::Sender<StreamEvent>,
    audit: Option<serde_json::Value>,
) -> anyhow::Result<(String, String)> {
    if let Some(base) = master_http(&st.cfg) {
        let prefix = model
            .split_once('/')
            .map(|(p, rest)| if rest.is_empty() { "" } else { p })
            .unwrap_or("");
        let remote = if prefix.is_empty() {
            !st.models.has_local_providers().await
        } else {
            !st.models.owns_provider(prefix).await
        };
        if remote {
            return relay_complete(base, model, system, turns, tx, audit).await;
        }
    }
    st.models.stream_chat(model, system, turns, tx).await
}

async fn relay_complete(
    base: &str,
    model: &str,
    system: &str,
    turns: &[ChatTurn],
    tx: tokio::sync::mpsc::Sender<StreamEvent>,
    audit: Option<serde_json::Value>,
) -> anyhow::Result<(String, String)> {
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .build()?;
    let url = format!("{}/api/internal/complete", base.trim_end_matches('/'));
    let mut resp = client
        .post(url)
        .json(&serde_json::json!({
            "model": model,
            "system": system,
            "turns": turns,
            "audit": audit
        }))
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("hub model route status {}", resp.status());
    }
    let mut carry = Vec::new();
    let mut text = String::new();
    let mut used = String::new();
    while let Some(chunk) = resp.chunk().await? {
        carry.extend_from_slice(&chunk);
        while let Some(pos) = carry.iter().position(|b| *b == b'\n') {
            let raw: Vec<u8> = carry.drain(..=pos).collect();
            let line = String::from_utf8_lossy(&raw);
            let Some(data) = line.trim().strip_prefix("data:") else {
                continue;
            };
            let Ok(ev) = serde_json::from_str::<StreamEvent>(data.trim()) else {
                continue;
            };
            match &ev {
                StreamEvent::Delta { text: delta } => text.push_str(delta),
                StreamEvent::Done { model_used } => used = model_used.clone(),
                _ => {}
            }
            let _ = tx.send(ev).await;
        }
    }
    if used.is_empty() {
        used = model.to_string();
    }
    Ok((text, used))
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
    let visibility = channel_visibility(&st, &req.channel).await;
    let public = visibility == "public" || visibility == "shared";
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
        let audit = if sync_hub {
            Some(serde_json::json!({
                "node_id": st_bg.cfg.node.id,
                "channel": req.channel,
                "thread": req.thread
            }))
        } else {
            None
        };
        let (author, final_text) =
            match dispatch_chat(&st_bg, &model, &ctx, &turns, tx.clone(), audit).await {
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

#[derive(Serialize, Deserialize)]
struct NodeQuery {
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    channel: String,
    #[serde(default)]
    thread: String,
    #[serde(default)]
    token: String,
}

#[derive(Deserialize)]
struct PrefBody {
    name: String,
    #[serde(default)]
    pinned: Option<bool>,
    #[serde(default)]
    muted: Option<bool>,
    #[serde(default)]
    archived: Option<bool>,
    #[serde(default)]
    closed: Option<bool>,
    #[serde(default)]
    mark_read: bool,
    #[serde(default)]
    bump_use: bool,
}

#[derive(Deserialize)]
struct PresenceBody {
    channel: String,
    #[serde(default)]
    viewing: bool,
}

#[derive(Serialize, Deserialize)]
struct ShareMsg {
    thread_id: String,
    seq: u64,
    author: String,
    content: String,
    created_at_ms: i64,
}

#[derive(Serialize, Deserialize)]
struct ShareEntry {
    id: String,
    namespace: String,
    source_path: String,
    content: String,
    content_hash: String,
    entry_type: i32,
}

#[derive(Serialize, Deserialize)]
struct ShareBody {
    channel: String,
    #[serde(default)]
    origin_node: String,
    #[serde(default)]
    members: Vec<String>,
    #[serde(default)]
    messages: Vec<ShareMsg>,
    #[serde(default)]
    entries: Vec<ShareEntry>,
    #[serde(default)]
    token_hash: String,
}

#[derive(Serialize, Deserialize)]
struct AuditBody {
    #[serde(default)]
    node_id: String,
    action: String,
    #[serde(default)]
    channel: String,
    #[serde(default)]
    thread: String,
    #[serde(default)]
    detail: String,
}

fn authorize(st: &WebState, headers: &HeaderMap, body: &[u8]) -> Result<String, StatusCode> {
    let node = headers
        .get("x-xmesh-node")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    let ts = headers
        .get("x-xmesh-ts")
        .and_then(|value| value.to_str().ok())
        .and_then(|text| text.parse().ok())
        .unwrap_or(0);
    let sig = headers
        .get("x-xmesh-sig")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    crate::identity::resolve_peer(&st.peer_keys, &node, ts, sig, body).map_err(|_| StatusCode::FORBIDDEN)
}

async fn signed_post(
    st: &WebState,
    url: &str,
    kind: &str,
    parts: &[&str],
    payload: &impl Serialize,
) -> Result<reqwest::Response, StatusCode> {
    let body = serde_json::to_vec(payload).map_err(|_| StatusCode::BAD_REQUEST)?;
    let canon = crate::identity::canonical(kind, parts);
    let ts = now_ms();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let mut req = client
        .post(url)
        .header("content-type", "application/json")
        .header("x-xmesh-node", &st.cfg.node.id)
        .body(body);
    if let Some(key) = &st.node_key {
        let sig = crate::identity::sign(key, &st.cfg.node.id, ts, &canon);
        req = req
            .header("x-xmesh-ts", ts.to_string())
            .header("x-xmesh-sig", sig);
    }
    req.send().await.map_err(|_| StatusCode::BAD_GATEWAY)
}

async fn publish_audit(st: &WebState, action: &str, channel: &str, thread: &str, detail: &str) {
    if let Some(base) = master_http(&st.cfg) {
        let payload = AuditBody {
            node_id: st.cfg.node.id.clone(),
            action: action.to_string(),
            channel: channel.to_string(),
            thread: thread.to_string(),
            detail: detail.to_string(),
        };
        let _ = signed_post(
            st,
            &format!("{}/api/audit", base.trim_end_matches('/')),
            "audit",
            &[action, channel, thread, detail],
            &payload,
        )
        .await;
        return;
    }
    let _ = st
        .db
        .record_audit(&st.cfg.node.id, action, channel, thread, detail)
        .await;
}

async fn handle_channel_state(
    State(st): State<Arc<WebState>>,
    Json(body): Json<PrefBody>,
) -> Result<StatusCode, StatusCode> {
    if storage_name(&body.name).is_err() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut pref = st
        .db
        .get_pref(&body.name)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if let Some(v) = body.pinned {
        pref.pinned = v;
    }
    if let Some(v) = body.muted {
        pref.muted = v;
    }
    if body.name != "general" {
        if let Some(v) = body.archived {
            pref.archived = v;
        }
        if let Some(v) = body.closed {
            pref.closed = v;
        }
    }
    if body.bump_use {
        pref.use_count = pref.use_count.saturating_add(1);
    }
    if body.mark_read {
        let seq = st
            .db
            .max_seq(&body.name)
            .await
            .unwrap_or(pref.last_read_seq);
        pref.last_read_seq = pref.last_read_seq.max(seq);
    }
    st.db
        .save_pref(&pref)
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(StatusCode::OK)
}

fn storage_name(name: &str) -> Result<(), ()> {
    crate::storage::validate_channel_name(name).map_err(|_| ())
}

async fn handle_presence(
    State(st): State<Arc<WebState>>,
    Json(body): Json<PresenceBody>,
) -> StatusCode {
    if let Ok(mut presence) = st.presence.lock() {
        presence.channel = body.channel;
        presence.viewing = body.viewing;
    }
    StatusCode::OK
}

async fn handle_audit(
    State(st): State<Arc<WebState>>,
) -> Result<Json<Vec<AuditEvent>>, StatusCode> {
    if let Some(base) = master_http(&st.cfg) {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(8))
            .build()
            .map_err(|_| StatusCode::BAD_GATEWAY)?;
        let rows = client
            .get(format!("{}/api/audit", base.trim_end_matches('/')))
            .send()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?
            .json()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?;
        return Ok(Json(rows));
    }
    st.db
        .list_audit(80)
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn handle_audit_post(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    Json(body): Json<AuditBody>,
) -> Result<StatusCode, StatusCode> {
    if master_http(&st.cfg).is_some() {
        return Err(StatusCode::FORBIDDEN);
    }
    let canon = crate::identity::canonical(
        "audit",
        &[&body.action, &body.channel, &body.thread, &body.detail],
    );
    let verified = authorize(&st, &headers, &canon)?;
    let node = if st.peer_keys.is_empty() {
        if body.node_id.is_empty() {
            st.cfg.node.id.clone()
        } else {
            body.node_id
        }
    } else {
        verified
    };
    st.db
        .record_audit(
            &node,
            &body.action,
            &body.channel,
            &body.thread,
            &body.detail,
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::OK)
}

async fn handle_mesh_channels(
    State(st): State<Arc<WebState>>,
    Query(q): Query<NodeQuery>,
) -> Result<Json<Vec<ChannelRow>>, StatusCode> {
    st.db
        .channels_visible_to(&q.node_id)
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn handle_mesh_activity(
    State(st): State<Arc<WebState>>,
    Query(q): Query<NodeQuery>,
) -> Result<Json<std::collections::HashMap<String, i64>>, StatusCode> {
    let visible = st
        .db
        .channels_visible_to(&q.node_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let allowed: std::collections::HashSet<_> = visible.into_iter().map(|c| c.name).collect();
    let rows = st
        .db
        .activity()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(
        rows.into_iter()
            .filter(|(name, _)| allowed.contains(name))
            .collect(),
    ))
}

async fn handle_mesh_history(
    State(st): State<Arc<WebState>>,
    Query(q): Query<NodeQuery>,
) -> Result<Json<Vec<ShareMsg>>, StatusCode> {
    let Some(ch) = st
        .db
        .get_channel(&q.channel)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };
    if ch.visibility == "private" {
        return Err(StatusCode::FORBIDDEN);
    }
    if ch.visibility == "shared" {
        let member = st
            .db
            .member_state(&q.channel, &q.node_id)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if member.is_none() {
            return Err(StatusCode::FORBIDDEN);
        }
    }
    let thread = if q.thread.is_empty() {
        "main"
    } else {
        &q.thread
    };
    let msgs = st
        .db
        .load_thread(&q.channel, thread, 400)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(
        msgs.into_iter()
            .map(|m| ShareMsg {
                thread_id: m.thread_id,
                seq: m.seq,
                author: m.author,
                content: m.content,
                created_at_ms: m.created_at_ms,
            })
            .collect(),
    ))
}

async fn fetch_shared_history(
    base: &str,
    node_id: &str,
    channel: &str,
    thread: &str,
) -> Option<Vec<SessionMessage>> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .ok()?;
    let msgs: Vec<ShareMsg> = client
        .get(format!("{}/api/mesh/history", base.trim_end_matches('/')))
        .query(&[
            ("node_id", node_id),
            ("channel", channel),
            ("thread", thread),
        ])
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    Some(
        msgs.into_iter()
            .map(|m| SessionMessage {
                channel_id: channel.to_string(),
                thread_id: m.thread_id,
                seq: m.seq,
                author: m.author,
                content: m.content,
                created_at_ms: m.created_at_ms,
            })
            .collect(),
    )
}

async fn apply_share(st: &WebState, body: ShareBody) -> Result<(), StatusCode> {
    if storage_name(&body.channel).is_err() || body.channel == "general" {
        return Err(StatusCode::BAD_REQUEST);
    }
    st.db
        .upsert_channel(&ChannelRow {
            name: body.channel.clone(),
            visibility: "shared".into(),
            namespace: body.channel.clone(),
            origin_node: body.origin_node.clone(),
            context_namespaces: vec![body.channel.clone()],
            updated_at_ms: 0,
        })
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut members = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for node in std::iter::once(body.origin_node.clone()).chain(body.members.into_iter()) {
        if node.is_empty() || !seen.insert(node.clone()) {
            continue;
        }
        let role = if node == body.origin_node {
            "owner"
        } else {
            "member"
        };
        members.push((node, role.to_string(), "active".to_string()));
    }
    st.db
        .set_members(&body.channel, &members)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    for msg in body.messages.into_iter().take(400) {
        let _ = st
            .db
            .append_message(&SessionMessage {
                channel_id: body.channel.clone(),
                thread_id: msg.thread_id,
                seq: msg.seq,
                author: msg.author,
                content: msg.content,
                created_at_ms: msg.created_at_ms,
            })
            .await;
    }
    for entry in body.entries.into_iter().take(200) {
        let _ = st
            .db
            .upsert_proto(
                &ContextEntry {
                    id: entry.id,
                    namespace: entry.namespace,
                    entry_type: entry.entry_type,
                    source_path: entry.source_path,
                    content: entry.content,
                    content_hash: entry.content_hash,
                    embedding: vec![],
                    updated_at_ms: now_ms(),
                    origin_node: body.origin_node.clone(),
                    meta: Default::default(),
                },
                false,
            )
            .await;
    }
    let _ = st
        .db
        .record_audit(
            &body.origin_node,
            "channel.share",
            &body.channel,
            "main",
            &members
                .iter()
                .map(|(n, _, _)| n.as_str())
                .collect::<Vec<_>>()
                .join(","),
        )
        .await;
    if !body.token_hash.is_empty() {
        let _ = st.db.put_invite(&body.channel, &body.token_hash).await;
    }
    Ok(())
}

async fn handle_mesh_share(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    Json(mut body): Json<ShareBody>,
) -> Result<StatusCode, StatusCode> {
    let listed = body.members.join(",");
    let canon = crate::identity::canonical(
        "share",
        &[&body.channel, &body.origin_node, &listed, &body.token_hash],
    );
    if !st.peer_keys.is_empty() {
        body.origin_node = authorize(&st, &headers, &canon)?;
    }
    apply_share(&st, body).await?;
    Ok(StatusCode::OK)
}

async fn apply_join(st: &WebState, node_id: &str, channel: &str, token: &str) -> Result<(), StatusCode> {
    let member = st
        .db
        .member_state(channel, node_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if member.is_none() {
        let hash = blake3::hash(token.as_bytes()).to_hex().to_string();
        let invited = !token.is_empty()
            && st
                .db
                .invite_matches(channel, &hash)
                .await
                .unwrap_or(false);
        if !invited {
            return Err(StatusCode::FORBIDDEN);
        }
    }
    let ch = st
        .db
        .get_channel(channel)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let mut names = st
        .db
        .list_members(channel)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if !names.iter().any(|name| name == node_id) {
        names.push(node_id.to_string());
    }
    let members = names
        .into_iter()
        .map(|name| {
            let role = if name == ch.origin_node {
                "owner"
            } else {
                "member"
            };
            (name, role.to_string(), "active".to_string())
        })
        .collect::<Vec<_>>();
    st.db
        .set_members(channel, &members)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let _ = st
        .db
        .record_audit(node_id, "channel.join", channel, "main", "")
        .await;
    Ok(())
}

async fn handle_mesh_join(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    Json(body): Json<NodeQuery>,
) -> Result<StatusCode, StatusCode> {
    let canon = crate::identity::canonical("join", &[&body.channel, &body.node_id, &body.token]);
    let node = if st.peer_keys.is_empty() {
        body.node_id.clone()
    } else {
        authorize(&st, &headers, &canon)?
    };
    apply_join(&st, &node, &body.channel, &body.token).await?;
    Ok(StatusCode::OK)
}

async fn handle_join(
    State(st): State<Arc<WebState>>,
    Json(body): Json<NodeQuery>,
) -> Result<StatusCode, StatusCode> {
    if let Some(base) = master_http(&st.cfg) {
        let payload = NodeQuery {
            node_id: st.cfg.node.id.clone(),
            channel: body.channel.clone(),
            thread: String::new(),
            token: body.token.clone(),
        };
        let resp = signed_post(
            &st,
            &format!("{}/api/mesh/join", base.trim_end_matches('/')),
            "join",
            &[&payload.channel, &payload.node_id, &payload.token],
            &payload,
        )
        .await?;
        return if resp.status().is_success() {
            Ok(StatusCode::OK)
        } else if resp.status() == StatusCode::FORBIDDEN {
            Err(StatusCode::FORBIDDEN)
        } else {
            Err(StatusCode::BAD_GATEWAY)
        };
    }
    apply_join(&st, &st.cfg.node.id, &body.channel, &body.token).await?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct InspectBody {
    #[serde(default)]
    diff: String,
}

async fn handle_inspect(
    State(st): State<Arc<WebState>>,
    Json(body): Json<InspectBody>,
) -> Json<serde_json::Value> {
    let mut findings = crate::executor::inspect_diff(&body.diff);
    if let Some(bin) = st.cfg.git.inspect_command.first() {
        let mut cmd = tokio::process::Command::new(bin);
        cmd.args(st.cfg.git.inspect_command.iter().skip(1));
        cmd.stdin(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        match cmd.spawn() {
            Ok(mut child) => {
                if let Some(mut stdin) = child.stdin.take() {
                    use tokio::io::AsyncWriteExt;
                    let _ = stdin.write_all(body.diff.as_bytes()).await;
                }
                match tokio::time::timeout(
                    std::time::Duration::from_secs(20),
                    child.wait_with_output(),
                )
                .await
                {
                    Ok(Ok(out)) => {
                        for line in String::from_utf8_lossy(&out.stdout).lines().take(20) {
                            if !line.trim().is_empty() {
                                findings.push(line.trim().to_string());
                            }
                        }
                        if !out.status.success() {
                            findings.push("external inspector reported a problem".into());
                        }
                    }
                    _ => findings.push("external inspector timed out".into()),
                }
            }
            Err(_) => findings.push("external inspector could not start".into()),
        }
    }
    Json(serde_json::json!({ "findings": findings }))
}

async fn handle_share(
    State(st): State<Arc<WebState>>,
    Json(body): Json<ShareBody>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let origin = st.cfg.node.id.clone();
    let channel = body.channel.clone();
    let mut members = body.members;
    let messages = if body.messages.is_empty() {
        st.db
            .load_channel_messages(&channel)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .into_iter()
            .take(400)
            .map(|m| ShareMsg {
                thread_id: m.thread_id,
                seq: m.seq,
                author: m.author,
                content: m.content,
                created_at_ms: m.created_at_ms,
            })
            .collect()
    } else {
        body.messages
    };
    let entries = if body.entries.is_empty() {
        st.db
            .load_entries(Some(&channel))
            .await
            .unwrap_or_default()
            .into_iter()
            .take(200)
            .map(|e| ShareEntry {
                id: e.id,
                namespace: e.namespace,
                source_path: e.source_path,
                content: e.content,
                content_hash: e.content_hash,
                entry_type: e.entry_type,
            })
            .collect()
    } else {
        body.entries
    };
    let payload = ShareBody {
        channel: channel.clone(),
        origin_node: origin,
        members: members.split_off(0),
        messages,
        entries,
        token_hash: String::new(),
    };
    let (token, token_hash) = crate::identity::new_token();
    let mut payload = payload;
    payload.token_hash = token_hash.clone();
    let listed = payload.members.join(",");
    if let Some(base) = master_http(&st.cfg) {
        let resp = signed_post(
            &st,
            &format!("{}/api/mesh/share", base.trim_end_matches('/')),
            "share",
            &[&payload.channel, &payload.origin_node, &listed, &token_hash],
            &payload,
        )
        .await?;
        if resp.status() == StatusCode::FORBIDDEN {
            return Err(StatusCode::FORBIDDEN);
        }
        if !resp.status().is_success() {
            warn!(status = %resp.status(), "hub rejected the share");
            return Err(StatusCode::BAD_GATEWAY);
        }
    }
    apply_share(&st, payload).await?;
    Ok(Json(serde_json::json!({ "token": token })))
}
