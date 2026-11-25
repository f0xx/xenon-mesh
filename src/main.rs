mod config;
mod executor;
mod identity;
mod git_sync;
mod hub;
mod migrate;
mod models;
mod retrieve;
mod storage;
mod watcher;
mod web_ui;

use anyhow::{bail, Context, Result};
use config::{redact_url, AppConfig, NodeMode};
use models::ModelHub;
use std::sync::Arc;
use storage::pb::cluster_service_server::{ClusterService, ClusterServiceServer};
use storage::pb::{
    ChannelRecord, ContextQuery, ListChannelsRequest, ListChannelsResponse, NodeTaskRequest,
    NodeTaskResponse, SessionMessage, SyncAck, SyncEnvelope,
};
use storage::{ChannelRow, Storage};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{error, info, warn};

struct NodeRpcServer {
    cfg: Arc<AppConfig>,
    db: Arc<Storage>,
    hmac_key: [u8; 32],
    peer_keys: std::collections::HashMap<String, [u8; 32]>,
}

#[tonic::async_trait]
impl ClusterService for NodeRpcServer {
    async fn sync_contexts(&self, req: Request<SyncEnvelope>) -> Result<Response<SyncAck>, Status> {
        let env = req.into_inner();
        let (mut ok, mut skip) = (0u32, 0u32);
        for ch in &env.channels {
            if ch.visibility != "public" {
                skip += 1;
                continue;
            }
            self.db
                .upsert_channel(&ChannelRow::from_proto(ch))
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            ok += 1;
        }
        for e in &env.entries {
            match self.db.upsert_proto(e, false).await {
                Ok(true) => ok += 1,
                Ok(false) => skip += 1,
                Err(err) => return Err(Status::internal(err.to_string())),
            }
        }
        for msg in &env.messages {
            if self
                .db
                .thread_visibility(&msg.channel_id, &msg.thread_id)
                .await
                .unwrap_or_default()
                == "private"
            {
                skip += 1;
                continue;
            }
            self.db
                .append_message(msg)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            ok += 1;
        }
        Ok(Response::new(SyncAck {
            accepted_entries: ok,
            skipped_unchanged: skip,
        }))
    }

    async fn query_node_context(
        &self,
        req: Request<ContextQuery>,
    ) -> Result<Response<SyncEnvelope>, Status> {
        let ns = req.into_inner().namespace;
        self.db
            .export_public_envelope(&self.cfg.node.id, Some(&ns))
            .await
            .map(Response::new)
            .map_err(|e| Status::internal(e.to_string()))
    }

    async fn execute_task(
        &self,
        req: Request<NodeTaskRequest>,
    ) -> Result<Response<NodeTaskResponse>, Status> {
        executor::run_node_task(&self.cfg, &req.into_inner(), &self.hmac_key)
            .await
            .map(Response::new)
            .map_err(|e| Status::permission_denied(e.to_string()))
    }

    async fn list_channels(
        &self,
        _req: Request<ListChannelsRequest>,
    ) -> Result<Response<ListChannelsResponse>, Status> {
        let channels: Vec<ChannelRecord> = self
            .db
            .list_public_channels()
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .iter()
            .map(ChannelRow::to_proto)
            .collect();
        Ok(Response::new(ListChannelsResponse { channels }))
    }

    async fn append_message(
        &self,
        req: Request<SessionMessage>,
    ) -> Result<Response<SessionMessage>, Status> {
        let node_claim = req
            .metadata()
            .get("x-xmesh-node")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown")
            .to_string();
        let ts = req
            .metadata()
            .get("x-xmesh-ts")
            .and_then(|v| v.to_str().ok())
            .and_then(|text| text.parse().ok())
            .unwrap_or(0);
        let sig = req
            .metadata()
            .get("x-xmesh-sig")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let msg = req.into_inner();
        let body = identity::canonical(
            "append",
            &[&msg.channel_id, &msg.thread_id, &msg.author, &msg.content],
        );
        let node_id = identity::resolve_peer(&self.peer_keys, &node_claim, ts, &sig, &body)
            .map_err(Status::permission_denied)?;
        if let Some(ch) = self
            .db
            .get_channel(&msg.channel_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
        {
            if ch.visibility == "private" {
                return Err(Status::permission_denied("channel is private"));
            }
            if ch.visibility == "shared" {
                let member = self
                    .db
                    .member_state(&ch.name, &node_id)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                if member.is_none() {
                    return Err(Status::permission_denied("invitation required"));
                }
            }
        }
        let stored = self
            .db
            .append_public_message(&msg)
            .await
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let action = if stored.author == "user" {
            "chat.request"
        } else {
            "chat.reply"
        };
        let _ = self
            .db
            .record_audit(
                &node_id,
                action,
                &stored.channel_id,
                &stored.thread_id,
                &stored.content,
            )
            .await;
        Ok(Response::new(stored))
    }
}

fn usage() -> &'static str {
    "usage:\n  xmeshd\n  xmeshd check [local.yaml] [cluster.yaml]\n  xmeshd models <base_url>\n  xmeshd migrate <from_url> <to_url>\n  xmeshd export <db_url> <out.pb.zst>\n  xmeshd import <db_url> <in.pb.zst>"
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("migrate") => {
            migrate::direct_migrate(
                args.get(2).context(usage())?,
                args.get(3).context(usage())?,
                false,
            )
            .await?;
            return Ok(());
        }
        Some("export") => {
            let db = Storage::connect(Some(args.get(2).context(usage())?), "/tmp/xmeshd-export.db")
                .await?;
            migrate::export_to_file(&db, "cli", None, args.get(3).context(usage())?).await?;
            return Ok(());
        }
        Some("import") => {
            let db = Storage::connect(Some(args.get(2).context(usage())?), "/tmp/xmeshd-import.db")
                .await?;
            db.init_messages_schema().await?;
            migrate::import_from_file(&db, args.get(3).context(usage())?, false).await?;
            return Ok(());
        }
        Some("models") => {
            let base = args.get(2).context(usage())?;
            let report = ModelHub::probe(base).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            return Ok(());
        }
        Some("check") => {
            let local = args
                .get(2)
                .map(String::as_str)
                .unwrap_or("/etc/xmeshd/local.yaml");
            let cluster = args
                .get(3)
                .map(String::as_str)
                .unwrap_or("/etc/xmeshd/cluster.yaml");
            let cfg = AppConfig::load(cluster, local)?;
            println!(
                "node={} mode={:?} http={} grpc={}",
                cfg.node.id, cfg.node.mode, cfg.node.http_addr, cfg.node.listen_addr
            );
            match &cfg.storage.primary_url {
                Some(url) if !url.is_empty() => println!("storage={}", redact_url(url)),
                _ => println!("storage={}", cfg.storage.fallback_sqlite),
            }
            if cfg.models.providers.is_empty() {
                println!(
                    "provider id=gemini kind=gemini chat={}",
                    cfg.gemini.chat_model
                );
            }
            for p in &cfg.models.providers {
                println!(
                    "provider id={} kind={} base={} chat={}",
                    p.id, p.kind, p.base_url, p.chat_model
                );
            }
            return Ok(());
        }
        Some(other) => bail!("{}\nunknown command: {other}", usage()),
        None => {}
    }

    let cfg = Arc::new(AppConfig::load(
        "/etc/xmeshd/cluster.yaml",
        "/etc/xmeshd/local.yaml",
    )?);
    let db = Arc::new(
        Storage::connect(
            cfg.storage.primary_url.as_deref(),
            &cfg.storage.fallback_sqlite,
        )
        .await?,
    );
    db.init_messages_schema().await?;
    if matches!(cfg.node.mode, NodeMode::Master) && db.get_channel("general").await?.is_none() {
        db.upsert_channel(&ChannelRow {
            name: "general".into(),
            visibility: "public".into(),
            namespace: "general".into(),
            origin_node: cfg.node.id.clone(),
            context_namespaces: vec!["general".into()],
            updated_at_ms: 0,
        })
        .await?;
    }

    let hmac_bytes = std::fs::read(&cfg.node.hmac_secret_path)
        .with_context(|| format!("reading HMAC secret {}", cfg.node.hmac_secret_path))?;
    let hmac_key = *blake3::hash(&hmac_bytes).as_bytes();
    let model_hub = ModelHub::from_config(&cfg)?;
    if let Err(err) = model_hub.discover().await {
        warn!("model discovery failed: {err}");
    }
    info!("default model {}", model_hub.default_model().await);

    let status = Arc::new(tokio::sync::RwLock::new(web_ui::IndexStatus {
        phase: "ready".into(),
        mode: match cfg.node.mode {
            NodeMode::Master => "master".into(),
            NodeMode::Node => "node".into(),
        },
        embeddings: model_hub.embeddings_configured().await,
        ..Default::default()
    }));
    let node_key = cfg
        .node
        .node_key_path
        .as_deref()
        .and_then(|path| identity::key_from_file(std::path::Path::new(path)));
    let peer_keys = cfg
        .node
        .peer_keys_dir
        .as_deref()
        .map(|path| identity::load_peers(std::path::Path::new(path)))
        .unwrap_or_default();
    if cfg.node.node_key_path.is_some() && node_key.is_none() {
        warn!("node key path is set but the file could not be read");
    }
    info!(
        "node identity key present: {}, peer keys: {}",
        node_key.is_some(),
        peer_keys.len()
    );
    let web = Arc::new(web_ui::WebState {
        cfg: cfg.clone(),
        db: db.clone(),
        models: model_hub.clone(),
        hmac_key,
        node_key,
        peer_keys: peer_keys.clone(),
        status: status.clone(),
        presence: Arc::new(std::sync::Mutex::new(web_ui::Presence {
            channel: "general".into(),
            viewing: false,
        })),
    });
    let app = web_ui::router(web);
    let listener = tokio::net::TcpListener::bind(&cfg.node.http_addr).await?;
    let http_addr = listener.local_addr()?;
    let http_app = app.clone();
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, http_app).await {
            error!("HTTP server stopped: {err}");
        }
    });
    info!("HTTP UI listening on http://{http_addr}");
    if let (Some(addr), Some(cert), Some(key)) = (
        cfg.node.https_addr.clone().filter(|s| !s.trim().is_empty()),
        cfg.node
            .tls_cert_path
            .clone()
            .filter(|s| !s.trim().is_empty()),
        cfg.node
            .tls_key_path
            .clone()
            .filter(|s| !s.trim().is_empty()),
    ) {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        match axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert, &key).await {
            Ok(tls) => match addr.parse::<std::net::SocketAddr>() {
                Ok(https) => {
                    info!("HTTPS UI listening on https://{https}");
                    let https_app = app.into_make_service();
                    tokio::spawn(async move {
                        if let Err(err) =
                            axum_server::bind_rustls(https, tls).serve(https_app).await
                        {
                            error!("HTTPS server stopped: {err}");
                        }
                    });
                }
                Err(err) => warn!("HTTPS address {addr} is invalid: {err}"),
            },
            Err(err) => warn!("HTTPS disabled, certificate was not loaded: {err}"),
        }
    }

    if matches!(cfg.node.mode, NodeMode::Master) {
        if let Some(root) = cfg.git.bare_repos_root.clone() {
            let (db_bg, models_bg, st_bg) = (db.clone(), model_hub.clone(), status.clone());
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let r_clone = root.clone();
                let repos = tokio::task::spawn_blocking(move || {
                    git_sync::discover_bare_repos(std::path::Path::new(&r_clone))
                })
                .await
                .unwrap_or_default();
                let total = repos.len();
                {
                    let mut s = st_bg.write().await;
                    s.busy = true;
                    s.phase = "indexing".into();
                    s.total_repos = total;
                }
                for (ns, _) in &repos {
                    if storage::validate_channel_name(ns).is_err() {
                        warn!("skipping channel for {ns}: unsupported name");
                        continue;
                    }
                    if let Err(err) = db_bg.ensure_channel(ns, "public", "index").await {
                        warn!("channel {ns}: {err}");
                    }
                }
                for (idx, (ns, path)) in repos.into_iter().enumerate() {
                    {
                        let mut s = st_bg.write().await;
                        s.done_repos = idx;
                        s.current_repo = ns.clone();
                    }
                    info!("scanning repo [{}/{}]: {}", idx + 1, total, ns);
                    let ns_c = ns.clone();
                    let scan_fut =
                        tokio::task::spawn_blocking(move || git_sync::scan_bare_repo(&path, &ns_c));
                    let entries =
                        match tokio::time::timeout(std::time::Duration::from_secs(20), scan_fut)
                            .await
                        {
                            Ok(Ok(Ok(list))) => list,
                            Ok(Ok(Err(err))) => {
                                warn!("{err}");
                                continue;
                            }
                            _ => {
                                warn!("timed out scanning repo: {ns}");
                                continue;
                            }
                        };
                    let mut embed_ok = true;
                    for mut e in entries.into_iter().take(300) {
                        if db_bg
                            .is_unchanged(&e.id, &e.content_hash)
                            .await
                            .unwrap_or(false)
                        {
                            continue;
                        }
                        if embed_ok {
                            match models_bg.embed_text(&e.content).await {
                                Ok(emb) if !emb.is_empty() => {
                                    e.embedding = emb;
                                    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
                                }
                                Ok(_) => {}
                                Err(err) => {
                                    warn!("embedding paused ({err}), saving raw text");
                                    embed_ok = false;
                                }
                            }
                        }
                        if db_bg.upsert_proto(&e, false).await.unwrap_or(false) {
                            st_bg.write().await.indexed_files += 1;
                        }
                    }
                }
                let mut s = st_bg.write().await;
                s.busy = false;
                s.phase = "ready".into();
                s.done_repos = total;
                s.current_repo.clear();
                s.embeddings = models_bg.embeddings_configured().await;
                info!("background indexing complete ({total} repos)");
            });
        }
    }

    spawn_project_scan(cfg.clone(), db.clone(), status.clone());

    if !cfg.git.watched_repos.is_empty() {
        let (c, d, m) = (cfg.clone(), db.clone(), model_hub.clone());
        tokio::spawn(async move {
            if let Err(err) = watcher::run_node_watcher(c, d, m).await {
                error!("watcher: {err}");
            }
        });
    }
    if cfg.git.watch_changes {
        let (c, d) = (cfg.clone(), db.clone());
        tokio::spawn(async move {
            if let Err(err) = watcher::run_tree_watcher(c, d).await {
                error!("tree watcher: {err}");
            }
        });
    }

    fn spawn_project_scan(
        cfg: Arc<AppConfig>,
        db: Arc<Storage>,
        status: Arc<tokio::sync::RwLock<web_ui::IndexStatus>>,
    ) {
        if cfg.git.project_roots.is_empty() {
            return;
        }
        tokio::spawn(async move {
            let mut listed = Vec::new();
            for root in &cfg.git.project_roots {
                let path = root.path.clone();
                let visibility = root.visibility.clone();
                let dirs = tokio::task::spawn_blocking(move || {
                    git_sync::discover_project_dirs(std::path::Path::new(&path))
                })
                .await
                .unwrap_or_default();
                for (name, dir) in dirs {
                    listed.push((name, dir, visibility.clone()));
                }
            }
            let total = listed.len();
            {
                let mut s = status.write().await;
                s.busy = true;
                s.phase = "indexing".into();
                s.total_repos = total;
                s.done_repos = 0;
            }
            info!("scanning {total} project directories");
            for (idx, (name, dir, visibility)) in listed.into_iter().enumerate() {
                {
                    let mut s = status.write().await;
                    s.done_repos = idx;
                    s.current_repo = name.clone();
                }
                if storage::validate_channel_name(&name).is_err() {
                    warn!("skipping project {name}: unsupported channel name");
                    continue;
                }
                let vis = storage::normalize_visibility(&visibility).unwrap_or("private");
                if let Err(err) = db
                    .upsert_channel(&ChannelRow {
                        name: name.clone(),
                        visibility: vis.into(),
                        namespace: name.clone(),
                        origin_node: "project".into(),
                        context_namespaces: vec![name.clone()],
                        updated_at_ms: 0,
                    })
                    .await
                {
                    warn!("project channel {name}: {err}");
                    continue;
                }
                let ns = name.clone();
                let origin = cfg.node.id.clone();
                let files =
                    tokio::task::spawn_blocking(move || git_sync::scan_workdir(&dir, &ns, &origin))
                        .await
                        .ok()
                        .and_then(|r| r.ok())
                        .unwrap_or_default();
                for e in files {
                    let _ = db.upsert_proto(&e, false).await;
                }
            }
            let mut s = status.write().await;
            s.busy = false;
            s.phase = "ready".into();
            s.done_repos = total;
            s.current_repo.clear();
            info!("project scan complete ({total} directories)");
        });
    }

    let addr = cfg.node.listen_addr.parse()?;
    Server::builder()
        .add_service(ClusterServiceServer::new(NodeRpcServer {
            cfg,
            db,
            hmac_key,
            peer_keys,
        }))
        .serve(addr)
        .await?;
    Ok(())
}
