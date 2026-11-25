mod config;
mod executor;
mod git_sync;
mod storage;
mod gemini;
mod web_ui;
mod migrate;
mod watcher;

use anyhow::Result;
use config::{AppConfig, NodeMode};
use std::sync::Arc;
use storage::pb::cluster_service_server::{ClusterService, ClusterServiceServer};
use storage::pb::{ContextQuery, NodeTaskRequest, NodeTaskResponse, SyncAck, SyncEnvelope};
use storage::Storage;
use tonic::{transport::Server, Request, Response, Status};
use tracing::info;
use gemini::GeminiClient;

struct NodeRpcServer {
    cfg: Arc<AppConfig>,
    db: Arc<Storage>,
    hmac_key: [u8; 32],
}

#[tonic::async_trait]
impl ClusterService for NodeRpcServer {
    async fn sync_contexts(&self, req: Request<SyncEnvelope>) -> Result<Response<SyncAck>, Status> {
        let env = req.into_inner();
        let (mut accepted, mut skipped) = (0, 0);
        for entry in &env.entries {
            match self.db.upsert_proto(entry, false).await {
                Ok(true) => accepted += 1,
                Ok(false) => skipped += 1,
                Err(e) => return Err(Status::internal(e.to_string())),
            }
        }
        Ok(Response::new(SyncAck { accepted_entries: accepted, skipped_unchanged: skipped }))
    }

    async fn query_node_context(&self, req: Request<ContextQuery>) -> Result<Response<SyncEnvelope>, Status> {
        let q = req.into_inner();
        let env = self.db.export_envelope(&self.cfg.node.id, Some(&q.namespace))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(env))
    }

    async fn execute_task(&self, req: Request<NodeTaskRequest>) -> Result<Response<NodeTaskResponse>, Status> {
        let res = executor::run_node_task(&self.cfg, &req.into_inner(), &self.hmac_key)
            .await
            .map_err(|e| Status::permission_denied(e.to_string()))?;
        Ok(Response::new(res))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cfg = Arc::new(AppConfig::load("/etc/xmeshd/cluster.yaml", "/etc/xmeshd/local.yaml")?);
    let db = Arc::new(Storage::connect(cfg.storage.primary_url.as_deref(), &cfg.storage.fallback_sqlite).await?);

    let raw_key = std::fs::read(&cfg.node.hmac_secret_path)?;
    let hmac_key = *blake3::hash(&raw_key).as_bytes();

    if matches!(cfg.node.mode, NodeMode::Node) {
        info!("Starting in NODE mode: {}", cfg.node.id);
    } else {
        info!("Starting in MASTER mode: {}", cfg.node.id);
    }

    let addr = cfg.node.listen_addr.parse()?;
    let rpc = NodeRpcServer { cfg: cfg.clone(), db: db.clone(), hmac_key };

    if matches!(cfg.node.mode, NodeMode::Master) {
        let api_key = std::env::var("GEMINI_API_KEY")?;
        let gemini = Arc::new(GeminiClient::new(api_key, "gemini-2.5-pro", "gemini-embedding-001"));
        let web_state = Arc::new(web_ui::WebState { db: db.clone(), gemini, hmac_key });
        let app = web_ui::router(web_state);
        let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
        info!("Master Web UI listening on http://0.0.0.0");
    }


    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(|s| s.as_str()) == Some("migrate") {
        let from = args.get(2).expect("missing <from_url>");
        let to = args.get(3).expect("missing <to_url>");
        migrate::direct_migrate(from, to, false).await?;
        return Ok(());
    }

    Server::builder()
        .add_service(ClusterServiceServer::new(rpc))
        .serve(addr)
        .await?;

    Ok(())
}





