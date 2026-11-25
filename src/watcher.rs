use crate::config::AppConfig;
use crate::git_sync::extract_local_diff;
use crate::hub;
use crate::models::ModelHub;
use crate::storage::pb::{ContextEntry, SyncEnvelope};
use crate::storage::{now_ms, Storage};
use anyhow::Result;
use notify_debouncer_mini::{new_debouncer, notify::RecursiveMode, DebounceEventResult};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

pub async fn run_node_watcher(
    cfg: Arc<AppConfig>,
    db: Arc<Storage>,
    models: Arc<ModelHub>,
) -> Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let mut debouncer = new_debouncer(Duration::from_secs(10), move |res: DebounceEventResult| {
        if res.is_ok() {
            let _ = tx.blocking_send(());
        }
    })?;

    for repo in &cfg.git.watched_repos {
        if let Err(err) = debouncer
            .watcher()
            .watch(Path::new(&repo.local_path), RecursiveMode::Recursive)
        {
            warn!("not watching {}: {err}", repo.local_path);
        }
    }

    while rx.recv().await.is_some() {
        let mut batch: Vec<ContextEntry> = Vec::new();
        for repo in &cfg.git.watched_repos {
            let Ok(Some(mut entry)) = extract_local_diff(
                Path::new(&repo.local_path),
                &repo.namespace,
                &repo.base_branch,
                &cfg.node.id,
            ) else {
                continue;
            };
            if let Ok(emb) = models.embed_text(&entry.content).await {
                entry.embedding = emb;
            }
            let _ = db.upsert_proto(&entry, false).await;
            if db
                .namespace_is_public(&repo.namespace)
                .await
                .unwrap_or(false)
            {
                batch.push(entry);
            }
        }

        if batch.is_empty() {
            continue;
        }
        let Some(master_url) = cfg.node.master_url.as_deref() else {
            continue;
        };
        let env = SyncEnvelope {
            schema_version: 2,
            source_node: cfg.node.id.clone(),
            timestamp_ms: now_ms(),
            entries: batch,
            ..Default::default()
        };
        match hub::push_envelope(master_url, env).await {
            Ok(()) => info!("synced public diffs to the hub"),
            Err(err) => error!("failed to sync diffs: {err}"),
        }
    }
    Ok(())
}
