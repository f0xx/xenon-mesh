use crate::config::AppConfig;
use crate::git_sync::{discover_bare_repos, discover_project_dirs, extract_local_diff, scan_bare_repo, scan_workdir};
use crate::hub;
use crate::models::ModelHub;
use crate::storage::pb::{ContextEntry, SyncEnvelope};
use crate::storage::{now_ms, ChannelRow, Storage};
use anyhow::Result;
use notify_debouncer_mini::{new_debouncer, notify::RecursiveMode, DebounceEventResult};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
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

fn noisy(path: &Path) -> bool {
    path.components().any(|part| {
        matches!(
            part.as_os_str().to_str(),
            Some(".git" | "target" | "node_modules" | "dist" | ".venv" | "vendor" | "build" | ".xmesh")
        )
    })
}

/// Watches project roots and bare repos.
/// A directory that disappears leaves the channel list.
/// A file saved outside the browser is reindexed.
/// `git.watch_changes: false` turns this off. The startup scan still runs.
pub async fn run_tree_watcher(cfg: Arc<AppConfig>, db: Arc<Storage>) -> Result<()> {
    if !cfg.git.watch_changes {
        info!("tree watch is off (git.watch_changes)");
        return Ok(());
    }
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<PathBuf>>(8);
    let mut debouncer = new_debouncer(Duration::from_secs(15), move |res: DebounceEventResult| {
        if let Ok(events) = res {
            let paths: Vec<PathBuf> = events.into_iter().map(|event| event.path).collect();
            let _ = tx.blocking_send(paths);
        }
    })?;
    for root in &cfg.git.project_roots {
        if let Err(err) = debouncer
            .watcher()
            .watch(Path::new(&root.path), RecursiveMode::Recursive)
        {
            warn!(path = %root.path, error = %err, "could not watch project root");
        }
    }
    if let Some(root) = &cfg.git.bare_repos_root {
        if let Err(err) = debouncer
            .watcher()
            .watch(Path::new(root), RecursiveMode::Recursive)
        {
            warn!(path = %root, error = %err, "could not watch bare repos");
        }
    }
    info!("watching project and bare trees for changes");
    while let Some(paths) = rx.recv().await {
        if paths.iter().all(|path| noisy(path)) {
            continue;
        }
        if let Err(err) = reconcile(&cfg, &db, &paths).await {
            warn!(error = %err, "tree reconcile failed");
        }
    }
    Ok(())
}

async fn reconcile(cfg: &AppConfig, db: &Storage, paths: &[PathBuf]) -> Result<()> {
    let mut live_projects = HashSet::new();
    let mut touched = HashSet::new();
    for root in &cfg.git.project_roots {
        let root_path = PathBuf::from(&root.path);
        let dirs = discover_project_dirs(&root_path);
        for (name, dir) in &dirs {
            live_projects.insert(name.clone());
            let hit = paths.iter().any(|path| path.starts_with(dir));
            if hit {
                touched.insert(name.clone());
            }
        }
        for (name, dir) in dirs {
            if !touched.contains(&name) {
                continue;
            }
            if crate::storage::validate_channel_name(&name).is_err() {
                continue;
            }
            let vis = crate::storage::normalize_visibility(&root.visibility).unwrap_or("private");
            db.upsert_channel(&ChannelRow {
                name: name.clone(),
                visibility: vis.into(),
                namespace: name.clone(),
                origin_node: "project".into(),
                context_namespaces: vec![name.clone()],
                updated_at_ms: 0,
            })
            .await?;
            let ns = name.clone();
            let files = tokio::task::spawn_blocking(move || scan_workdir(&dir, &ns, "project"))
                .await
                .ok()
                .and_then(|r| r.ok())
                .unwrap_or_default();
            for entry in files {
                let _ = db.upsert_proto(&entry, false).await;
            }
            info!(channel = %name, "reindexed a project after a change on disk");
        }
    }
    if let Some(root) = &cfg.git.bare_repos_root {
        let root_path = PathBuf::from(root);
        let repos = discover_bare_repos(&root_path);
        let names: HashSet<String> = repos.iter().map(|(name, _)| name.clone()).collect();
        for channel in db.list_channel_rows().await? {
            if channel.origin_node == "index" && !names.contains(&channel.name) {
                db.delete_channel(&channel.name).await?;
                info!(channel = %channel.name, "removed a bare repo channel that is no longer on disk");
            }
        }
        for (name, dir) in repos {
            if !paths.iter().any(|path| path.starts_with(&dir)) {
                continue;
            }
            db.upsert_channel(&ChannelRow {
                name: name.clone(),
                visibility: "public".into(),
                namespace: name.clone(),
                origin_node: "index".into(),
                context_namespaces: vec![name.clone()],
                updated_at_ms: 0,
            })
            .await?;
            let ns = name.clone();
            let files = tokio::task::spawn_blocking(move || scan_bare_repo(&dir, &ns))
                .await
                .ok()
                .and_then(|r| r.ok())
                .unwrap_or_default();
            for entry in files {
                let _ = db.upsert_proto(&entry, false).await;
            }
        }
    }
    for channel in db.list_channel_rows().await? {
        if channel.origin_node == "project" && !live_projects.contains(&channel.name) {
            db.delete_channel(&channel.name).await?;
            info!(channel = %channel.name, "removed a project channel that is no longer on disk");
        }
    }
    Ok(())
}
