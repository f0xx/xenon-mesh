use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sqlx::{any::AnyPoolOptions, AnyPool, Row};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

pub mod pb {
    tonic::include_proto!("xmeshd.v1");
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbKind {
    Sqlite,
    Postgres,
    Mysql,
}

pub struct Storage {
    pub pool: AnyPool,
    pub kind: DbKind,
}

#[derive(Serialize)]
pub struct MemoryItem {
    pub id: String,
    pub kind: String,
    pub source_path: String,
    pub content: String,
}

#[derive(Clone, Debug)]
pub struct ChannelPref {
    pub channel_name: String,
    pub pinned: bool,
    pub muted: bool,
    pub archived: bool,
    pub closed: bool,
    pub last_read_seq: i64,
    pub use_count: i64,
}

impl ChannelPref {
    pub fn bare(name: &str) -> Self {
        Self {
            channel_name: name.to_string(),
            pinned: false,
            muted: false,
            archived: false,
            closed: false,
            last_read_seq: 0,
            use_count: 0,
        }
    }
}

fn sql_flag(row: &sqlx::any::AnyRow, name: &str) -> bool {
    row.try_get::<i64, _>(name)
        .or_else(|_| row.try_get::<i32, _>(name).map(|v| v as i64))
        .unwrap_or(0)
        != 0
}

fn sql_i64(row: &sqlx::any::AnyRow, name: &str) -> i64 {
    row.try_get::<i64, _>(name)
        .or_else(|_| row.try_get::<i32, _>(name).map(|v| v as i64))
        .unwrap_or(0)
}

fn pref_from_row(row: &sqlx::any::AnyRow) -> ChannelPref {
    ChannelPref {
        channel_name: row.get("channel_name"),
        pinned: sql_flag(row, "pinned"),
        muted: sql_flag(row, "muted"),
        archived: sql_flag(row, "archived"),
        closed: sql_flag(row, "closed"),
        last_read_seq: sql_i64(row, "last_read_seq"),
        use_count: sql_i64(row, "use_count"),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditEvent {
    pub node_id: String,
    pub action: String,
    pub channel: String,
    pub thread: String,
    pub detail: String,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelRow {
    pub name: String,
    pub visibility: String,
    pub namespace: String,
    pub origin_node: String,
    pub context_namespaces: Vec<String>,
    pub updated_at_ms: i64,
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub fn rewrite_placeholders(kind: DbKind, sql: &str) -> String {
    if kind != DbKind::Mysql {
        return sql.to_string();
    }
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 1 {
                out.push('?');
                i = j;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

pub fn validate_channel_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 128 {
        bail!("channel name must be 1..=128 characters");
    }
    if name
        .split('/')
        .any(|part| part.is_empty() || part == "." || part == "..")
    {
        bail!("channel name contains an empty or relative path segment");
    }
    if name.chars().any(|c| c.is_control() || c == '\\') {
        bail!("channel name contains unsupported characters");
    }
    Ok(())
}

pub fn normalize_visibility(value: &str) -> Result<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "public" | "open" => Ok("public"),
        "shared" => Ok("shared"),
        "" | "private" => Ok("private"),
        _ => bail!("visibility must be public, private, or shared"),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ChannelGroup {
    General,
    Public,
    Private,
    Other,
}

pub fn channel_group(name: &str, visibility: &str) -> ChannelGroup {
    if name == "general" {
        ChannelGroup::General
    } else if visibility == "public" {
        ChannelGroup::Public
    } else if visibility == "private" {
        ChannelGroup::Private
    } else {
        ChannelGroup::Other
    }
}

#[derive(Clone, Debug)]
pub struct SidebarKey {
    pub group: ChannelGroup,
    pub pinned: bool,
    pub use_count: i64,
    pub name: String,
}

/// Pinned channels stay at the top of their group, A to Z.
/// Everything else is most-used first, then A to Z.
pub fn cmp_sidebar(a: &SidebarKey, b: &SidebarKey) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    let group = a.group.cmp(&b.group);
    if group != Equal {
        return group;
    }
    let pin = b.pinned.cmp(&a.pinned);
    if pin != Equal {
        return pin;
    }
    if a.pinned {
        return a.name.to_lowercase().cmp(&b.name.to_lowercase());
    }
    let used = b.use_count.cmp(&a.use_count);
    if used != Equal {
        return used;
    }
    a.name.to_lowercase().cmp(&b.name.to_lowercase())
}

fn message_id(channel: &str, thread: &str, seq: u64) -> String {
    blake3::hash(format!("{channel}:{thread}:{seq}").as_bytes())
        .to_hex()
        .to_string()
}

#[derive(Clone, Debug)]
pub struct StoredPreference {
    pub id: String,
    pub thread_id: String,
    pub prompt_seq: i64,
    pub prompt: String,
    pub chosen_seq: i64,
    pub rejected_seq: i64,
    pub chosen: String,
    pub rejected: String,
    pub source: String,
    pub exported_at_ms: i64,
}

#[derive(Clone, Debug)]
pub struct ShownMessage {
    pub message: pb::SessionMessage,
    pub edited_at_ms: i64,
    pub started_at_ms: i64,
    pub finished_at_ms: i64,
    pub vote: i32,
}

#[derive(Clone, Debug)]
pub struct SyncRow {
    pub message: pb::SessionMessage,
    pub deleted: bool,
    pub edited_at_ms: i64,
}

#[derive(Clone, Debug)]
pub struct P2pLink {
    pub channel: String,
    pub peer_base: String,
    #[allow(dead_code)]
    pub peer_name: String,
    pub token: String,
}

fn detail_ms(detail: &str, key: &str) -> i64 {
    serde_json::from_str::<serde_json::Value>(detail)
        .ok()
        .and_then(|v| v.get(key).and_then(|n| n.as_i64()))
        .unwrap_or(0)
}

fn edited_at(detail: &str) -> i64 {
    detail_ms(detail, "edited_at_ms")
}

fn detail_vote(detail: &str) -> i32 {
    serde_json::from_str::<serde_json::Value>(detail)
        .ok()
        .and_then(|v| v.get("feedback")?.get("operator_vote")?.as_i64())
        .unwrap_or(0) as i32
}

fn shown_from_row(row: &sqlx::any::AnyRow) -> ShownMessage {
    let seq: i64 = row.get("seq");
    let detail: String = row.try_get("detail").unwrap_or_default();
    ShownMessage {
        message: pb::SessionMessage {
            channel_id: row.get("channel_id"),
            thread_id: row.get("thread_id"),
            seq: seq as u64,
            author: row.get("author"),
            content: row.get("content"),
            created_at_ms: row.get("created_at_ms"),
        },
        edited_at_ms: edited_at(&detail),
        started_at_ms: detail_ms(&detail, "started_at_ms"),
        finished_at_ms: detail_ms(&detail, "finished_at_ms"),
        vote: detail_vote(&detail),
    }
}

fn decode_namespaces(raw: &str, fallback: &str) -> Vec<String> {
    serde_json::from_str(raw).unwrap_or_else(|_| {
        if fallback.is_empty() {
            Vec::new()
        } else {
            vec![fallback.to_string()]
        }
    })
}

impl Storage {
    fn q(&self, sql: &str) -> String {
        rewrite_placeholders(self.kind, sql)
    }

    pub async fn connect(primary: Option<&str>, sqlite_fallback: &str) -> Result<Self> {
        sqlx::any::install_default_drivers();

        if let Some(url) = primary.filter(|u| !u.trim().is_empty()) {
            if let Ok(pool) = AnyPoolOptions::new().max_connections(32).connect(url).await {
                let kind = if url.starts_with("postgres") {
                    DbKind::Postgres
                } else if url.starts_with("mysql") || url.starts_with("mariadb") {
                    DbKind::Mysql
                } else {
                    DbKind::Sqlite
                };
                let st = Self { pool, kind };
                st.tune_pool().await;
                st.init_schema().await?;
                info!("Storage initialized on primary backend: {:?}", kind);
                return Ok(st);
            }
            warn!("Primary DB unavailable, falling back to SQLite: {sqlite_fallback}");
        }

        if let Some(parent) = std::path::Path::new(sqlite_fallback).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).ok();
            }
        }
        let url = format!("sqlite://{sqlite_fallback}?mode=rwc");
        let pool = AnyPoolOptions::new()
            .max_connections(32)
            .connect(&url)
            .await
            .context("Failed to initialize fallback SQLite")?;
        let st = Self {
            pool,
            kind: DbKind::Sqlite,
        };
        st.tune_pool().await;
        st.init_schema().await?;
        info!("Storage initialized on fallback SQLite");
        Ok(st)
    }

    async fn tune_pool(&self) {
        if self.kind != DbKind::Sqlite {
            return;
        }
        let _ = sqlx::query("PRAGMA journal_mode=WAL")
            .fetch_optional(&self.pool)
            .await;
        let _ = sqlx::query("PRAGMA busy_timeout=5000")
            .execute(&self.pool)
            .await;
    }

    pub async fn init_schema(&self) -> Result<()> {
        let blob_col = match self.kind {
            DbKind::Postgres => "BYTEA",
            DbKind::Mysql => "LONGBLOB",
            DbKind::Sqlite => "BLOB",
        };
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS context_entries (
                id VARCHAR(64) PRIMARY KEY,
                namespace VARCHAR(128) NOT NULL,
                entry_type INTEGER NOT NULL,
                source_path TEXT NOT NULL,
                content TEXT NOT NULL,
                content_hash VARCHAR(64) NOT NULL,
                embedding {blob_col} NOT NULL,
                updated_at_ms BIGINT NOT NULL,
                origin_node VARCHAR(128) NOT NULL
            )"
        );
        sqlx::query(&ddl).execute(&self.pool).await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_ns_type ON context_entries(namespace, entry_type)",
        )
        .execute(&self.pool)
        .await?;
        self.init_messages_schema().await?;
        Ok(())
    }

    pub async fn init_messages_schema(&self) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS session_messages (
                id VARCHAR(64) PRIMARY KEY,
                channel_id VARCHAR(128) NOT NULL,
                thread_id VARCHAR(128) NOT NULL,
                seq BIGINT NOT NULL,
                author TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at_ms BIGINT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_msg_thread ON session_messages(channel_id, thread_id, seq)")
            .execute(&self.pool)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS message_meta (
                id VARCHAR(64) PRIMARY KEY,
                deleted INTEGER NOT NULL DEFAULT 0,
                detail TEXT NOT NULL DEFAULT ''
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS invite_tokens (
                token_hash VARCHAR(64) PRIMARY KEY,
                channel_name VARCHAR(128) NOT NULL,
                created_at_ms BIGINT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS p2p_links (
                channel_name VARCHAR(128) PRIMARY KEY,
                peer_base TEXT NOT NULL,
                peer_name TEXT NOT NULL,
                token TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;
        self.widen_author().await;
        self.init_aux_tables().await?;
        Ok(())
    }

    async fn widen_author(&self) {
        let stmt = match self.kind {
            DbKind::Postgres => "ALTER TABLE session_messages ALTER COLUMN author TYPE TEXT",
            DbKind::Mysql => "ALTER TABLE session_messages MODIFY author TEXT NOT NULL",
            DbKind::Sqlite => return,
        };
        let _ = sqlx::query(stmt).execute(&self.pool).await;
    }

    async fn init_aux_tables(&self) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS channels (
                name VARCHAR(128) PRIMARY KEY,
                visibility VARCHAR(16) NOT NULL,
                namespace VARCHAR(128) NOT NULL,
                origin_node VARCHAR(128) NOT NULL,
                context_namespaces TEXT NOT NULL,
                updated_at_ms BIGINT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS threads (
                channel_id VARCHAR(128) NOT NULL,
                thread_id VARCHAR(128) NOT NULL,
                visibility VARCHAR(16) NOT NULL,
                PRIMARY KEY (channel_id, thread_id)
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS node_settings (
                setting_key VARCHAR(64) PRIMARY KEY,
                value TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS chat_model_prefs (
                channel_name VARCHAR(128) NOT NULL,
                thread_id VARCHAR(128) NOT NULL,
                models TEXT NOT NULL,
                multi INTEGER NOT NULL,
                PRIMARY KEY (channel_name, thread_id)
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS thread_reads (
                channel_name VARCHAR(128) NOT NULL,
                thread_id VARCHAR(128) NOT NULL,
                last_read_seq BIGINT NOT NULL,
                PRIMARY KEY (channel_name, thread_id)
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS channel_prefs (
                channel_name VARCHAR(128) PRIMARY KEY,
                pinned INTEGER NOT NULL,
                muted INTEGER NOT NULL,
                archived INTEGER NOT NULL,
                closed INTEGER NOT NULL,
                last_read_seq BIGINT NOT NULL,
                use_count BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS channel_members (
                channel_name VARCHAR(128) NOT NULL,
                node_id VARCHAR(128) NOT NULL,
                role VARCHAR(16) NOT NULL,
                state VARCHAR(16) NOT NULL,
                PRIMARY KEY (channel_name, node_id)
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS audit_events (
                id VARCHAR(64) PRIMARY KEY,
                node_id VARCHAR(128) NOT NULL,
                action VARCHAR(64) NOT NULL,
                channel_name VARCHAR(128) NOT NULL,
                thread_id VARCHAR(128) NOT NULL,
                detail TEXT NOT NULL,
                created_at_ms BIGINT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_audit_time ON audit_events(created_at_ms)")
            .execute(&self.pool)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS channel_invites (
                channel_name VARCHAR(128) PRIMARY KEY,
                token_hash VARCHAR(64) NOT NULL,
                created_at_ms BIGINT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS preference_pairs (
                id VARCHAR(64) PRIMARY KEY,
                channel_id VARCHAR(128) NOT NULL,
                thread_id VARCHAR(128) NOT NULL,
                prompt_seq BIGINT NOT NULL,
                prompt TEXT NOT NULL,
                chosen_seq BIGINT NOT NULL,
                rejected_seq BIGINT NOT NULL,
                chosen TEXT NOT NULL,
                rejected TEXT NOT NULL,
                source VARCHAR(32) NOT NULL,
                exported_at_ms BIGINT NOT NULL DEFAULT 0,
                created_at_ms BIGINT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_pref_channel ON preference_pairs(channel_id, exported_at_ms)",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS entry_stats (
                id VARCHAR(64) PRIMARY KEY,
                score INTEGER NOT NULL,
                hits INTEGER NOT NULL
            )",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

impl ChannelRow {
    pub fn to_proto(&self) -> pb::ChannelRecord {
        pb::ChannelRecord {
            name: self.name.clone(),
            visibility: self.visibility.clone(),
            namespace: self.namespace.clone(),
            origin_node: self.origin_node.clone(),
            updated_at_ms: self.updated_at_ms,
            context_namespaces: self.context_namespaces.clone(),
        }
    }

    pub fn from_proto(c: &pb::ChannelRecord) -> Self {
        let namespace = if c.namespace.is_empty() {
            c.name.clone()
        } else {
            c.namespace.clone()
        };
        let mut context_namespaces = c.context_namespaces.clone();
        if context_namespaces.is_empty() {
            context_namespaces.push(namespace.clone());
        }
        Self {
            name: c.name.clone(),
            visibility: if c.visibility == "public" {
                "public".into()
            } else {
                "private".into()
            },
            namespace,
            origin_node: c.origin_node.clone(),
            context_namespaces,
            updated_at_ms: c.updated_at_ms,
        }
    }
}

impl Storage {
    pub async fn upsert_proto(&self, entry: &pb::ContextEntry, force: bool) -> Result<bool> {
        if !force {
            let sql = self.q("SELECT entry_type, content_hash FROM context_entries WHERE id = $1");
            let row = sqlx::query(&sql)
                .bind(&entry.id)
                .fetch_optional(&self.pool)
                .await?;
            if let Some(r) = row {
                let etype: i32 = r.get("entry_type");
                let hash: String = r.get("content_hash");
                if etype == pb::EntryType::Pinned as i32 || hash == entry.content_hash {
                    return Ok(false);
                }
            }
        }

        let emb_bytes: &[u8] = bytemuck::cast_slice(&entry.embedding);
        let mut tx = self.pool.begin().await?;
        let del = self.q("DELETE FROM context_entries WHERE id = $1");
        sqlx::query(&del).bind(&entry.id).execute(&mut *tx).await?;
        let ins = self.q(
            "INSERT INTO context_entries
             (id, namespace, entry_type, source_path, content, content_hash, embedding, updated_at_ms, origin_node)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        );
        sqlx::query(&ins)
            .bind(&entry.id)
            .bind(&entry.namespace)
            .bind(entry.entry_type)
            .bind(&entry.source_path)
            .bind(&entry.content)
            .bind(&entry.content_hash)
            .bind(emb_bytes)
            .bind(entry.updated_at_ms)
            .bind(&entry.origin_node)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(true)
    }

    fn entry_from_row(&self, r: &sqlx::any::AnyRow) -> pb::ContextEntry {
        let raw_emb: Vec<u8> = r.get("embedding");
        let embedding = if raw_emb.len() % 4 == 0 {
            bytemuck::pod_collect_to_vec(&raw_emb)
        } else {
            Vec::new()
        };
        pb::ContextEntry {
            id: r.get("id"),
            namespace: r.get("namespace"),
            entry_type: r.get("entry_type"),
            source_path: r.get("source_path"),
            content: r.get("content"),
            content_hash: r.get("content_hash"),
            embedding,
            updated_at_ms: r.get("updated_at_ms"),
            origin_node: r.get("origin_node"),
            meta: Default::default(),
        }
    }

    pub async fn load_entries(&self, namespace: Option<&str>) -> Result<Vec<pb::ContextEntry>> {
        let rows = if let Some(ns) = namespace {
            let sql = self.q("SELECT * FROM context_entries WHERE namespace = $1");
            sqlx::query(&sql).bind(ns).fetch_all(&self.pool).await?
        } else {
            sqlx::query("SELECT * FROM context_entries")
                .fetch_all(&self.pool)
                .await?
        };
        Ok(rows.iter().map(|r| self.entry_from_row(r)).collect())
    }

    pub async fn export_envelope(
        &self,
        node_name: &str,
        namespace: Option<&str>,
    ) -> Result<pb::SyncEnvelope> {
        self.export_filtered(node_name, namespace, true).await
    }

    pub async fn export_public_envelope(
        &self,
        node_name: &str,
        namespace: Option<&str>,
    ) -> Result<pb::SyncEnvelope> {
        self.export_filtered(node_name, namespace, false).await
    }

    async fn export_filtered(
        &self,
        node_name: &str,
        namespace: Option<&str>,
        include_private: bool,
    ) -> Result<pb::SyncEnvelope> {
        if !include_private {
            if let Some(ns) = namespace {
                if let Some(ch) = self.get_channel(ns).await? {
                    if ch.visibility != "public" {
                        return Ok(pb::SyncEnvelope {
                            schema_version: 2,
                            source_node: node_name.to_string(),
                            timestamp_ms: now_ms(),
                            ..Default::default()
                        });
                    }
                }
            }
        }
        Ok(pb::SyncEnvelope {
            schema_version: 2,
            source_node: node_name.to_string(),
            timestamp_ms: now_ms(),
            entries: self.load_entries(namespace).await?,
            messages: self.load_messages(namespace, include_private).await?,
            channels: self
                .list_channel_rows()
                .await?
                .into_iter()
                .filter(|c| {
                    namespace
                        .map(|ns| c.name == ns || c.namespace == ns)
                        .unwrap_or(true)
                })
                .filter(|c| include_private || c.visibility == "public")
                .map(|c| c.to_proto())
                .collect(),
        })
    }

    pub async fn is_unchanged(&self, id: &str, content_hash: &str) -> Result<bool> {
        let sql = self.q("SELECT entry_type, content_hash FROM context_entries WHERE id = $1");
        let row = sqlx::query(&sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(match row {
            Some(r) => {
                let etype: i32 = r.get("entry_type");
                let hash: String = r.get("content_hash");
                etype == pb::EntryType::Pinned as i32 || hash == content_hash
            }
            None => false,
        })
    }

    pub async fn list_channel_memory(&self, namespace: &str) -> Result<Vec<MemoryItem>> {
        let sql = self.q(
            "SELECT id, entry_type, source_path, content FROM context_entries
             WHERE namespace = $1 AND entry_type IN (2, 3, 4) ORDER BY source_path ASC",
        );
        let rows = sqlx::query(&sql)
            .bind(namespace)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                let et: i32 = r.get("entry_type");
                MemoryItem {
                    id: r.get("id"),
                    kind: if et == pb::EntryType::Pinned as i32 {
                        "PIN".into()
                    } else if et == crate::retrieve::ENTRY_EXEMPLAR {
                        "EXEMPLAR".into()
                    } else {
                        "LEARN".into()
                    },
                    source_path: r.get("source_path"),
                    content: r.get("content"),
                }
            })
            .collect())
    }

    pub async fn delete_entry(&self, id: &str) -> Result<()> {
        let sql = self.q("DELETE FROM context_entries WHERE id = $1");
        sqlx::query(&sql).bind(id).execute(&self.pool).await?;
        let sql = self.q("DELETE FROM entry_stats WHERE id = $1");
        sqlx::query(&sql).bind(id).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn entry_score(&self, id: &str) -> Result<Option<i32>> {
        let sql = self.q("SELECT score FROM entry_stats WHERE id = $1");
        let row = sqlx::query(&sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("score")))
    }

    pub async fn put_entry_score(&self, id: &str, score: i32) -> Result<()> {
        let sql = match self.kind {
            DbKind::Mysql => self.q(
                "INSERT INTO entry_stats (id, score, hits) VALUES ($1, $2, 0)
                 ON DUPLICATE KEY UPDATE score = VALUES(score)",
            ),
            _ => self.q(
                "INSERT INTO entry_stats (id, score, hits) VALUES ($1, $2, 0)
                 ON CONFLICT(id) DO UPDATE SET score = $2",
            ),
        };
        sqlx::query(&sql)
            .bind(id)
            .bind(score)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn entry_scores(&self) -> Result<std::collections::HashMap<String, i32>> {
        let rows = sqlx::query("SELECT id, score FROM entry_stats")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .iter()
            .map(|r| (r.get("id"), r.get("score")))
            .collect())
    }

    pub async fn bump_hits(&self, ids: &[String]) -> Result<()> {
        let sql = self.q("UPDATE entry_stats SET hits = hits + 1 WHERE id = $1");
        for id in ids {
            sqlx::query(&sql).bind(id).execute(&self.pool).await?;
        }
        Ok(())
    }
}

impl Storage {
    pub async fn append_message(&self, msg: &pb::SessionMessage) -> Result<()> {
        let ts = if msg.created_at_ms > 0 {
            msg.created_at_ms
        } else {
            now_ms()
        };
        let id = message_id(&msg.channel_id, &msg.thread_id, msg.seq);
        let mut tx = self.pool.begin().await?;
        let del = self.q("DELETE FROM session_messages WHERE id = $1");
        sqlx::query(&del).bind(&id).execute(&mut *tx).await?;
        let ins = self.q(
            "INSERT INTO session_messages (id, channel_id, thread_id, seq, author, content, created_at_ms)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        );
        sqlx::query(&ins)
            .bind(&id)
            .bind(&msg.channel_id)
            .bind(&msg.thread_id)
            .bind(msg.seq as i64)
            .bind(&msg.author)
            .bind(&msg.content)
            .bind(ts)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn save_message_detail(
        &self,
        channel: &str,
        thread: &str,
        seq: u64,
        detail: &str,
    ) -> Result<()> {
        let id = message_id(channel, thread, seq);
        let sql = match self.kind {
            DbKind::Mysql => self.q(
                "INSERT INTO message_meta (id, deleted, detail) VALUES ($1, 0, $2)
                 ON DUPLICATE KEY UPDATE detail = VALUES(detail)",
            ),
            _ => self.q(
                "INSERT INTO message_meta (id, deleted, detail) VALUES ($1, 0, $2)
                 ON CONFLICT(id) DO UPDATE SET detail = $2",
            ),
        };
        sqlx::query(&sql)
            .bind(&id)
            .bind(detail)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn set_message_deleted(
        &self,
        channel: &str,
        thread: &str,
        seq: u64,
        deleted: bool,
    ) -> Result<()> {
        let id = message_id(channel, thread, seq);
        let flag = if deleted { 1 } else { 0 };
        let sql = match self.kind {
            DbKind::Mysql => self.q(
                "INSERT INTO message_meta (id, deleted, detail) VALUES ($1, $2, '')
                 ON DUPLICATE KEY UPDATE deleted = VALUES(deleted)",
            ),
            _ => self.q(
                "INSERT INTO message_meta (id, deleted, detail) VALUES ($1, $2, '')
                 ON CONFLICT(id) DO UPDATE SET deleted = $2",
            ),
        };
        sqlx::query(&sql)
            .bind(&id)
            .bind(flag)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn rewrite_message(
        &self,
        channel: &str,
        thread: &str,
        seq: u64,
        content: &str,
    ) -> Result<()> {
        let sql = self.q(
            "UPDATE session_messages SET content = $1 WHERE channel_id = $2 AND thread_id = $3 AND seq = $4",
        );
        let result = sqlx::query(&sql)
            .bind(content)
            .bind(channel)
            .bind(thread)
            .bind(seq as i64)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            bail!("message not found");
        }
        Ok(())
    }

    pub async fn message_record(
        &self,
        channel: &str,
        thread: &str,
        seq: u64,
    ) -> Result<Option<serde_json::Value>> {
        let sql = self.q(
            "SELECT s.channel_id, s.thread_id, s.seq, s.author, s.content, s.created_at_ms,
                    COALESCE(m.deleted, 0) AS deleted, COALESCE(m.detail, '') AS detail
             FROM session_messages s
             LEFT JOIN message_meta m ON m.id = s.id
             WHERE s.channel_id = $1 AND s.thread_id = $2 AND s.seq = $3",
        );
        let row = sqlx::query(&sql)
            .bind(channel)
            .bind(thread)
            .bind(seq as i64)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let seq: i64 = row.get("seq");
        let deleted = row
            .try_get::<i64, _>("deleted")
            .or_else(|_| row.try_get::<i32, _>("deleted").map(|v| i64::from(v)))
            .unwrap_or(0);
        let detail: String = row.try_get("detail").unwrap_or_default();
        let detail_json = serde_json::from_str::<serde_json::Value>(&detail)
            .unwrap_or(serde_json::Value::String(detail));
        Ok(Some(serde_json::json!({
            "channel_id": row.get::<String, _>("channel_id"),
            "thread_id": row.get::<String, _>("thread_id"),
            "seq": seq,
            "author": row.get::<String, _>("author"),
            "content": row.get::<String, _>("content"),
            "created_at_ms": row.get::<i64, _>("created_at_ms"),
            "deleted": deleted != 0,
            "detail": detail_json
        })))
    }

    pub async fn next_seq(&self, channel: &str, thread: &str) -> Result<u64> {
        let sql = self.q("SELECT COALESCE(MAX(seq), 0) AS s FROM session_messages WHERE channel_id = $1 AND thread_id = $2");
        let row = sqlx::query(&sql)
            .bind(channel)
            .bind(thread)
            .fetch_one(&self.pool)
            .await?;
        let s: i64 = row.try_get("s").unwrap_or(0);
        Ok((s.max(0) as u64).saturating_add(1))
    }

    async fn message_content(&self, id: &str) -> Result<Option<String>> {
        let sql = self.q("SELECT content FROM session_messages WHERE id = $1");
        let row = sqlx::query(&sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("content")))
    }

    pub async fn append_public_message(
        &self,
        msg: &pb::SessionMessage,
    ) -> Result<pb::SessionMessage> {
        if self
            .thread_visibility(&msg.channel_id, &msg.thread_id)
            .await?
            == "private"
        {
            bail!("thread is private");
        }
        let ts = if msg.created_at_ms > 0 {
            msg.created_at_ms
        } else {
            now_ms()
        };
        let mut seq = if msg.seq == 0 {
            self.next_seq(&msg.channel_id, &msg.thread_id).await?
        } else {
            msg.seq
        };
        let ins_sql = match self.kind {
            DbKind::Mysql => self.q(
                "INSERT IGNORE INTO session_messages (id, channel_id, thread_id, seq, author, content, created_at_ms)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            ),
            _ => self.q(
                "INSERT INTO session_messages (id, channel_id, thread_id, seq, author, content, created_at_ms)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT(id) DO NOTHING",
            ),
        };
        for _ in 0..1000 {
            let id = message_id(&msg.channel_id, &msg.thread_id, seq);
            let res = sqlx::query(&ins_sql)
                .bind(&id)
                .bind(&msg.channel_id)
                .bind(&msg.thread_id)
                .bind(seq as i64)
                .bind(&msg.author)
                .bind(&msg.content)
                .bind(ts)
                .execute(&self.pool)
                .await?;
            if res.rows_affected() > 0 {
                let mut stored = msg.clone();
                stored.seq = seq;
                stored.created_at_ms = ts;
                return Ok(stored);
            }
            if let Some(existing) = self.message_content(&id).await? {
                if existing == msg.content {
                    let mut stored = msg.clone();
                    stored.seq = seq;
                    return Ok(stored);
                }
            }
            seq += 1;
        }
        bail!("could not allocate a message sequence")
    }

    fn message_from_row(r: &sqlx::any::AnyRow) -> pb::SessionMessage {
        let seq: i64 = r.get("seq");
        pb::SessionMessage {
            channel_id: r.get("channel_id"),
            thread_id: r.get("thread_id"),
            seq: seq as u64,
            author: r.get("author"),
            content: r.get("content"),
            created_at_ms: r.get("created_at_ms"),
        }
    }

    pub async fn load_thread(
        &self,
        channel_id: &str,
        thread_id: &str,
        limit: i64,
    ) -> Result<Vec<pb::SessionMessage>> {
        let sql = self.q(
            "SELECT channel_id, thread_id, seq, author, content, created_at_ms
             FROM session_messages
             WHERE channel_id = $1 AND thread_id = $2
             AND id NOT IN (SELECT id FROM message_meta WHERE deleted != 0)
             ORDER BY seq ASC LIMIT $3",
        );
        let rows = sqlx::query(&sql)
            .bind(channel_id)
            .bind(thread_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.iter().map(Self::message_from_row).collect())
    }

    pub async fn load_shown(
        &self,
        channel_id: &str,
        thread_id: &str,
        limit: i64,
    ) -> Result<Vec<ShownMessage>> {
        let sql = self.q(
            "SELECT s.channel_id, s.thread_id, s.seq, s.author, s.content, s.created_at_ms,
                    COALESCE(m.detail, '') AS detail
             FROM session_messages s
             LEFT JOIN message_meta m ON m.id = s.id
             WHERE s.channel_id = $1 AND s.thread_id = $2
             AND s.id NOT IN (SELECT id FROM message_meta WHERE deleted != 0)
             ORDER BY s.seq ASC LIMIT $3",
        );
        let rows = sqlx::query(&sql)
            .bind(channel_id)
            .bind(thread_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.iter().map(shown_from_row).collect())
    }

    pub async fn load_sync(
        &self,
        channel_id: &str,
        thread_id: &str,
        limit: i64,
    ) -> Result<Vec<SyncRow>> {
        let sql = self.q(
            "SELECT s.channel_id, s.thread_id, s.seq, s.author, s.content, s.created_at_ms,
                    COALESCE(m.deleted, 0) AS deleted, COALESCE(m.detail, '') AS detail
             FROM session_messages s
             LEFT JOIN message_meta m ON m.id = s.id
             WHERE s.channel_id = $1 AND s.thread_id = $2
             ORDER BY s.seq ASC LIMIT $3",
        );
        let rows = sqlx::query(&sql)
            .bind(channel_id)
            .bind(thread_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .iter()
            .map(|row| {
                let deleted = row
                    .try_get::<i64, _>("deleted")
                    .or_else(|_| row.try_get::<i32, _>("deleted").map(i64::from))
                    .unwrap_or(0);
                SyncRow {
                    message: Self::message_from_row(row),
                    deleted: deleted != 0,
                    edited_at_ms: edited_at(&row.try_get::<String, _>("detail").unwrap_or_default()),
                }
            })
            .collect())
    }

    pub async fn put_p2p(
        &self,
        channel: &str,
        peer_base: &str,
        peer_name: &str,
        token: &str,
    ) -> Result<()> {
        let sql = match self.kind {
            DbKind::Mysql => self.q(
                "INSERT INTO p2p_links (channel_name, peer_base, peer_name, token)
                 VALUES ($1, $2, $3, $4)
                 ON DUPLICATE KEY UPDATE peer_base = VALUES(peer_base), peer_name = VALUES(peer_name), token = VALUES(token)",
            ),
            _ => self.q(
                "INSERT INTO p2p_links (channel_name, peer_base, peer_name, token)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT(channel_name) DO UPDATE SET peer_base = $2, peer_name = $3, token = $4",
            ),
        };
        sqlx::query(&sql)
            .bind(channel)
            .bind(peer_base)
            .bind(peer_name)
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_chat_models(&self, channel: &str, thread: &str) -> Result<(Vec<String>, bool)> {
        let sql = self.q(
            "SELECT models, multi FROM chat_model_prefs WHERE channel_name = $1 AND thread_id = $2",
        );
        let Some(row) = sqlx::query(&sql)
            .bind(channel)
            .bind(thread)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok((Vec::new(), false));
        };
        let raw: String = row.get("models");
        let multi: i32 = row.try_get("multi").unwrap_or(0);
        let models = raw
            .split('\n')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        Ok((models, multi != 0))
    }

    pub async fn set_chat_models(
        &self,
        channel: &str,
        thread: &str,
        models: &[String],
        multi: bool,
    ) -> Result<()> {
        let sql = match self.kind {
            DbKind::Mysql => self.q(
                "INSERT INTO chat_model_prefs (channel_name, thread_id, models, multi)
                 VALUES ($1, $2, $3, $4)
                 ON DUPLICATE KEY UPDATE models = VALUES(models), multi = VALUES(multi)",
            ),
            _ => self.q(
                "INSERT INTO chat_model_prefs (channel_name, thread_id, models, multi)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT(channel_name, thread_id) DO UPDATE SET models = $3, multi = $4",
            ),
        };
        sqlx::query(&sql)
            .bind(channel)
            .bind(thread)
            .bind(models.join("\n"))
            .bind(i32::from(multi))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_setting(&self, key: &str) -> Result<String> {
        let sql = self.q("SELECT value FROM node_settings WHERE setting_key = $1");
        let row = sqlx::query(&sql)
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("value")).unwrap_or_default())
    }

    pub async fn set_setting(&self, key: &str, value: &str) -> Result<()> {
        let sql = match self.kind {
            DbKind::Mysql => self.q(
                "INSERT INTO node_settings (setting_key, value) VALUES ($1, $2)
                 ON DUPLICATE KEY UPDATE value = VALUES(value)",
            ),
            _ => self.q(
                "INSERT INTO node_settings (setting_key, value) VALUES ($1, $2)
                 ON CONFLICT(setting_key) DO UPDATE SET value = $2",
            ),
        };
        sqlx::query(&sql)
            .bind(key)
            .bind(value)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_p2p(&self, channel: &str) -> Result<Option<P2pLink>> {
        let sql = self.q(
            "SELECT channel_name, peer_base, peer_name, token FROM p2p_links WHERE channel_name = $1",
        );
        let row = sqlx::query(&sql)
            .bind(channel)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| P2pLink {
            channel: r.get("channel_name"),
            peer_base: r.get("peer_base"),
            peer_name: r.get("peer_name"),
            token: r.get("token"),
        }))
    }

    async fn load_messages(
        &self,
        channel: Option<&str>,
        include_private: bool,
    ) -> Result<Vec<pb::SessionMessage>> {
        let rows = match (channel, include_private) {
            (Some(ch), true) => {
                let sql = self.q(
                    "SELECT channel_id, thread_id, seq, author, content, created_at_ms
                     FROM session_messages WHERE channel_id = $1 ORDER BY thread_id, seq",
                );
                sqlx::query(&sql).bind(ch).fetch_all(&self.pool).await?
            }
            (None, true) => {
                sqlx::query(
                    "SELECT channel_id, thread_id, seq, author, content, created_at_ms
                     FROM session_messages ORDER BY channel_id, thread_id, seq",
                )
                .fetch_all(&self.pool)
                .await?
            }
            (Some(ch), false) => {
                let sql = self.q(
                    "SELECT m.channel_id, m.thread_id, m.seq, m.author, m.content, m.created_at_ms
                     FROM session_messages m
                     LEFT JOIN threads t ON t.channel_id = m.channel_id AND t.thread_id = m.thread_id
                     WHERE m.channel_id = $1 AND COALESCE(t.visibility, 'inherit') <> 'private'
                     ORDER BY m.seq",
                );
                sqlx::query(&sql).bind(ch).fetch_all(&self.pool).await?
            }
            (None, false) => {
                sqlx::query(
                    "SELECT m.channel_id, m.thread_id, m.seq, m.author, m.content, m.created_at_ms
                     FROM session_messages m
                     LEFT JOIN threads t ON t.channel_id = m.channel_id AND t.thread_id = m.thread_id
                     WHERE COALESCE(t.visibility, 'inherit') <> 'private'
                     ORDER BY m.channel_id, m.thread_id, m.seq",
                )
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(rows.iter().map(Self::message_from_row).collect())
    }

    pub async fn list_threads(&self, channel_id: &str) -> Result<Vec<String>> {
        let sql = self.q("SELECT DISTINCT thread_id FROM session_messages WHERE channel_id = $1 ORDER BY thread_id ASC");
        let rows = sqlx::query(&sql)
            .bind(channel_id)
            .fetch_all(&self.pool)
            .await?;
        let mut threads: Vec<String> = rows.into_iter().map(|r| r.get("thread_id")).collect();
        if !threads.iter().any(|t| t == "main") {
            threads.insert(0, "main".into());
        }
        Ok(threads)
    }

    pub async fn thread_visibility(&self, channel_id: &str, thread_id: &str) -> Result<String> {
        let sql = self.q("SELECT visibility FROM threads WHERE channel_id = $1 AND thread_id = $2");
        let row = sqlx::query(&sql)
            .bind(channel_id)
            .bind(thread_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row
            .map(|r| r.get::<String, _>("visibility"))
            .unwrap_or_else(|| "inherit".into()))
    }

    pub async fn set_thread_visibility(
        &self,
        channel_id: &str,
        thread_id: &str,
        visibility: &str,
    ) -> Result<()> {
        let visibility = if visibility == "private" {
            "private"
        } else {
            "inherit"
        };
        let mut tx = self.pool.begin().await?;
        let del = self.q("DELETE FROM threads WHERE channel_id = $1 AND thread_id = $2");
        sqlx::query(&del)
            .bind(channel_id)
            .bind(thread_id)
            .execute(&mut *tx)
            .await?;
        let ins =
            self.q("INSERT INTO threads (channel_id, thread_id, visibility) VALUES ($1, $2, $3)");
        sqlx::query(&ins)
            .bind(channel_id)
            .bind(thread_id)
            .bind(visibility)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn fork_thread(
        &self,
        channel_id: &str,
        src_thread: &str,
        dst_thread: &str,
        keep_head: usize,
        keep_tail: usize,
    ) -> Result<usize> {
        validate_channel_name(channel_id)?;
        if dst_thread.is_empty() || dst_thread.len() > 128 {
            bail!("thread name length");
        }
        let all = self.load_thread(channel_id, src_thread, 10_000).await?;
        if all.is_empty() {
            return Ok(0);
        }
        self.set_thread_visibility(channel_id, dst_thread, "private")
            .await?;
        let total = all.len();
        let selected: Vec<_> = all
            .into_iter()
            .enumerate()
            .filter(|(idx, _)| {
                *idx < keep_head || (keep_tail > 0 && *idx >= total.saturating_sub(keep_tail))
            })
            .map(|(_, msg)| msg)
            .collect();
        let count = selected.len();
        for (new_seq, mut msg) in selected.into_iter().enumerate() {
            msg.thread_id = dst_thread.to_string();
            msg.seq = (new_seq + 1) as u64;
            self.append_message(&msg).await?;
        }
        Ok(count)
    }
}

impl Storage {
    pub async fn upsert_channel(&self, row: &ChannelRow) -> Result<()> {
        validate_channel_name(&row.name)?;
        let visibility = normalize_visibility(&row.visibility)?;
        let namespace = if row.namespace.is_empty() {
            row.name.clone()
        } else {
            row.namespace.clone()
        };
        let contexts = if row.context_namespaces.is_empty() {
            vec![namespace.clone()]
        } else {
            row.context_namespaces.clone()
        };
        let encoded = serde_json::to_string(&contexts)?;
        let updated = if row.updated_at_ms > 0 {
            row.updated_at_ms
        } else {
            now_ms()
        };
        let mut tx = self.pool.begin().await?;
        let del = self.q("DELETE FROM channels WHERE name = $1");
        sqlx::query(&del).bind(&row.name).execute(&mut *tx).await?;
        let ins = self.q(
            "INSERT INTO channels (name, visibility, namespace, origin_node, context_namespaces, updated_at_ms)
             VALUES ($1, $2, $3, $4, $5, $6)",
        );
        sqlx::query(&ins)
            .bind(&row.name)
            .bind(visibility)
            .bind(&namespace)
            .bind(&row.origin_node)
            .bind(&encoded)
            .bind(updated)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_channel(&self, name: &str) -> Result<Option<ChannelRow>> {
        let sql = self.q(
            "SELECT name, visibility, namespace, origin_node, context_namespaces, updated_at_ms
             FROM channels WHERE name = $1",
        );
        let row = sqlx::query(&sql)
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| self.channel_from_row(&r)))
    }

    fn channel_from_row(&self, r: &sqlx::any::AnyRow) -> ChannelRow {
        let namespace: String = r.get("namespace");
        let raw: String = r.get("context_namespaces");
        ChannelRow {
            name: r.get("name"),
            visibility: r.get("visibility"),
            origin_node: r.get("origin_node"),
            updated_at_ms: r.get("updated_at_ms"),
            context_namespaces: decode_namespaces(&raw, &namespace),
            namespace,
        }
    }

    /// Insert a channel only when that name is not already stored.
    pub async fn ensure_channel(&self, name: &str, visibility: &str, origin: &str) -> Result<()> {
        if self.get_channel(name).await?.is_some() {
            return Ok(());
        }
        self.upsert_channel(&ChannelRow {
            name: name.to_string(),
            visibility: visibility.to_string(),
            namespace: name.to_string(),
            origin_node: origin.to_string(),
            context_namespaces: vec![name.to_string()],
            updated_at_ms: 0,
        })
        .await
    }

    pub async fn list_channel_rows(&self) -> Result<Vec<ChannelRow>> {
        let rows = sqlx::query(
            "SELECT name, visibility, namespace, origin_node, context_namespaces, updated_at_ms
             FROM channels ORDER BY name ASC",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(|r| self.channel_from_row(r)).collect())
    }

    pub async fn list_public_channels(&self) -> Result<Vec<ChannelRow>> {
        Ok(self
            .list_channel_rows()
            .await?
            .into_iter()
            .filter(|c| c.visibility == "public")
            .collect())
    }

    pub async fn load_channel_messages(&self, channel: &str) -> Result<Vec<pb::SessionMessage>> {
        self.load_messages(Some(channel), true).await
    }

    pub async fn max_seq(&self, channel: &str) -> Result<i64> {
        let sql =
            self.q("SELECT COALESCE(MAX(seq), 0) AS s FROM session_messages WHERE channel_id = $1");
        let row = sqlx::query(&sql)
            .bind(channel)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.try_get("s").unwrap_or(0))
    }

    pub async fn get_pref(&self, name: &str) -> Result<ChannelPref> {
        let sql = self.q(
            "SELECT channel_name, pinned, muted, archived, closed, last_read_seq, use_count
             FROM channel_prefs WHERE channel_name = $1",
        );
        if let Some(row) = sqlx::query(&sql)
            .bind(name)
            .fetch_optional(&self.pool)
            .await?
        {
            return Ok(pref_from_row(&row));
        }
        Ok(ChannelPref::bare(name))
    }

    pub async fn save_pref(&self, pref: &ChannelPref) -> Result<()> {
        if pref.channel_name == "general" && (pref.archived || pref.closed) {
            bail!("general cannot be archived or closed");
        }
        let sql = self.q("DELETE FROM channel_prefs WHERE channel_name = $1");
        sqlx::query(&sql)
            .bind(&pref.channel_name)
            .execute(&self.pool)
            .await?;
        let sql = self.q(
            "INSERT INTO channel_prefs
             (channel_name, pinned, muted, archived, closed, last_read_seq, use_count, updated_at_ms)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        );
        sqlx::query(&sql)
            .bind(&pref.channel_name)
            .bind(i32::from(pref.pinned))
            .bind(i32::from(pref.muted))
            .bind(i32::from(pref.archived))
            .bind(i32::from(pref.closed))
            .bind(pref.last_read_seq)
            .bind(pref.use_count)
            .bind(now_ms())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn thread_progress(&self, channel: &str, thread: &str) -> Result<(i64, i64)> {
        let total_sql = self.q(
            "SELECT COUNT(*) AS n FROM session_messages s
             WHERE s.channel_id = $1 AND s.thread_id = $2
             AND s.id NOT IN (SELECT id FROM message_meta WHERE deleted != 0)",
        );
        let total: i64 = sqlx::query(&total_sql)
            .bind(channel)
            .bind(thread)
            .fetch_optional(&self.pool)
            .await?
            .map(|row| sql_i64(&row, "n"))
            .unwrap_or(0);
        let read_sql = self.q(
            "SELECT last_read_seq FROM thread_reads WHERE channel_name = $1 AND thread_id = $2",
        );
        let seen: i64 = sqlx::query(&read_sql)
            .bind(channel)
            .bind(thread)
            .fetch_optional(&self.pool)
            .await?
            .map(|row| sql_i64(&row, "last_read_seq"))
            .unwrap_or(0);
        let unread_sql = self.q(
            "SELECT COUNT(*) AS n FROM session_messages s
             WHERE s.channel_id = $1 AND s.thread_id = $2 AND s.seq > $3
             AND s.id NOT IN (SELECT id FROM message_meta WHERE deleted != 0)",
        );
        let unread: i64 = sqlx::query(&unread_sql)
            .bind(channel)
            .bind(thread)
            .bind(seen)
            .fetch_optional(&self.pool)
            .await?
            .map(|row| sql_i64(&row, "n"))
            .unwrap_or(0);
        let _ = total;
        Ok((total, unread.max(0)))
    }

    pub async fn mark_thread_read(&self, channel: &str, thread: &str) -> Result<i64> {
        let max_sql = self.q(
            "SELECT COALESCE(MAX(seq), 0) AS n FROM session_messages WHERE channel_id = $1 AND thread_id = $2",
        );
        let max_seq: i64 = sqlx::query(&max_sql)
            .bind(channel)
            .bind(thread)
            .fetch_optional(&self.pool)
            .await?
            .map(|row| sql_i64(&row, "n"))
            .unwrap_or(0);
        let sql = match self.kind {
            DbKind::Mysql => self.q(
                "INSERT INTO thread_reads (channel_name, thread_id, last_read_seq) VALUES ($1, $2, $3)
                 ON DUPLICATE KEY UPDATE last_read_seq = VALUES(last_read_seq)",
            ),
            _ => self.q(
                "INSERT INTO thread_reads (channel_name, thread_id, last_read_seq) VALUES ($1, $2, $3)
                 ON CONFLICT(channel_name, thread_id) DO UPDATE SET last_read_seq = excluded.last_read_seq",
            ),
        };
        sqlx::query(&sql)
            .bind(channel)
            .bind(thread)
            .bind(max_seq)
            .execute(&self.pool)
            .await?;
        Ok(max_seq)
    }

    pub async fn mark_read(&self, name: &str, seq: i64) -> Result<()> {
        let mut pref = self.get_pref(name).await?;
        if seq > pref.last_read_seq {
            pref.last_read_seq = seq;
        }
        self.save_pref(&pref).await
    }

    pub async fn set_members(
        &self,
        channel: &str,
        members: &[(String, String, String)],
    ) -> Result<()> {
        let sql = self.q("DELETE FROM channel_members WHERE channel_name = $1");
        sqlx::query(&sql).bind(channel).execute(&self.pool).await?;
        for (node_id, role, state) in members {
            let sql = self.q(
                "INSERT INTO channel_members (channel_name, node_id, role, state) VALUES ($1, $2, $3, $4)",
            );
            sqlx::query(&sql)
                .bind(channel)
                .bind(node_id)
                .bind(role)
                .bind(state)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    pub async fn list_members(&self, channel: &str) -> Result<Vec<String>> {
        let sql =
            self.q("SELECT node_id FROM channel_members WHERE channel_name = $1 ORDER BY node_id");
        let rows = sqlx::query(&sql)
            .bind(channel)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.iter().map(|r| r.get("node_id")).collect())
    }

    pub async fn member_state(&self, channel: &str, node_id: &str) -> Result<Option<String>> {
        let sql =
            self.q("SELECT state FROM channel_members WHERE channel_name = $1 AND node_id = $2");
        let row = sqlx::query(&sql)
            .bind(channel)
            .bind(node_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("state")))
    }

    pub async fn channels_visible_to(&self, node_id: &str) -> Result<Vec<ChannelRow>> {
        let mut rows = self.list_public_channels().await?;
        let sql = self.q(
            "SELECT c.name, c.visibility, c.namespace, c.origin_node, c.context_namespaces, c.updated_at_ms
             FROM channels c
             JOIN channel_members m ON m.channel_name = c.name
             WHERE c.visibility = 'shared' AND m.node_id = $1
             ORDER BY c.name",
        );
        let shared = sqlx::query(&sql)
            .bind(node_id)
            .fetch_all(&self.pool)
            .await?;
        for row in &shared {
            let ch = self.channel_from_row(row);
            if !rows.iter().any(|r| r.name == ch.name) {
                rows.push(ch);
            }
        }
        Ok(rows)
    }

    pub async fn activity(&self) -> Result<Vec<(String, i64)>> {
        let rows = sqlx::query(
            "SELECT channel_id, COALESCE(MAX(seq), 0) AS s FROM session_messages GROUP BY channel_id",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| (r.get("channel_id"), r.try_get("s").unwrap_or(0)))
            .collect())
    }

    pub async fn record_audit(
        &self,
        node_id: &str,
        action: &str,
        channel: &str,
        thread: &str,
        detail: &str,
    ) -> Result<()> {
        let detail: String = detail.chars().take(1500).collect();
        let id = blake3::hash(
            format!(
                "{node_id}:{action}:{channel}:{thread}:{detail}:{}",
                now_ms()
            )
            .as_bytes(),
        )
        .to_hex()
        .to_string();
        let sql = self.q(
            "INSERT INTO audit_events (id, node_id, action, channel_name, thread_id, detail, created_at_ms)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        );
        sqlx::query(&sql)
            .bind(&id)
            .bind(node_id)
            .bind(action)
            .bind(channel)
            .bind(thread)
            .bind(&detail)
            .bind(now_ms())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn list_audit(&self, limit: i64) -> Result<Vec<AuditEvent>> {
        let sql = self.q(
            "SELECT node_id, action, channel_name, thread_id, detail, created_at_ms
             FROM audit_events ORDER BY created_at_ms DESC LIMIT $1",
        );
        let rows = sqlx::query(&sql)
            .bind(limit.clamp(1, 200))
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .iter()
            .map(|r| AuditEvent {
                node_id: r.get("node_id"),
                action: r.get("action"),
                channel: r.get("channel_name"),
                thread: r.get("thread_id"),
                detail: r.get("detail"),
                created_at_ms: r.get("created_at_ms"),
            })
            .collect())
    }

    pub async fn delete_channel(&self, name: &str) -> Result<()> {
        if name == "general" {
            return Ok(());
        }
        let sql = self.q("DELETE FROM channels WHERE name = $1");
        sqlx::query(&sql).bind(name).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn put_invite(&self, channel: &str, token_hash: &str) -> Result<()> {
        let sql = self.q("DELETE FROM channel_invites WHERE channel_name = $1");
        sqlx::query(&sql).bind(channel).execute(&self.pool).await?;
        let sql = self.q(
            "INSERT INTO channel_invites (channel_name, token_hash, created_at_ms) VALUES ($1, $2, $3)",
        );
        sqlx::query(&sql)
            .bind(channel)
            .bind(token_hash)
            .bind(now_ms())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn add_invite(&self, channel: &str, token_hash: &str) -> Result<()> {
        let sql = self.q(
            "INSERT INTO invite_tokens (token_hash, channel_name, created_at_ms) VALUES ($1, $2, $3)",
        );
        sqlx::query(&sql)
            .bind(token_hash)
            .bind(channel)
            .bind(now_ms())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn invite_matches(&self, channel: &str, token_hash: &str) -> Result<bool> {
        let sql = self.q(
            "SELECT token_hash FROM channel_invites WHERE channel_name = $1",
        );
        let row = sqlx::query(&sql)
            .bind(channel)
            .fetch_optional(&self.pool)
            .await?;
        if row
            .map(|r| r.get::<String, _>("token_hash") == token_hash)
            .unwrap_or(false)
        {
            return Ok(true);
        }
        let sql = self.q(
            "SELECT token_hash FROM invite_tokens WHERE channel_name = $1 AND token_hash = $2",
        );
        let extra = sqlx::query(&sql)
            .bind(channel)
            .bind(token_hash)
            .fetch_optional(&self.pool)
            .await?;
        Ok(extra.is_some())
    }

    pub async fn message_detail(&self, channel: &str, thread: &str, seq: u64) -> Result<String> {
        let id = message_id(channel, thread, seq);
        let sql = self.q("SELECT detail FROM message_meta WHERE id = $1");
        let row = sqlx::query(&sql)
            .bind(&id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.get("detail")).unwrap_or_default())
    }

    pub async fn merge_message_detail(
        &self,
        channel: &str,
        thread: &str,
        seq: u64,
        patch: serde_json::Value,
    ) -> Result<()> {
        let current = self.message_detail(channel, thread, seq).await?;
        let mut base = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&current)
            .unwrap_or_default();
        if let Some(obj) = patch.as_object() {
            for (key, value) in obj {
                base.insert(key.clone(), value.clone());
            }
        }
        let text = serde_json::Value::Object(base).to_string();
        self.save_message_detail(channel, thread, seq, &text).await
    }

    pub async fn list_preferences(&self, channel: &str) -> Result<Vec<StoredPreference>> {
        let sql = self.q(
            "SELECT id, thread_id, prompt_seq, prompt, chosen_seq, rejected_seq, chosen, rejected, source, exported_at_ms
             FROM preference_pairs WHERE channel_id = $1 ORDER BY prompt_seq ASC",
        );
        let rows = sqlx::query(&sql)
            .bind(channel)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .iter()
            .map(|r| StoredPreference {
                id: r.get("id"),
                thread_id: r.get("thread_id"),
                prompt_seq: r.get("prompt_seq"),
                prompt: r.get("prompt"),
                chosen_seq: r.get("chosen_seq"),
                rejected_seq: r.get("rejected_seq"),
                chosen: r.get("chosen"),
                rejected: r.get("rejected"),
                source: r.get("source"),
                exported_at_ms: r.get("exported_at_ms"),
            })
            .collect())
    }

    pub async fn record_operator_vote(
        &self,
        channel: &str,
        thread: &str,
        seq: u64,
        vote: i32,
        note: &str,
    ) -> Result<()> {
        self.merge_message_detail(
            channel,
            thread,
            seq,
            serde_json::json!({
                "feedback": { "operator_vote": vote, "operator_note": note }
            }),
        )
        .await?;
        let shown = self.load_shown(channel, thread, 2000).await?;
        let turns: Vec<crate::adapt::RatedTurn> = shown
            .iter()
            .map(|m| crate::adapt::RatedTurn {
                seq: m.message.seq,
                human: crate::models::is_human_role(&m.message.author),
                content: m.message.content.clone(),
                vote: m.vote,
            })
            .collect();
        let drafts = crate::adapt::synthesize_pairs(&turns);
        self.replace_operator_pairs(channel, thread, &drafts).await
    }

    async fn replace_operator_pairs(
        &self,
        channel: &str,
        thread: &str,
        drafts: &[crate::adapt::PreferenceDraft],
    ) -> Result<()> {
        let existing = self.list_preferences(channel).await?;
        let keep: Vec<_> = existing
            .into_iter()
            .filter(|row| row.thread_id == thread && row.source == "operator_vote")
            .collect();
        let del = self.q(
            "DELETE FROM preference_pairs WHERE channel_id = $1 AND thread_id = $2 AND source = $3",
        );
        sqlx::query(&del)
            .bind(channel)
            .bind(thread)
            .bind("operator_vote")
            .execute(&self.pool)
            .await?;
        for draft in drafts {
            let id = crate::adapt::pair_id(channel, thread, draft.prompt_seq);
            let exported = keep
                .iter()
                .find(|row| {
                    row.id == id
                        && row.chosen_seq == draft.chosen_seq as i64
                        && row.rejected_seq == draft.rejected_seq as i64
                })
                .map(|row| row.exported_at_ms)
                .unwrap_or(0);
            let sql = self.q(
                "INSERT INTO preference_pairs
                 (id, channel_id, thread_id, prompt_seq, prompt, chosen_seq, rejected_seq, chosen, rejected, source, exported_at_ms, created_at_ms)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
            );
            sqlx::query(&sql)
                .bind(&id)
                .bind(channel)
                .bind(thread)
                .bind(draft.prompt_seq as i64)
                .bind(&draft.prompt)
                .bind(draft.chosen_seq as i64)
                .bind(draft.rejected_seq as i64)
                .bind(&draft.chosen)
                .bind(&draft.rejected)
                .bind("operator_vote")
                .bind(exported)
                .bind(now_ms())
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    /// JSONL of every pair on this channel. Rows that have not been exported
    /// are stamped. A later export of the same votes keeps the old stamp.
    pub async fn export_preferences_jsonl(&self, channel: &str) -> Result<(String, i64)> {
        let rows = self.list_preferences(channel).await?;
        let mut lines = String::new();
        for row in &rows {
            if !lines.is_empty() {
                lines.push('\n');
            }
            lines.push_str(&crate::adapt::preference_line(
                channel,
                &row.thread_id,
                &row.prompt,
                &row.chosen,
                &row.rejected,
            ));
        }
        if rows.iter().any(|row| row.exported_at_ms == 0) {
            let sql = self.q(
                "UPDATE preference_pairs SET exported_at_ms = $1 WHERE channel_id = $2 AND exported_at_ms = 0",
            );
            sqlx::query(&sql)
                .bind(now_ms())
                .bind(channel)
                .execute(&self.pool)
                .await?;
        }
        Ok((lines, rows.len() as i64))
    }

    pub async fn namespace_is_public(&self, namespace: &str) -> Result<bool> {
        Ok(self
            .list_channel_rows()
            .await?
            .into_iter()
            .any(|c| c.visibility == "public" && (c.namespace == namespace || c.name == namespace)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_placeholders_are_question_marks() {
        assert_eq!(
            rewrite_placeholders(DbKind::Mysql, "SELECT $1, $10, $2"),
            "SELECT ?, ?, ?"
        );
        assert_eq!(
            rewrite_placeholders(DbKind::Sqlite, "SELECT $1"),
            "SELECT $1"
        );
        assert_eq!(
            rewrite_placeholders(DbKind::Postgres, "SELECT $2"),
            "SELECT $2"
        );
    }

    #[test]
    fn names_and_visibility() {
        assert!(validate_channel_name("proj/api").is_ok());
        assert!(validate_channel_name("has space").is_ok());
        assert!(validate_channel_name("a/../b").is_err());
        assert_eq!(normalize_visibility("Open").unwrap(), "public");
        assert_eq!(normalize_visibility("").unwrap(), "private");
        assert_eq!(normalize_visibility("shared").unwrap(), "shared");
    }

    #[test]
    fn sidebar_puts_general_first_then_pins_then_use() {
        let mut rows = vec![
            SidebarKey {
                group: channel_group("zeta", "public"),
                pinned: false,
                use_count: 1,
                name: "zeta".into(),
            },
            SidebarKey {
                group: channel_group("general", "public"),
                pinned: false,
                use_count: 0,
                name: "general".into(),
            },
            SidebarKey {
                group: channel_group("alpha", "public"),
                pinned: true,
                use_count: 0,
                name: "alpha".into(),
            },
            SidebarKey {
                group: channel_group("mu", "public"),
                pinned: true,
                use_count: 9,
                name: "mu".into(),
            },
            SidebarKey {
                group: channel_group("mid", "public"),
                pinned: false,
                use_count: 5,
                name: "mid".into(),
            },
            SidebarKey {
                group: channel_group("local", "private"),
                pinned: false,
                use_count: 100,
                name: "local".into(),
            },
            SidebarKey {
                group: channel_group("team", "shared"),
                pinned: false,
                use_count: 3,
                name: "team".into(),
            },
        ];
        rows.sort_by(cmp_sidebar);
        let names: Vec<_> = rows.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(
            names,
            ["general", "alpha", "mu", "mid", "zeta", "local", "team"]
        );
    }

    #[tokio::test]
    async fn prefs_stay_on_this_node_and_general_cannot_close() {
        let db = temp_db().await;
        let mut pref = ChannelPref::bare("demo");
        pref.pinned = true;
        pref.muted = true;
        db.save_pref(&pref).await.unwrap();
        let loaded = db.get_pref("demo").await.unwrap();
        assert!(loaded.pinned && loaded.muted);
        db.mark_read("demo", 4).await.unwrap();
        assert_eq!(db.get_pref("demo").await.unwrap().last_read_seq, 4);
        let mut general = ChannelPref::bare("general");
        general.closed = true;
        assert!(db.save_pref(&general).await.is_err());
        db.record_audit("node-a", "chat.request", "general", "main", "hello")
            .await
            .unwrap();
        assert_eq!(db.list_audit(10).await.unwrap().len(), 1);
        db.set_members(
            "demo",
            &[("node-b".into(), "member".into(), "active".into())],
        )
        .await
        .unwrap();
        assert_eq!(
            db.list_members("demo").await.unwrap(),
            vec!["node-b".to_string()]
        );
    }

    async fn temp_db() -> Storage {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "xmesh-db-{}-{}-{}.db",
            std::process::id(),
            now_ms(),
            n
        ));
        let _ = std::fs::remove_file(&path);
        Storage::connect(None, path.to_str().unwrap())
            .await
            .unwrap()
    }

    fn entry(id: &str, hash: &str, kind: i32) -> pb::ContextEntry {
        pb::ContextEntry {
            id: id.into(),
            namespace: "demo".into(),
            entry_type: kind,
            source_path: "src/lib.rs".into(),
            content: "fn demo() {}".into(),
            content_hash: hash.into(),
            embedding: vec![1.0, 0.0],
            updated_at_ms: 1,
            origin_node: "node-a".into(),
            meta: Default::default(),
        }
    }

    #[tokio::test]
    async fn sqlite_roundtrip_covers_pins_channels_and_private_forks() {
        let db = temp_db().await;
        let e = entry("abc", "h1", pb::EntryType::Dynamic as i32);
        assert!(db.upsert_proto(&e, false).await.unwrap());
        assert!(!db.upsert_proto(&e, false).await.unwrap());
        assert!(db.is_unchanged("abc", "h1").await.unwrap());

        let mut pinned = entry("pin1", "p1", pb::EntryType::Pinned as i32);
        pinned.content = "keep tests".into();
        assert!(db.upsert_proto(&pinned, true).await.unwrap());
        pinned.content_hash = "p2".into();
        pinned.content = "changed".into();
        assert!(!db.upsert_proto(&pinned, false).await.unwrap());

        let loaded = db.load_entries(Some("demo")).await.unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(
            loaded.iter().find(|e| e.id == "abc").unwrap().embedding,
            vec![1.0, 0.0]
        );

        db.upsert_channel(&ChannelRow {
            name: "demo".into(),
            visibility: "public".into(),
            namespace: "demo".into(),
            origin_node: "hub".into(),
            context_namespaces: vec!["demo".into()],
            updated_at_ms: 0,
        })
        .await
        .unwrap();
        assert_eq!(db.list_public_channels().await.unwrap().len(), 1);

        let stored = db
            .append_public_message(&pb::SessionMessage {
                channel_id: "demo".into(),
                thread_id: "main".into(),
                seq: 0,
                author: "user".into(),
                content: "hello".into(),
                created_at_ms: 0,
            })
            .await
            .unwrap();
        assert_eq!(stored.seq, 1);
        assert_eq!(
            db.fork_thread("demo", "main", "scratch", 1, 0)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            db.thread_visibility("demo", "scratch").await.unwrap(),
            "private"
        );

        let public_env = db
            .export_public_envelope("hub", Some("demo"))
            .await
            .unwrap();
        assert!(public_env.messages.iter().all(|m| m.thread_id != "scratch"));
        assert!(public_env.messages.iter().any(|m| m.thread_id == "main"));
        assert_eq!(public_env.schema_version, 2);
        assert_eq!(public_env.channels.len(), 1);

        let full = db.export_envelope("hub", Some("demo")).await.unwrap();
        assert!(full.messages.iter().any(|m| m.thread_id == "scratch"));
    }

    fn chat(seq: u64, author: &str, content: &str) -> pb::SessionMessage {
        pb::SessionMessage {
            channel_id: "demo".into(),
            thread_id: "main".into(),
            seq,
            author: author.into(),
            content: content.into(),
            created_at_ms: 1,
        }
    }

    #[tokio::test]
    async fn an_operator_vote_pair_exports_once_and_a_note_can_be_dropped() {
        let db = temp_db().await;
        db.append_message(&chat(1, "ada", "fix the parser"))
            .await
            .unwrap();
        db.append_message(&chat(2, "local/coder", "bad diff"))
            .await
            .unwrap();
        db.append_message(&chat(3, "local/coder", "good diff"))
            .await
            .unwrap();
        db.save_message_detail("demo", "main", 2, r#"{"started_at_ms":5}"#)
            .await
            .unwrap();
        db.record_operator_vote("demo", "main", 2, -1, "")
            .await
            .unwrap();
        assert!(db.list_preferences("demo").await.unwrap().is_empty());
        let shown = db.load_shown("demo", "main", 20).await.unwrap();
        assert_eq!(shown[1].vote, -1);
        assert_eq!(shown[1].started_at_ms, 5);
        db.record_operator_vote("demo", "main", 3, 1, "keep the match")
            .await
            .unwrap();
        let rows = db.list_preferences("demo").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].chosen, "good diff");
        assert_eq!(rows[0].rejected, "bad diff");
        assert_eq!(rows[0].exported_at_ms, 0);
        let (jsonl, n) = db.export_preferences_jsonl("demo").await.unwrap();
        assert_eq!(n, 1);
        assert!(jsonl.contains("good diff"));
        let stamped = db.list_preferences("demo").await.unwrap()[0].exported_at_ms;
        assert!(stamped > 0);
        db.record_operator_vote("demo", "main", 3, 1, "keep the match")
            .await
            .unwrap();
        assert_eq!(
            db.list_preferences("demo").await.unwrap()[0].exported_at_ms,
            stamped
        );
        db.put_entry_score("note", 1).await.unwrap();
        db.bump_hits(&["note".into()]).await.unwrap();
        assert_eq!(db.entry_score("note").await.unwrap(), Some(1));
        assert_eq!(db.entry_scores().await.unwrap().get("note"), Some(&1));
    }
}
