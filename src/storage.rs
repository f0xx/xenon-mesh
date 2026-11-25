use anyhow::{Context, Result};
use sqlx::{any::AnyPoolOptions, AnyPool, Row};
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

impl Storage {
    pub async fn connect(primary: Option<&str>, sqlite_fallback: &str) -> Result<Self> {
        sqlx::any::install_default_drivers();

        if let Some(url) = primary {
            if let Ok(pool) = AnyPoolOptions::new().max_connections(10).connect(url).await {
                let kind = if url.starts_with("postgres") {
                    DbKind::Postgres
                } else if url.starts_with("mysql") || url.starts_with("mariadb") {
                    DbKind::Mysql
                } else {
                    DbKind::Sqlite
                };
                let st = Self { pool, kind };
                st.init_schema().await?;
                info!("Storage initialized on primary backend: {:?}", kind);
                return Ok(st);
            }
            warn!("Primary DB unavailable, falling back to SQLite: {sqlite_fallback}");
        }

        let url = format!("sqlite://{sqlite_fallback}?mode=rwc");
        let pool = AnyPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await
            .context("Failed to initialize fallback SQLite")?;
        let st = Self { pool, kind: DbKind::Sqlite };
        st.init_schema().await?;
        info!("Storage initialized on fallback SQLite");
        Ok(st)
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
            );"
        );
        sqlx::query(&ddl).execute(&self.pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_ns_type ON context_entries(namespace, entry_type);")
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}


impl Storage {
    /// Записывает Proto-запись в текущую СУБД. Возвращает true, если запись обновлена.
    pub async fn upsert_proto(&self, entry: &pb::ContextEntry, force: bool) -> Result<bool> {
        if !force {
            let row = sqlx::query("SELECT entry_type, content_hash FROM context_entries WHERE id = $1")
                .bind(&entry.id)
                .fetch_optional(&self.pool)
                .await?;

            if let Some(r) = row {
                let etype: i32 = r.get("entry_type");
                let hash: String = r.get("content_hash");
                // Не перезаписываем PINNED-контексты и неизмененные чанки
                if etype == pb::EntryType::Pinned as i32 || hash == entry.content_hash {
                    return Ok(false);
                }
            }
        }

        let emb_bytes: &[u8] = bytemuck::cast_slice(&entry.embedding);
        let mut tx = self.pool.begin().await?;

        sqlx::query("DELETE FROM context_entries WHERE id = $1")
            .bind(&entry.id)
            .execute(&mut *tx)
            .await?;

        sqlx::query(
            "INSERT INTO context_entries
             (id, namespace, entry_type, source_path, content, content_hash, embedding, updated_at_ms, origin_node)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
        )
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

    /// Экспорт данных в версионный Protobuf-конверт для сети или миграции между СУБД
    pub async fn export_envelope(&self, node_name: &str, namespace: Option<&str>) -> Result<pb::SyncEnvelope> {
        let rows = if let Some(ns) = namespace {
            sqlx::query("SELECT * FROM context_entries WHERE namespace = $1")
                .bind(ns)
                .fetch_all(&self.pool)
                .await?
        } else {
            sqlx::query("SELECT * FROM context_entries").fetch_all(&self.pool).await?
        };

        let mut entries = Vec::with_capacity(rows.len());
        for r in rows {
            let raw_emb: Vec<u8> = r.get("embedding");
            let embedding: Vec<f32> = bytemuck::pod_collect_to_vec(&raw_emb);
            entries.push(pb::ContextEntry {
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
            });
        }

        Ok(pb::SyncEnvelope {
            schema_version: 1,
            source_node: node_name.to_string(),
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as i64,
            entries,
            messages: if namespace.is_none() { self.export_all_messages().await? } else { vec![] },
        })
    }
}




impl Storage {
    /// Вызывать внутри init_schema() вслед за созданием context_entries
    pub async fn init_messages_schema(&self) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS session_messages (
                id VARCHAR(64) PRIMARY KEY,
                channel_id VARCHAR(128) NOT NULL,
                thread_id VARCHAR(128) NOT NULL,
                seq BIGINT NOT NULL,
                author VARCHAR(64) NOT NULL,
                content TEXT NOT NULL,
                created_at_ms BIGINT NOT NULL
            );"
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_msg_thread ON session_messages(channel_id, thread_id, seq);")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Сохраняет одно сообщение треда в БД (совместимо с SQLite, Postgres и MariaDB)
    pub async fn append_message(&self, msg: &pb::SessionMessage) -> Result<()> {
        let ts = if msg.created_at_ms > 0 {
            msg.created_at_ms
        } else {
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as i64
        };
        let id = blake3::hash(format!("{}:{}:{}", msg.channel_id, msg.thread_id, msg.seq).as_bytes())
            .to_hex()
            .to_string();

        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM session_messages WHERE id = $1").bind(&id).execute(&mut *tx).await?;
        sqlx::query(
            "INSERT INTO session_messages (id, channel_id, thread_id, seq, author, content, created_at_ms)
             VALUES ($1, $2, $3, $4, $5, $6, $7)"
        )
        .bind(&id).bind(&msg.channel_id).bind(&msg.thread_id).bind(msg.seq as i64)
        .bind(&msg.author).bind(&msg.content).bind(ts)
        .execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn export_all_messages(&self) -> Result<Vec<pb::SessionMessage>> {
        let rows = sqlx::query("SELECT channel_id, thread_id, seq, author, content, created_at_ms FROM session_messages")
            .fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(|r| {
            let seq: i64 = r.get("seq");
            pb::SessionMessage {
                channel_id: r.get("channel_id"), thread_id: r.get("thread_id"), seq: seq as u64,
                author: r.get("author"), content: r.get("content"), created_at_ms: r.get("created_at_ms"),
            }
        }).collect())
    }

    /// Загружает последние N сообщений треда в хронологическом порядке
    pub async fn load_thread(&self, channel_id: &str, thread_id: &str, limit: i64) -> Result<Vec<pb::SessionMessage>> {
        let rows = sqlx::query(
            "SELECT channel_id, thread_id, seq, author, content, created_at_ms
             FROM session_messages
             WHERE channel_id = $1 AND thread_id = $2
             ORDER BY seq ASC LIMIT $3"
        )
        .bind(channel_id)
        .bind(thread_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let seq: i64 = r.get("seq");
            out.push(pb::SessionMessage {
                channel_id: r.get("channel_id"),
                thread_id: r.get("thread_id"),
                seq: seq as u64,
                author: r.get("author"),
                content: r.get("content"),
                created_at_ms: r.get("created_at_ms"),
            });
        }
        Ok(out)
    }
}


impl Storage {
    pub async fn list_threads(&self, channel_id: &str) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT DISTINCT thread_id FROM session_messages WHERE channel_id = $1 ORDER BY thread_id ASC"
        )
        .bind(channel_id)
        .fetch_all(&self.pool)
        .await?;

        let mut threads: Vec<String> = rows.into_iter().map(|r| r.get("thread_id")).collect();
        if !threads.contains(&"main".to_string()) {
            threads.insert(0, "main".to_string());
        }
        Ok(threads)
    }
}

