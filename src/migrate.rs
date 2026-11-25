use crate::storage::{pb::SyncEnvelope, Storage};
use anyhow::{Context, Result};
use prost::Message;
use std::fs;
use tracing::info;

pub async fn export_to_file(db: &Storage, node: &str, ns: Option<&str>, out_path: &str) -> Result<usize> {
    let env = db.export_envelope(node, ns).await?;
    let count = env.entries.len();
    let raw_pb = env.encode_to_vec();
    let compressed = zstd::bulk::compress(&raw_pb, 3)?;
    fs::write(out_path, compressed).context("Failed to write .pb.zst dump")?;
    info!("Exported {count} entries (schema v{}) to {out_path}", env.schema_version);
    Ok(count)
}

pub async fn import_from_file(db: &Storage, in_path: &str, overwrite_pinned: bool) -> Result<usize> {
    let compressed = fs::read(in_path).context("Failed to read .pb.zst dump")?;
    let raw_pb = zstd::bulk::decompress(&compressed, 256 * 1024 * 1024)?; // 256mb hardcoded limit
    let env = SyncEnvelope::decode(raw_pb.as_slice()).context("Corrupted Protobuf stream")?;

    let mut applied = 0;
    for entry in &env.entries {
        if db.upsert_proto(entry, overwrite_pinned).await? {
            applied += 1;
        }
    }
    for msg in &env.messages {
        db.append_message(msg).await?;
    }
    info!("Imported {applied}/{} entries from {in_path}", env.entries.len());
    Ok(applied)
}

pub async fn direct_migrate(from_url: &str, to_url: &str, overwrite_pinned: bool) -> Result<usize> {
    let src = Storage::connect(Some(from_url), "/tmp/unused_src.db").await?;
    let dst = Storage::connect(Some(to_url), "/tmp/unused_dst.db").await?;
    let env = src.export_envelope("migrator", None).await?;

    let mut applied = 0;
    for entry in &env.entries {
        if dst.upsert_proto(entry, overwrite_pinned).await? {
            applied += 1;
        }
    }
    info!("Direct migration complete: {applied} entries moved ({:?} -> {:?})", src.kind, dst.kind);
    Ok(applied)
}

