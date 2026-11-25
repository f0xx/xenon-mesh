#!/bin/sh
set -eu

DB_PATH="${1:-/var/lib/xmeshd/cache.db}"
mkdir -p "$(dirname "$DB_PATH")"
if id xmeshd >/dev/null 2>&1; then
    chown xmeshd:xmeshd "$(dirname "$DB_PATH")" || true
fi

sqlite3 "$DB_PATH" <<'EOF'
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS context_entries (
    id VARCHAR(64) PRIMARY KEY,
    namespace VARCHAR(128) NOT NULL,
    entry_type INTEGER NOT NULL,
    source_path TEXT NOT NULL,
    content TEXT NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    embedding BLOB NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    origin_node VARCHAR(128) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ns_type ON context_entries(namespace, entry_type);

CREATE TABLE IF NOT EXISTS session_messages (
    id VARCHAR(64) PRIMARY KEY,
    channel_id VARCHAR(128) NOT NULL,
    thread_id VARCHAR(128) NOT NULL,
    seq BIGINT NOT NULL,
    author TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_msg_thread ON session_messages(channel_id, thread_id, seq);

CREATE TABLE IF NOT EXISTS channels (
    name VARCHAR(128) PRIMARY KEY,
    visibility VARCHAR(16) NOT NULL,
    namespace VARCHAR(128) NOT NULL,
    origin_node VARCHAR(128) NOT NULL,
    context_namespaces TEXT NOT NULL,
    updated_at_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS threads (
    channel_id VARCHAR(128) NOT NULL,
    thread_id VARCHAR(128) NOT NULL,
    visibility VARCHAR(16) NOT NULL,
    PRIMARY KEY (channel_id, thread_id)
);
EOF
