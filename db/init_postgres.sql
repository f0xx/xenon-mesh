-- psql -U postgres -f init_postgres.sql

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'xmesh') THEN
        CREATE ROLE xmesh WITH LOGIN PASSWORD 'labpass';
    END IF;
END
$$;

SELECT 'CREATE DATABASE xmesh_lab OWNER xmesh ENCODING ''UTF8'''
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'xmesh_lab')\gexec

\c xmesh_lab

GRANT ALL PRIVILEGES ON DATABASE xmesh_lab TO xmesh;
GRANT ALL ON SCHEMA public TO xmesh;

SET ROLE xmesh;

CREATE TABLE IF NOT EXISTS context_entries (
    id VARCHAR(64) PRIMARY KEY,
    namespace VARCHAR(128) NOT NULL,
    entry_type INTEGER NOT NULL,
    source_path TEXT NOT NULL,
    content TEXT NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    embedding BYTEA NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    origin_node VARCHAR(128) NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ns_type ON context_entries(namespace, entry_type);

CREATE TABLE IF NOT EXISTS session_messages (
    id VARCHAR(64) PRIMARY KEY,
    channel_id VARCHAR(128) NOT NULL,
    thread_id VARCHAR(128) NOT NULL,
    seq BIGINT NOT NULL,
    author VARCHAR(64) NOT NULL,
    content TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_msg_thread ON session_messages(channel_id, thread_id, seq);
