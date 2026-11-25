-- mysql -u root < init_mysql.sql

CREATE DATABASE IF NOT EXISTS xmesh_lab
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'xmesh'@'127.0.0.1' IDENTIFIED BY 'labpass';
CREATE USER IF NOT EXISTS 'xmesh'@'localhost' IDENTIFIED BY 'labpass';

GRANT ALL PRIVILEGES ON xmesh_lab.* TO 'xmesh'@'127.0.0.1';
GRANT ALL PRIVILEGES ON xmesh_lab.* TO 'xmesh'@'localhost';
FLUSH PRIVILEGES;

USE xmesh_lab;

CREATE TABLE IF NOT EXISTS context_entries (
    id VARCHAR(64) PRIMARY KEY,
    namespace VARCHAR(128) NOT NULL,
    entry_type INTEGER NOT NULL,
    source_path TEXT NOT NULL,
    content TEXT NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    embedding LONGBLOB NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    origin_node VARCHAR(128) NOT NULL,
    INDEX idx_ns_type (namespace, entry_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS session_messages (
    id VARCHAR(64) PRIMARY KEY,
    channel_id VARCHAR(128) NOT NULL,
    thread_id VARCHAR(128) NOT NULL,
    seq BIGINT NOT NULL,
    author VARCHAR(64) NOT NULL,
    content TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    INDEX idx_msg_thread (channel_id, thread_id, seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

