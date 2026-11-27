-- Schema only. Create the account from the environment with db/init_mysql.sh.
-- Apply this file after USE of the target database.

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
    author TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    INDEX idx_msg_thread (channel_id, thread_id, seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS channels (
    name VARCHAR(128) PRIMARY KEY,
    visibility VARCHAR(16) NOT NULL,
    namespace VARCHAR(128) NOT NULL,
    origin_node VARCHAR(128) NOT NULL,
    context_namespaces TEXT NOT NULL,
    updated_at_ms BIGINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS preference_pairs (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE INDEX IF NOT EXISTS idx_pref_channel ON preference_pairs(channel_id, exported_at_ms);

CREATE TABLE IF NOT EXISTS entry_stats (
    id VARCHAR(64) PRIMARY KEY,
    score INTEGER NOT NULL,
    hits INTEGER NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS threads (
    channel_id VARCHAR(128) NOT NULL,
    thread_id VARCHAR(128) NOT NULL,
    visibility VARCHAR(16) NOT NULL,
    PRIMARY KEY (channel_id, thread_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
