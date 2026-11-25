# Data and API

Schema version on the wire is 2. Protobuf field numbers only grow, so a version 1 dump still imports. New tables are created with `CREATE TABLE IF NOT EXISTS` on startup. On PostgreSQL and MariaDB/MySQL the daemon also widens `session_messages.author` to `TEXT` so a `provider/model` id fits.

## DR-23. Tables

`context_entries` holds source chunks, pins, and learned notes.

| Column | Notes |
| --- | --- |
| `id` | BLAKE3 hex, primary key |
| `namespace` | Channel or repository name |
| `entry_type` | 1 dynamic, 2 pinned, 3 learned. Pinned rows are not replaced unless the write sets force |
| `embedding` | Raw little-endian `f32` bytes. Empty when embeddings are off |
| `origin_node` | `node.id` of the writer |

`session_messages` holds chat turns. `threads.visibility` is `private` or `inherit`. A missing row means inherit. Public exports and hub sync skip `private` threads.

`channels` holds `name`, `visibility` (`public` or `private`), `namespace`, `origin_node`, `context_namespaces` (JSON array), and `updated_at_ms`.

MySQL statements use `?` placeholders. PostgreSQL and SQLite use `$1`. The rewrite is covered by a unit test; the SQLite path is covered by a round-trip test.

Files:

- `db/init_postgres.sql`, `db/init_mysql.sql`, `db/init_sqlite.sh` — schema
- `db/init_postgres.sh`, `db/init_mysql.sh` — role creation from `XMESH_DB_USER`, `XMESH_DB_PASSWORD`, `XMESH_DB_NAME`

`xmeshd export <db_url> <file.pb.zst>` writes a zstd protobuf envelope, including private rows. That file is a backup of one machine. `xmeshd import` loads it. `xmeshd migrate <from_url> <to_url>` copies entries, messages, and channels between two URLs.

## DR-24. gRPC `xmeshd.v1.ClusterService`

| RPC | Behavior |
| --- | --- |
| `SyncContexts` | Upserts entries. Applies messages unless the thread is private. Applies channel records only when `visibility` is `public` |
| `QueryNodeContext` | Public envelope for one namespace: entries, non-private messages, public channel rows |
| `ListChannels` | Public channels only |
| `AppendMessage` | Stores one public message. Sequence 0 is assigned. Rejects a private thread |
| `ExecuteTask` | Verifies HMAC and expiry, then apply and optional pipeline |

## DR-25. HTTP

The UI is `GET /`. JSON routes:

| Method | Path | Body or query |
| --- | --- | --- |
| `GET` | `/api/health` | `{ok, mode, embeddings}` |
| `GET` | `/api/config` | `{default_model, models[], mode, node_id, embeddings}` |
| `GET` | `/api/status` | Index progress. `embeddings: false` means lexical retrieval |
| `GET` | `/api/channels` | Channel objects. Includes hub public channels on a node, plus `general`. On a hub, each indexed bare repo is a public channel. On a node, each directory under `git.project_roots` is a channel |
| `POST` | `/api/internal/complete` | One streamed completion. Used by nodes so the hub can reach every configured host. Each call is independent |
| `POST` | `/api/channels` | `{name, visibility, namespace, context_namespaces}`. `public` is pushed to the hub |
| `GET` | `/api/threads?channel=` | Thread ids. `main` is always present |
| `POST` | `/api/threads/fork` | Copies head and tail into a new private thread |
| `GET` | `/api/history?channel=&thread=` | Pulls a public thread from the hub, then returns local rows |
| `POST` | `/api/chat` | SSE. Events are JSON with `type` of `status`, `thought`, `delta`, or `done` |
| `GET` | `/api/memory?channel=` | Pins and learned notes |
| `DELETE` | `/api/memory?id=` | Deletes one context row |
| `POST` | `/api/memory/learn` | `{channel, title, text, pin}`. `pin: false` asks the model to distill first |
| `POST` | `/api/task/approve` | Signs and forwards a diff. See [architecture.md](architecture.md) |
| `POST` | `/api/internal/reindex?namespace=` | Re-reads one bare repo. The `post-receive` hook calls this on the hub |
| `POST` | `/v1/chat/completions` | OpenAI-shaped chat. `model` may be `channel:<namespace>` to select context. The reply uses the configured default model |

Channel names are 1 to 128 characters from `[A-Za-z0-9_.:/-]`.

## DR-26. UI bindings

The page loads `/api/config` and fills the model select from `models[].id`, labeled `model · provider`. Strings are still accepted so an older response shape does not blank the list. Ctrl/Cmd+Enter sends. The composer textarea is not named `prompt`, so it does not replace `window.prompt`. Code fences show the fence language, not the UI language. A `diff` fence, including one that arrives at the end of a stream, adds an inspect button. New channels ask for `private` or `public`. Public names are prefixed with `#`, private names with `·`.
