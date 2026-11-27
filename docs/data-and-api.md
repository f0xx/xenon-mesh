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
| `GET` | `/api/channels` | Sidebar rows. Group, pin, mute, archive, close, read cursor, and unread count are per node. On a hub, each indexed bare repo is a public channel. On a node, each directory under `git.project_roots` is a channel, plus hub channels this node is allowed to see |
| `POST` | `/api/channel-state` | Update that node's pin, mute, archive, close, or read cursor. `# general` rejects archive and close |
| `POST` | `/api/presence` | Which channel this node's UI is looking at. A model reply is unread when the UI is elsewhere or the page is hidden |
| `POST` | `/api/share` | Invite node ids into a private channel. Returns an invitation token once. The hub stores the token hash, the transcript, and the membership |
| `POST` | `/api/join` | `{channel, token}`. The hub adds this node when the token matches or the node is already a member |
| `GET` | `/api/audit` | Hub log of public and shared activity. A node proxies this from the hub |
| `POST` | `/api/internal/complete` | One streamed completion. Used by nodes so the hub can reach every configured host. Each call is independent |
| `POST` | `/api/channels` | `{name, visibility, namespace, context_namespaces}`. `public` is pushed to the hub |
| `GET` | `/api/threads?channel=` | Thread ids. `main` is always present |
| `POST` | `/api/threads/fork` | Copies head and tail into a new private thread |
| `GET` | `/api/history?channel=&thread=` | Pulls a public thread from the hub, then returns local rows. A main-thread row that opened a child includes `reply_thread`, `thread_total`, and `thread_unread` |
| `POST` | `/api/thread-read` | `{channel, thread}` marks that thread read on this node |
| `GET`/`POST` | `/api/agentic` | Per-channel switch. Missing means on. `POST {channel, enabled}` |
| `POST` | `/api/inspect` | `{diff}` returns `{findings, blocks_approve:false}` from the built-in inspector, `git.inspect_command`, and `git.status_command` |
| `POST` | `/api/chat` | SSE. `agentic: false` stores the message and does not call a model. The field defaults to true |
| `GET` | `/api/memory?channel=` | Pins and learned notes |
| `DELETE` | `/api/memory?id=` | Deletes one context row |
| `POST` | `/api/memory/learn` | `{channel, title, text, pin}`. `pin: false` asks the model to distill first |
| `POST` | `/api/task/approve` | Signs and forwards a diff. See [architecture.md](architecture.md) |
| `POST` | `/api/internal/reindex?namespace=` | Re-reads one bare repo. The `post-receive` hook calls this on the hub |
| `POST` | `/v1/chat/completions` | OpenAI-shaped chat. `model` may be `channel:<namespace>` to select context. The reply uses the configured default model |

Channel names are 1 to 128 characters from `[A-Za-z0-9_.:/-]`.

## DR-26. UI bindings

The page loads `/api/config` and fills the model select from `models[].id`, labeled `model : host`, grouped by host. Proxied models, including Gemini, are a group under the hub. Strings are still accepted so an older response shape does not blank the list. Enter sends. Shift+Enter inserts a newline. The composer textarea is not named `prompt`, so it does not replace `window.prompt`. Code fences show the fence language, not the UI language. A `diff` fence, including one that arrives at the end of a stream, adds an inspect button. The side panel then asks `/api/inspect` and shows the findings above the diff. History draws a day divider and a clock without seconds. The stored value stays `created_at_ms`. New channels ask for `private` or `public`. Public names are prefixed with `#`, private names with `·`.
