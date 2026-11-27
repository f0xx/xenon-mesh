# Architecture

```text
 browser  --HTTP-->  node or hub daemon  --gRPC-->  hub daemon
                         |                              |
                     local SQLite                 Postgres / MariaDB / SQLite
                         |                              |
                     working tree                 bare repos under git root
                         |
                     model provider (OpenAI-compatible, Ollama native, or Gemini)
```

Both modes serve the same HTTP UI and the same gRPC `ClusterService`. The mode flag changes what they persist and what they index.

## DR-8. Processes

| Piece | Role |
| --- | --- |
| `xmeshd` gRPC | Context sync, public channel list, public message append, signed task execution |
| `xmeshd` HTTP | Chat UI, history, memory, diff approval, OpenAI-compatible `/v1/chat/completions` |
| Indexer (hub only) | Walks bare repos, skips binaries and files over 64 KiB, stores up to 300 text files per repo |
| Watcher | On `git.watched_repos`, debounces 10 seconds, stores the worktree diff, and uploads it only when that namespace has a public channel. A second watch, on by default, follows `git.project_roots` and `git.bare_repos_root` and updates the channel list |
| Model hub | Discovers models, streams tokens, embeds when the endpoint supports it |

The HTTP listener defaults to `127.0.0.1:8080`. Set `node.http_addr` to `0.0.0.0:8080` when other machines on the LAN should open the UI. gRPC defaults to whatever `node.listen_addr` says (the examples use `0.0.0.0:8443`). `/api/mesh/*` on those UI ports answers only for a localhost peer. Remote mesh uses `node.mesh_tls_addr` with a client certificate, and gRPC uses the same CA when the mutual-TLS files are set. There is no user login on the UI.

## DR-9. Chat flow

1. The UI `POST /api/chat` names a channel, a thread, a model id (`provider/model` or a bare model name), and the user text.
2. The daemon decides whether the thread is public. Public threads on a node are appended through the hub's `AppendMessage` RPC, which assigns the sequence. A private thread, or a hub outage, stays in the local database.
3. Context is assembled from local rows, hub rows for public namespaces, and a bounded scan of a matching watched worktree (200 files, 32 KiB each, no symlink that leaves the root, no `target/`, `.git/`, `node_modules/`, or similar).
4. Ranking uses cosine similarity when the query and the chunk share an embedding. Otherwise it uses token overlap, including overlap with the file path. Embeddings are optional. If the model server does not implement them, retrieval still works and the UI status shows `lexical`.
5. The model streams `status`, `thought`, `delta`, and `done` events as SSE. Thinking models surface `reasoning` or Gemini thought parts as `thought` events; those are not stored as the answer.
6. The answer is stored with the author set to the provider and model that actually answered.

## DR-10. Applying a change

1. The UI extracts a `diff` fence into the inspector. The user fills the node gRPC URL, the workspace path, and an optional pipeline name from `deploy_pipelines`.
2. `POST /api/task/approve` signs the task with the local HMAC key and calls `ExecuteTask`.
3. The node checks expiry and the signature, then `git apply`. On failure it returns stderr and does not run the pipeline.
4. A pipeline runs either directly or under `bwrap` with an optional network unshare. The bubblewrap command bind-mounts the workspace, hides `/etc/xmeshd` and `/root` with a tmpfs, and exits if the daemon does. The command list must be non-empty. The workspace must already exist and must sit inside `allowed_workspaces`.

## DR-11. Failure behavior

| Failure | What the daemon does |
| --- | --- |
| Primary database is down | Opens the SQLite file, creating the parent directory if needed |
| Model returns 404, 429, or 5xx | Tries up to two further models from the same provider, coding rank first |
| Model returns 401 or 403, or the connection is refused | Stops. Another model name will not fix a missing key or a down server |
| Embeddings endpoint refuses the call | Disables embeddings for the process and keeps lexical retrieval |
| Hub is unreachable during a public send | Stores the message locally and logs the failure. The next history load retries the pull |
| Bare repo scan exceeds 20 seconds | Skips that repo and continues |

## DR-12. Identifiers

Context row ids are BLAKE3 hashes of a stable string (`master:{namespace}:{path}` for bare files, `local:{node}:{namespace}:{path}` for a worktree scan, `diff:{node}:{namespace}` for a diff). Message ids are BLAKE3 of `channel:thread:seq`. Re-applying the same row replaces it. A public append with sequence 0 allocates `max(seq)+1`. If that sequence already holds different content, the hub bumps the sequence instead of overwriting.
