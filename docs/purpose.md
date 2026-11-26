# Purpose

## DR-1. One process, optionally a hub

One machine with `mode: master` and SQLite is a complete install: the UI, the channels, and the model calls all stay on that host. A hub is only needed when several machines should share public repositories and public chats. Those machines run the same binary as nodes, point `node.master_url` at the hub's gRPC address, and use a copy of the same HMAC key.

Neighbours on the same network can also open a direct chat. That history stays on the two peers. It does not replace the hub for shared project channels.

A node may hold private projects that are never pushed. It may also clone a hub repository and join the public channel for that repository.

## DR-2. A channel is a project context

The user picks a channel. That choice is what the daemon and the model are allowed to read.

- The channel namespace is the repository name used when the hub indexed bare repos (`git/bare_repos_root`).
- `context_namespaces` may list more than one namespace when a discussion needs more than one tree. That is the cross-repo context.
- Pinned and learned notes in the same namespace are invariants. They are always placed in the prompt.
- Uncommitted work on a node is stored as an `@diff:` entry and is also always placed in the prompt.

## DR-3. Public and private memory stay apart

| Kind of data | Where it lives |
| --- | --- |
| Bare repositories and their indexed text | Hub |
| Public channel messages, public pins, public diffs | Hub, cached on the node that is talking |
| Private channels, private forks, unshared working trees | The node that created them |

Forking a thread marks the new thread `private`. Later messages in that thread are not uploaded, including when the parent channel is public. This is the targeted-development fork: same starting context, local continuation.

`general` is the open public channel. The hub creates it. Nodes show it even before the hub has been contacted.

## DR-4. The model is a coding collaborator

There is no topic filter. The default system preamble still treats the session as collaborative programming:

- retrieved blocks are source the model may read
- code changes are proposed as one `git apply`-able unified diff inside a `diff` fence
- the user reviews that diff in the side panel and approves execution

Execution is a signed task. The hub (or the UI on any node that has the HMAC key) signs `task_id:repo:diff:pipeline:expiry`. The target node checks the signature, the expiry, and that the workspace is under `allowed_workspaces` before `git apply` or a named pipeline runs.

## DR-5. Storage degrades in order

1. PostgreSQL or TimescaleDB when `storage.primary_url` connects.
2. MariaDB or MySQL when that URL uses the `mysql://` or `mariadb://` scheme.
3. SQLite at `storage.fallback_sqlite` when the primary URL is empty or the server is down.

The daemon creates the tables it needs. The files under `db/` are the same schema for operators who want to provision the database first.

## DR-6. The model endpoint is configuration

Gemini remains available. It is not the only backend. Operators add OpenAI-compatible servers (Ollama's `/v1`, vLLM, llama.cpp, and others) or Ollama's native API. See [models.md](models.md).

Coding models are preferred when `coding_bias` is true and the operator did not pin a `chat_model`. Rank is name-based (`coder` above `code` above `instruct`). Nothing in the source tree names a particular deployment's models.

## DR-7. No lab secrets in the tree

Hostnames, account names, database passwords, API keys, and private addresses belong in `/etc/xmeshd/` or the process environment. Example files use placeholders (`USER`, `PASSWORD`, `hub.example`). `xmeshd check` redacts userinfo passwords before printing a database URL.
