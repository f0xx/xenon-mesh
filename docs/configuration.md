# Configuration

`xmeshd` loads `/etc/xmeshd/cluster.yaml`, then `/etc/xmeshd/local.yaml`, then environment variables prefixed with `XMESHD_`. Nested keys use `__` (`XMESHD_NODE__HTTP_ADDR`). Later sources override earlier ones.

`xmeshd check [local.yaml] [cluster.yaml]` prints the merged result. Database URLs are printed with the password replaced by `***`.

Examples:

- `configs/local.master.example.yaml` — hub
- `configs/common.example.yaml` — node
- `configs/local.example.yaml` — overlay fragment, not a complete file

## DR-13. Node

| Key | Meaning |
| --- | --- |
| `node.id` | Operator-chosen label stored on rows this machine writes. Not a secret, but it is local configuration |
| `node.mode` | `master` or `node` |
| `node.listen_addr` | gRPC listen address |
| `node.http_addr` | HTTP UI. Default `0.0.0.0:8080` |
| `node.https_addr` | Optional TLS listener for the same UI, for example `0.0.0.0:8448`. Do not reuse the gRPC port |
| `node.tls_cert_path` | PEM certificate. A self-signed certificate is enough once the browser is told to allow it |
| `node.tls_key_path` | PEM private key for that certificate |
| `node.master_url` | Node only. Hub gRPC URL, including the scheme (`http://hub.example:8443`) |
| `node.master_http` | Node only. Hub HTTP URL. The node lists models from there and sends each chat completion as its own request. The hub does not queue those requests behind one another |
| `node.hmac_secret_path` | Shared key file. The daemon hashes the file with BLAKE3 and uses that as the HMAC key. Copy the file, not a generated string |
| `node.node_key_path` | This machine's identity key. The daemon hashes the file. Do not commit it |
| `node.peer_keys_dir` | Hub. Directory of `<node-id>.key` files. When it contains at least one key, mutating hub calls must be signed with the matching key |
| `node.allowed_workspaces` | Absolute directories a signed `git apply` may touch |

## DR-14. Storage and Git

| Key | Meaning |
| --- | --- |
| `storage.primary_url` | `postgres://…`, `mysql://…`, or `mariadb://…`. Empty or null skips straight to SQLite |
| `storage.fallback_sqlite` | File path. The parent directory is created on startup |
| `git.bare_repos_root` | Hub. Directory of bare repos. A repo containing `.xmeshignore` is skipped |
| `git.watched_repos` | `{namespace, local_path, base_branch}`. Diffs are against that ref. The worktree is also scanned when a chat's namespace matches |
| `git.project_roots` | `{path, visibility}`. Each immediate child directory becomes a channel and is readable in that chat. `private` keeps the thread on this node. `public` publishes the thread to the hub. A hub also turns each bare repo under `bare_repos_root` into a public channel |
| `git.watch_changes` | Default true. inotify on project roots and bare repos. A change reindexes that tree. A directory that disappears drops the channel the scanner created. Set false to keep the startup scan only |
| `git.harvest_command` | Optional argv. The project path is appended. Stdout becomes the chat's project outline. `scripts/harvest-context.sh` is the bundled walker |
| `git.inspect_command` | Optional argv. The diff is written to stdin. Stdout lines are findings in the side panel |

`deploy_pipelines.<name>` is `{workdir, command, use_bwrap, allow_network, git_ref, git_force}`. `command` is an argv list. The UI sends the pipeline name; it does not send a shell string. `git_ref` and `git_force` are recorded when the command is `git push`.

## DR-15. Models

When `models.providers` is non-empty, it is the whole provider list. The legacy `gemini:` section is used only when that list is empty, so existing hub files keep working until an operator adds a provider.

```yaml
models:
  default: ""                  # or "local/some-model"
  embed_provider: ""           # optional provider id
  system_prompt: ""            # prepended to the coding preamble's retrieval block
  providers:
    - id: local
      host: local              # label after the model name; empty uses id
      kind: auto               # openai | ollama | gemini | google | auto
      command: []              # google/web: argv, query on stdin
      user_agent: ""           # google/web: sent to the script as XMESH_UA
      base_url: "http://127.0.0.1:11434/v1"
      api_key_env: ""          # name of an environment variable, never the key
      openapi_url: ""          # optional OpenAPI document
      chat_model: ""           # empty + coding_bias: pick after discovery
      embed_model: ""
      discover: true
      coding_bias: true
      timeout_secs: 300
```

`kind: openai` also accepts the aliases `openapi` and `openai-compat`. A base URL without a trailing `/v1` gets `/v1` appended for that kind. `kind: ollama` uses the native API on the same host with `/v1` stripped. `kind: auto` fetches `openapi_url` when set, then `/openapi.json`, `/swagger.json`, and `/v1/openapi.json`, and otherwise probes `/v1/models` and `/api/tags`.

## DR-16. Secrets

| Secret | Where it lives |
| --- | --- |
| Gemini or other API keys | The environment variable named by `api_key_env`. OpenRC can export it from `/etc/conf.d/xmeshd`, which is not part of the repo |
| Database password | The URL in `/etc/xmeshd/local.yaml`, or `XMESH_DATABASE_URL` for the provision script |
| HMAC key | The file named by `hmac_secret_path`, mode `0640`, group `xmeshd` |
| Role bootstrap | `XMESH_DB_USER`, `XMESH_DB_PASSWORD`, `XMESH_DB_NAME` consumed by `db/init_postgres.sh` and `db/init_mysql.sh` |

Do not put those values in example YAML, SQL, or scripts. The provision scripts refuse to invent a database password: without `XMESH_DATABASE_URL` the hub uses SQLite.

## DR-17. Environment overrides

Any YAML field can be overridden without editing the file:

```sh
XMESHD_NODE__HTTP_ADDR=127.0.0.1:8080
XMESHD_MODELS__DEFAULT=local/example-model
```

`xmeshd models <base_url>` probes a server and prints the detected kind, each model, and its coding rank. It does not read `/etc`.
