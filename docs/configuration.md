# Configuration

Schema **v1**. `xmeshd` loads `/etc/xmeshd/cluster.yaml`, then `/etc/xmeshd/local.yaml`, then environment variables prefixed with `XMESHD_`. Nested keys use `__` (`XMESHD_NODE__HTTP_ADDR`). Later sources override earlier ones.

`xmeshd check [local.yaml] [cluster.yaml]` prints the merged result. Database URLs are printed with the password replaced by `***`.

Examples:

- `configs/local.master.example.yaml` — hub
- `configs/common.example.yaml` — node
- `configs/local.example.yaml` — overlay fragment, not a complete file

A missing `schema` key means v1. `schema: 1` and `schema: v1` are the same. Any other value is refused at startup.

## How to read the tables

**Parser** is what the file must contain for `xmeshd` to start. **Empty** means the key may be omitted and the default is used. A key marked required fails the process when it is missing or, for `node.id`, blank.

## Top level

| Key | Parser | Format |
| --- | --- | --- |
| `schema` | empty means `1` | `1` or `v1` |
| `node` | required | map, below |
| `storage` | required | map, below |
| `git` | required | map, below. `bare_repos_root` may be `null` |
| `gemini` | empty | legacy map, used only when `models.providers` is empty |
| `models` | empty | map, below |
| `deploy_pipelines` | empty | map of name → pipeline |

```yaml
schema: 1
node:
  id: "hub"
  mode: "master"
  listen_addr: "0.0.0.0:8443"
  http_addr: "0.0.0.0:8080"
  hmac_secret_path: "/etc/xmeshd/hmac.key"
storage:
  primary_url: null
  fallback_sqlite: "/var/lib/xmeshd/cache.db"
git:
  bare_repos_root: null
```

## `node`

| Key | Parser | Format |
| --- | --- | --- |
| `id` | required, not blank | string. Stored as the author of messages this machine sends. Leading and trailing spaces are removed |
| `mode` | required | `master` or `node` |
| `listen_addr` | required | `host:port`, gRPC |
| `http_addr` | empty → `0.0.0.0:8080` | `host:port`, the chat UI |
| `https_addr` | empty | `host:port` for the same UI over TLS. Do not reuse the gRPC port |
| `tls_cert_path` | empty | PEM path. A self-signed certificate is enough once the browser allows it |
| `tls_key_path` | empty | PEM private key |
| `master_url` | empty | node only. Hub gRPC URL, including the scheme (`http://hub.example:8443`) |
| `master_http` | empty | node only. Hub HTTP URL. Each chat is its own request. The hub does not run those requests one behind another |
| `hmac_secret_path` | required | path to the shared key file. The daemon hashes the file with BLAKE3. Copy the file, not a generated string |
| `node_key_path` | empty | this machine's identity key. The daemon hashes the file. Do not commit it |
| `peer_keys_dir` | empty | hub. Directory of `<node-id>.key` files. When it contains at least one key, mutating hub calls must be signed |
| `discover` | empty → `true` | bool. Advertise on the local network and list neighbours |
| `allowed_workspaces` | empty → `[]` | list of absolute directories a signed `git apply` may touch |

`node.id` is the name shown on chat messages from this machine, in the form `[hub]` or `[build-2]`. It is not a login.

## `storage`

| Key | Parser | Format |
| --- | --- | --- |
| `primary_url` | required key, value may be `null` | `postgres://…`, `mysql://…`, or `mariadb://…`. `null` skips straight to SQLite |
| `fallback_sqlite` | required | file path. The parent directory is created on startup |

## `git`

| Key | Parser | Format |
| --- | --- | --- |
| `bare_repos_root` | required key, value may be `null` | hub directory of bare repos. A repo containing `.xmeshignore` is skipped. Each scan keeps at most 200 text files and stops after 8 seconds |
| `watched_repos` | empty → `[]` | list of `{namespace, local_path, base_branch}`. Diffs are against that ref |
| `project_roots` | empty → `[]` | list of `{path, visibility}`. Each immediate child directory becomes a channel. `visibility` defaults to `private`. `public` publishes the thread to the hub |
| `watch_changes` | empty → `true` | bool. inotify on project roots and bare repos. A change reindexes that tree. Set `false` to keep the startup scan only |
| `harvest_command` | empty → `[]` | argv. The project path is appended. Stdout becomes the chat's project outline. `scripts/harvest-context-cloc.sh` uses `cloc` when it is installed and otherwise runs `scripts/harvest-context.sh` |
| `inspect_command` | empty → `[]` | argv. The diff is written to stdin. Stdout lines are findings in the side panel |

```yaml
git:
  bare_repos_root: "/srv/git"
  watch_changes: true
  project_roots:
    - path: "/srv/projects"
      visibility: "private"
  watched_repos:
    - namespace: "app"
      local_path: "/srv/projects/app"
      base_branch: "origin/master"
  harvest_command: ["/usr/local/libexec/xmeshd/harvest-context.sh"]
```

## `models`

When `models.providers` is non-empty, it is the whole provider list. The legacy `gemini:` section is used only when that list is empty.

| Key | Parser | Format |
| --- | --- | --- |
| `default` | empty | `provider/model`. Empty picks the highest coding rank after discovery |
| `embed_provider` | empty | provider id used for embeddings. Empty skips embeddings and retrieval stays lexical |
| `system_prompt` | empty | text prepended to the coding preamble |
| `providers` | empty → `[]` | list of providers, below |
| `providers_file` | empty | optional JSON file of extra providers. See `configs/providers.example.json`. A missing file adds nothing. A provider with no key is skipped unless `auth_optional` is true. The hub serves those models and nodes reach them through the hub, the same way as Gemini |
| `sampling.temperature` | unset | sent on chat requests. Unset keeps the built-in 0.2 for OpenAI-compatible and Gemini calls. Unset leaves Ollama on the model file, which is often 0.8. Set `0.2` on an Ollama provider when coding answers wander |
| `sampling.top_p` | unset | sent only when set |
| `sampling.repeat_penalty` | unset | Ollama `options.repeat_penalty` when set |

Each provider (`#[serde(default)]` on the whole map, so a short entry is valid):

| Key | Default | Format |
| --- | --- | --- |
| `id` | `""` | short name. The model id is `id/model`. Set this |
| `host` | `""` | label after the model name. Empty uses `id` |
| `kind` | `openai` | `openai`, `openapi`, `openai-compat`, `ollama`, `gemini`, `google`, or `auto` |
| `base_url` | `""` | HTTP origin. For `openai`, a URL without `/v1` gets `/v1` appended. For `ollama`, `/v1` is stripped |
| `api_key_env` | `""` | name of an environment variable, never the key itself |
| `chat_model` | `""` | used when the request does not name a model |
| `embed_model` | `""` | embeddings model |
| `models` | `[]` | extra model names to offer when discovery is off |
| `discover` | `true` | ask the server which models it has |
| `coding_bias` | `true` | prefer names that look like code models |
| `timeout_secs` | `300` | idle time between stream chunks, and the limit for a non-streaming call. Values below 1 are raised to 1 |
| `openapi_url` | `""` | optional OpenAPI document for `kind: auto` |
| `command` | `[]` | `google` only. Argv. The query is written to stdin |
| `user_agent` | `""` | `google` only. Passed to the script as `XMESH_UA` |
| `sampling` | unset fields | overrides `models.sampling` for this provider, one field at a time |
| `max_tokens` | `0` | OpenAI-compatible `max_tokens`, Ollama `num_predict`, Gemini `maxOutputTokens`. `0` omits the field |

```yaml
models:
  default: ""
  embed_provider: ""
  system_prompt: ""
  providers:
    - id: local
      host: local
      kind: auto
      base_url: "http://127.0.0.1:11434/v1"
      api_key_env: ""
      discover: true
      coding_bias: true
      timeout_secs: 300
    - id: google
      host: Google
      kind: google
      discover: false
      command: ["/usr/local/libexec/xmeshd/google-ask.sh"]
      user_agent: ""
      base_url: "https://www.google.com/search?hl=en&q={query}"
      timeout_secs: 40
```

`kind: auto` fetches `openapi_url` when set, then `/openapi.json`, `/swagger.json`, and `/v1/openapi.json`, and otherwise probes `/v1/models` and `/api/tags`.

## `gemini` (legacy)

Ignored once `models.providers` is non-empty. Defaults: `chat_model` `gemini-2.5-flash`, `embed_model` `gemini-embedding-001`, `api_key_env` `GEMINI_API_KEY`, `available_models` those two chat names.

| Key | Format |
| --- | --- |
| `chat_model` | model name |
| `embed_model` | model name |
| `api_key_env` | environment variable name |
| `available_models` | list of model names |

## `deploy_pipelines.<name>`

| Key | Parser | Format |
| --- | --- | --- |
| `workdir` | required | absolute directory |
| `command` | required | argv list. The UI sends the pipeline name, not a shell string |
| `use_bwrap` | required | bool |
| `allow_network` | required | bool |
| `git_ref` | empty | recorded when the command is `git push` |
| `git_force` | empty → `false` | the operator marked this pipeline as rewriting remote history |

```yaml
deploy_pipelines:
  run-tests:
    workdir: "/srv/projects/app"
    command: ["cargo", "test", "--offline"]
    use_bwrap: true
    allow_network: false
    git_ref: ""
    git_force: false
```

## Secrets

| Secret | Where it lives |
| --- | --- |
| API keys | The environment variable named by `api_key_env`. OpenRC can export it from `/etc/conf.d/xmeshd`, which is not part of the repo |
| Database password | The URL in `/etc/xmeshd/local.yaml`, or `XMESH_DATABASE_URL` for the provision script |
| HMAC key | The file named by `hmac_secret_path`, mode `0640`, group `xmeshd` |
| Role bootstrap | `XMESH_DB_USER`, `XMESH_DB_PASSWORD`, `XMESH_DB_NAME` consumed by `db/init_postgres.sh` and `db/init_mysql.sh` |

Do not put those values in example YAML, SQL, or scripts. The provision scripts refuse to invent a database password: without `XMESH_DATABASE_URL` the hub uses SQLite.

## Environment overrides

Any YAML field can be overridden without editing the file:

```sh
XMESHD_NODE__ID=build-2
XMESHD_NODE__HTTP_ADDR=127.0.0.1:8080
XMESHD_MODELS__DEFAULT=local/example-model
XMESHD_SCHEMA=1
```

`xmeshd models <base_url>` probes a server and prints the detected kind, each model, and its coding rank. It does not read `/etc`.
