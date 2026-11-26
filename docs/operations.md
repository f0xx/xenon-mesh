# Operations

## DR-27. Build and test

```sh
cargo test
cargo build --release
```

The binary is `target/release/xmeshd`. Tests cover HMAC checks, workspace confinement, SQLite round-trips (including private forks), placeholder rewrite, config merge, retrieval, OpenAPI kind detection, and an in-process OpenAI stream. They do not call a live model server.

```sh
xmeshd models http://127.0.0.1:11434
xmeshd check /etc/xmeshd/local.yaml /etc/xmeshd/cluster.yaml
```

## DR-28. Install a hub

1. Build the release binary.
2. Choose a database URL or leave it empty to use SQLite.
3. Export `XMESH_DATABASE_URL` only if a server database should be used. Create the role with `db/init_postgres.sh` or `db/init_mysql.sh` first. Those scripts require `XMESH_DB_USER`, `XMESH_DB_PASSWORD`, and `XMESH_DB_NAME`.
4. Export `XMESHD_OPENAI_BASE_URL` (for example `http://127.0.0.1:11434/v1`) when a local OpenAI-compatible server should be the provider.
5. Run `scripts/provision_example.sh` as root. It creates the `xmeshd` user, `/var/lib/xmeshd`, `/var/log/xmeshd`, an HMAC key if one is missing, and `/etc/xmeshd/local.yaml`.
6. Put API keys in `/etc/conf.d/xmeshd` if a provider needs them.
7. Start it with `rc-service xmeshd restart` on OpenRC.

Copy `/etc/xmeshd/hmac.key` to every node. Do not commit it.

The hub indexes `git.bare_repos_root` in the background. A bare repo with an empty HEAD is skipped. Each repo contributes at most 200 text files and the walk stops after 8 seconds, so a large tree cannot pin the process. Drop a `.xmeshignore` file in a bare repo to exclude it. Install `githooks/post-receive` on the bare repo so a push calls `/api/internal/reindex`.

`node.id` is required. The daemon refuses to start when it is missing or blank. That id is the author shown on messages this machine sends, unless the name box in the UI (or `xmeshd cli` `/alias`) stores a different name. The override is one row on that node.

`xmeshd cli` is a prompt against a daemon that is already running. It does not bind a port. See [cli.md](cli.md).

`scripts/harvest-context-cloc.sh` is the outline command to use when `cloc` is installed. It prints a cloc summary and then source and header bodies, and stops after a size budget. If `cloc` is missing it runs `scripts/harvest-context.sh`.

## DR-29. Install a node

```sh
scripts/provision_node_example.sh ./target/release/xmeshd /path/to/hmac.key http://hub.example:8443
```

Set `XMESHD_OPENAI_BASE_URL` when the model server is not on `127.0.0.1:11434`. Set `NODE_ID` when the short hostname is not the label you want stored on rows.

The node serves the UI on port 8080 and gRPC on 8443. Private channels never leave the machine. Public channels and the `general` channel are read from the hub.

To watch a checkout:

```yaml
git:
  watched_repos:
    - namespace: "app"
      local_path: "/srv/projects/app"
      base_branch: "origin/master"
```

Create a channel whose namespace is `app`. Leave it private and the diff stays local. Mark it public and the diff is uploaded.

## DR-30. Day to day

- Open `http://<machine>:8080`. Pick a channel. Ask for a change. Inspect the diff. Approve it only after the workspace and the pipeline name are filled in.
- Fork a public thread when the next steps should stay on this machine. The fork prompt asks for a destination name and `head,tail` counts (how many leading and trailing messages to copy).
- Pin a rule with "Pin Raw" when the wording must be exact. "Distill Learn" asks the model to compress a note first.
- `xmeshd export` before a database move. The `.pb.zst` file contains private rows; treat it as private data.

## DR-31. Syncing this repository

Source moves by Git. `rsync` is only a fallback when a machine cannot fetch. Do not rsync `/etc/xmeshd/` between machines: the HMAC key is copied by hand, and each `local.yaml` has a different `node.id`, mode, and model URL.

After a pull, rebuild, install the binary over `/usr/local/bin/xmeshd`, and restart the service. A running daemon keeps its old binary until the restart.

## DR-32. What the tests do not prove

They do not open a real Postgres or MariaDB, and they do not apply a diff to a Git worktree. After pointing `storage.primary_url` at a server, restart once and confirm `/api/health` returns `"ok": true`. `xmeshd check` confirms the file parsed before that restart.
