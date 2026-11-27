# Roadmap

## In the tree

- Public, private, and shared channels. Shared channels are an invitation list stored on the hub.
- Sidebar groups: `# general`, public hub repos, private project trees, shared channels, then archived and closed.
- Inside a group, pinned channels stay on top in reverse alphabetical order. The rest follow how often the node opens them, then reverse alphabetical order.
- Per-node pin, mute, archive, close, and read cursor. `# general` can be muted and cannot be archived or closed. Closing hides a channel and leaves its messages in the database.
- Unread counts on the node that has not looked at the reply. Mute hides the count.
- Hub audit of public and shared requests, model calls, shares, and pipelines.
- A key file per node. When the hub has `node.peer_keys_dir`, mutating calls must carry that node's MAC. The audit row uses the verified id. Without the directory, the old self-declared label still works so a lab can roll keys after the binary is up.
- Shared channels keep the node-id invite and also an invitation token. The hub stores the token hash. The token is shown once to the owner. Joining with the token adds the caller.
- Git push pipelines record old sha, new sha, the ref, and whether the push was forced. The marker is `XMESH_GIT` on the task output. `--force`, `--force-with-lease`, a `+refspec`, or `git_force: true` on the pipeline counts as a force-push.
- Diff inspector in the side panel: file and line counts, conflict markers, a large-diff warning, trailing whitespace, plus optional `git.inspect_command` and `git.status_command`. `scripts/inspect-diff.sh` and `scripts/status-check.sh` print one finding per line. Approve is still required before a write, and findings do not disable the button.
- Mutual TLS for gRPC and for `/api/mesh/*` when `node.mtls_ca_path`, `node.mtls_cert_path`, and `node.mtls_key_path` are set. `node.mesh_tls_addr` is the client-certificate port. The UI ports serve `/api/mesh/*` only to localhost. The per-node MAC is unchanged.
- Read-only calendar context from `calendar.ics_url`. A schedule question inserts the next seven days. See [integrations.md](integrations.md).
- Agentic review is a per-channel switch next to Send. Off stores the message and does not call a model. The default is on.
- A public reply's thread link shows `thread (total/unread)`. The unread count is the red bubble. Opening the thread marks it read on this node.
- inotify on `git.project_roots` and `git.bare_repos_root` when `git.watch_changes` is true (the default). A save outside the browser reindexes that project. A deleted or renamed directory drops the channel the scanner created. `git.watch_changes: false` leaves the startup scan and turns the watch off.
- Hub-proxied models, including Gemini, are selectable on every node. The menu groups them under the hub's node id because the API key stays on the hub.
- Chat times are epoch milliseconds in the database. The UI draws a day divider and a clock without seconds on each bubble.
- HTTP and HTTPS for the same UI. Desktop notifications work in a secure context: localhost, or HTTPS after the browser allows the certificate.
- SSH local forwards for a node that is not on the hub LAN. See [remote-access.md](remote-access.md).

## QA, when you want it

Not a new product. A named pipeline and the inspector hooks are enough to start:

- A `test` pipeline whose argv is the repo's test command. The audit row is `task.execute` with the exit code. The chat does not get a shell string.
- Point `git.inspect_command` at `scripts/inspect-diff.sh` or at a wrapper that prints one finding per line. Approve stays manual.
- Point `git.status_command` at `scripts/status-check.sh` or at another read-only status command. Failing findings stay visible and do not block approve.
- Keep flaky suites out of the default path. A separate pipeline name (`test-full`) is easier to audit than a flag buried in a prompt.

## Not planned

- User accounts, passwords, or a nickname that follows a person between machines.
- Replacing Wrike, Miro, or Google Docs. The chat can point at them and, later, file an audited action. It does not become the board.
