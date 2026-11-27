# Security model

This is the trust model for a small lab that shares one HMAC key and does not have user accounts. It is not a multi-tenant control plane.

## DR-33. Who is trusted

Every machine that has `/etc/xmeshd/hmac.key` is a peer. The daemon hashes that file and uses it to sign task execution. There is no login, password, or per-person nickname.

`node.node_key_path` is a second file, one per machine. The hub reads `node.peer_keys_dir`, a directory of `<node-id>.key` copies. Share, join, audit, public message append, and `SyncContexts` then require a BLAKE3 MAC over a canonical string, with a five-minute timestamp. The sync MAC covers a digest of the entries, messages, and channels, so a captured header cannot be reused with a different payload. The stored origin is the verified id. If the directory is missing or empty, the hub still accepts the label the caller sends, so keys can be installed after the binary. A peer that has the shared HMAC key and another node's key file can still speak as that node. The key file is what stops a casual relabel.

The practical boundary is the network plus that shared key:

| Inside | Outside |
| --- | --- |
| Hub gRPC, hub HTTP, and node HTTP on the lab LAN or through an SSH forward | Anything that cannot open those ports |
| Operators who can read the key file | Everyone else |

Private chats and private project trees stay in the node's own database. They are not written to the hub audit log and they are not returned by the hub's public channel list.

## DR-34. Channel visibility

| Visibility | Who sees the channel | Where the transcript lives |
| --- | --- | --- |
| `public` | Every node that can call the hub | Hub |
| `private` | The node that scanned or created it | That node |
| `shared` | The origin node and the node ids named in the invitation | Hub, and a copy on the origin |

Sharing is an explicit action from the channel menu. The owner sends the node ids, the transcript, and the retrieved source excerpts. The hub stores the membership. Another node sees the channel only when its `node.id` is on that list. `POST /api/mesh/join` fails with "invitation required" when the id is absent.

This is collaboration among trusted peers, not an access-control system against someone who already has the HMAC key. With peer keys installed, a join or a share has to be signed by the node that claims the id. An invitation token is a second way in: the hub stores only its hash, the owner sees the token once, and `POST /api/join` with that token adds the caller. Without peer keys, a peer can still claim another node's id.

`# general` cannot be archived or closed. Mute, pin, and read state are rows in `channel_prefs` on that node only. One node marking a channel read does not clear it on another node.

## DR-35. Audit

The hub table `audit_events` records public and shared activity:

| Action | When |
| --- | --- |
| `chat.request` / `chat.reply` | A public or shared message is stored on the hub |
| `model.request` | A node asks the hub to run a model for a public or shared channel. The detail is the model id and a short preview of the user text, not the retrieved source |
| `channel.share` / `channel.join` | Membership changes |
| `task.execute` | An approved diff or pipeline runs |
| `git.push` / `git.force_push` | A pipeline whose argv is `git push`. The detail stores the ref, the sha before, the sha after, and `force`. Force is `--force`, `--force-with-lease`, a `+refspec`, or `git_force: true` on the pipeline |

Private channel traffic is not written there. The log is how the lab answers "which node asked the hub to do this" after a force-push pipeline or a public chat. It is not a substitute for git reflogs. The node id is the label the caller sent.

## DR-36. Transport

HTTP on the LAN is unauthenticated beyond reachability. HTTPS uses a certificate the operator generates, including a self-signed one. Trusting that certificate in the browser makes the page a secure context, which is what desktop notifications require. The certificate does not identify a person.

`scripts/xmesh-tls.sh` writes a local key and certificate. Keep the key next to the HMAC file, mode `0640`, and do not commit either.

SSH local forwards, described in [remote-access.md](remote-access.md), are the supported way to cross a network boundary. The forward carries the same trust as the lab LAN: anyone who can use that SSH session can talk to the hub as a peer.

## DR-37. What this model refuses to pretend

- It does not authenticate a human.
- It does not stop a peer who has both the HMAC key and another node's key file from speaking as that node.
- It does not encrypt private project text while a node is asking the hub to run a model. The prompt is sent to the configured model host. It is not copied into the audit log.
- It does not treat a self-signed certificate as a public CA.

Mutual TLS is separate from that MAC. When `node.mtls_ca_path`, `node.mtls_cert_path`, and `node.mtls_key_path` are set, gRPC requires a client certificate signed by that CA. `node.mesh_tls_addr` is an HTTPS port that requires the same client certificate and serves `/api/mesh/*`. On the plain HTTP port and on the UI HTTPS port, `/api/mesh/*` answers only when the TCP peer is localhost, which is the SSH forward case. `scripts/xmesh-mtls.sh` writes a lab CA and one node certificate. The MAC still names the node. The certificate only proves the caller holds a key the CA signed.

A pipeline with `use_bwrap: true` runs under bubblewrap. The workspace is the read-write bind. `/etc/xmeshd` and `/root` are covered by a tmpfs so the sandbox does not read the daemon config or root's home, and `--die-with-parent` stops the command if the daemon exits. The rest of the filesystem is still visible read-only, so a pipeline should only be approved for a workspace you trust.
