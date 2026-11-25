# Security model

This is the trust model for a small lab that shares one HMAC key and does not have user accounts. It is not a multi-tenant control plane.

## DR-33. Who is trusted

Every machine that has `/etc/xmeshd/hmac.key` is a peer. The daemon hashes that file and uses it to sign task execution. There is no login, password, or per-person nickname. `node.id` is a label the node writes on its own requests. A peer that already has the key can put any label it wants.

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

This is collaboration among trusted peers, not an access-control system against someone who already has the HMAC key. A peer can claim another node's id. Per-node keys are the follow-up in the roadmap.

`# general` cannot be archived or closed. Mute, pin, and read state are rows in `channel_prefs` on that node only. One node marking a channel read does not clear it on another node.

## DR-35. Audit

The hub table `audit_events` records public and shared activity:

| Action | When |
| --- | --- |
| `chat.request` / `chat.reply` | A public or shared message is stored on the hub |
| `model.request` | A node asks the hub to run a model for a public or shared channel. The detail is the model id and a short preview of the user text, not the retrieved source |
| `channel.share` / `channel.join` | Membership changes |
| `task.execute` | An approved diff or pipeline runs |
| `git.push` / `git.force_push` | The pipeline name contains `push`, and `git.force_push` when it also contains `--force` or `force-with-lease` |

Private channel traffic is not written there. The log is how the lab answers "which node asked the hub to do this" after a force-push pipeline or a public chat. It is not a substitute for git reflogs. The node id is the label the caller sent.

## DR-36. Transport

HTTP on the LAN is unauthenticated beyond reachability. HTTPS uses a certificate the operator generates, including a self-signed one. Trusting that certificate in the browser makes the page a secure context, which is what desktop notifications require. The certificate does not identify a person.

`scripts/xmesh-tls.sh` writes a local key and certificate. Keep the key next to the HMAC file, mode `0640`, and do not commit either.

SSH local forwards, described in [remote-access.md](remote-access.md), are the supported way to cross a network boundary. The forward carries the same trust as the lab LAN: anyone who can use that SSH session can talk to the hub as a peer.

## DR-37. What this model refuses to pretend

- It does not authenticate a human.
- It does not stop a peer with the HMAC key from impersonating a node id.
- It does not encrypt private project text while a node is asking the hub to run a model. The prompt is sent to the configured model host. It is not copied into the audit log.
- It does not treat a self-signed certificate as a public CA.

The next hardening step is mutual TLS with a key per node, and an audit row that records the certificate identity rather than a self-declared `node.id`.
