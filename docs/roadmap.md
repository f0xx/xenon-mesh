# Roadmap

## In the tree

- Public, private, and shared channels. Shared channels are an invitation list stored on the hub.
- Sidebar groups: `# general`, public hub repos, private project trees, shared channels, then archived and closed.
- Inside a group, pinned channels stay on top in reverse alphabetical order. The rest follow how often the node opens them, then reverse alphabetical order.
- Per-node pin, mute, archive, close, and read cursor. `# general` can be muted and cannot be archived or closed. Closing hides a channel and leaves its messages in the database.
- Unread counts on the node that has not looked at the reply. Mute hides the count.
- Hub audit of public and shared requests, model calls, shares, and pipelines whose names look like a git push.
- HTTP and HTTPS for the same UI. Desktop notifications work in a secure context: localhost, or HTTPS after the browser allows the certificate.
- SSH local forwards for a node that is not on the hub LAN. See [remote-access.md](remote-access.md).

## Next

1. A key per node, checked on hub RPCs, so the audit identity is the caller's certificate rather than a label the caller picked.
2. Mutual TLS on gRPC and on `/api/mesh/*`, and stop serving those routes on plain HTTP except through the SSH forward.
3. Calendar read-only context and an external diff inspector behind the existing approve button. See [integrations.md](integrations.md).
4. A real invitation token for shared channels, so joining does not depend on typing a node id that any peer can claim.
5. Git push and force-push as first-class audited pipelines, with the hub storing the ref update (old sha, new sha, force or not) instead of inferring it from the pipeline name.

## Not planned

- User accounts, passwords, or a nickname that follows a person between machines.
- Replacing Wrike, Miro, or Google Docs. The chat can point at them and, later, file an audited action. It does not become the board.
