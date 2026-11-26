# xmeshd

xmeshd is a local-first AI-empowered collaborative tool for working on code with a model. A channel is a project. The daemon reads that project's tree, sends the relevant parts with the question, and keeps the thread. You review a proposed diff before anything is applied. It is a small-lab AI orchestration suite: one hub holds the bare Git repositories and the related public project chats, as well as private ones. Other machines run the same daemon as nodes: they keep private work locally, join public channels on the hub, and call whatever chat model the operator configured. This is all-in-one simple collaborative development suite: one may use it as a self-hosted assistant, or powerful hub-centralized star-, mesh- or multinode-topology suite with deep git integration and AI agentic coding assistance.

It is one static binary and a few shell scripts. SQLite is built in, so a single host is a complete install: no separate database, no account service, and no central hub you have to run. Add a hub only when several machines should share public repositories and public channels. The same binary is the hub or a node; `node.mode` is the difference. Machines on the same network can also open a direct chat without a hub.

Point it at whatever model server you already run. OpenAI-compatible HTTP, Ollama, Gemini, and a web-search script are providers in the config. The daemon does not download a model and does not require a GPU.

`ldd` on a typical release build shows the C library, libgcc, libm, and zlib. TLS, Git, and SQLite are linked into the binary rather than installed beside it. The scripts call `git`, `python3`, and `curl` when you use harvesting, search, or provisioning. Those tools are not required to start the UI.

```sh
cargo build --release
sudo install -m 755 target/release/xmeshd /usr/local/bin/xmeshd
sudo install -d /etc/xmeshd /var/lib/xmeshd
# Edit configs/local.master.example.yaml: set node.id, then copy it to
# /etc/xmeshd/local.yaml and point a provider at your model server.
xmeshd
```

Open `http://127.0.0.1:8080`. `node.id` is required and is the name on messages from this machine. The name box above the channel list stores a different label on this node when you want one. `xmeshd cli` is the same chat from a terminal and does not bind a port; see [docs/cli.md](docs/cli.md).

Packaging sketches for Gentoo, Debian, and Alpine live in [`packaging/`](packaging/README.md). Operator detail lives in the docs:



| Document | What it settles |
| --- | --- |
| [docs/purpose.md](docs/purpose.md) | Why the system exists and the requirements it has to meet |
| [docs/architecture.md](docs/architecture.md) | Processes, trust boundary, and the main flows |
| [docs/configuration.md](docs/configuration.md) | YAML, environment, and where secrets live |
| [docs/models.md](docs/models.md) | Gemini, OpenAI-compatible endpoints, and Ollama |
| [docs/data-and-api.md](docs/data-and-api.md) | Tables, gRPC, and HTTP |
| [docs/operations.md](docs/operations.md) | Build, install, join a hub, back up, test |
| [docs/cli.md](docs/cli.md) | `xmeshd cli` commands |
| [docs/security.md](docs/security.md) | Trust boundary, channels, audit, and what the daemon does not authenticate |
| [docs/integrations.md](docs/integrations.md) | Proposed company tools around the chat, not shipped adapters |
| [docs/remote-access.md](docs/remote-access.md) | Reach a hub over SSH local forwards |
| [docs/roadmap.md](docs/roadmap.md) | What is in the tree and what comes later |

Read [docs/purpose.md](docs/purpose.md) first if you are changing behavior. Read [docs/operations.md](docs/operations.md) if you are installing a machine.

## License

The source in this repository is [MIT](LICENSE). That is a good fit: the daemon is a tool you run yourself, and MIT lets a host package it without a copyleft obligation on your own programs.

The release binary also contains third-party code that stays under its own license. [libgit2](https://github.com/libgit2/libgit2) is GPL-2.0 with a linking exception that allows it to be linked into this binary. aws-lc, used for TLS, is Apache-2.0. See [NOTICE](NOTICE). You can publish this project under MIT. You should keep `NOTICE` next to the binary so those terms travel with it.
