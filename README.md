# xmeshd

xmeshd is a small-lab AI orchestration daemon. One hub holds the bare Git repositories and the public project chats. Other machines run the same daemon as nodes: they keep private work locally, join public channels on the hub, and call whatever chat model the operator configured.

The interface is a single-page chat. A channel is a project. The daemon retrieves source for that channel, asks the selected model, and can apply a reviewed unified diff on a node.

This directory is the design and operator specification:

| Document | What it settles |
| --- | --- |
| [docs/purpose.md](docs/purpose.md) | Why the system exists and the requirements it has to meet |
| [docs/architecture.md](docs/architecture.md) | Processes, trust boundary, and the main flows |
| [docs/configuration.md](docs/configuration.md) | YAML, environment, and where secrets live |
| [docs/models.md](docs/models.md) | Gemini, OpenAI-compatible endpoints, and Ollama |
| [docs/data-and-api.md](docs/data-and-api.md) | Tables, gRPC, and HTTP |
| [docs/operations.md](docs/operations.md) | Build, install, join a hub, back up, test |
| [docs/security.md](docs/security.md) | Trust boundary, channels, audit, and what the daemon does not authenticate |
| [docs/integrations.md](docs/integrations.md) | Proposed company tools around the chat, not shipped adapters |
| [docs/remote-access.md](docs/remote-access.md) | Reach a hub over SSH local forwards |
| [docs/roadmap.md](docs/roadmap.md) | What is in the tree and what comes later |

Read [docs/purpose.md](docs/purpose.md) first if you are changing behavior. Read [docs/operations.md](docs/operations.md) if you are installing a machine.
