# xmeshd

xmeshd is a small-lab AI orchestration daemon. One hub holds the bare Git repositories and the public project chats. Other machines run the same daemon as nodes: they keep private work locally, join public channels on the hub, and call whatever chat model the operator configured.

The interface is a single-page chat. A channel is a project. The daemon retrieves source for that channel, asks the selected model, and can apply a reviewed unified diff on a node.

This directory is the design and operator specification:

| Document | What it settles |
| --- | --- |
| [purpose.md](purpose.md) | Why the system exists and the requirements it has to meet |
| [architecture.md](architecture.md) | Processes, trust boundary, and the main flows |
| [configuration.md](configuration.md) | YAML, environment, and where secrets live |
| [models.md](models.md) | Gemini, OpenAI-compatible endpoints, and Ollama |
| [data-and-api.md](data-and-api.md) | Tables, gRPC, and HTTP |
| [operations.md](operations.md) | Build, install, join a hub, back up, test |
| [security.md](security.md) | Trust boundary, channels, audit, and what the daemon does not authenticate |
| [integrations.md](integrations.md) | Proposed company tools around the chat, not shipped adapters |
| [remote-access.md](remote-access.md) | Reach a hub over SSH local forwards |
| [roadmap.md](roadmap.md) | What is in the tree and what comes later |

Read [purpose.md](purpose.md) first if you are changing behavior. Read [operations.md](operations.md) if you are installing a machine.
