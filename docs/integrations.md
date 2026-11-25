# Company tools around the chat

These are adapters for a small IT company. The diff inspector in DR-40 is in the daemon. The rest are still proposals. Each one should be a provider or a side panel, with the hub audit recording that a node asked for the action, and with private channels staying on the node.

## DR-38. Calendars and reminders

A calendar provider speaks CalDAV or Google Calendar's HTTP API. The chat can ask "what is on the desk tomorrow" and the daemon inserts a short agenda into the context. Reminders are the other direction: a due item opens or badges a channel, using the same per-node unread counter as a model reply the operator has not looked at.

Personal reminders stay on the node. A shared channel can subscribe to a team calendar the owner named. The daemon stores the calendar URL and a token in the environment, the same way model API keys are stored, not in git.

## DR-39. Boards and documents

| Tool | Fit |
| --- | --- |
| draw.io / diagrams.net | A channel attachment. The file is XML. The model can propose a diff to the diagram the same way it proposes a code diff, and the UI already has an inspect step before anything is applied |
| Miro | Link plus exported frame. Live board editing is a poor fit for a diff workflow. Store the board URL on the channel and fetch a snapshot when the operator asks |
| Google Docs and Sheets | Read-only context by default. Write access is an approved action, like `task.execute`, with the hub audit naming the node and the document id |
| Wrike or monday.com | A task list for the channel's project. Creating or closing a task is an audited action. The chat does not become the system of record for the board |

## DR-40. Code and automation

The side panel inspects the unified diff before approve. Built-in findings are the file count, added and removed lines, conflict markers, and a warning when the diff is large. `git.inspect_command` is an optional argv. The daemon writes the diff to its stdin and appends up to twenty stdout lines. A non-zero exit is itself a finding. Nothing is written until the operator approves.

External inspectors (a linter, CodeQL, a CI status API) should use that command, or a small wrapper around it. They do not get a second write path.

Automation stays a named deploy pipeline: a name in YAML, an argv, a workdir inside `allowed_workspaces`. A pipeline whose command is `git push` records the ref, the old sha, the new sha, and whether the update was forced. `git_ref` and `git_force` on the pipeline are the explicit form of that record. The chat selects the pipeline name. It does not send a shell string.

## DR-42. QA

QA belongs on the same two hooks, not in a new service.

| Check | Where it runs | What the operator sees |
| --- | --- | --- |
| Unit or integration tests | A pipeline such as `test`, argv only | Audit `task.execute` with the exit code. The transcript can quote stdout |
| Lint or format on the proposed diff | `git.inspect_command` | Findings in the side panel, before approve |
| Coverage or a required CI status | A read-only inspector, or a pipeline that prints status and does not deploy | The same findings list, or an audit row |
| A long or flaky suite | A separate pipeline name | It is not the default approve path |

Failing findings should stay visible and should not disable approve until the lab decides a check is mandatory. A mandatory check is then a pipeline the operator runs on purpose, with the exit code in the audit, rather than a hidden gate.

Private channels can run a local inspector. The hub audit learns about it when the channel is public or shared and the action is an approved task.

## DR-41. Shape of an adapter

An adapter is configuration, not a new chat product:

1. A YAML block names the kind, the base URL, and the environment variable that holds the token.
2. The channel menu gains one action, "Attach calendar" or "Ask inspector", instead of a new application.
3. Reads can enter the model context. Writes go through the existing approve button and the hub audit.
4. Private channels may use an adapter locally. The hub learns about it only when the channel is shared or the action is public.

The diff inspector is in place. Calendar read is the remaining daily-chat adapter, and it is waiting on a decision. Neither one should take a second system of record away from the board the company already uses.
