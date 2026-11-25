# Company tools around the chat

These are proposed adapters for a small IT company. None of them ship in the daemon today. Each one should be a provider or a side panel, with the hub audit recording that a node asked for the action, and with private channels staying on the node.

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

External inspectors (a linter service, CodeQL, or a CI status API) belong next to the diff inspector. The daemon asks the inspector about the unified diff the model produced, shows the findings in the side panel, and still requires the operator to approve before a pipeline runs.

Automation tools (a webhook, a small job runner the company already uses) should be more deploy pipelines: a name in YAML, an argv, a workdir inside `allowed_workspaces`. The chat selects the pipeline name. It does not send a shell string.

## DR-41. Shape of an adapter

An adapter is configuration, not a new chat product:

1. A YAML block names the kind, the base URL, and the environment variable that holds the token.
2. The channel menu gains one action, "Attach calendar" or "Ask inspector", instead of a new application.
3. Reads can enter the model context. Writes go through the existing approve button and the hub audit.
4. Private channels may use an adapter locally. The hub learns about it only when the channel is shared or the action is public.

Build the calendar read and the diff inspector first. They pay off in daily chat without taking a second system of record away from the board the company already uses.
