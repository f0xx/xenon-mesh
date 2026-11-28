#!/bin/sh
# OpenAI-compatible chat. XMESH_BASE_URL is the origin, with or without /v1.
# XMESH_API_KEY may be empty for a server that does not require one.
exec python3 - "$@" <<'PY'
import json, os, sys, urllib.request
raw = sys.stdin.read()
req = json.loads(raw) if raw.strip() else {}
base = os.environ.get("XMESH_BASE_URL", "").rstrip("/")
if base.endswith("/v1"):
    url = base + "/chat/completions"
else:
    url = base + "/v1/chat/completions"
model = req.get("model") or os.environ.get("XMESH_MODEL") or ""
messages = []
system = (req.get("system") or "").strip()
if system:
    messages.append({"role": "system", "content": system})
for msg in req.get("messages") or []:
    role = msg.get("role") or "user"
    if role not in ("system", "user", "assistant"):
        role = "assistant"
    messages.append({"role": role, "content": msg.get("content") or ""})
body = json.dumps({"model": model, "messages": messages}).encode()
headers = {"Content-Type": "application/json"}
key = os.environ.get("XMESH_API_KEY", "")
if key:
    headers["Authorization"] = "Bearer " + key
http = urllib.request.Request(url, data=body, headers=headers, method="POST")
try:
    with urllib.request.urlopen(http, timeout=180) as resp:
        payload = json.loads(resp.read().decode())
except Exception as exc:
    print(str(exc), file=sys.stderr)
    sys.exit(1)
choice = ((payload.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
text = choice.strip() if isinstance(choice, str) else ""
if not text:
    print("empty chat response", file=sys.stderr)
    sys.exit(1)
sys.stdout.write(text)
PY
