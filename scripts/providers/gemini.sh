#!/bin/sh
# Gemini generateContent. The key arrives as XMESH_API_KEY. Model names such
# as gemini-4-* are chosen by the daemon config, not by this file.
exec python3 - "$@" <<'PY'
import json, os, sys, urllib.request
raw = sys.stdin.read()
req = json.loads(raw) if raw.strip() else {}
key = os.environ.get("XMESH_API_KEY", "")
model = req.get("model") or os.environ.get("XMESH_MODEL") or ""
if not key or not model:
    print("gemini script needs XMESH_API_KEY and a model", file=sys.stderr)
    sys.exit(2)
contents = []
for msg in req.get("messages") or []:
    role = "model" if msg.get("role") not in ("user", "system") else "user"
    text = msg.get("content") or ""
    if not text:
        continue
    if contents and contents[-1]["role"] == role:
        contents[-1]["parts"].append({"text": text})
    else:
        contents.append({"role": role, "parts": [{"text": text}]})
body = {
    "contents": contents,
    "generationConfig": {"maxOutputTokens": 8192},
}
system = (req.get("system") or "").strip()
if system:
    body["systemInstruction"] = {"parts": [{"text": system}]}
if model.startswith("gemini-4"):
    body["generationConfig"]["thinkingConfig"] = {"includeThoughts": True}
url = "https://generativelanguage.googleapis.com/v1beta/models/" + model + ":generateContent"
http = urllib.request.Request(
    url,
    data=json.dumps(body).encode(),
    headers={"Content-Type": "application/json", "x-goog-api-key": key},
    method="POST",
)
try:
    with urllib.request.urlopen(http, timeout=120) as resp:
        payload = json.loads(resp.read().decode())
except Exception as exc:
    print(str(exc), file=sys.stderr)
    sys.exit(1)
parts = (((payload.get("candidates") or [{}])[0].get("content") or {}).get("parts") or [])
out = []
for part in parts:
    if part.get("thought"):
        continue
    if part.get("text"):
        out.append(part["text"])
text = "".join(out).strip()
if not text:
    print("empty gemini response", file=sys.stderr)
    sys.exit(1)
sys.stdout.write(text)
PY
