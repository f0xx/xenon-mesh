#!/bin/sh
# Web search for the google model.
# The query is stdin. XMESH_UA and XMESH_SEARCH_URL come from the provider.
# {query} in the URL is replaced with the encoded question.
# Edit this script when the search page changes. No daemon rebuild is required.

set -eu

query=$(cat)
ua=${XMESH_UA:-Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36}
template=${XMESH_SEARCH_URL:-}
if [ -z "$template" ]; then
  template='https://www.google.com/search?hl=en&udm=50&q={query}'
fi

enc=$(printf '%s' "$query" | python3 -c 'import urllib.parse,sys; print(urllib.parse.quote(sys.stdin.read()[:300]))')
url=$(printf '%s' "$template" | sed "s/{query}/$enc/g")

fetch() {
  curl -fsSL --compressed --max-time 25 \
    -A "$ua" \
    -H 'Accept-Language: en' \
    -H 'Accept: text/html' \
    "$1" || true
}

browser=""
for candidate in chromium chromium-browser google-chrome google-chrome-stable; do
  if command -v "$candidate" >/dev/null 2>&1; then
    browser=$candidate
    break
  fi
done

body=""
if [ -n "$browser" ]; then
  body=$("$browser" --headless --disable-gpu --virtual-time-budget=8000 --dump-dom "$url" 2>/dev/null || true)
fi
if [ -z "$body" ]; then
  body=$(fetch "$url")
fi
if [ -z "$body" ]; then
  plain=$(printf '%s' "https://www.google.com/search?hl=en&q={query}" | sed "s/{query}/$enc/g")
  body=$(fetch "$plain")
fi

case "$body" in
  *trouble\ accessing\ Google\ Search*|*enablejs*|"")
    printf 'OPEN_BROWSER\t%s\n' "$url"
    exit 0
    ;;
esac

printf '%s' "$body" | python3 -c '
import re, sys
html = sys.stdin.read()
html = re.sub(r"(?is)<(script|style|noscript).*?>.*?</\1>", " ", html)
html = re.sub(r"(?is)<br\s*/?>", "\n", html)
html = re.sub(r"(?is)</p>", "\n", html)
text = re.sub(r"(?is)<[^>]+>", " ", html)
text = re.sub(r"&nbsp;", " ", text)
text = re.sub(r"&amp;", "&", text)
text = re.sub(r"&lt;", "<", text)
text = re.sub(r"&gt;", ">", text)
text = re.sub(r"[ \t]+", " ", text)
text = re.sub(r"\n\s+", "\n", text)
lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
# Drop the consent and navigation noise that a logged-out Google page repeats.
skip = ("cookie", "sign in", "privacy", "terms", "settings", "before you continue")
kept = [ln for ln in lines if not any(s in ln.lower() for s in skip)]
out = "\n".join(kept[:80])
print(out[:8000] if out else "The search page had no readable text.")
'
