#!/bin/sh
# Compact project context for an outer model.
# Uses cloc for a size summary, then prints source and header bodies up to a budget.
# If cloc is not installed, falls back to harvest-context.sh beside this file.
# Usage: harvest-context-cloc.sh <project-dir>

set -eu

root=${1:-.}
here=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
if ! command -v cloc >/dev/null 2>&1; then
  exec "$here/harvest-context.sh" "$root"
fi

cd "$root"
echo "### [CLOC]"
cloc . --quiet --hide-rate \
  --exclude-dir=.git,target,node_modules,dist,.venv,vendor,build,.xmesh \
  || true

budget=120000
used=0
find . -type d \( \
  -name .git -o -name target -o -name node_modules -o -name dist \
  -o -name .venv -o -name vendor -o -name build -o -name .xmesh \
\) -prune -o -type f \( \
  -name '*.c' -o -name '*.h' -o -name '*.cc' -o -name '*.cpp' -o -name '*.hpp' \
  -o -name '*.rs' -o -name '*.py' -o -name '*.go' -o -name '*.java' \
  -o -name '*.js' -o -name '*.ts' -o -name '*.cmake' -o -name 'CMakeLists.txt' \
  -o -name '*.toml' -o -name '*.md' -o -name '*.sh' -o -name 'Makefile' \
\) -print \
  | sed 's#^\./##' \
  | sort \
  | while IFS= read -r path; do
      [ -f "$path" ] || continue
      size=$(wc -c < "$path" | tr -d ' ')
      if [ "$size" -gt 32768 ]; then
        echo
        echo "### [CODE: $path]"
        echo "(skipped, ${size} bytes)"
        continue
      fi
      if [ $((used + size)) -gt "$budget" ]; then
        echo
        echo "### [CODE]"
        echo "(stopped: context budget reached)"
        break
      fi
      echo
      echo "### [CODE: $path]"
      cat "$path"
      used=$((used + size))
    done
