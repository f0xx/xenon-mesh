#!/bin/sh
# Bounded project outline for chat context.
# Usage: harvest-context.sh <project-dir>
# Prints a path index and the start of overview files (README, manifests, docs).
# Skips build and dependency directories. Safe to edit when a tree layout changes.

set -eu

root=${1:-.}
cd "$root"

find . -type d \( \
  -name .git -o -name target -o -name node_modules -o -name dist \
  -o -name .venv -o -name vendor -o -name build -o -name .xmesh \
\) -prune -o -type f -print \
  | sed 's#^\./##' \
  | grep -Ev '\.(png|jpg|jpeg|gif|webp|pdf|zip|gz|woff|wasm|so|a|o|lock)$' \
  | sort \
  | head -n 500 \
  | awk 'BEGIN { print "### [TREE]" } { print; n++; if (n >= 400) { print "…"; exit } }'

# Overview files, first 80 lines each, stop around 24k of output.
find . -type f \( \
  -iname 'README*' -o -name 'Cargo.toml' -o -name 'package.json' \
  -o -name 'AGENTS.md' -o -name 'go.mod' -o -name 'pyproject.toml' \
  -o -path './docs/*' \
\) \
  ! -path '*/target/*' ! -path '*/.git/*' ! -path '*/node_modules/*' \
  | sed 's#^\./##' \
  | sort \
  | while IFS= read -r path; do
      echo
      echo "### [CODE: $path]"
      head -n 80 "$path"
    done
