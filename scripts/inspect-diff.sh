#!/bin/sh
# Read a unified diff on stdin and print one finding per line.
# Extra arguments are a command. The same diff is written to its stdin and
# its stdout is forwarded. A non-zero status becomes one finding.
# This script does not modify the tree and does not decide approval.
set -eu
diff=$(cat)
printf '%s\n' "$diff" | awk '
  /^diff --git / { file=$NF; sub(/^b\//, "", file); line=0; next }
  /^@@ / {
    for (i = 1; i <= NF; i++) if ($i ~ /^\+[0-9]/) {
      split(substr($i, 2), a, ",")
      line = a[1] + 0
    }
    next
  }
  /^\+/ && !/^\+\+\+/ {
    body = substr($0, 2)
    if (body ~ /[ \t]$/) printf "lint: %s:%d: trailing whitespace\n", file, line
    if (length(body) > 160) printf "lint: %s:%d: line is longer than 160 characters\n", file, line
    line++
    next
  }
  /^\\/ { next }
  /^-/ && !/^---/ { next }
  { if (line > 0) line++ }
'
if [ "$#" -gt 0 ]; then
  if ! printf '%s\n' "$diff" | "$@"; then
    echo "lint: wrapper command failed"
  fi
fi
exit 0
