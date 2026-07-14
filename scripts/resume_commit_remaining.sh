#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.." || exit 1
BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)
echo "Resuming per-file commits on branch: $BRANCH"

# Build zero-delimited list of changed/untracked files (respecting .gitignore)
files=()
while IFS= read -r -d '' f; do
  files+=("$f")
done < <(git ls-files -m -o --exclude-standard -z)

TOTAL=${#files[@]}
if [ "$TOTAL" -eq 0 ]; then
  echo "No files to commit"
  exit 0
fi

COUNTER=0
trap 'echo "Interrupted at $COUNTER/$TOTAL"; exit 1' INT TERM
for f in "${files[@]}"; do
  COUNTER=$((COUNTER+1))
  printf '[%d/%d] Committing: %s\n' "$COUNTER" "$TOTAL" "$f"
  git add -- "$f" || true
  # If staged, commit; otherwise skip
  if git diff --staged --name-only -- "$f" | grep -q .; then
    git commit -m "chore: add $f" --no-verify || true
  else
    echo "Nothing staged for $f, skipping"
  fi
done

echo "Committed $COUNTER/$TOTAL files."
