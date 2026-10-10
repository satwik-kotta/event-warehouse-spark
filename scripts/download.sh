#!/usr/bin/env bash
# Fetch the raw JSON at the pinned commit (about 13 GB, 8,551 files) into data/raw.
# Files are pulled individually: a normal clone of the source repository is far larger.
set -euo pipefail
cd "$(dirname "$0")/.."
SHA=$(python -c "from warehouse.config import SOURCE_COMMIT; print(SOURCE_COMMIT)" 2>/dev/null \
      || sed -n 's/^SOURCE_COMMIT = "\(.*\)"/\1/p' warehouse/config.py)
BASE="https://raw.githubusercontent.com/statsbomb/open-data/$SHA"
TMP=$(mktemp -d)
git clone -q --depth 1 --filter=blob:none --no-checkout https://github.com/statsbomb/open-data "$TMP/src"
git -C "$TMP/src" fetch -q --depth 1 origin "$SHA"
git -C "$TMP/src" ls-tree -r --name-only "$SHA" \
  | grep -E '^data/(competitions\.json|matches/|lineups/|events/)' | sed 's#^data/##' > "$TMP/files.txt"
echo "downloading $(wc -l < "$TMP/files.txt") files"
mkdir -p data/raw && cd data/raw
xargs -P 16 -I{} curl -sS --fail --retry 5 --retry-all-errors --create-dirs -o {} "$BASE/data/{}" < "$TMP/files.txt"
echo "done: $(find . -name '*.json' | wc -l) files, $(du -sh . | cut -f1)"
