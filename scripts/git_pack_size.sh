#!/usr/bin/env bash
# Reports git pack size after aggressive GC. Run before and after switching
# DB storage formats to compare how well git compresses successive snapshots.
set -e

echo "Running git gc --aggressive (this may take a moment)..."
git gc --aggressive --quiet

echo ""
git count-objects -vH | grep -E "^(count|size-pack):"
