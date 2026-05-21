#!/usr/bin/env bash
# Local script to test, upgrade, and re-test dependencies.
# Mirrors the logic in .github/workflows/update-deps.yml so you can verify
# a dependency upgrade locally before the CI workflow opens a PR.
#
# Usage:
#   bash scripts/update_deps.sh
#
# Requirements: uv must be installed and available on PATH.

set -euo pipefail

echo "=== [1/5] Installing project with locked dependencies ==="
uv sync --locked --all-extras

echo ""
echo "=== [2/5] Running baseline tests ==="
uv run pytest tests/ -v

echo ""
echo "=== [3/5] Upgrading dependencies ==="
uv lock --upgrade | tee /tmp/uv_upgrade_report.txt

echo ""
echo "=== [4/5] Installing updated dependencies ==="
uv sync --all-extras

echo ""
echo "=== [5/5] Running tests with updated dependencies ==="
uv run pytest tests/ -v

echo ""
echo "=== All steps passed. Review uv.lock changes and commit if satisfied. ==="
