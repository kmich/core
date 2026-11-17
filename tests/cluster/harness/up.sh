#!/usr/bin/env bash
# Start the harness from pristine node configs (both nodes share one
# config entry id, as a replicated .storage would give them). The e2e fixture
# does the same steps in Python.
set -euo pipefail
cd "$(dirname "$0")"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
rm -rf runtime
for node in node_a node_b; do
  mkdir -p "runtime/$node"
  cp -r template/. "runtime/$node/"
done
docker compose up -d "$@"
