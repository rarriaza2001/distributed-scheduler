#!/usr/bin/env sh
# Redis-only queue benchmark (see backend/cmd/main).
set -e
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT/backend"
[ -n "${REDIS_ADDR:-}" ] && echo "Using REDIS_ADDR=$REDIS_ADDR"
exec go run ./cmd/main
