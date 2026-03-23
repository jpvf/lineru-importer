#!/bin/bash
# Starts MySQL in Docker and runs the app on the host (required for Twingate access)
set -e

export PATH=/opt/homebrew/bin:$PATH
export DOCKER_HOST=unix:///Users/juanvelandia/.colima/default/docker.sock

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Load .env
set -a; source .env; set +a

# Start MySQL container only
docker compose up -d mysql
echo "Waiting for MySQL to be healthy..."
until docker inspect aurora-sync-mysql --format='{{.State.Health.Status}}' 2>/dev/null | grep -q healthy; do
    sleep 2
done
echo "MySQL ready."

# Run the app on the host (needs Twingate network access)
LOCAL_HOST=127.0.0.1 \
LOCAL_PORT=3308 \
DATABASE_PATH="$SCRIPT_DIR/data/aurora_sync.db" \
venv/bin/uvicorn app.main:app --host 0.0.0.0 --port "${APP_PORT:-8090}" --workers 1
