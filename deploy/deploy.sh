#!/usr/bin/env bash
# deploy.sh: build the flow image from the CURRENT COMMIT and (re)deploy the TiDES deployments
# (tides-ingest/spo_4most-ingest hourly, tides-ostd-sync/spo_4most-ostd-sync daily; see prefect.yaml).
#
#   cd /opt/tides/tides-ingest && git pull && deploy/deploy.sh
#
# It reads /etc/tides/worker.env (Prefect API URL + auth) and /etc/tides/deploy.env (TIDES_ENV_FILE,
# optional overrides), refuses to run on
# uncommitted code (the image tag IS the commit, recorded in every provenance row), and then runs
# `prefect deploy --all` with the venv's Prefect. Running flows are not affected; the next scheduled
# run uses the new image. Roll back = check out the previous commit and run this again (old images
# stay on the host: docker image ls spo_4most_ingest).
set -euo pipefail
cd "$(dirname "$0")/.."

for f in ${WORKER_ENV:-/etc/tides/worker.env} ${DEPLOY_ENV:-/etc/tides/deploy.env}; do
    if [ -r "$f" ]; then set -a; . "$f"; set +a; else echo "note: $f not readable, using the environment" >&2; fi
done

PREFECT_BIN=${PREFECT_BIN:-/opt/tides/venv/bin/prefect}
: "${TIDES_ENV_FILE:?absolute path of the production .env (set in /etc/tides/deploy.env)}"
: "${PREFECT_API_URL:?the Prefect API, e.g. http://127.0.0.1:4200/api (set in /etc/tides/worker.env)}"
export TIDES_DOCKER_NETWORK=${TIDES_DOCKER_NETWORK:-prefect_net}
export PREFECT_INTERNAL_API_URL=${PREFECT_INTERNAL_API_URL:-http://server:4200/api}

case "$TIDES_ENV_FILE" in /*) ;; *) echo "TIDES_ENV_FILE must be an absolute path" >&2; exit 1 ;; esac
[ -f "$TIDES_ENV_FILE" ] || { echo "TIDES_ENV_FILE=$TIDES_ENV_FILE does not exist" >&2; exit 1; }
[ -x "$PREFECT_BIN" ] || { echo "no Prefect at $PREFECT_BIN (set PREFECT_BIN)" >&2; exit 1; }
docker network inspect "$TIDES_DOCKER_NETWORK" >/dev/null 2>&1 \
    || { echo "Docker network $TIDES_DOCKER_NETWORK not found: is deploy/prefect-server up?" >&2; exit 1; }
curl -fsS -m 10 "$PREFECT_API_URL/health" >/dev/null || { echo "Prefect API $PREFECT_API_URL not reachable" >&2; exit 1; }

if [ -n "$(git status --porcelain --untracked-files=no)" ] && [ "${ALLOW_DIRTY:-0}" != 1 ]; then
    echo "Uncommitted changes in $(pwd): commit (and push) first. The image tag and the git_sha in the" >&2
    echo "provenance tables must identify exactly the code that ran. (ALLOW_DIRTY=1 only on a test server.)" >&2
    exit 1
fi
export GIT_SHA=$(git rev-parse --short=12 HEAD)
[ "${ALLOW_DIRTY:-0}" = 1 ] && [ -n "$(git status --porcelain --untracked-files=no)" ] && GIT_SHA="${GIT_SHA}-dirty"

echo "Deploying commit $GIT_SHA ($(git log -1 --format=%s))"
echo "  .env mounted into runs: $TIDES_ENV_FILE"
echo "  network: $TIDES_DOCKER_NETWORK   API (inside the network): $PREFECT_INTERNAL_API_URL"
"$PREFECT_BIN" --no-prompt deploy --all
echo
echo "Done. Image: spo_4most_ingest:$GIT_SHA. Check: $PREFECT_BIN deployment ls; the next run uses it."
