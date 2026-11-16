#!/usr/bin/env bash
# tides-db-backup.sh: nightly logical backups of
#   1. the TiDES database (on the database VM): THE provenance record. It cannot be rebuilt.
#   2. the Prefect server's database (in Docker on this VM): run history, deployments, automations.
# Run by tides-db-backup.timer as root. Settings: /etc/tides/backup.env (see backup.env.example).
#
# - Uses pg_dump from the postgres:17 image, so the client always matches the server version
#   (Ubuntu's own pg_dump is older and refuses to dump a newer server).
# - Each dump is checked with pg_restore --list. On ANY failure a message goes to the alert webhook.
# - Keeps daily dumps for KEEP_DAYS days, and the dump of the 1st of each month for KEEP_MONTHLY_DAYS.
# - OFFSITE_CMD (optional) is run after a successful night, e.g. an rclone/rsync copy to other
#   storage. A backup that only exists on the same cloud is not a backup.
#
# Restore (into an EMPTY database; see the operations runbook):
#   docker run --rm -i --network host -v /etc/tides/backup.pgpass:/pgpass:ro -e PGPASSFILE=/pgpass \
#     postgres:17 pg_restore -h HOST -U ADMIN -d EMPTY_DB --no-owner < tides_YYYY-MM-DD.dump
set -uo pipefail

ENV_FILE=${1:-/etc/tides/backup.env}
# shellcheck disable=SC1090
. "$ENV_FILE" || { echo "cannot read $ENV_FILE" >&2; exit 1; }
: "${BACKUP_DIR:?}" "${TIDES_DB_HOST:?}" "${TIDES_DB_USER:?}" "${TIDES_DB_DATABASE:?}"
TIDES_DB_PORT=${TIDES_DB_PORT:-5432}
PGPASS_FILE=${PGPASS_FILE:-/etc/tides/backup.pgpass}
PG_IMAGE=${PG_IMAGE:-postgres:17}
PREFECT_COMPOSE_PROJECT=${PREFECT_COMPOSE_PROJECT:-tides-prefect}
KEEP_DAYS=${KEEP_DAYS:-14}
KEEP_MONTHLY_DAYS=${KEEP_MONTHLY_DAYS:-2000}

DAY=$(date -u +%F)
HOST=$(hostname -s)
mkdir -p "$BACKUP_DIR/tides" "$BACKUP_DIR/prefect"
chmod 700 "$BACKUP_DIR"
umask 077
errors=()

alert() {   # alert TEXT: post to the Slack/Mattermost incoming webhook, if configured
    echo "$1" >&2
    [ -n "${ALERT_WEBHOOK_URL:-}" ] || return 0
    local text=${1//\\/\\\\}; text=${text//\"/\\\"}
    curl -fsS -m 20 -H 'Content-Type: application/json' \
         -d "{\"text\": \"$text\"}" "$ALERT_WEBHOOK_URL" >/dev/null || echo "alert webhook failed" >&2
}

check_dump() {   # check_dump FILE: non-empty and readable by pg_restore
    [ -s "$1" ] && docker run --rm -i "$PG_IMAGE" pg_restore --list < "$1" >/dev/null 2>&1
}

# 1. TiDES database (remote), with the matching pg_dump from the postgres image
out="$BACKUP_DIR/tides/${TIDES_DB_DATABASE}_${DAY}.dump"
if docker run --rm --network host -v "$PGPASS_FILE:/pgpass:ro" -e PGPASSFILE=/pgpass "$PG_IMAGE" \
       pg_dump -Fc -h "$TIDES_DB_HOST" -p "$TIDES_DB_PORT" -U "$TIDES_DB_USER" -d "$TIDES_DB_DATABASE" \
       > "$out.part" && check_dump "$out.part"; then
    mv "$out.part" "$out"
    echo "TiDES database: $out ($(du -h "$out" | cut -f1))"
else
    rm -f "$out.part"; errors+=("TiDES database $TIDES_DB_DATABASE@$TIDES_DB_HOST")
fi

# 2. Prefect database (the db container of the compose project)
cid=$(docker ps -q --filter "label=com.docker.compose.project=$PREFECT_COMPOSE_PROJECT" \
                   --filter "label=com.docker.compose.service=db" | head -1)
out="$BACKUP_DIR/prefect/prefect_${DAY}.dump"
if [ -n "$cid" ] && docker exec "$cid" pg_dump -Fc -U prefect -d prefect > "$out.part" && check_dump "$out.part"; then
    mv "$out.part" "$out"
    echo "Prefect database: $out ($(du -h "$out" | cut -f1))"
else
    rm -f "$out.part"; errors+=("Prefect database (container of project $PREFECT_COMPOSE_PROJECT)")
fi

# 3. Retention: daily dumps for KEEP_DAYS days; dumps of the 1st of the month for KEEP_MONTHLY_DAYS
find "$BACKUP_DIR/tides" "$BACKUP_DIR/prefect" -name '*.dump' ! -name '*-01.dump' -mtime +"$KEEP_DAYS" -delete
find "$BACKUP_DIR/tides" "$BACKUP_DIR/prefect" -name '*-01.dump' -mtime +"$KEEP_MONTHLY_DAYS" -delete

# 4. Off-site copy
if [ ${#errors[@]} -eq 0 ] && [ -n "${OFFSITE_CMD:-}" ]; then
    bash -c "$OFFSITE_CMD" || errors+=("off-site copy (OFFSITE_CMD)")
fi

df -h "$BACKUP_DIR" | tail -1
if [ ${#errors[@]} -gt 0 ]; then
    alert ":warning: TiDES backup on $HOST FAILED for: ${errors[*]} (journalctl -u tides-db-backup)"
    exit 1
fi
echo "$(date -u '+%F %T') OK" > "$BACKUP_DIR/LAST_SUCCESS"
