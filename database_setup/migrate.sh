#!/bin/bash
# Apply database_setup/migrations/NNN_*.sql in order, each exactly once.
# Applied files are recorded in the table schema_migrations, so you can always see what a
# database has had:   SELECT * FROM schema_migrations ORDER BY version;
#
# Usage (from database_setup/, after setupDB.sh on a new database):   ./migrate.sh
# Reads TIDES_DB_HOST / TIDES_DB_USER / TIDES_DB_DATABASE from ../.env, like setupDB.sh.
# Each file runs in one transaction: it is applied completely or not at all.
set -euo pipefail
cd "$(dirname "$0")"

ENV_FILE="../.env"
get() { grep "^\s*$1=" "$ENV_FILE" | tail -n 1 | cut -d'=' -f2- | tr -d "'\""; }
HOST=$(get TIDES_DB_HOST); USER=$(get TIDES_DB_USER); NAME=$(get TIDES_DB_DATABASE)

echo "Migrating database '$NAME' on $HOST as $USER"
read -p "Proceed? (y/n): " confirm
[[ $confirm == [yY] ]] || exit 1

PSQL=(psql -h "$HOST" -U "$USER" -d "$NAME" -X -q -v ON_ERROR_STOP=1)
"${PSQL[@]}" -c "CREATE TABLE IF NOT EXISTS schema_migrations (
                     version TEXT PRIMARY KEY,
                     applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP);"

for f in migrations/*.sql; do
    v=$(basename "$f" .sql)
    if [ "$("${PSQL[@]}" -tAc "SELECT 1 FROM schema_migrations WHERE version = '$v'")" = "1" ]; then
        echo "  already applied: $v"
        continue
    fi
    echo "  applying:        $v"
    "${PSQL[@]}" --single-transaction -f "$f" -c "INSERT INTO schema_migrations (version) VALUES ('$v');"
done
echo "Done."
