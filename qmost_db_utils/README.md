# 4MOST API Target Management Utilities (`qmost_db_utils`)

This directory contains convenience command-line utilities to query, deactivate, or delete transient targets on the 4MOST API database, and optionally synchronize these changes back to the local `tides_master` database.

---

## Prerequisites

These scripts depend on the root directory's configuration and `tides_flows/submit_transients.py`.

### Environment Variables
The utilities read database credentials and 4MOST API credentials from your `.env` file at the project root:

```bash
# 4MOST API Credentials
FOURMOST_USERNAME='your_username'
FOURMOST_PASSWORD='your_password'
FOURMOST_SCHEMA="https://4most.mpe.mpg.de/QFSwi/targetCat/transients/"
FOURMOST_ACCESS_TOKEN='your_access_token'

# TiDES Database Credentials
TIDES_DB_USER='your_db_user'
TIDES_DB_PASS='your_db_password'
TIDES_DB_DATABASE='your_db_name'
TIDES_DB_PORT='5432'
TIDES_DB_HOST='localhost'
```

### Python Dependencies
Ensure your virtual environment is active and libraries are installed:
```bash
pip install -r requirements.txt
```

---

## 1. Query Local Database and Modify 4MOST API (`query_tides_db.py`)

This script queries the local `tides_master` database based on a WHERE clause or custom SQL statement, retrieves matching targets, resolves their identifiers on 4MOST, and performs a bulk `deactivate` or `delete` operation.

### Usage
```bash
python qmost_db_utils/query_tides_db.py --action [deactivate|delete] --match-by [pk|name] [options]
```

### Key Arguments
*   `--action` *(required)*: Action to execute.
    *   `deactivate`: Sets `is_active=False` on the 4MOST database.
    *   `delete`: Deletes target from the 4MOST database (note: only possible if target has not yet been ingested).
*   `--match-by` *(required)*: Specifies how to identify the target on the 4MOST API:
    *   `pk`: Uses the local `pk_4most` database column to target the exact 4MOST primary key.
    *   `name`: Queries the 4MOST API search endpoint for targets matching the target's `name` to retrieve its primary key first. Useful if `pk_4most` is missing or invalid in the local database.
*   `--where`: A custom SQL where clause to filter rows from the `tides_master` database. (Default: `"active = True"`)
*   `--query`: A full custom SQL query to execute. Must select at least `tides_id`, `pk_4most`, and `name`. (Mutually exclusive with `--where`)
*   `--sync-local` / `--no-sync-local`: Whether to update the local `tides_master` rows. Deactivation marks them as inactive locally. Deletion marks them inactive and clears the `pk_4most` identifier. (Default: `True` / enabled)
*   `--extra-data`: Optional JSON string (e.g. `'{"mag": 21.0}'`) containing other parameters to update during deactivation.
*   `--dry-run`: Performs a dry-run and prints the list of matching local targets and the API requests that would be sent, without making any modifications.

### Examples

**Dry-run to verify which active local targets would be deactivated**:
```bash
python qmost_db_utils/query_tides_db.py --where "active = True" --action deactivate --match-by name --dry-run
```

**Deactivate matching targets by their name matching a pattern**:
```bash
python qmost_db_utils/query_tides_db.py --where "name LIKE 'TiDES26%'" --action deactivate --match-by name
```

**Delete specific target IDs using the local pk_4most mappings**:
```bash
python qmost_db_utils/query_tides_db.py --where "tides_id IN (124, 125, 126)" --action delete --match-by pk
```

---

## 2. Cleanup 4MOST Database by Date (`cleanup_by_date.py`)

Queries the 4MOST API database directly for all targets submitted/created after a user-specified date and performs a bulk `deactivate` or `delete` operation.

### Usage
```bash
python qmost_db_utils/cleanup_by_date.py --date "YYYY-MM-DD" --action [deactivate|delete] [options]
```

### Key Arguments
*   `--date` *(required)*: Date threshold in `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS` format.
*   `--operator`: Date comparison operator.
    *   `gt`: Greater than (default).
    *   `gte`: Greater than or equal to.
*   `--action` *(required)*: Action to execute (`deactivate` or `delete`).
*   `--sync-local` / `--no-sync-local`: Whether to propagate the deletion/deactivation to matching local database records. (Default: `True`)
*   `--match-by`: Determines how local records are matched to targets returned by the API during local synchronization:
    *   `pk`: Matches the API ID to the local `pk_4most` column.
    *   `name`: Matches by name.
*   `--extra-data`: Optional JSON string containing other parameters to update during deactivation.
*   `--dry-run`: Performs a dry-run query to view matching API targets without altering database/API state.

### Examples

**Preview all targets created in 4MOST after July 1st, 2026**:
```bash
python qmost_db_utils/cleanup_by_date.py --date "2026-07-01" --action deactivate --dry-run
```

**Deactivate all targets created in 4MOST on or after July 10th, 2026 and sync local DB**:
```bash
python qmost_db_utils/cleanup_by_date.py --date "2026-07-10" --operator gte --action deactivate --match-by name
```

**Delete targets created after a specific datetime**:
```bash
python qmost_db_utils/cleanup_by_date.py --date "2026-07-12 14:00:00" --action delete --match-by pk
```
