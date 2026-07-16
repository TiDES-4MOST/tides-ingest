#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
query_tides_db.py

Queries the local tides_master database using a user-specified WHERE clause or custom SQL query.
For all matching objects, it performs an action (deactivate or delete) on the 4MOST API database
using the submit_transients module.
"""

import os
import sys
import argparse
import json
import logging
import sqlalchemy
from dotenv import load_dotenv, find_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("query_tides_db")

# Add parent and tides_flows directories to sys.path so we can import submit_transients
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)
sys.path.append(os.path.join(ROOT_DIR, 'tides_flows'))

try:
    import submit_transients as st
except ImportError as e:
    logger.error("Failed to import submit_transients. Ensure you run this from the project structure.")
    logger.error(str(e))
    sys.exit(1)


def parse_api_targets(response):
    """
    Parses the response returned from submit_transients.get_list.
    Handles standard lists, dictionaries, and paginated lists.
    """
    if isinstance(response, str):
        try:
            response = json.loads(response)
        except Exception:
            return []

    if isinstance(response, list):
        if len(response) == 1 and isinstance(response[0], dict) and 'results' in response[0]:
            return response[0].get('results', [])
        return response
    elif isinstance(response, dict):
        if 'results' in response:
            return response['results']
        return [response]
    return []


def load_credentials(env_path=None):
    """
    Loads 4MOST API and database credentials from environment variables / .env file.
    """
    if env_path:
        load_dotenv(env_path)
    else:
        load_dotenv(find_dotenv())

    # Set up 4MOST API Credentials in submit_transients
    st.USERNAME = os.getenv('FOURMOST_USERNAME')
    st.PASSWORD = os.getenv('FOURMOST_PASSWORD')
    schema = os.getenv('FOURMOST_SCHEMA')
    if schema:
        st.URL_SCHEMA = schema
    st.ACCESS_TOKEN = os.getenv('FOURMOST_ACCESS_TOKEN')

    # Get Database Credentials
    db_user = os.getenv('TIDES_DB_USER')
    db_pass = os.getenv('TIDES_DB_PASS') or ''
    db_host = os.getenv('TIDES_DB_HOST') or 'localhost'
    db_port = os.getenv('TIDES_DB_PORT') or '5432'
    db_name = os.getenv('TIDES_DB_DATABASE')

    return db_user, db_pass, db_host, db_port, db_name


def get_db_engine(db_user, db_pass, db_host, db_port, db_name):
    """
    Creates and returns a SQLAlchemy database engine.
    """
    if not all([db_user, db_name]):
        raise ValueError("Missing database credentials (TIDES_DB_USER or TIDES_DB_DATABASE) in .env file.")
    connection_string = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    return sqlalchemy.create_engine(connection_string, future=True)


def main():
    parser = argparse.ArgumentParser(
        description="Query local tides_master database and deactivate/delete matching targets on 4MOST API.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--where", type=str, default="active = True",
        help="SQL WHERE clause to filter tides_master table."
    )
    group.add_argument(
        "--query", type=str, default=None,
        help="Full custom SQL query to execute. Must return tides_id, pk_4most, and name."
    )

    parser.add_argument(
        "--action", type=str, required=True, choices=["deactivate", "delete"],
        help="Action to perform on the 4MOST API database."
    )
    parser.add_argument(
        "--match-by", type=str, required=True, choices=["pk", "name"],
        help="Choose whether to locate targets on 4MOST by primary key (pk_4most) or by name."
    )
    parser.add_argument(
        "--sync-local", action="store_true", default=True,
        help="Synchronize the status back to the local tides_master database."
    )
    parser.add_argument(
        "--no-sync-local", dest="sync_local", action="store_false",
        help="Do NOT synchronize status back to the local database."
    )
    parser.add_argument(
        "--extra-data", type=str, default=None,
        help="Optional JSON string containing additional features/properties to update on 4MOST (for deactivate)."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview target matching and actions without modifying 4MOST or the local database."
    )
    parser.add_argument(
        "--env", type=str, default=None,
        help="Path to an alternative .env file containing credentials."
    )

    args = parser.parse_args()

    # Load credentials & establish connection
    db_user, db_pass, db_host, db_port, db_name = load_credentials(args.env)
    
    # Simple validation of API credentials before proceeding
    if not st.ACCESS_TOKEN and not (st.USERNAME and st.PASSWORD):
        logger.error("Missing 4MOST API credentials in environment or .env file.")
        sys.exit(1)

    try:
        engine = get_db_engine(db_user, db_pass, db_host, db_port, db_name)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        sys.exit(1)

    # Determine query to run
    if args.query:
        query_str = args.query
    else:
        query_str = f"SELECT tides_id, pk_4most, name FROM tides_master WHERE {args.where};"

    logger.info(f"Querying local database: {query_str}")
    try:
        with engine.connect() as conn:
            result = conn.execute(sqlalchemy.text(query_str))
            rows = result.mappings().all()
    except Exception as e:
        logger.error(f"SQL execution failed: {e}")
        sys.exit(1)

    if not rows:
        logger.info("No matching targets found in local database.")
        sys.exit(0)

    # Validate output columns
    first_row = rows[0]
    required_cols = {'tides_id', 'pk_4most', 'name'}
    missing_cols = required_cols - set(first_row.keys())
    if missing_cols:
        logger.error(f"Query missing required columns: {missing_cols}")
        sys.exit(1)

    logger.info(f"Found {len(rows)} matching target(s) locally.")
    if args.dry_run:
        logger.info("--- DRY-RUN MODE: No changes will be saved to 4MOST or the database ---")

    success_count = 0
    fail_count = 0
    skipped_count = 0

    for idx, row in enumerate(rows, start=1):
        tides_id = row['tides_id']
        pk_4most = row['pk_4most']
        name = row['name']

        logger.info(f"[{idx}/{len(rows)}] Processing target '{name}' (tides_id={tides_id}, pk_4most={pk_4most})")

        # Resolve 4MOST Primary Key (target_pk)
        target_pk = None
        if args.match_by == 'pk':
            if pk_4most is None:
                logger.warning(f"Skipping: pk_4most is NULL in local DB for target '{name}'.")
                skipped_count += 1
                continue
            try:
                target_pk = int(pk_4most)
            except ValueError:
                logger.error(f"Skipping: Invalid non-integer pk_4most '{pk_4most}' for target '{name}'.")
                skipped_count += 1
                continue
        else:  # match-by name
            if not name:
                logger.warning(f"Skipping: target name is empty for tides_id={tides_id}.")
                skipped_count += 1
                continue
            logger.info(f"Resolving name '{name}' via 4MOST API...")
            api_resp = st.get_list(flt=f"name={name}")
            targets = parse_api_targets(api_resp)
            if not targets:
                logger.warning(f"Skipping: Target '{name}' not found in 4MOST API database.")
                skipped_count += 1
                continue
            elif len(targets) > 1:
                logger.warning(f"Warning: Multiple targets ({len(targets)}) found for name '{name}' on 4MOST API. Using first match.")
            target_pk = int(targets[0]['id'])
            logger.info(f"Resolved target '{name}' to 4MOST pk={target_pk}")

        # Perform Action
        if args.dry_run:
            logger.info(f"[DRY-RUN] Would {args.action} target on 4MOST (pk={target_pk})")
            if args.sync_local:
                logger.info(f"[DRY-RUN] Would synchronize local tides_master (tides_id={tides_id})")
            success_count += 1
            continue

        try:
            if args.action == 'delete':
                logger.info(f"Deleting target {name} (4MOST pk={target_pk}) on API...")
                resp = st.delete_transient(pk=target_pk, printout=False, return_mode="response")
                if resp.status_code == 404:
                    logger.warning(f"Target '{name}' (4MOST pk={target_pk}) not found on 4MOST API (404).")
                    # If it doesn't exist on 4MOST, we can optionally still clean up local DB
                    if args.sync_local:
                        logger.info("Syncing local DB to remove invalid pk_4most...")
                        with engine.connect() as conn, conn.begin():
                            conn.execute(sqlalchemy.text("""
                                UPDATE tides_master
                                SET active = False, pk_4most = NULL, sync_pending = False, updated = CURRENT_TIMESTAMP
                                WHERE tides_id = :tides_id
                            """), {"tides_id": tides_id})
                    success_count += 1
                    continue
                elif resp.status_code not in (200, 204):
                    logger.error(f"Failed to delete target '{name}' on 4MOST. Code: {resp.status_code}. Response: {resp.text}")
                    fail_count += 1
                    continue
                
                logger.info(f"Deleted target '{name}' on 4MOST API.")

                if args.sync_local:
                    with engine.connect() as conn, conn.begin():
                        conn.execute(sqlalchemy.text("""
                            UPDATE tides_master
                            SET active = False, pk_4most = NULL, sync_pending = False, updated = CURRENT_TIMESTAMP
                            WHERE tides_id = :tides_id
                        """), {"tides_id": tides_id})
                    logger.info("Local database updated: active=False, pk_4most=NULL, sync_pending=False.")
                
                success_count += 1

            elif args.action == 'deactivate':
                logger.info(f"Deactivating target {name} (4MOST pk={target_pk}) on API...")
                data_to_send = {"is_active": False}
                if args.extra_data:
                    try:
                        extra = json.loads(args.extra_data)
                        data_to_send.update(extra)
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid extra-data JSON: {e}")
                        fail_count += 1
                        continue

                resp = st.update_transient(pk=target_pk, data=data_to_send, printout=False, return_mode="response")
                if resp.status_code == 404:
                    logger.warning(f"Target '{name}' (4MOST pk={target_pk}) not found on 4MOST API (404).")
                    if args.sync_local:
                        logger.info("Syncing local DB to remove invalid pk_4most...")
                        with engine.connect() as conn, conn.begin():
                            conn.execute(sqlalchemy.text("""
                                UPDATE tides_master
                                SET active = False, pk_4most = NULL, sync_pending = False, updated = CURRENT_TIMESTAMP
                                WHERE tides_id = :tides_id
                            """), {"tides_id": tides_id})
                    success_count += 1
                    continue
                elif resp.status_code not in (200, 201, 204):
                    logger.error(f"Failed to deactivate target '{name}' on 4MOST. Code: {resp.status_code}. Response: {resp.text}")
                    fail_count += 1
                    continue

                logger.info(f"Deactivated target '{name}' on 4MOST API.")

                if args.sync_local:
                    with engine.connect() as conn, conn.begin():
                        conn.execute(sqlalchemy.text("""
                            UPDATE tides_master
                            SET active = False, sync_pending = False, updated = CURRENT_TIMESTAMP
                            WHERE tides_id = :tides_id
                        """), {"tides_id": tides_id})
                    logger.info("Local database updated: active=False, sync_pending=False.")

                success_count += 1

        except Exception as e:
            logger.error(f"Exception encountered for target '{name}': {e}")
            fail_count += 1

    logger.info("--- Execution Summary ---")
    logger.info(f"Successfully processed: {success_count}")
    logger.info(f"Failed:                 {fail_count}")
    logger.info(f"Skipped (Not Found):    {skipped_count}")


if __name__ == '__main__':
    main()
