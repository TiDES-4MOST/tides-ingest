#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
cleanup_by_date.py

Queries the 4MOST API database for all targets created/submitted after a user-specified date
and performs an action (deactivate or delete) on them.
It also optionally synchronizes this status back to the local tides_master database.
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
logger = logging.getLogger("cleanup_by_date")

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

    # Get Database Credentials for local sync
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


def fetch_all_targets(flt):
    """
    Queries the 4MOST API for transients with the given filter.
    Handles paginated response and automatically loops to fetch all pages.
    """
    logger.info(f"Querying 4MOST API with filter: {flt}")
    response = st.get_list(flt=flt, limit=None)

    if isinstance(response, str):
        try:
            response = json.loads(response)
        except Exception:
            logger.error(f"Error string returned from API: {response}")
            return []

    targets = []
    if isinstance(response, list):
        if len(response) == 1 and isinstance(response[0], dict) and 'results' in response[0]:
            # Response is a paginated dictionary wrapped in a list
            results_dict = response[0]
            targets = results_dict.get('results', [])
            next_url = results_dict.get('next')
            
            while next_url:
                logger.info(f"Fetching next page: {next_url}")
                session = st.get_session(username=st.USERNAME, password=st.PASSWORD, token=st.ACCESS_TOKEN)
                r = session.get(next_url, timeout=15)
                page_data = st.check_request(request=r, caller="get_list_page()")
                if isinstance(page_data, list) and len(page_data) == 1 and isinstance(page_data[0], dict):
                    page_dict = page_data[0]
                    targets.extend(page_dict.get('results', []))
                    next_url = page_dict.get('next')
                else:
                    break
        else:
            targets = response
    elif isinstance(response, dict):
        if 'results' in response:
            targets = response['results']
        else:
            targets = [response]

    return targets


def main():
    parser = argparse.ArgumentParser(
        description="Query 4MOST API database for targets created after a certain date and deactivate/delete them.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--date", type=str, required=True,
        help="Date threshold in YYYY-MM-DD or ISO-8601 format (e.g. '2026-07-01' or '2026-07-01 12:00:00')."
    )
    parser.add_argument(
        "--operator", type=str, default="gt", choices=["gt", "gte"],
        help="Comparison operator for date query: 'gt' (greater than) or 'gte' (greater than or equal)."
    )
    parser.add_argument(
        "--action", type=str, required=True, choices=["deactivate", "delete"],
        help="Action to perform on matching targets: deactivate or delete."
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
        "--match-by", type=str, default="pk", choices=["pk", "name"],
        help="Field to match on local database for local sync: 'pk' (match id to pk_4most) or 'name'."
    )
    parser.add_argument(
        "--extra-data", type=str, default=None,
        help="Optional JSON string containing additional features/properties to update on 4MOST (for deactivate)."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview target queries and actions without executing changes."
    )
    parser.add_argument(
        "--env", type=str, default=None,
        help="Path to an alternative .env file containing credentials."
    )

    args = parser.parse_args()

    # Load credentials
    db_user, db_pass, db_host, db_port, db_name = load_credentials(args.env)

    # Simple validation of API credentials before proceeding
    if not st.ACCESS_TOKEN and not (st.USERNAME and st.PASSWORD):
        logger.error("Missing 4MOST API credentials in environment or .env file.")
        sys.exit(1)

    # Establish db engine if local sync is enabled
    engine = None
    if args.sync_local:
        try:
            engine = get_db_engine(db_user, db_pass, db_host, db_port, db_name)
        except Exception as e:
            logger.error(f"Failed to connect to database for local sync: {e}")
            logger.error("Proceeding without local sync option.")
            args.sync_local = False

    # Fetch matching targets from the 4MOST API
    flt = f"date_submitted__{args.operator}={args.date}"
    targets = fetch_all_targets(flt)

    if not targets:
        logger.info("No matching targets found in the 4MOST API database.")
        sys.exit(0)

    logger.info(f"Found {len(targets)} matching target(s) on 4MOST API.")
    if args.dry_run:
        logger.info("--- DRY-RUN MODE: No changes will be saved to 4MOST or the database ---")

    success_count = 0
    fail_count = 0

    for idx, target in enumerate(targets, start=1):
        target_pk = target.get('id')
        name = target.get('name')

        if target_pk is None:
            logger.warning(f"[{idx}/{len(targets)}] Skipping target with missing ID. Details: {target}")
            fail_count += 1
            continue

        logger.info(f"[{idx}/{len(targets)}] Processing target '{name}' (4MOST pk={target_pk})")

        if args.dry_run:
            logger.info(f"[DRY-RUN] Would {args.action} target on 4MOST (pk={target_pk})")
            if args.sync_local:
                logger.info(f"[DRY-RUN] Would synchronize local tides_master matching by {args.match_by}")
            success_count += 1
            continue

        try:
            if args.action == 'delete':
                logger.info(f"Deleting target '{name}' (4MOST pk={target_pk}) on API...")
                resp = st.delete_transient(pk=target_pk, printout=False, return_mode="response")
                if resp.status_code not in (200, 204):
                    logger.error(f"Failed to delete target '{name}' on 4MOST. Code: {resp.status_code}. Response: {resp.text}")
                    fail_count += 1
                    continue
                logger.info(f"Deleted target '{name}' on 4MOST API.")

                # Synchronize local database if requested
                if args.sync_local:
                    if args.match_by == 'pk':
                        update_q = sqlalchemy.text("""
                            UPDATE tides_master
                            SET active = False, pk_4most = NULL, sync_pending = False, updated = CURRENT_TIMESTAMP
                            WHERE pk_4most = :target_pk
                        """)
                        params = {"target_pk": target_pk}
                    else:
                        update_q = sqlalchemy.text("""
                            UPDATE tides_master
                            SET active = False, pk_4most = NULL, sync_pending = False, updated = CURRENT_TIMESTAMP
                            WHERE name = :name
                        """)
                        params = {"name": name}

                    with engine.connect() as conn, conn.begin():
                        res = conn.execute(update_q, params)
                        if res.rowcount > 0:
                            logger.info(f"Locally synchronized {res.rowcount} row(s) to active=False, pk_4most=NULL.")
                        else:
                            logger.info("No matching local row found to update.")

                success_count += 1

            elif args.action == 'deactivate':
                logger.info(f"Deactivating target '{name}' (4MOST pk={target_pk}) on API...")
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
                if resp.status_code not in (200, 201, 204):
                    logger.error(f"Failed to deactivate target '{name}' on 4MOST. Code: {resp.status_code}. Response: {resp.text}")
                    fail_count += 1
                    continue
                logger.info(f"Deactivated target '{name}' on 4MOST API.")

                # Synchronize local database if requested
                if args.sync_local:
                    if args.match_by == 'pk':
                        update_q = sqlalchemy.text("""
                            UPDATE tides_master
                            SET active = False, sync_pending = False, updated = CURRENT_TIMESTAMP
                            WHERE pk_4most = :target_pk
                        """)
                        params = {"target_pk": target_pk}
                    else:
                        update_q = sqlalchemy.text("""
                            UPDATE tides_master
                            SET active = False, sync_pending = False, updated = CURRENT_TIMESTAMP
                            WHERE name = :name
                        """)
                        params = {"name": name}

                    with engine.connect() as conn, conn.begin():
                        res = conn.execute(update_q, params)
                        if res.rowcount > 0:
                            logger.info(f"Locally synchronized {res.rowcount} row(s) to active=False.")
                        else:
                            logger.info("No matching local row found to update.")

                success_count += 1

        except Exception as e:
            logger.error(f"Exception encountered for target '{name}': {e}")
            fail_count += 1

    logger.info("--- Execution Summary ---")
    logger.info(f"Successfully processed: {success_count}")
    logger.info(f"Failed:                 {fail_count}")


if __name__ == '__main__':
    main()
