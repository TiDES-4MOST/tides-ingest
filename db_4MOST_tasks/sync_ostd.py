#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
sync_ostd.py

This script provides a Prefect flow to synchronize OSTD target IDs (ostd_targ_id, ostd_u_obj_id)
and ingestion dates (date_ingested) from the 4MOST transients database (via the API)
into the local tides_master database.
"""

import os
import sys
import sqlalchemy
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv, find_dotenv
from prefect import flow, task
from prefect.cache_policies import NO_CACHE

# Add root and opr4_flows directories to sys.path so we can import submit_transients
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)
sys.path.append(os.path.join(ROOT_DIR, 'tides_flows'))

import submit_transients as st

# Must be the same number as TIDES_RUN_LOCK_KEY in tides_flows/tides_controller.py
TIDES_RUN_LOCK_KEY = 0x7469444553

@task(cache_policy=NO_CACHE, log_prints=True)
def load_4most_credentials():
    """
    Loads the 4MOST settings via submit_transients.configure_from_env() (this sets
    st.URL_SCHEMA; the old code set st.SCHEMA, which nothing reads). This flow only reads
    from 4MOST, so FOURMOST_MODE does not matter here.
    """
    load_dotenv(find_dotenv())
    st.configure_from_env()
    if not all([st.USERNAME, st.PASSWORD]) and not st.ACCESS_TOKEN:
        print("Warning: no 4MOST credentials set in environment.")

@task(cache_policy=NO_CACHE, log_prints=True)
def get_db_engine():
    """
    Connects to the local database and returns an SQLAlchemy engine.
    Ensures that the date_ingested column exists in tides_master.
    """
    load_dotenv(find_dotenv())
    
    db_user = os.getenv('TIDES_DB_USER')
    db_pass = os.getenv('TIDES_DB_PASS') or ''
    db_host = os.getenv('TIDES_DB_HOST') or 'localhost'
    db_port = os.getenv('TIDES_DB_PORT') or '5432'
    db_name = os.getenv('TIDES_DB_DATABASE')
    
    if not all([db_user, db_name]):
        raise ValueError("Missing database credentials (TIDES_DB_USER or TIDES_DB_DATABASE) in .env file.")
        
    connection_string = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    engine = sqlalchemy.create_engine(connection_string, future=True)
    # (The inline ALTER TABLE that used to be here is in the baseline schema / migrations now.)
    return engine

@task(cache_policy=NO_CACHE, log_prints=True)
def get_last_sync_timestamp(engine, force_full_sync=False):
    """
    Queries tides_master for the maximum date_ingested value.
    If force_full_sync is True, returns None.
    """
    if force_full_sync:
        print("Force full sync enabled. Ignoring previous sync timestamps.")
        return None
        
    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text("SELECT MAX(date_ingested) FROM tides_master;")).scalar()
        
    if result:
        print(f"Last synchronized target ingestion timestamp: {result}")
        return result
    else:
        print("No previously synchronized target ingestion timestamp found.")
        return None

@task(cache_policy=NO_CACHE, log_prints=True)
def fetch_newly_ingested_targets(last_timestamp):
    """
    Queries the 4MOST API for targets that have been ingested.
    Filters to only fetch targets ingested since last_timestamp (if provided).
    """
    if last_timestamp is not None:
        # Convert timestamp to UTC and format without timezone offset to avoid URL parsing issues
        utc_time = last_timestamp.astimezone(timezone.utc)
        time_str = utc_time.strftime('%Y-%m-%dT%H:%M:%S.%f') + 'Z'
        flt = f"date_ingested__gt={time_str}"
    else:
        # Django filters on the 4MOST side may not support isnull=False on timezone-aware fields properly,
        # so we query since a default historical starting point.
        flt = "date_ingested__gt=2026-08-01" # This is some arbitrary date at the start of the survey
    # Survey 15 is shared with another programme: only ask for TiDES transients.
    flt += "&subsurvey=tides-sn"
        
    print(f"Fetching targets from 4MOST transients list with filter: {flt}")
    response = st.get_list(flt=flt, limit=None)
    
    if isinstance(response, str):
        print(f"Error returned from 4MOST API: {response}")
        return []
        
    targets = []
    if isinstance(response, list):
        if len(response) == 1 and isinstance(response[0], dict) and 'results' in response[0]:
            # Response is a paginated dictionary wrapped in a list
            results_dict = response[0]
            targets = results_dict.get('results', [])
            next_url = results_dict.get('next')
            
            while next_url:
                print(f"Fetching next page: {next_url}")
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
            
    print(f"Retrieved {len(targets)} targets from 4MOST transients API.")
    return targets

@task(cache_policy=NO_CACHE, log_prints=True)
def update_local_tides_master(engine, targets):
    """
    Updates the tides_master table with ostd_targ_id, ostd_u_obj_id,
    and date_ingested for all matching pk_4most IDs.
    """
    if not targets:
        print("No targets to synchronize.")
        return 0
        
    update_q = sqlalchemy.text("""
        UPDATE tides_master
        SET ostd_targ_id = :ostd_targ_id,
            ostd_u_obj_id = :ostd_u_obj_id,
            date_ingested = :date_ingested
        WHERE pk_4most = :pk_4most
    """)   # no "updated = CURRENT_TIMESTAMP": this is bookkeeping, not a detection
    
    updated_count = 0
    with engine.connect() as conn, conn.begin():
        for t in targets:
            pk_4most = t.get('id')
            ostd_targ_id = t.get('ostd_targ_id')
            ostd_u_obj_id = t.get('ostd_u_obj_id')
            date_ingested_str = t.get('date_ingested')
            
            # We must have both the local mapping key and the 4MOST OSTD identifier
            if pk_4most is None or ostd_targ_id is None:
                continue
                
            date_ingested = None
            if date_ingested_str:
                try:
                    date_ingested = datetime.fromisoformat(date_ingested_str)
                except Exception as e:
                    print(f"Warning: Failed to parse date_ingested timestamp '{date_ingested_str}': {e}")
                    
            res = conn.execute(update_q, {
                'pk_4most': pk_4most,
                'ostd_targ_id': ostd_targ_id,
                'ostd_u_obj_id': ostd_u_obj_id,
                'date_ingested': date_ingested
            })
            
            if res.rowcount > 0:
                updated_count += res.rowcount
                print(f"Synced target name='{t.get('name')}' (pk_4most={pk_4most}) -> "
                      f"ostd_targ_id={ostd_targ_id}, ostd_u_obj_id={ostd_u_obj_id}")
                
    print(f"Successfully synchronized {updated_count} target(s) in tides_master.")
    return updated_count

@flow(name="Sync OSTD Targets Flow")
def sync_ostd_flow(force_full_sync=False):
    """
    Main flow to synchronize target ingestion state from 4MOST.
    """
    load_4most_credentials()
    engine = get_db_engine()
    with engine.connect() as lock_conn:
        # Same run lock as the ingest flow, so the two never interleave.
        got_lock = lock_conn.execute(sqlalchemy.text("SELECT pg_try_advisory_lock(:k)"), {'k': TIDES_RUN_LOCK_KEY}).scalar()
        lock_conn.commit()
        if not got_lock:
            print("The TiDES ingest flow is running: skipping this OSTD sync.")
            return 0
        try:
            last_timestamp = get_last_sync_timestamp(engine, force_full_sync=force_full_sync)
            targets = fetch_newly_ingested_targets(last_timestamp)
            updated_rows = update_local_tides_master(engine, targets)
        finally:
            lock_conn.execute(sqlalchemy.text("SELECT pg_advisory_unlock(:k)"), {'k': TIDES_RUN_LOCK_KEY})
            lock_conn.commit()
    return updated_rows

if __name__ == "__main__":
    sync_ostd_flow()
