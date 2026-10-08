#!/usr/bin/env python
"""
test_4most_downtime.py

This script verifies the resilience of the tides-ingest pipeline during 4MOST API downtime.
It NEVER talks to the real 4MOST: all 4MOST traffic goes to testing/fake_4most.py.

  1. ingest the mock streams (test_mode never writes to 4MOST)  -> 3 rows pending, no 4MOST id
  2. 4MOST down                                                 -> rows stay pending ('ambiguous')
  3. POST is created but the response is lost (timeout)         -> rows stay pending ('ambiguous')
  4. 4MOST healthy                                              -> rows found by name and adopted:
                                                                   all synced, and NO duplicates
"""

import sys
import os
import sqlalchemy
import pandas as pd
from dotenv import load_dotenv

# Ensure the root and opr4_flows directories are in the import path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'tides_flows'))

import tides_controller
import fourmost_sync
import fake_4most

def run_test():
    load_dotenv()
    
    # 1. Enforce Test Database Safety
    db_user = os.getenv('TIDES_DB_USER')
    db_pass = os.getenv('TIDES_DB_PASS')
    db_host = os.getenv('TIDES_DB_HOST') or 'localhost'
    db_port = os.getenv('TIDES_DB_PORT') or '5432'
    
    # Determine the test database name
    db_name = os.getenv('TIDES_TEST_DB_DATABASE')
    if not db_name:
        default_db = os.getenv('TIDES_DB_DATABASE')
        if default_db and 'test' in default_db.lower():
            db_name = default_db
        else:
            print("ERROR: TIDES_TEST_DB_DATABASE is not set in .env, and TIDES_DB_DATABASE does not contain 'test'.")
            print("Please configure a dedicated test database (e.g., TIDES_TEST_DB_DATABASE='dopr4_tides_test') in .env.")
            sys.exit(1)

    # Safety Guardrails against destructive runs on production databases
    is_local = db_host in ['localhost', '127.0.0.1', '::1']
    is_test_db = 'test' in db_name.lower() or 'mock' in db_name.lower()
    tides_env = os.getenv('TIDES_ENV', 'development').lower()
    
    if tides_env == 'production' or not is_local or not is_test_db:
        print("==========================================================================")
        print("CRITICAL SAFETY WARNING: destructive test aborted to prevent database loss!")
        print("==========================================================================")
        print(f"Target Database: {db_name}")
        print(f"Target Host:     {db_host}")
        print(f"Environment:     {tides_env}")
        print("\nEnsure that:")
        print("1. Your database host is local (localhost or 127.0.0.1).")
        print("2. Your database name contains 'test' or 'mock' (e.g., dopr4_tides_test).")
        print("3. TIDES_ENV is not set to 'production' in your environment.")
        sys.exit(1)

    # Apply environment override so tides_controller utilizes the test database
    os.environ['TIDES_DB_DATABASE'] = db_name
    
    connection_string = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    print(f"Connecting to database: {db_name} on {db_host}...")
    engine = sqlalchemy.create_engine(connection_string)
    
    # Auto-initialize schema if tides_master doesn't exist
    with engine.connect() as conn:
        table_exists = conn.execute(sqlalchemy.text(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'tides_master');"
        )).scalar()
        
        if not table_exists:
            print("tides_master table not found in test database. Initializing full schema...")
            # Enable q3c extension
            conn.execute(sqlalchemy.text("CREATE EXTENSION IF NOT EXISTS q3c;"))
            
            script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            db_setup_dir = os.path.join(script_dir, 'database_setup')
            
            sql_files = [
                'createMasterTable.sql',
                'surveyIDs.sql',
                'surveyConnector.sql',
                'pipelines.sql',
                'pipelineSelections.sql',
                'createHostTable.sql'
            ] + sorted(os.path.join('migrations', f) for f in os.listdir(os.path.join(db_setup_dir, 'migrations'))
                       if f.endswith('.sql'))
            
            for file_name in sql_files:
                sql_file = os.path.join(db_setup_dir, file_name)
                print(f"Executing {file_name}...")
                with open(sql_file, 'r') as f:
                    sql_commands = f.read()
                    conn.execute(sqlalchemy.text(sql_commands))
            conn.commit()
            print("Full schema initialized successfully.")
            
    print("Resetting database (truncating tides_master)...")
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text("TRUNCATE tides_master RESTART IDENTITY CASCADE;"))
        conn.execute(sqlalchemy.text("TRUNCATE survey_nights, tides_4most_log;"))
        conn.execute(sqlalchemy.text("ALTER SEQUENCE tides_seq RESTART WITH 0;"))
        conn.commit()
    print("Database cleared!")

    def state():
        with engine.connect() as conn:
            return pd.read_sql("SELECT tides_id, name, pk_4most, active, sync_pending, sync_state FROM tides_master ORDER BY tides_id", conn)

    def fail(msg):
        print(f"FAILURE: {msg}")
        sys.exit(1)

    # 2. Ingest the mock streams. test_mode never writes to 4MOST (live is downgraded to dry_run).
    print("\n[STEP 1] Running pipeline in test mode...")
    try:
        tides_controller.run_target_workflow(connect_db=True, test_mode=True)
    except Exception as e:
        fail(f"Pipeline crashed with exception: {e}")
    df = state()
    print(df)
    if not (len(df) == 3 and df['sync_pending'].all() and df['pk_4most'].isnull().all()):
        fail("expected 3 transients, all sync_pending and without a 4MOST id")

    # From here on, every 4MOST request goes to the in-memory fake (never to the real 4MOST).
    fake = fake_4most.install()

    # 3. 4MOST down: nothing may be marked as synced
    print("\n[STEP 2] 4MOST down...")
    fake.fail_mode = 'down'
    fourmost_sync.sync_pending_to_4most(engine, 'live')
    df = state()
    print(df)
    if not (df['sync_pending'].all() and df['pk_4most'].isnull().all() and len(fake.targets) == 0):
        fail("4MOST was down but rows were marked synced")

    # 4. POST created the targets but the response was lost: still pending, but 'ambiguous'
    print("\n[STEP 3] 4MOST creates the targets but the response is lost...")
    fake.fail_mode = 'lose_response'
    fourmost_sync.sync_pending_to_4most(engine, 'live')
    df = state()
    print(df)
    if not (df['sync_pending'].all() and (df['sync_state'] == 'ambiguous').all() and len(fake.targets) == 3):
        fail("expected 3 ambiguous rows and 3 targets in the fake 4MOST")

    # 5. 4MOST healthy again: the ambiguous rows must be FOUND (by name), not POSTed again
    print("\n[STEP 4] 4MOST healthy again...")
    fake.fail_mode = None
    fourmost_sync.sync_pending_to_4most(engine, 'live')
    df = state()
    print(df)
    if df['sync_pending'].any() or df['pk_4most'].isnull().any():
        fail("some transients are still pending or have no 4MOST id")
    if len(fake.targets) != 3:
        fail(f"duplicates were created in 4MOST: {len(fake.targets)} targets for 3 transients")
    if any('fake4most.local' not in url for _, url in fake.calls):
        fail("a request was addressed to something other than the fake 4MOST")

    print("\nINTEGRATION TEST PASSED: downtime, lost responses and recovery work, with no duplicates.")

if __name__ == "__main__":
    run_test()
