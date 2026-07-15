#!/usr/bin/env python
"""
test_4most_downtime.py

This script verifies the resilience of the tides-ingest pipeline during 4MOST API downtime.
It simulates a downtime event by pointing the 4MOST API to a bogus URL, confirms that the
local database ingest succeeds and marks targets as sync_pending = True, and then restores
the actual API endpoint to verify successful recovery and syncing.
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
import submit_transients as st

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
            ]
            
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
        conn.execute(sqlalchemy.text("ALTER SEQUENCE tides_seq RESTART WITH 0;"))
        conn.commit()
    print("Database cleared!")

    # 2. Simulate 4MOST Downtime
    original_schema = st.URL_SCHEMA
    bogus_schema = "https://invalid-host-4most-down.mpe.mpg.de/"
    st.URL_SCHEMA = bogus_schema
    print(f"\n[STEP 1] Overriding 4MOST URL to: {bogus_schema} (Simulating Downtime)")
    
    print("Running pipeline in test mode...")
    try:
        tides_controller.run_target_workflow(connect_db=True, test_mode=True)
    except Exception as e:
        print(f"CRITICAL FAILURE: Pipeline crashed with exception: {e}")
        sys.exit(1)
        
    # 3. Verify Local DB holds pending transients
    print("\n[STEP 2] Verifying local database state...")
    with engine.connect() as conn:
        df = pd.read_sql("SELECT tides_id, name, pk_4most, active, sync_pending FROM tides_master", conn)
        print(df)
        
        pending_count = len(df[df['sync_pending'] == True])
        null_pk_count = len(df[df['pk_4most'].isnull()])
        
        if pending_count == 3 and null_pk_count == 3:
            print("SUCCESS: 3 transients ingested locally and marked as sync_pending = True with no 4MOST ID.")
        else:
            print(f"FAILURE: Expected 3 pending transients with no ID, got {pending_count} pending and {null_pk_count} null IDs.")
            sys.exit(1)

    # 4. Restore 4MOST URL and Sync
    print(f"\n[STEP 3] Restoring 4MOST URL to: {original_schema} (Simulating Recovery)")
    st.URL_SCHEMA = original_schema
    
    print("Triggering sync process...")
    with engine.connect() as conn:
        tides_controller.sync_pending_to_4most(conn)
        conn.commit()

    # 5. Verify Successful Sync
    print("\n[STEP 4] Verifying final database state...")
    with engine.connect() as conn:
        df2 = pd.read_sql("SELECT tides_id, name, pk_4most, active, sync_pending FROM tides_master", conn)
        print(df2)
        
        pending_count_after = len(df2[df2['sync_pending'] == True])
        null_pk_count_after = len(df2[df2['pk_4most'].isnull()])
        
        if pending_count_after == 0 and null_pk_count_after == 0:
            print("\nINTEGRATION TEST PASSED: Fallback and recovery work exactly as expected!")
        else:
            print(f"\nFAILURE: Some transients are still pending or missing IDs (Pending: {pending_count_after}, Null IDs: {null_pk_count_after})")
            sys.exit(1)

if __name__ == "__main__":
    run_test()
