#!/usr/bin/env python3
"""
test_host_matching.py

Verifies host galaxy matching, host catalog deduplication, and follow-up queue
logic (active/inactive states based on 50-day unobserved and faint-LSST criteria).
"""

import sys
import os
import sqlalchemy
import pandas as pd
import json
import time
from dotenv import load_dotenv

# Add project root and host_matching directory to import paths
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'host_tools', 'tides_host_matching'))

import tides_match_gal_local

def run_test():
    load_dotenv()
    
    # 1. Enforce Test Database Safety
    db_user = os.getenv('TIDES_DB_USER')
    db_pass = os.getenv('TIDES_DB_PASS')
    db_host = os.getenv('TIDES_DB_HOST') or 'localhost'
    db_port = os.getenv('TIDES_DB_PORT') or '5432'
    db_name = os.getenv('TIDES_TEST_DB_DATABASE')
    
    if not db_name:
        print("ERROR: TIDES_TEST_DB_DATABASE is not set in .env.")
        sys.exit(1)

    is_local = db_host in ['localhost', '127.0.0.1', '::1']
    is_test_db = 'test' in db_name.lower() or 'mock' in db_name.lower()
    
    if not is_local or not is_test_db:
        print("Safety check failed. target database must be local and contain 'test' or 'mock'.")
        sys.exit(1)

    # Use test DB
    os.environ['TIDES_DB_DATABASE'] = db_name
    
    connection_string = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    print(f"Connecting to test database: {db_name}...")
    engine = sqlalchemy.create_engine(connection_string)
    
    # Clean up master & host tables
    print("Truncating tables...")
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text("TRUNCATE tides_master RESTART IDENTITY CASCADE;"))
        conn.execute(sqlalchemy.text("TRUNCATE tides_host_catalog RESTART IDENTITY CASCADE;"))
        conn.execute(sqlalchemy.text("ALTER SEQUENCE tides_seq RESTART WITH 0;"))
        conn.commit()
    print("Database cleared!")

    # Calculate current MJD for testing offsets
    current_mjd = time.time() / 86400.0 + 40587.0
    print(f"Current MJD: {current_mjd:.2f}")

    # 2. First, insert all transients as active so they get matched and associated
    print("Inserting active transients to establish host matching...")
    with engine.connect() as conn:
        for i in range(1, 5):
            conn.execute(sqlalchemy.text(
                """
                INSERT INTO tides_master (ra, dec, jdmin, jdmax, active, sync_pending, latest_mags)
                VALUES (150.0, 2.0, :mjd_start, :mjd_end, True, False, '{}'::jsonb)
                """
            ), {"mjd_start": current_mjd - 10, "mjd_end": current_mjd - 2})
        conn.commit()
        
    # 3. Trigger initial host matching
    print("Running initial host matching...")
    tides_match_gal_local.match_galaxies_flow(pipeline_name="sherlock", pipeline_version="v1.0")

    # 4. Update the transient states in tides_master to match our test scenarios
    print("\nUpdating transient active/inactive states for scenarios...")
    with engine.connect() as conn:
        # Scenario 1 (tides_id = 1): remains active = True (should NOT trigger host queue)
        # Scenario 2 (tides_id = 2): active = False, jdmax = current_mjd - 60 (should trigger host queue)
        conn.execute(sqlalchemy.text(
            "UPDATE tides_master SET active = False, jdmax = :jd WHERE tides_id = 2"
        ), {"jd": current_mjd - 60})
        
        # Scenario 3 (tides_id = 3): active = False, jdmax = current_mjd - 5, latest_mags = '{"g_lsst": 24.5}' (should trigger host queue)
        conn.execute(sqlalchemy.text(
            "UPDATE tides_master SET active = False, jdmax = :jd, latest_mags = CAST(:mags AS jsonb) WHERE tides_id = 3"
        ), {"jd": current_mjd - 5, "mags": json.dumps({"g_lsst": 24.5, "r_lsst": 25.0})})
        
        # Scenario 4 (tides_id = 4): active = False, jdmax = current_mjd - 5, latest_mags = '{"g_lsst": 21.0}' (should NOT trigger host queue)
        conn.execute(sqlalchemy.text(
            "UPDATE tides_master SET active = False, jdmax = :jd, latest_mags = CAST(:mags AS jsonb) WHERE tides_id = 4"
        ), {"jd": current_mjd - 5, "mags": json.dumps({"g_lsst": 21.0, "r_lsst": 24.5})})
        
        conn.commit()

    # 5. Run follow-up queue update and 4MOST sync programmatically
    print("\nRunning follow-up queue evaluation and syncing...")
    with engine.connect() as conn:
        changed_count = tides_match_gal_local.update_host_queue_status(conn)
        print(f"Queue status changed for {changed_count} association(s)")
        synced_count = tides_match_gal_local.sync_hosts_to_4most(conn)
        print(f"Synced {synced_count} host(s) to 4MOST")
        conn.commit()

    # 6. Verify final states in the database
    print("\nVerifying database state...")
    with engine.connect() as conn:
        hosts_df = pd.read_sql("SELECT * FROM tides_host_catalog", conn)
        assoc_df = pd.read_sql(
            """
            SELECT th.association_id, th.tides_id, th.rank, th.active, th.sync_pending, th.pk_4most,
                   tm.name as transient_name, thc.host_name
            FROM tides_host th
            JOIN tides_master tm ON th.tides_id = tm.tides_id
            JOIN tides_host_catalog thc ON th.host_id = thc.host_id
            ORDER BY th.tides_id ASC
            """, conn
        )
        
        print("\n=== tides_host_catalog ===")
        print(hosts_df)
        print("\n=== tides_host (Associations) ===")
        print(assoc_df)
        
        passed = True
        
        # Verify Scenario 1: tides_id = 1 (Transient_Active) -> active should be False
        s1 = assoc_df[assoc_df['tides_id'] == 1].iloc[0]
        if s1['active'] == False:
            print("Scenario 1 Passed: Active transient host not queued.")
        else:
            print(f"Scenario 1 Failed: Active transient host queued (active = {s1['active']})")
            passed = False
            
        # Verify Scenario 2: tides_id = 2 (Transient_Dead_50d) -> active should be True
        s2 = assoc_df[assoc_df['tides_id'] == 2].iloc[0]
        if s2['active'] == True and s2['pk_4most'] is not None:
            print("Scenario 2 Passed: Inactive transient > 50 days host queued and registered.")
        else:
            print(f"Scenario 2 Failed: Inactive transient > 50 days host status mismatch (active = {s2['active']}, pk_4most = {s2['pk_4most']})")
            passed = False
            
        # Verify Scenario 3: tides_id = 3 (Transient_Faint_LSST) -> active should be True
        s3 = assoc_df[assoc_df['tides_id'] == 3].iloc[0]
        if s3['active'] == True and s3['pk_4most'] is not None:
            print("Scenario 3 Passed: Inactive transient with all LSST detections > 24 mag host queued and registered.")
        else:
            print(f"Scenario 3 Failed: Inactive transient with all LSST detections > 24 mag host status mismatch (active = {s3['active']}, pk_4most = {s3['pk_4most']})")
            passed = False
            
        # Verify Scenario 4: tides_id = 4 (Transient_Bright_LSST) -> active should be False
        s4 = assoc_df[assoc_df['tides_id'] == 4].iloc[0]
        if s4['active'] == False:
            print("Scenario 4 Passed: Inactive transient with bright LSST detection <= 24 mag host not queued.")
        else:
            print(f"Scenario 4 Failed: Inactive transient with bright LSST detection <= 24 mag host queued (active = {s4['active']})")
            passed = False
            
        # Check catalog deduplication
        if len(hosts_df) == 1 and hosts_df.iloc[0]['host_name'] == 'COSMOS1304074':
            print("Deduplication Verification Passed: All transients mapped to the single catalog host galaxy 'COSMOS1304074'.")
        else:
            print(f"Deduplication Verification Failed: Expected 1 host galaxy, found: {len(hosts_df)}")
            passed = False
            
        if passed:
            print("\nINTEGRATION TEST PASSED: Host follow-up queue rules and database schema work perfectly!")
            sys.exit(0)
        else:
            print("\nINTEGRATION TEST FAILED: Schema or queue logic mismatch.")
            sys.exit(1)

if __name__ == "__main__":
    run_test()
