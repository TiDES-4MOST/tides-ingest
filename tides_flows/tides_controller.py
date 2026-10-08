"""
tides_controller.py

This file is the specific implementation of the "Controller" process for the 4MOST operational rehearsal.
It uses Prefect to orchestrate the workflow and manages 4MOST credentials.

Usage:
    prefect deployment run 'run-opr4-workflow/main'
    # OR python tides_controller.py
"""

from prefect import flow, task
# from prefect_dask import DaskTaskRunner # Optional: for parallel execution
import yaml
import os
from dotenv import load_dotenv
import tides_ztf # Import the ZTF data source module
import tides_lsst # Import the LSST #data source module
import pandas as pd
import numpy as np
import sqlalchemy
from prefect.cache_policies import NO_CACHE
import submit_transients as st
import json 
from pandas.api import types
from prefect.artifacts import create_markdown_artifact
from datetime import datetime, timezone
import sys
import math
import traceback
import stream_utils
import fourmost_sync
import provenance

# --- Settings (environment variables override the defaults) ---------------------------------
MATCH_RADIUS_DEG = 0.000277778          # 1 arcsec: the ONLY place the match radius is defined
DEACTIVATE_AFTER_DAYS = float(os.getenv('DEACTIVATE_AFTER_DAYS', '10'))   # Chris: choose 7-10
FAINT_LIMIT_MAG = 22.5                  # deactivate when every recorded band is fainter than this
KAFKA_MAX_MESSAGES = int(os.getenv('KAFKA_MAX_MESSAGES', '20000'))
# pg_try_advisory_lock key, so two runs never overlap. sync_ostd.py uses the SAME number.
TIDES_RUN_LOCK_KEY = 0x7469444553       # the bytes of "tiDES"

# Global config placeholders
# 4MOST API Credentials
# USERNAME = None
# PASSWORD = None
# SCHEMA = None
# ACCESS_TOKEN = None

@task
def ingest_report(tableNewTransients, tableUpdatedTransients, tableDeactivatedTransients):
    """
    Generates a markdown report for the ingested/updated transients.
    
    Args:
        tableNewTransients (pd.DataFrame): The DataFrame containing the new transients.
        tableUpdatedTransients (pd.DataFrame): The DataFrame containing the updated transients.
        tableDeactivatedTransients (pd.DataFrame): The DataFrame containing the deactivated transients.
    """
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    

    markdown_report = f"# TiDES Ingest Report\n"
    markdown_report += f"**Date:** {date_str}\n\n"

    def generate_section(title, data, columns=None):
        # Check if data is a list (and empty) or None
        if isinstance(data, list):
            if not data:
                return ""
            # If it's a non-empty list, we assume it might be a list of dicts that can be converted
            # But the user code implies these should be DataFrames. 
            # If it's a list, let's try to convert it just in case, or ignore if truly just []
            try:
                df = pd.DataFrame(data)
            except:
                return ""
        else:
            df = data

        # Check if it's a DataFrame and not empty
        if isinstance(df, pd.DataFrame) and not df.empty:
            section = f"## {title}\n"
            
            # Select columns if provided and they exist
            if columns:
                existing_cols = [c for c in columns if c in df.columns]
                if existing_cols:
                    df = df[existing_cols]
            
            try:
                section += df.to_markdown(index=False, tablefmt="pipe")
            except ImportError:
                section += df.to_csv(sep="|", index=False)
            
            return section + "\n\n"
        
        return ""

    markdown_report += generate_section("New Transients", tableNewTransients, ['tides_id','pk_4most','name','ra','dec'])
    markdown_report += generate_section("Updated Transients", tableUpdatedTransients)
    markdown_report += generate_section("Deactivated Transients", tableDeactivatedTransients)

    # If report is empty (besides header), mention it
    if len(markdown_report.split('\n')) <= 4:
         markdown_report += "_No changes to report._"

    # Create the artifact
    artifact_key = f"tides-ingest-report"

    create_markdown_artifact(
        key=artifact_key,
        markdown=markdown_report,
        description=f"TiDES Ingest Report - {date_str}"
    )

    return markdown_report

# @task
# def sqlalchemy_credentials_flow():
#     dbUsername = os.getenv('TIDES_DB_USER')
#     dbPassword = os.getenv('TIDES_DB_PASS')
#     dbDatabase = os.getenv('TIDES_DB_DATABASE')
#     sqlalchemy_credentials = DatabaseCredentials(
#         driver=AsyncDriver.POSTGRESQL_ASYNCPG,
#         username=dbUsername,
#         password=dbPassword,
#         database=dbDatabase,
#         host="localhost",
#         port=5432,
#     )
#     print(sqlalchemy_credentials.get_engine())
#     return sqlalchemy_credentials.get_engine()

@task(cache_policy=NO_CACHE)
def sqlalchmey_engine():
    dbUsername = os.getenv('TIDES_DB_USER')
    dbPassword = os.getenv('TIDES_DB_PASS')
    dbDatabase = os.getenv('TIDES_DB_DATABASE')
    dbHost = os.getenv('TIDES_DB_HOST')
    dbPort = os.getenv('TIDES_DB_PORT')
    url = 'postgresql+psycopg2://'+str(dbUsername)+':'+str(dbPassword)+'@'+str(dbHost)+':'+str(dbPort)+'/'+str(dbDatabase)
    engine = sqlalchemy.create_engine(url,future=True)
    return engine

@task(cache_policy=NO_CACHE)
def fetch_ztf_targets(engine, pipeline_name=None, pipeline_version=None, topic=None, group_id=None):
    """
    Calls the tides_ztf module to get the latest list of targets.
    """
    pipeline_name = pipeline_name or os.getenv('ZTF_PIPELINE_NAME', 'TiDES-ZTF-default')
    pipeline_version = pipeline_version or os.getenv('ZTF_PIPELINE_VERSION', 'v1.0')
    topic = topic or os.getenv('LASAIR_ZTF_TOPIC')
    group_id = group_id or stream_utils.group_id_from_env('LASAIR_ZTF_GROUP_ID', 'tides-ztf')

    print(f"Fetching targets from opr4_ztf using pipeline: {pipeline_name} ({pipeline_version})...")

    if engine is not None:
        query = sqlalchemy.text("SELECT pipeline_id FROM pipelines WHERE pipeline_name = :n AND version = :v")
        with engine.connect() as conn:
            result = conn.execute(query, {'n': pipeline_name, 'v': pipeline_version}).fetchone()
        
        if not result:
            raise ValueError(f"CRITICAL FIX REQUIRED: Pipeline '{pipeline_name}' version '{pipeline_version}' does not exist in the database.")
            
        pipeline_id = int(result[0])
    else:
        print(f"DEV MODE: Bypassing DB check for pipeline {pipeline_name}...")
        pipeline_id = -1
        
    # Returns (targets, consumer): offsets are committed by the flow AFTER the DB commit
    return tides_ztf.get_targets(pipeline_id=pipeline_id, topic=topic, group_id=group_id,
                                 max_messages=KAFKA_MAX_MESSAGES)

#TODO: Create LSST module
@task(cache_policy=NO_CACHE)
def fetch_lsst_targets(engine, pipeline_name=None, pipeline_version=None, topic=None, group_id=None):
    """
    Calls the tides_lsst module to get the latest list of targets.
    """
    pipeline_name = pipeline_name or os.getenv('LSST_PIPELINE_NAME', 'TiDES-LSST-default')
    pipeline_version = pipeline_version or os.getenv('LSST_PIPELINE_VERSION', 'v1.0')
    topic = topic or os.getenv('LASAIR_LSST_TOPIC', 'lasair_16TiDES_Frohmaier_et_al_2025')
    group_id = group_id or stream_utils.group_id_from_env('LASAIR_LSST_GROUP_ID', 'tides-lsst')

    print(f"Fetching targets from tides_lsst using pipeline: {pipeline_name} ({pipeline_version})...")

    if engine is not None:
        query = sqlalchemy.text("SELECT pipeline_id FROM pipelines WHERE pipeline_name = :n AND version = :v")
        with engine.connect() as conn:
            result = conn.execute(query, {'n': pipeline_name, 'v': pipeline_version}).fetchone()
        
        if not result:
            raise ValueError(f"CRITICAL FIX REQUIRED: Pipeline '{pipeline_name}' version '{pipeline_version}' does not exist in the database.")
            
        pipeline_id = int(result[0])
    else:
        print(f"DEV MODE: Bypassing DB check for pipeline {pipeline_name}...")
        pipeline_id = -1
        
    # Returns (targets, consumer): offsets are committed by the flow AFTER the DB commit
    return tides_lsst.get_targets(pipeline_id=pipeline_id, topic=topic, group_id=group_id,
                                  max_messages=KAFKA_MAX_MESSAGES)

@task(cache_policy=NO_CACHE)
def submit_to_4most(targets):
    """
    Iterates through the targets and submits them to the 4MOST facility.
    
    Args:
        targets: List of targets to submit.
    """
    # TODO: Initialize 4MOST API connection using loaded credentials
    
    # TODO: Loop through targets and format for 4MOST submission
    # for target in targets:
    #     upload_params = format_for_upload(target)
    #     result = st.create_transient(data=upload_params)
    
    print(f"Submitting {len(targets)} targets to 4MOST...")
    pass

def map_dtype(series):
    if types.is_integer_dtype(series):
        return "BIGINT"
    if types.is_float_dtype(series):
        return "DOUBLE PRECISION"
    if types.is_datetime64_any_dtype(series):  # Handles both naive and aware
        return "TIMESTAMP"
    if types.is_bool_dtype(series):
        return "BOOLEAN"
    return "TEXT"

def read_sql_file(name):
    with open(f'./sql_tasks/{name}', 'r') as f:
        return sqlalchemy.text(f.read())

def current_mjd():
    return datetime.now(timezone.utc).timestamp() / 86400.0 + 40587.0

def validate_targets(targets):
    """
    Drop (and print) rows that would break the survey's database transaction, so one bad
    alert cannot block a whole night. Returns a clean copy with a fresh 0..n-1 index.
    """
    if targets is None or len(targets) == 0:
        return pd.DataFrame(columns=stream_utils.CONTRACT_COLUMNS)
    df = targets[stream_utils.CONTRACT_COLUMNS].copy().reset_index(drop=True)
    latest_ok_mjd = current_mjd() + 1.0

    def is_number(x):
        try:
            return x is not None and math.isfinite(float(x))
        except (TypeError, ValueError):
            return False

    def problem(row):
        if row['object_id'] is None or pd.isna(row['object_id']):
            return 'no object_id'
        for col in ('ra', 'dec', 'jdmax', 'survey_id', 'pipeline_id'):
            if not is_number(row[col]):
                return f'{col} missing or not finite'
        if not (0.0 <= float(row['ra']) < 360.0 and -90.0 <= float(row['dec']) <= 90.0):
            return 'ra/dec out of range'
        if not (40000.0 < float(row['jdmax']) <= latest_ok_mjd):   # an MJD: not a JD, not in the future
            return f"jdmax {row['jdmax']} is not a plausible MJD"
        for col in ('latest_filter', 'latest_mag', 'n_sources'):
            try:
                values = json.loads(row[col])
            except (TypeError, ValueError):
                return f'{col} is not JSON'
            if not isinstance(values, dict) or not all(is_number(v) for v in values.values()):
                return f'{col} has non-finite values'
        return None

    problems = df.apply(problem, axis=1)
    bad = problems.notna()
    for i in df.index[bad]:
        print(f"Dropping alert {df.at[i, 'object_id']} (survey {df.at[i, 'survey_id']}): {problems[i]}")
    df = df[~bad].reset_index(drop=True)
    df['object_id'] = df['object_id'].astype(str)
    return df

@task(cache_policy=NO_CACHE)
def createTransientStage(dataTable, cnx):
    """Load one survey's validated targets into the temporary tides_stage table."""
    cnx.execute(read_sql_file('createStage.sql'))
    dataTable.to_sql('tides_stage', con=cnx, if_exists='append', index=True, index_label='stage_row')
    cnx.execute(read_sql_file('indexStage.sql'))

@task(cache_policy=NO_CACHE)
def upsertToMaster(cnx):
    """
    Merge tides_stage into tides_master. Returns (new_rows, updated_rows, new_selections).

    1. Attach each staged alert to the nearest existing master within 1".
    2. Loop: insert one new master per group of unmatched neighbours, then attach again.
       Each pass inserts at least one row and attaches it, so the loop always finishes;
       normally it runs once (twice only for a chain of alerts each within 1" of the next).
    3. One UPDATE per matched master (replay-safe, see updateMatchedMasters.sql).
    4. Junction tables from the tides_id found in 1-2 (no second spatial match).
    """
    radius = {'radius_deg': MATCH_RADIUS_DEG}
    n_staged = cnx.execute(sqlalchemy.text("SELECT count(*) FROM tides_stage")).scalar()

    cnx.execute(read_sql_file('matchStageToMaster.sql'), radius)
    new_rows = []
    for _ in range(n_staged + 1):
        n_unmatched = cnx.execute(sqlalchemy.text("SELECT count(*) FROM tides_stage WHERE tides_id IS NULL")).scalar()
        if n_unmatched == 0:
            break
        new_rows += cnx.execute(read_sql_file('insertNewMasters.sql'), radius).mappings().all()
        cnx.execute(read_sql_file('matchStageToMaster.sql'), radius)
    else:
        raise RuntimeError("upsertToMaster: staged rows still unmatched after inserting; check ra/dec")

    new_rows = pd.DataFrame(new_rows)
    updated = pd.DataFrame(cnx.execute(read_sql_file('updateMatchedMasters.sql')).mappings().all())
    if len(new_rows) and len(updated):
        updated = updated[~updated['tides_id'].isin(new_rows['tides_id'])]   # report new rows once

    cnx.execute(read_sql_file('insertSurveys.sql'))
    selections = pd.DataFrame(cnx.execute(read_sql_file('insertPipelineSelections.sql')).mappings().all())
    return new_rows, updated, selections

# @task(cache_policy=NO_CACHE)
# def upsertStaged2(upsertStage,cnx):
#     upsertStage.columns = map(str.lower, upsertStage.columns)
#     cols_with_types = ", ".join([f"{name} {map_dtype(dtype)}" for name, dtype in upsertStage.dtypes.items()])
#     cnx.execute(sqlalchemy.text(f"CREATE TEMPORARY TABLE tides_stage2 ({cols_with_types})"))
#     upsertStage.to_sql('tides_stage2', con=cnx, if_exists='append', index=False)
#     print('Upserted Stage 2data', upsertStage)


@task(cache_policy=NO_CACHE)
def deactivateUnobservedTransients(engine):
    """Deactivate stale/faint objects in their own transaction (see deactivateUnobserved.sql)."""
    params = {'days': DEACTIVATE_AFTER_DAYS,
              'stale_reason': f'stale_{DEACTIVATE_AFTER_DAYS:g}d',
              'faint_mag': FAINT_LIMIT_MAG,
              'now_mjd': current_mjd()}
    with engine.begin() as conn:
        deactivated = pd.DataFrame(conn.execute(read_sql_file('deactivateUnobserved.sql'), params).mappings().all())
        provenance.log_events(conn, deactivated, 'deactivated')
    print(f"Deactivated {len(deactivated)} transient(s).")
    return deactivated

@task(cache_policy=NO_CACHE)
def ingest_survey_targets(engine, targets, survey_id, submitted_by=None):
    """
    Everything for ONE survey in ONE database transaction: stage, match/insert, update,
    junction tables, events, and the survey heartbeat. If anything fails, all of it is rolled
    back and the caller does not commit the Kafka offsets, so the alerts are read again next run.
    submitted_by: set for targets a person/external source gave us (recorded in tides_events).
    """
    targets = validate_targets(targets)
    new_rows = updated = pd.DataFrame()
    with engine.begin() as conn:
        if len(targets) > 0:
            createTransientStage(targets, conn)
            new_rows, updated, selections = upsertToMaster(conn)

            # Provenance (P2-K): one 'alert' (or 'manual_submission') event per staged alert,
            # 'created' per new object, 'activated' when a new detection revives an inactive one,
            # 'selected' the first time a (survey, pipeline) selects an object.
            conn.execute(read_sql_file('logStageAlerts.sql'),
                         {'submitted_by': submitted_by, 'git_sha': provenance.git_sha(),
                          'flow_run_id': provenance.flow_run_id()})
            provenance.log_events(conn, new_rows, 'created')
            if len(updated):
                revived = updated[updated['new_detection'] & ~updated['old_active'].astype(bool) & updated['active']]
                provenance.log_events(conn, revived, 'activated', reason='redetected')
            provenance.log_events(conn, selections, 'selected')

            # The survey clock: record tonight as an observing night for this survey (validated dates only).
            conn.execute(read_sql_file('recordHeartbeat.sql'))
    print(f"Survey {survey_id}: {len(new_rows)} new, {len(updated)} matched existing objects.")
    return new_rows, updated

@task(cache_policy=NO_CACHE)
def prepare4MOSTUpdate(cnx):
  query = open('./sql_tasks/stage4MOSTupdates.sql')
  updates = pd.read_sql(sqlalchemy.text(query.read()), con=cnx)
  # row = cnx.execute(sqlalchemy.text(query.read()))
  # print(row.mappings().all())
  #query.close()
  return updates

@task(cache_policy=NO_CACHE)
def createNewTransientin4MOST(tableIn):
  if len(tableIn)==0:
    return []
  for index,row in tableIn.iterrows():
    catDict = row.to_dict()
    
    latest_mags = catDict.get('latest_mags', {})
    if isinstance(latest_mags, str):
        latest_mags = json.loads(latest_mags)
        
    min_mag = 29.99
    min_filter = "unknown"
    if latest_mags:
        min_filter = min(latest_mags, key=lambda k: float(latest_mags[k]))
        min_mag = float(latest_mags[min_filter])
    
    uploadParams = {
    "uploadedfor_survey_id": 15,
    "name" : str(catDict['name']),
    "ra": np.float64(catDict['ra']),
    "dec": np.float64(catDict['dec']),
    "pmra": 0.0,
    "pmdec": 0.0,
    "epoch": 2000,
    "resolution": 1,
    "subsurvey": "tides-sn",
    "cadence": 1048576,
    "template": 'SN_spec_specid56_snt1_phase5_redshift0.169.fits',
    "ruleset": 'tides_snMay2024',
    # "redshift_estimate": None,
    # "redshift_error": None,
    "extent_flag": 0,
    "extent_parameter": 0,
    "extent_index": 0,
    "mag": min_mag,
    # "mag_err": None,
    "mag_type": f"LSST_r_AB",
    # "reddening": None,
    # "date_earliest": None,
    # "date_latest": None,
    "t_exp_d": 38.0,
    "t_exp_g": 38.0,
    "t_exp_b": 4300.,
    "t_exp_s": 4300.,
    # "template_redshift": None,
    # "cal_mag_blue": None,
    # "cal_mag_green": None,
    # "cal_mag_red": None,
    # "cal_mag_err_blue": None,
    # "cal_mag_err_blue": None,
    # "cal_mag_err_green": None,
    # "cal_mag_id_green": None,
    # "cal_mag_id_red": None,
    # "cal_mag_id_red": None,
    "classification": "TRA",
    # "completeness": None,
    # "parallax": None,
    "is_active": True,
    }
    
    #print(uploadParams)
    uppedObjectJSONstring = st.create_transient(data=uploadParams, printout=False) 
    uppedObjectJSON = json.loads(uppedObjectJSONstring)
    uppedObject = pd.DataFrame(uppedObjectJSON)
    tableIn.loc[index,'pk_4most'] = np.int64(uppedObject['id'])
  return tableIn

@task(cache_policy=NO_CACHE)
def updateExisitingTransient(tableIn):
  if len(tableIn)==0:
    return []
  for index,row in tableIn.iterrows():
    catDict = row.to_dict()
    #print(catDict['name'])
    uploadParams = {
    "is_active": catDict['active']}

    print(catDict['pk_4most'])
    updatedObject = st.update_transient(pk=int(catDict['pk_4most']), data=uploadParams, printout=False) 
    print('Updated:', updatedObject)

@task(cache_policy=NO_CACHE)
def updateTiDESMasterwith4MOSTKey(newTable, cnx):
  newTable.columns = map(str.lower, newTable.columns)
  newTable['pk_4most'] = newTable['pk_4most'].astype(int).copy()
  newTable.to_sql('latest_4most', con=cnx, if_exists='replace', index=False)
  query = open('./sql_tasks/updateMasterWith4MOSTkey.sql')
  updates = cnx.execute(sqlalchemy.text(query.read()))
  # row = cnx.execute(sqlalchemy.text(query.read()))
  # print(row.mappings().all())
  query.close()

SURVEY_IDS = {'lsst': 1, 'ztf': 2}

def fetch_survey(name, engine, test_mode):
    """Returns (targets, consumer). test_mode uses the mock streams and has no consumer."""
    if test_mode:
        sys.path.append('.')
        from testing import mock_streams
        mocks = {'lsst': mock_streams.generate_mock_lsst_targets, 'ztf': mock_streams.generate_mock_ztf_targets}
        return mocks[name](), None
    fetchers = {'lsst': fetch_lsst_targets, 'ztf': fetch_ztf_targets}
    return fetchers[name](engine=engine)

@flow(name="delta OPR4 Workflow", log_prints=True, timeout_seconds=3600)
def run_target_workflow(connect_db=True, test_mode=False):
    """
    The main Prefect flow.

      for each enabled survey (SURVEYS_ENABLED, default 'lsst'):
          read Kafka -> one DB transaction -> commit Kafka offsets
          (a failing survey is recorded and skipped; the others carry on)
      deactivate stale/faint objects            (always, even on an empty night)
      sync tides_master to 4MOST                (always, according to FOURMOST_MODE)
      if any step failed: raise at the end, so Prefect marks the run Failed

    Survey plugins return the columns in stream_utils.CONTRACT_COLUMNS.
    """
    load_dotenv()
    fourmost_mode = st.configure_from_env(test_mode=test_mode)
    surveys = [x.strip().lower() for x in os.getenv('SURVEYS_ENABLED', 'lsst').split(',') if x.strip()]
    if test_mode:
        surveys = ['lsst', 'ztf']     # the mock streams cover both

    if not connect_db:
        # DEV MODE: read the streams and print. Nothing is written and NO offsets are committed.
        for name in surveys:
            targets, consumer = fetch_survey(name, None, test_mode)
            stream_utils.close_consumer(consumer)
            print(f"DEV MODE: {name}: {len(targets)} targets (nothing written).")
        return None

    engine = sqlalchmey_engine()
    if test_mode and not any(w in engine.url.database.lower() for w in ('test', 'scratch', 'mock')):
        raise RuntimeError(f"test_mode needs a test database (name containing test/scratch/mock), not {engine.url.database!r}")

    failed = []
    newTransients = updatedTransients = deactivatedTransients = pd.DataFrame()
    with engine.connect() as lock_conn:
        got_lock = lock_conn.execute(sqlalchemy.text("SELECT pg_try_advisory_lock(:k)"), {'k': TIDES_RUN_LOCK_KEY}).scalar()
        lock_conn.commit()     # the lock belongs to the session, not the transaction: keep it, end the transaction
        if not got_lock:
            print("Another TiDES run (ingest or sync_ostd) holds the run lock: doing nothing this time.")
            return None
        try:
            # 1. Each survey on its own: Kafka -> DB transaction -> commit offsets
            for name in surveys:
                consumer = None
                try:
                    targets, consumer = fetch_survey(name, engine, test_mode)
                    ingest_survey_targets(engine, targets, SURVEY_IDS[name])
                    stream_utils.commit_offsets(consumer)      # only after the DB commit above
                except Exception:
                    failed.append(name)
                    print(f"!!! Survey {name} FAILED; its alerts will be re-read next run.\n{traceback.format_exc()}")
                finally:
                    stream_utils.close_consumer(consumer)

            # 2. Deactivation always runs. A failed survey's clock did not move, so its objects are safe.
            try:
                deactivateUnobservedTransients(engine)
            except Exception:
                failed.append('deactivation')
                print(f"!!! Deactivation FAILED\n{traceback.format_exc()}")

            # 3. 4MOST sync always runs (it works from tides_master, not from this run's alerts)
            try:
                newTransients, updatedTransients, deactivatedTransients = fourmost_sync.sync_pending_to_4most(engine, fourmost_mode)
            except Exception:
                failed.append('4most_sync')
                print(f"!!! 4MOST sync FAILED\n{traceback.format_exc()}")
        finally:
            lock_conn.execute(sqlalchemy.text("SELECT pg_advisory_unlock(:k)"), {'k': TIDES_RUN_LOCK_KEY})
            lock_conn.commit()

    print("Generating report...")
    ingest_report(newTransients, updatedTransients, deactivatedTransients)
    if failed:
        raise RuntimeError(f"These steps failed (the others completed): {failed}")

if __name__ == "__main__":
    run_target_workflow(connect_db=True, test_mode=False)
