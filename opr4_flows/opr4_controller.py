"""
opr4_controller.py

This file is the specific implementation of the "Controller" process for the 4MOST operational rehearsal.
It uses Prefect to orchestrate the workflow and manages 4MOST credentials.

Usage:
    prefect deployment run 'run-opr4-workflow/main'
    # OR python opr4_controller.py
"""

from prefect import flow, task
# from prefect_dask import DaskTaskRunner # Optional: for parallel execution
import yaml
import os
from dotenv import load_dotenv
import opr4_ztf # Import the data source module
import pandas as pd
import numpy as np
import sqlalchemy
from prefect.cache_policies import NO_CACHE
import submit_transients as st
import json 
import ingest_report as ir
from pandas.api import types

# Global config placeholders
# 4MOST API Credentials
# USERNAME = None
# PASSWORD = None
# SCHEMA = None
# ACCESS_TOKEN = None

@task
def load_credentials():
    """
    Loads 4MOST API credentials from a YAML file.
    
    This mirrors the 'connect4MOST_API' and 'loadTiDESdbSettings' logic from tidesCom.py.
    """
    global USERNAME, PASSWORD, SCHEMA, ACCESS_TOKEN
    
    # Load credentials from .env
    load_dotenv()
    
    USERNAME = os.getenv('FOURMOST_USERNAME')
    PASSWORD = os.getenv('FOURMOST_PASSWORD')
    SCHEMA = os.getenv('FOURMOST_SCHEMA')
    ACCESS_TOKEN = os.getenv('FOURMOST_ACCESS_TOKEN')
    
    st.USERNAME = USERNAME
    st.PASSWORD = PASSWORD
    st.SCHEMA = SCHEMA
    st.ACCESS_TOKEN = ACCESS_TOKEN

    # Check if critical credentials are loaded
    if not all([USERNAME, PASSWORD]):
        print("Warning: 4MOST credentials not found in .env")
    
    print("Loading credentials from environment variables...")
    return None

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
def fetch_ztf_targets():
    """
    Calls the opr4_ztf module to get the latest list of targets.
    """
    print("Fetching targets from opr4_ztf...")
    targets = opr4_ztf.get_targets()
    return targets

#TODO: Create LSST module
@task(cache_policy=NO_CACHE)
def fetch_lsst_targets():
    """
    Calls the opr4_lsst module to get the latest list of targets.
    """
    print("Fetching targets from opr4_lsst...")
    targets = opr4_lsst.get_targets()
    return targets

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
        return "INTEGER"
    if types.is_float_dtype(series):
        return "DOUBLE PRECISION"
    if types.is_datetime64_any_dtype(series):  # Handles both naive and aware
        return "TIMESTAMP"
    if types.is_bool_dtype(series):
        return "BOOLEAN"
    return "TEXT"

@task(cache_policy=NO_CACHE)
def createTransientStage(dataTable, cnx):
    
    dataTable.columns = map(str.lower, dataTable.columns)
    cols_with_types = ", ".join([f"{name} {map_dtype(dtype)}" for name, dtype in dataTable.dtypes.items()])
    cnx.execute(sqlalchemy.text(f"CREATE TEMPORARY TABLE tides_stage ({cols_with_types})"))
    dataTable[dataTable['pass']==True].to_sql('tides_stage', con=cnx, if_exists='append', index=False)
    ## Below is faster when millions of rows, we are not at that stage
    # dataTable.head(0)to_sql('tides_stage', con=cnx, index=False, if_exists='replace') # head(0) uses only the header
    # # set index=False to avoid bringing the dataframe index in as a column 

    # raw_con = cnx.raw_connection() # assuming you set up cnx as above
    # cur  = raw_con.cursor()
    # out = StringIO()

    # # write just the body of your dataframe to a csv-like file object
    # dataTable.to_csv(out, sep='\t', header=False, index=False) 

    # out.seek(0) # sets the pointer on the file object to the first line
    # contents = out.getvalue()
    # cur.copy_from(out, 'table_name', null="") # copies the contents of the file object into the SQL cursor and sets null values to empty strings
    # raw_con.commit()

@task(cache_policy=NO_CACHE)
def upsertToMaster(cnx):
  query = open('./sql_tasks/upsertTiDESstage.sql', 'r')
  dataReturned = cnx.execute(sqlalchemy.text(query.read()))
  result = dataReturned.mappings().all()
  
  # Convert to pandas DataFrame
  upsertStage = pd.DataFrame(result)
  return upsertStage

@task(cache_policy=NO_CACHE)
def upsertStaged2(upsertStage,cnx):
    upsertStage.columns = map(str.lower, upsertStage.columns)
    cols_with_types = ", ".join([f"{name} {map_dtype(dtype)}" for name, dtype in upsertStage.dtypes.items()])
    cnx.execute(sqlalchemy.text(f"CREATE TEMPORARY TABLE tides_stage2 ({cols_with_types})"))
    upsertStage.to_sql('tides_stage2', con=cnx, if_exists='append', index=False)
    print('Upserted Stage 2data', upsertStage)


@task(cache_policy=NO_CACHE)
def deactivateUnobservedTransients(cnx):
  query = open('./sql_tasks/deactivateUnobserved.sql')
  dataReturned = cnx.execute(sqlalchemy.text(query.read()))
  result = dataReturned.mappings().all()
  
  # Convert to pandas DataFrame
  deactivated = pd.DataFrame(result)
  print(deactivated)
  return deactivated

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
    #print(catDict['name'])
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
    "mag": min(float(catDict['rlatest']), float(catDict['glatest'])),
    # "mag_err": None,
    "mag_type": "LSST_r_AB",
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

@flow(name="OPR4 Workflow")
def run_opr4_workflow():
    """
    The main Prefect flow for the OPR4 process.
    """
    neededTargetColumns = ['objectId','ra', 'dec', 'jdmin', 'jdmax', 'gmag', 'rmag']
    
    # 1. Load configuration and credentials
    load_credentials()
    

    # 2. Fetch targets from the ZTF stream (via opr4_ztf)
    ztf_targets = fetch_ztf_targets()
    
    # Rename 'decl' to 'dec' if it exists
    # Lasair won't let you have dec as a column name, so I have decl.
    # But we need dec for the master table.
    if 'decl' in ztf_targets.columns:
        ztf_targets.rename(columns={'decl': 'dec'}, inplace=True)
    
    ## If adding LSST, do it here, returning a DataFrame 
    
    #Trim the targets to the columns needed for the master table
    ztf4master = ztf_targets[neededTargetColumns]

    ## Combine all the targets into a single DataFrame
    ## If adding LSST, do it here
    allTargets = pd.concat([ztf4master]) ## Combine all targets into a single DataFrame

    if len(allTargets) == 0:
        #print('!!! No Transients !!!')
        return None
    #print('All transients: ', len(allTargets))
    # 3. Check whether objects Pass addition slection criteria
    # and add a column 'pass' to the DataFrame
    # e.g. allTargets['pass'] = allTargets.apply(lambda row: row['gmag'] < 22 and row['rmag'] < 22, axis=1)
    # TODO: Add selection criteria
    # For now, just pass everything
    allTargets['pass'] = True

    
    engine = sqlalchmey_engine() ## Create the connection to the TiDES DB

    # 4. Let's start doing Database tasks

    with engine.connect() as conn, conn.begin() :

        createTransientStage(allTargets, conn) ## Create a temporary table for the recent detections

        upsertedData = upsertToMaster(conn) ## Upsert Recent data into the master table
        #print(upsertedData.columns)
        #print(upsertedData[['tides_id','pk_4most','old_status','active']])
        
        id_ChangeState = upsertedData[['tides_id', 'pk_4most']]\
            [(upsertedData['old_status'] != upsertedData['active']) & (upsertedData['pk_4most'].notnull())]
        #print('Change State',id_ChangeState)
        #upsertStaged2(upsertedData,conn) ## Upsert the recent data into the staged2 table
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
        deactivate_TiDES_IDs_All = deactivateUnobservedTransients(conn)
        if len(deactivate_TiDES_IDs_All)>0:
            deactivate_TiDES_IDs = deactivate_TiDES_IDs_All[~deactivate_TiDES_IDs_All['pk_4most'].isnull()]
        else:
            deactivate_TiDES_IDs = []
            
        
        
        #print(deactivate_TiDES_IDs)
        
        # toUpdate = prepare4MOSTUpdate(conn) I don't think we need to do this any more because upserted and deactivate are enough
        # print("!!!!----------------!!!")
        # print('New transients: ', upsertedData[upsertedData['pk_4most'].isnull()])
        # print("!!!!----------------!!!")
        # print('Existing transients with State Change: ', id_ChangeState)
        # print("!!!!----------------!!!")
        # print('Deactivated Transients',deactivate_TiDES_IDs)
        # print("!!!!----------------!!!")
        #print('Updating Transients',len(upsertedData[upsertedData['pk_4most'].notnull()]))
        #TODO: Better error reporting below when things don't go well
        #Perhaps put a try/except in and then report the error in a prefect log

        if len(upsertedData[upsertedData['pk_4most'].isnull()])>0:
            #print('Sending new {} transients to 4MOST'.format(len(upsertedData[upsertedData['pk_4most'].isnull()])))
            newTransients = createNewTransientin4MOST(upsertedData[upsertedData['pk_4most'].isnull()])
        else:
            #print('No new transients to send to 4MOST')
            newTransients = []
        
        if len(deactivate_TiDES_IDs)>0:
            #print('Deactivating {} transients in 4MOST'.format(len(deactivate_TiDES_IDs)))
            deactivatedTransients = updateExisitingTransient(deactivate_TiDES_IDs)
        else:
            #print('No transients to deactivate in 4MOST')
            deactivatedTransients = []
        
        if len(id_ChangeState)>0:
            #print('Updating {} transients in 4MOST due to False->True state change'.format(len(id_ChangeState)))
            updatedTransients = updateExisitingTransient(id_ChangeState)
        else:
            #print('No transients to update in 4MOST')
            updatedTransients = []
        #print(newTransients)
        
        if len(newTransients)==0:            
            return None
        else:
            updateTiDESMasterwith4MOSTKey(newTransients, conn)

    ir.ingest_report(newTransients, updatedTransients, deactivatedTransients)



if __name__ == "__main__":
    run_opr4_workflow()
