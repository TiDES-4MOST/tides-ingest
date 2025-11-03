from prefect import flow, task
from prefect_dask import DaskTaskRunner
from lasair import lasair_consumer
import lasair
import json
import yaml
from random import randrange
import pandas as pd
import numpy as np
import sys
#from prefect_sqlalchemy import DatabaseCredentials, AsyncDriver
import sqlalchemy
from io import StringIO
import submit_transients as st

# This is hardcoded for ZTF, will need to be changed for LSST as more filters are added.
filterDict = {1:'g', 2:'r', 3:'i', 'g':1, 'r':2, 'i':3}



@task 
def loadTopicSettings(key):
  '''
  '''
  settingsOpen = yaml.load(open('./flowSettings.yaml'), Loader=yaml.SafeLoader)
  topic = settingsOpen[key]['topic']
  groupID = settingsOpen[key]['groupID']
  return topic, groupID


def loadLasairDetails(key):
  '''
  '''
  settingsOpen = yaml.load(open('./flowSettings.yaml'), Loader=yaml.SafeLoader)
  lasairToken = settingsOpen[key]['lasairToken']
  return lasairToken

def loadSelectionFunctionDetails(key):
  '''
  '''
  settingsOpen = yaml.load(open('./flowSettings.yaml'), Loader=yaml.SafeLoader)
  functionPath = settingsOpen[key]['selectFunctionPath']
  functionName = settingsOpen[key]['selectFunction']
  return functionPath, functionName

def loadTiDESdbSettings(key):
  settingsOpen = yaml.load(open('./flowSettings.yaml'), Loader=yaml.SafeLoader)
  dbUsername = settingsOpen[key]['tidesDBUser']
  dbPassword = settingsOpen[key]['tidesDBpass']
  dbDatabase = settingsOpen[key]['tidesDBdatabase']
  return dbUsername, dbPassword, dbDatabase

def connect4MOST_API():
  settingsOpen = yaml.load(open('./4mostAPIDetails.yaml'), Loader=yaml.SafeLoader)
  st.SCHEMA = settingsOpen['connect']['schema']
  st.USERNAME = settingsOpen['connect']['username']
  st.PASSWORD = settingsOpen['connect']['password']
  st.ACCESS_TOKEN = settingsOpen['connect']['access_token']


@task
def getDevBatch():
  """
  The Lasair Kafka stream sometimes goes down or ZTF isn't operational.
  This isn't great for the development because it always happens at the worst time!
  
  In this funciton I just load up a text file and pretend its a datastream!
  """
  dataIn = pd.read_csv("../tidesTargeting/ztfIAListDemo.dat", names=['objectId'])
  return dataIn

@task
def getLatestBatch(consumer):
  '''
  This task will query the topic and download the data in one batch
  '''
  recentObjects = pd.DataFrame()
  
  while True:
    msg = consumer.poll(timeout=5) #The kafka poll will wait 5 seconds to hear back. If nothing is delivered the pipeline will end and only objects 
    if msg is None:
      print('no more transients')
      break
      
    if msg.error():
      print(str(msg.error()))
      break
    jmsg = json.loads(msg.value())
    recentObjects = pd.concat([recentObjects,pd.DataFrame(jmsg, columns=jmsg.keys(), index=[0])], ignore_index=True)
  #print('Length Recent Objects: ', len(recentObjects))
  if len(recentObjects)!=0:
    recentUniqueObjects = recentObjects.sort_values("jdmax", ascending = False).drop_duplicates(subset=["objectId"], inplace=False, keep="first")
  else: recentUniqueObjects = recentObjects
  #print(recentObjects)
  return recentUniqueObjects

@task
def splitIntoChunks(inLst, n):
    """The Lasair API can only handle 50 light curves a time.
    This function will split the list up into chunks of n objects. 
    """
    for i in range(0, len(inLst), n):
        yield inLst[i:i + n]

@task
def lightcurveSatify(criteria,lightcurve, ztfname):
    '''
    Our paper states that the tides slection criteria is as follows:
    - only consider griz
    - must have at least 3 5sigma detections
    - Must have 5sigma detections across 2 nights
    - Must reach brighter than 22.5mag
    '''
    #print(lightcurve.columns)
    needFilters = criteria['filters']
    needSignificance = criteria['significance']
    minBands = criteria['minBands']
    minNights = criteria['minNights']
    magLimit = criteria['magLimit']
    
    filtersBool = np.array([filterDict[x] in inputCriteriaName['filters'] for x in lightcurve['fid']])
    significanceBool = 1.09/lightcurve['sigmapsf'] >= needSignificance
    
    sigAndFilterBool = filtersBool & significanceBool
    
    meetMinBands = len(np.unique(lightcurve['fid'][sigAndFilterBool])) >= minBands
    
    meetMinNight = len(np.unique(lightcurve['nid'][sigAndFilterBool])) >= minNights
    
    meetMagLimit = min(lightcurve['magpsf']) <= magLimit
    
    if meetMinBands == meetMinNight == meetMagLimit == True:
        return ztfname, True
    else:
        return ztfname, False

@task
def daskCheckLightcurves(ztfName, c):
  if len(c)==0:
    return(False)
  lc = pd.json_normalize(c)
  print(lc)

  #nonDets = lc['candid'].isna()

  
  ##Does the whole object Pass/Fail our cuts
  wholePF = lightcurveSatify(inputCriteriaName, lc, ztfName)
  
  return(ztfName, wholePF)

@flow(task_runner=DaskTaskRunner())
def chunkyAssign(ztfNameChunks):
  namePassFail = []
  for chunk in ztfNameChunks:
    c = L.lightcurves(chunk)
    
    for idx in range(len(c)):
      #print(c[idx])
      if len(c[idx])==0:
        namePassFail.append([chunk[idx], False])
        continue
      else:
        lc = c[idx]['candidates']
        namePF = lightcurveSatify.submit(inputCriteriaName, pd.json_normalize(lc) ,chunk[idx])
        namePassFail.append(namePF)
  return namePassFail


@flow#(task_runner=DaskTaskRunner())
def checkChunksOfLightcurves(ztfLoopIn):
  passFail = []
  trigD = []
  c = L.lightcurves(ztfLoopIn)
  for i in range(len(c)): #Change the range to len(c)
      ztfN = ztfLoopIn[i]        
      print(ztfN)
      if len(c[i])==0:
          passFail.append('No Data')
          trigD.append(-9999)
          continue
      lc = pd.json_normalize(c[i])
      #print(lc)

      nonDets = lc['candid'].isna()

      
      ##Does the whole object Pass/Fail our cuts
      wholePF = lightcurveSatify(inputCriteriaName, lc)
      passFail.append(wholePF)
      doIwantToStepThrough = False
      if wholePF == True and doIwantToStepThrough == True:
          ## Now we step through all the detections to test _when_ the object passed
          datesTest = np.unique(lc['jd'][~nonDets])
          pf = np.array(list((map(lambda x: lightcurveSatify(inputCriteriaName, lc[lc['jd']<=x]), datesTest))))
          dateItPasses = min(datesTest[pf])
      else: 
          dateItPasses = -9999
      trigD.append(dateItPasses)
  return(passFail,trigD)
  
    # totalListPassFail.append(passFail)
    # totalDateTrigger.append(trigD)
    # return totalListPassFail, totalDateTrigger

@task
def passFailResultsDFandMerge(rPF, latestT):
  nPFdf = pd.DataFrame(rPF, columns=['Name','Pass'])
  merged = latestT.merge(nPFdf, left_on='objectId', right_on='Name', how='left')
  return merged




@flow
def sqlalchemy_credentials_flow():
    dbUsername, dbPassword, dbDatabase = loadTiDESdbSettings('devConfig')
    sqlalchemy_credentials = DatabaseCredentials(
        driver=AsyncDriver.POSTGRESQL_ASYNCPG,
        username=dbUsername,
        password=dbPassword,
        database=dbDatabase,
        host="localhost",
        port=5432,
    )
    print(sqlalchemy_credentials.get_engine())
    return sqlalchemy_credentials.get_engine()

@task
def sqlalchmey_engine():
  dbUsername, dbPassword, dbDatabase = loadTiDESdbSettings('devConfig')
  url = 'postgresql+psycopg2://'+str(dbUsername)+':'+str(dbPassword)+'@localhost:5432/'+str(dbDatabase)
  engine = sqlalchemy.create_engine(url,future=True)
  return engine

@task
def createTransientStage(dataTable, cnx):
  dataTable.columns = map(str.lower, dataTable.columns)
  dataTable[dataTable['pass']==True].to_sql('tides_stage', con=cnx, if_exists='replace', index=False)
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
  
@task
def upsertToMaster(cnx):
  query = open('upsertTiDESstage.sql', 'r')
  cnx.execute(sqlalchemy.text(query.read()))
  query.close()

@task
def deactivateUnobservedTransients(cnx):
  query = open('deactivateUnobserved.sql')
  cnx.execute(sqlalchemy.text(query.read()))


@task
def prepare4MOSTUpdate(cnx):
  query = open('stage4MOSTupdates.sql')
  updates = pd.read_sql(sqlalchemy.text(query.read()), con=cnx)
  # row = cnx.execute(sqlalchemy.text(query.read()))
  # print(row.mappings().all())
  query.close()
  return updates

@task
def createNewTransientin4MOST(tableIn):
  if len(tableIn)==0:
    return []
  for index,row in tableIn.iterrows():
    catDict = row.to_dict()
    #print(catDict['name'])
    uploadParams = {"uploadedfor_survey_id": 15,
    "name" : str(catDict['name']),
    "ra": np.float64(catDict['ra']),
    "dec": min(40,np.float64(catDict['dec'])),
    "pmra": 0.0,"pmdec": 0.0,
    "epoch": 2000,
    "resolution": 1,
    "subsurvey": 'tides-sn',
    "cadence": 1048576,
    "template": 'SN_spec_specid56_snt1_phase5_redshift0.169.fits',
    "ruleset": 'tides_snJuly2022',
    "redshift_estimate": 0.1,"redshift_error": 0,
    "extent_flag": 0,"extent_parameter": 0,"extent_index": 0,
    "mag": max(float(catDict['rmag']), float(catDict['rmag'])),"mag_err": 0,"mag_type": 'SDSS_r_AB',
    "reddening": 0,
    "date_earliest": np.float64(catDict['jdmax']),"date_latest": np.float64(catDict['jdmax'])+4,
    "t_exp_b": 60.,"t_exp_d": 60.,"t_exp_g": 60.,
    "is_active": catDict['active']}

    #print(uploadParams)
    uppedObject = st.create_transient(data=uploadParams, printout=False) 
    #print(uppedObject)
    tableIn.loc[index,'pk_4most'] = np.int64(uppedObject['id'])
  return tableIn
   

@task
def updateExisitingTransient(tableIn):
  if len(tableIn)==0:
    return []
  for index,row in tableIn.iterrows():
    catDict = row.to_dict()
    #print(catDict['name'])
    uploadParams = {"uploadedfor_survey_id": 15,
    "name" : str(catDict['name']),
    "ra": np.float64(catDict['ra']),
    "dec": min(40,np.float64(catDict['dec'])),
    "pmra": 0.0,"pmdec": 0.0,
    "epoch": 2000,
    "resolution": 1,
    "subsurvey": 'tides-sn',
    "cadence": 1048576,
    "template": 'SN_spec_specid56_snt1_phase5_redshift0.169.fits',
    "ruleset": 'tides_snJuly2022',
    "redshift_estimate": 0.1,"redshift_error": 0,
    "extent_flag": 0,"extent_parameter": 0,"extent_index": 0,
    "mag": max(float(catDict['rmag']), float(catDict['rmag'])),"mag_err": 0,"mag_type": 'SDSS_r_AB',
    "reddening": 0,
    "date_earliest": np.float64(catDict['jdmax']),"date_latest": np.float64(catDict['jdmax'])+4,
    "t_exp_b": 60.,"t_exp_d": 60.,"t_exp_g": 60.,
    "is_active": catDict['active']}

    print(catDict['pk_4most'])
    updatedObject = st.update_transient(pk=int(catDict['pk_4most']), data=uploadParams, printout=False) 

@task
def updateTiDESMasterwith4MOSTKey(newTable, cnx):
  newTable.columns = map(str.lower, newTable.columns)
  newTable['pk_4most'] = newTable['pk_4most'].astype(int).copy()
  newTable.to_sql('latest_4most', con=cnx, if_exists='replace', index=False)
  query = open('updateMasterWith4MOSTkey.sql')
  updates = cnx.execute(sqlalchemy.text(query.read()))
  # row = cnx.execute(sqlalchemy.text(query.read()))
  # print(row.mappings().all())
  query.close()

@flow
def executeCommPipe():

  my_topic, group_id =  loadTopicSettings('devConfig')

  print(my_topic, group_id)
  group_id = 'test{}'.format(randrange(1000)) ## Comment this out when doing pipeline for real
  print('Using group_id', group_id) #We'll fix our Group ID in production, but for now we randomise it so we have a good selection of objects. 


  consumer = lasair_consumer('kafka.lsst.ac.uk:9092', group_id, my_topic) ## Just accessing the Lasir interface to pull transients  

  latestTransients = getLatestBatch(consumer=consumer) ## (un)comment to use the real data stream.
  #print(latestTransients)
  #latestTransients = getDevBatch() ## Uncomment to use a test stream. i.e. Read a text file of objects
  if len(latestTransients) == 0:
    print('!!! No Transients !!!')
    return None
  print('All transients: ', len(latestTransients))

## DEV PURPOSES
  #latestTransients = latestTransients.sample(n=20).copy() ## We just take the a random sample of 20 transients so it doesn't take for ever in testing!
## DEV PURPOSES

  ztfNames = np.unique(latestTransients['objectId'])
  print('Unique transients: ', len(ztfNames))


  ztfNameChunks = list(splitIntoChunks(ztfNames, 50))

  print(ztfNameChunks)
  #print(list(map(checkChunksOfLightcurves, ztfNameChunks)))

  nPF = chunkyAssign(ztfNameChunks)
  resultPassFail = [x.result() for x in nPF]
  
  print(len(ztfNameChunks), len(nPF), len(resultPassFail), len(latestTransients))
  mergedDF = passFailResultsDFandMerge(resultPassFail, latestTransients) ## Pandas dataframe of all Lasair detections and pass/fail criteria
  #print(mergedDF)

  engine = sqlalchmey_engine() ## Create the connection to the TiDES DB

  createTransientStage(mergedDF, engine) ## Create a temporary table for the recent detections

  #Starting the session with the local TiDES Database
  with engine.connect() as conn, conn.begin() :
    upsertToMaster(conn)
    deactivateUnobservedTransients(conn)
    toUpdate = prepare4MOSTUpdate(conn)
    print('New Transients',len(toUpdate[toUpdate['pk_4most'].isnull()]))
    print('Updating Transients',len(toUpdate[~toUpdate['pk_4most'].isnull()]))
    newTransients = createNewTransientin4MOST(toUpdate[toUpdate['pk_4most'].isnull()])
    updatedTransients = updateExisitingTransient(toUpdate[~toUpdate['pk_4most'].isnull()])
    #print(newTransients)
    if len(newTransients)==0:
      print('No new transients to send to 4MOST')
      return None
    else:
      updateTiDESMasterwith4MOSTKey(newTransients, conn)
    

  
lasairToken = loadLasairDetails('devConfig')
L = lasair.lasair_client(lasairToken)

inputCriteriaPath, selectFuncName =  loadSelectionFunctionDetails('devConfig')
inputCriteriaOpen = yaml.load(open(inputCriteriaPath), Loader=yaml.SafeLoader)
inputCriteriaName = inputCriteriaOpen[str(selectFuncName)]
connect4MOST_API()

if __name__ == "__main__":

  executeCommPipe()