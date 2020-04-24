# changes to the db ww col -> int 
# alle3  decimal(8,2) naar float

# pip3 install io zipfile pandas numpy urllib
# python3 -m pip install mysql-connector
import io
import os
import zipfile
import urllib.request
import xml.etree.ElementTree as ElementTree
import pandas as pd
import numpy as np
import urllib.request
import mysql.connector
import time
import csv
import datetime
from datetime import  timedelta



# PHP Legacy
def isset(variable):
	    return variable in locals() or variable in globals()

class Parse:
    # constructur
    def __init__(self):


        self.run( fcdate = datetime.datetime.now() - timedelta(hours = 6) ) 
        self.run( fcdate = datetime.datetime.now() )
        
    
    def run(self, fcdate):
        
        fcdate = self.getFcDate(fcdate)
        print('Running : ' + str(fcdate))
        stations = self.getStations()
        # parameters for measuring the loop
        counter = 0
        startTime = time.time()
        

        # loop stations & parse latest
        for station in stations:
            counter += 1
            if(time.time() - startTime):
                print('parsing station: '+station+' '+str(counter/(time.time() - startTime)) + " -- " + str(counter) + " -- " + str((time.time() - startTime)) + " secs")
            self.parse(station=station, runId=runId, fcdate=fcdate)
        runTime = time.time()-startTime 
        
        self.activateRun(runId=runId)

   

    def getFcDate(self, fcdate):
        now = fcdate
        # quick & dirty (it works)
        if(now.hour<=23):
            fcdate = str(now.year)+'-'+(str(now.month)).zfill(2)+'-'+str(now.day).zfill(2)+' 21:00' 
        if(now.hour<21):
            fcdate = str(now.year)+'-'+(str(now.month)).zfill(2)+'-'+str(now.day).zfill(2)+' 15:00'
        if(now.hour<15):
            fcdate = str(now.year)+'-'+(str(now.month)).zfill(2)+'-'+str(now.day).zfill(2)+' 09:00'
        if(now.hour<9):
            fcdate = str(now.year)+'-'+(str(now.month)).zfill(2)+'-'+str(now.day).zfill(2)+' 03:00'
        if(now.hour<3):
            now = datetime.datetime.now() + timedelta(hours-3)
            fcdate = str(now.year)+'-'+(str(now.month)).zfill(2)+'-'+str(now.day).zfill(2)+' 21:00' 
        return fcdate   
    
    def getUrl(self, station, fcDate):
        ret = fcDate
        ret = ret.replace("-", "")
        ret = ret.replace(" ", "")
        ret = ret.replace(":00", "")
        
        ret = "http://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_L/single_stations/" + str(station) + "/kml/MOSMIX_L_"+ret+"_" + str(station) + ".kmz"
        
        return ret

    def getStations(self):        
        #  downloading local forecast single station file index from dwd
        files = pd.read_csv('https://opendata.dwd.de/weather/local_forecasts/content.log.bz2',  sep='|', compression='bz2', names=['file', 'size', 'date'])
        files = files[files.file.str.contains('MOSMIX_L/single_stations')]
        files['station'] = files['file'].str.split('/').str[4]
        files.sort_values(['station'], inplace=True)

        # create array with stations to parse
        stations = files['station'].unique()

        return stations



    
   


    def parse(self, station, runId, fcdate):
        # get/insert stationId
        stationId = self.getStation(station=station)

        if(self.checkFile(stationId=stationId, runId=runId) == True):
            print('skip already parsed')
        
        # get/insert fileId
        fileId = self.getFileId(stationId=stationId, runId=runId)

        url = self.getUrl(station, fcdate)
        
        try:
            filename = '/tmp/pyparser'    
            filename, headers = urllib.request.urlretrieve(url)
            
            # unzip kmz
            kmz = zipfile.ZipFile(filename, 'r')
            for name in kmz.namelist():
                kml = kmz.read(name)   

        except:
            print("An exception occurred")
            return
        # download kmz
  
        # parse xml
        root = ElementTree.fromstring(kml)

        ns = {'xmlns': "http://www.opengis.net/kml/2.2", 'dwd': 'https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd'}

        # temporary storage of forecast values
        timestamps = []
        forecasts = dict()

        # Find all timestamps
        for element in root.findall('.//dwd:IssueTime', namespaces=ns):
            issuetime = element.text
        
        # Find all timestamps
        for element in root.findall('.//dwd:TimeStep', namespaces=ns):
            timestamps.append(element.text)

        # Find all forecasts
        for element in root.findall('.//dwd:Forecast', ns):
            forecasts.update({element.attrib['{https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd}elementName']:element[0].text.split()})
        
        # init pandas dataframe
        df = pd.DataFrame(forecasts)
        
        # change column names to lowercase
        df.columns = [x.lower() for x in df.columns]

        # hylke klopt dit?
        df['wcode'] = station

        df['fc_date'] =  pd.datetime.today().strftime("%Y-%m-%d %H:%M") # ??? todo
        df['fc_date'] = issuetime
        df['fc_date'] = pd.to_datetime(df['fc_date'])

        # amateur parser
        df['fc_target_date'] = timestamps
        # df['fc_target_date'] = df['fc_target_date'].str.slice(0, 16)
        # df['fc_target_date'] = df['fc_target_date'].str.replace("T"," ")
        df['fc_target_date'] = pd.to_datetime(df['fc_target_date'])
        # df['fc_target_date'] = np.array(timestamps, dtype='datetime64[m]')
        # df['fc_target_date'] = pd.to_datetime(df['fc_target_date'] , format="%Y-%m-%d %H:%M")
    
        
        df['station'] = stationId
        df['kml_file_id'] = fileId
        df['mosrun'] = runId
        
        #  calculate diff in hours
        df['fc_target'] = (df['fc_target_date'] - df['fc_date'])
        df['fc_target'] = (df['fc_target'].astype('timedelta64[h]'))

        # columns to insert in mysql
        cols = ['mosrun','station', 'kml_file_id','fc_date','fc_target_date','fc_target','tt','td','tx','tn','dd','ff','fx','rr','rs','ww','w','n','nef','ncl','ncm','nch','pppp','tg','qsw','qgs','qlw']

        # fill empty cols / cleaning the data
        for col in cols:
            if col not in df.columns:
                df[col] = 0
        # replace - for 0        
        for col in df.columns:        
            df[col] = df[col].replace('-',0)
        
        

        # pandas select cols
        df = df[cols]


    
