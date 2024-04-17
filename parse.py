import io
import os
import zipfile
import urllib.request
import xml.etree.ElementTree as ElementTree
import pandas as pd
import numpy as np
import time
import csv
import datetime
from datetime import timedelta

# Ensure dependencies are installed
# pip3 install io zipfile pandas numpy urllib
# python3 -m pip install mysql-connector

class Parse:
    """
    A class for parsing weather forecast data from DWD (Deutscher Wetterdienst).
    """

    def __init__(self):
        """
        Constructor for Parse class.
        """
        self.run(fcdate=datetime.datetime.now() - timedelta(hours=6))
        self.run(fcdate=datetime.datetime.now())

    def run(self, fcdate):
        """
        Main method to run the parsing process.

        Args:
            fcdate (datetime): The forecast date.

        Returns:
            None
        """
        fcdate = self.getFcDate(fcdate)
        print('Running : ' + str(fcdate))
        stations = self.getStations()
        counter = 0
        startTime = time.time()

        for station in stations:
            counter += 1
            if(time.time() - startTime):
                print('parsing station: '+station+' '+str(counter/(time.time() - startTime)) + " -- " + str(counter) + " -- " + str((time.time() - startTime)) + " secs")
            self.parse(station=station, fcdate=fcdate)
        runTime = time.time()-startTime 

    def getFcDate(self, fcdate):
        """
        Adjust the forecast date based on the current hour.

        Args:
            fcdate (datetime): The forecast date.

        Returns:
            str: The adjusted forecast date string.
        """
        now = fcdate
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
        """
        Generate URL for downloading weather forecast data.

        Args:
            station (str): Station code.
            fcDate (str): Forecast date.

        Returns:
            str: URL for downloading weather forecast data.
        """
        ret = fcDate
        ret = ret.replace("-", "")
        ret = ret.replace(" ", "")
        ret = ret.replace(":00", "")
        
        ret = "http://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_L/single_stations/" + str(station) + "/kml/MOSMIX_L_"+ret+"_" + str(station) + ".kmz"
        
        return ret

    def getStations(self):
        """
        Retrieve station codes for parsing weather forecast data.

        Returns:
            list: List of station codes.
        """
        files = pd.read_csv('https://opendata.dwd.de/weather/local_forecasts/content.log.bz2',  sep='|', compression='bz2', names=['file', 'size', 'date'])
        files = files[files.file.str.contains('MOSMIX_L/single_stations')]
        files['station'] = files['file'].str.split('/').str[4]
        files.sort_values(['station'], inplace=True)
        stations = files['station'].unique()
        return stations

    def parse(self, station, fcdate):
        """
        Parse weather forecast data for a given station and forecast date.

        Args:
            station (str): Station code.
            fcdate (str): Forecast date.

        Returns:
            None
        """
        url = self.getUrl(station, fcdate)
        
        try:
            filename = '/tmp/pyparser'    
            filename, headers = urllib.request.urlretrieve(url)
            kmz = zipfile.ZipFile(filename, 'r')
            for name in kmz.namelist():
                kml = kmz.read(name)   

        except:
            print("An exception occurred")
            return
        
        root = ElementTree.fromstring(kml)
        ns = {'xmlns': "http://www.opengis.net/kml/2.2", 'dwd': 'https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd'}
        timestamps = []
        forecasts = dict()

        for element in root.findall('.//dwd:IssueTime', namespaces=ns):
            issuetime = element.text

        for element in root.findall('.//dwd:TimeStep', namespaces=ns):
            timestamps.append(element.text)

        for element in root.findall('.//dwd:Forecast', ns):
            forecasts.update({element.attrib['{https://opendata.dwd.de/weather/lib/pointforecast_dwd_extension_V1_0.xsd}elementName']:element[0].text.split()})
        
        df = pd.DataFrame(forecasts)
        df.columns = [x.lower() for x in df.columns]

        df['fc_date'] =  pd.datetime.today().strftime("%Y-%m-%d %H:%M")
        df['fc_date'] = issuetime
        df['fc_date'] = pd.to_datetime(df['fc_date'])

        df['fc_target_date'] = timestamps
        df['fc_target_date'] = pd.to_datetime(df['fc_target_date'])

        df['fc_target'] = (df['fc_target_date'] - df['fc_date'])
        df['fc_target'] = (df['fc_target'].astype('timedelta64[h]'))

        # Clean data
        cols = ['ww', 'alle3']
        for col in cols:
            if col not in df.columns:
                df[col] = 0
        
        for col in df.columns:        
            df[col] = df[col].replace('-',0)
        
        # Here, you would typically insert the data into your database
        # Example: df.to_sql('your_table_name', your_database_connection, if_exists='append', index=False)

# Instantiate the Parse class
d = Parse()
