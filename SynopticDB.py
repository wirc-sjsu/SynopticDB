# Import Necessary Libraries
import datetime as dt
from datetime import timedelta
#from MesoPy import Meso
import numpy as np
import pandas as pd
import sqlite3
import synoptic.services as ss
from synoptic.services import stations_timeseries
from typing import List

# Functions 

# Creates the database with the data tables
#
# @ Param dbName - name of the database 
#
def create_db(dbName):
    try:
        # Connect to the database (will create the file if it doesn't exist)
        conn = sqlite3.connect(f'{dbName}.db',timeout=3600)
        print("Creating Tables")
        # Create a cursor object to execute SQL queries
        c = conn.cursor()
        
        # Create the Stations table with three columns: STID, latitude, and longitude
        c.execute('''CREATE TABLE Stations
                  (STID TEXT PRIMARY KEY, latitude REAL, longitude REAL, elevation REAL, state TEXT,
                   UNIQUE(STID, state))''')
              
        # Create the Temperature table with three columns: STID, datetime, and value
        c.execute('''CREATE TABLE Temperature
                  (STID TEXT, datetime TEXT, value REAL, 
                   UNIQUE(STID, datetime))''')

        # Create the RelativeHumidity table with three columns: STID, datetime, and value
        c.execute('''CREATE TABLE RelativeHumidity
                  (STID TEXT, datetime TEXT, value REAL, 
                   UNIQUE(STID, datetime))''')
        
        # Create the WindSpeed table with three columns: STID, datetime, and value
        c.execute('''CREATE TABLE WindSpeed
                  (STID TEXT, datetime TEXT, value REAL, 
                   UNIQUE(STID, datetime))''')
        
        # Create the SolarRadiation table with three columns: STID, datetime, and value
        c.execute('''CREATE TABLE SolarRadiation
                  (STID TEXT, datetime TEXT, value REAL, 
                   UNIQUE(STID, datetime))''')
        
        # Create the Precip table with three columns: STID, datetime, and value
        c.execute('''CREATE TABLE Precip
                  (STID TEXT, datetime TEXT, value REAL, 
                   UNIQUE(STID, datetime))''')
                  
        # Create the FuelMoisture table with three columns: STID, datetime, and value
        c.execute('''CREATE TABLE FuelMoisture
                  (STID TEXT, datetime TEXT, value REAL, 
                   UNIQUE(STID, datetime))''')
        
        # Commit changes to the database
        conn.commit()

        # Close the database connection
        conn.close()
    except:
        print(f"{dbName}.db already exists")


# Queries the database based on the request parameters provided by the user
#
# @ Param dbName - the name of the database
# @ Param tableName - a list of tables from the database
# @ Param stationIDs - a list of STIDS (e.g., ["ATNC1","SFTC1",...])
# @ Param state - a list of abbrieviated states (e.g., ["CA","TX",...])
# @ Param bbox - list of 4 coordinates for a bounding box (e.g., [minLon, minLat, maxLon, maxLat])
# @ Param startDate - datetime which the user wants the temporal range to start
# @ Param endDate - datetime which the user wants the temporal range to end
#
# @ returns a dataframe containing all the data the user requested
#
def query_db(dbName, tableNames=None, stationIDs=None, state=None, bbox=None, startDate=None, endDate=None):
    
    # Open connection to sqlite database
    conn = sqlite3.connect(f'{dbName}.db')
    
    # Create a cursor object to execute SQL queries
    c = conn.cursor()
    
    # Check if tableNames parameter is provided and if the tables are available in the database
    if tableNames:
        availTables = []
        for table in tableNames:
            c.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
            if c.fetchone() is not None:
                availTables.append(table)
        if len(availTables) == 0:
            # If none of the strings in tableNames are valid table names, return all table names except Stations
            c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name!='Stations'")
            tables = c.fetchall()
            availTables = [table[0] for table in tables]
    else:
        # If tableNames parameter is not provided, set availTables to only Temperature table
        availTables = ['Temperature']
    
    # Check if stationIDs parameter is provided
    if stationIDs:
        stids = []
        for stid in stationIDs:
            c.execute(f"SELECT STID FROM Stations WHERE STID='{stid}'")
            if c.fetchone() is not None:
                stids.append(stid)
        if len(stids) == 0:
            print("None of the STIDs provided are in the database")
            return
    elif bbox:
        # If bbox parameter is provided but stationIDs is None, use bbox to find STIDs in Stations table
        bboxQuery = f"SELECT STID FROM Stations WHERE longitude>{bbox[0]} AND longitude<{bbox[2]} AND latitude>{bbox[1]} AND latitude<{bbox[3]}"
        c.execute(bboxQuery)
        stids = [row[0] for row in c.fetchall()]
        if len(stids) == 0:
            print("No STIDs in the database lie within the bbox coordinates")
            return
    elif state:
        # Check state parameter
        stids = []
        for s in state:
            c.execute("SELECT STID FROM Stations WHERE state = ?", (s,))
            result = c.fetchall()
            if result is not None:
                stids.extend([x[0] for x in result])
        if not stids:
            print("No STIDs in the database match the provided state(s).")
            return
    
    else:
        # If all three parameters are None, add all STID values from Stations table to stids list
        c.execute("SELECT STID FROM Stations")
        stids = [row[0] for row in c.fetchall()]
    
    # If either startDate or endDate are None values, grab the last day's data
    # Check startDate and endDate parameters
    if startDate is None or endDate is None:
        endDate = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        startDate = endDate - timedelta(days=1)
    
    # Query data from tables and STIDs within startDate and endDate
    dfs = []
    for table in availTables:
        query = f"SELECT STID, datetime, value FROM {table} WHERE STID IN ({','.join('?'*len(stids))}) AND datetime BETWEEN ? AND ?"
        c.execute(query, stids + [startDate, endDate])
        rows = c.fetchall()
        if len(rows) > 0:
            df = pd.DataFrame(rows, columns=['STID', 'datetime', 'value'])
            df['table'] = table
            dfs.append(df)
    
    # Close connection to sqlite database
    conn.close()
    
    # Concatenate dataframes and return result
    if len(dfs) > 0:
        result = pd.concat(dfs, axis=0)
        return result


# Insert data into the database
#
# @ Param dbName - the name of the database
# @ Param observationDf - dataframe containing all the data to be put in the database
# @ Param stationDf - dataframe containing all the station data to be put in the database
#
def insert_data(dbName, observationDf, stationDf):
    
    # Open connection to sqlite database
    conn = sqlite3.connect(f'{dbName}.db',timeout=3600)
    
    # Create a cursor object to execute SQL queries
    c = conn.cursor()

    # Insert station data to Stations table
    for stationID, row in stationDf.iterrows():
        values = [stationID, row['longitude'], row['latitude'], row['ELEVATION'], row['STATE']]
        c.execute("INSERT OR IGNORE INTO Stations (STID, longitude, latitude, elevation, state) VALUES (?, ?, ?, ?, ?)", values)

    # Insert observation data to corrsponding database tables
    for observationCol in observationDf.columns[2:]:
        if "temp" in observationCol.lower():
            tableName = "Temperature"
        elif "humidity" in observationCol.lower():
            tableName = "RelativeHumidity"
        elif "radiation" in observationCol.lower():
            tableName = "SolarRadiation"
        elif "precip" in observationCol.lower():
            tableName = "Precip"
        elif "speed" in observationCol.lower():
            tableName = "WindSpeed"
        elif "moisture" in observationCol.lower():
            tableName = "FuelMoisture"
        else:
            continue
        print(f"Filling {tableName} Table")
        # Fill the corresponding column into the database 
        # (e.g., air_temp column will go in the Temperature Table)
        for idx, row in observationDf.iterrows():
            stationID = row['STID']
            obsValue = row[observationCol]
            if pd.isna(obsValue):
                obsValue = None
            datetimeStr = row['date_time'].strftime('%Y-%m-%d %H:%M:%S')
            values = [stationID, datetimeStr, obsValue]
            c.execute(f"INSERT OR IGNORE INTO {tableName} (STID, datetime, value) VALUES (?, ?, ?)", values)
    
    # Commit changes to the database
    conn.commit()
    
    # close the database connection
    conn.close()


# Check the station data within the database
#
# @ Param dbName - the name of the database
#
# @ returns the station data from within the database
#
def check_stid(dbName):
    # create a connection to the mesoDB database
    conn = sqlite3.connect(f'{dbName}.db')

    # define the SQL query to select the STID, latitudes, and longitudes columns from the Station table
    query = "SELECT STID, latitude, longitude, elevation, state FROM Stations"

    # execute the query and store the results in a pandas dataframe
    dfStations = pd.read_sql_query(query, conn)

    # close the database connection
    conn.close()
    return dfStations


# Check the size of a table within the database
#
# @ Param dbName - the name of the database
# @ Param tableName - the name of the table to be requested
# 
# @ prints the number of rows in the requested table
#
def check_col_size(dbName,tableName):
    # Connect to the database
    conn = sqlite3.connect(f'{dbName}.db')

    # Create a cursor object to execute SQL queries
    c = conn.cursor()
    
    # Define the table name
    tableName = tableName

    # Count the number of rows in the table
    c.execute(f"SELECT COUNT(*) FROM {tableName}")

    # Fetch the result of the query
    result = c.fetchone()

    # Check if there are any rows in the table
    if result[0] > 0:
        print(f"There are {result[0]} rows in the {tableName} table.")
    else:
        print(f"There are no rows in the {tableName} table.")
    
    # Close the database connection
    conn.close()


# Check the contents of one of the tables within the database
#
# @ Param dbName - the name of the database
# @ Param tableName - the name of the table to be requested
#
# @ returns a dataframe of all the data from the requested table
#
def check_table(dbName,tableName):
    # create a connection to the mesoDB database
    conn = sqlite3.connect(f'{dbName}.db')

    # define the SQL query to select the STID, latitudes, and longitudes columns from the Station table
    query = f"SELECT STID, datetime, value FROM {tableName}"

    # execute the query and store the results in a pandas dataframe
    dfStations = pd.read_sql_query(query, conn)

    # close the database connection
    conn.close()
    return dfStations


# Tranform dataframe data from Syboptic API request to a format that can be inputted to the database
# 
# @ Param synData - data from synoptic api
# 
# @ returns two dataframes, one with all the data from the synData and one with the station data
#
def synData_to_df(synData):
    siteKeys = ['STID','longitude','latitude','ELEVATION','STATE']
    siteDic = {key: [] for key in siteKeys}
    dataKeys = synData[0].attrs["params"]["vars"]
    dataKeys.insert(0,"date_time")
    dataKeys.insert(1,"STID")
    dataDic = {key: [] for key in dataKeys}
    for site in range(len(synData)):
        for siteKey in siteKeys:
            siteDic[siteKey].append(synData[site].attrs[siteKey])
        dataLen = 0
        for dataKey in dataKeys:
            try:
                for data in synData[site][dataKey]:
                    dataDic[dataKey].append(data)
            except:
                if dataKey == "date_time":
                    for date in [dt.to_pydatetime() for dt in synData[site].index]:
                        dataDic[dataKey].append(date)
                    dataLen = len([dt.to_pydatetime() for dt in synData[site].index])
                elif dataKey == "STID":
                    for value in range(dataLen):
                        dataDic[dataKey].append(synData[site].attrs["STID"])
                else:
                    # Some stations do not have all of the variables requested
                    # Any variable not in the station is entered as a NaN
                    for value in range(dataLen):
                        dataDic[dataKey].append([np.nan])
    data = pd.DataFrame.from_dict(dataDic)
    sites = pd.DataFrame.from_dict(siteDic).set_index('STID')
    return data,sites


# Uses SynopticPy to get data from the Synoptic Weather Site
#
# @ Param startTime - datetime which the user wants the temporal range to start
# @ Param endTime - datetime which the user wants the temporal range to end
# @ Param bbox - bounding box from which the user can request a spatial range
# @ Param state - states the user wants data from
#
# @ returns a pandas dataframe that contains multitple dataframes with data from all of the station requested
#
def get_synData(startTime=None,endTime=None,bbox=None,state=["CA"]):
    
    # If either startTime or endTime are None values, grab the last day's data
    # Check startTime and endTime parameters
    if startTime is None or endTime is None:
        endTime = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        startTime = endTime - timedelta(days=1)
    startUtc = "{:04d}{:02d}{:02d}{:02d}{:02d}".format(startTime.year,startTime.month,startTime.day,startTime.hour,0)
    endUtc = "{:04d}{:02d}{:02d}{:02d}{:02d}".format(endTime.year,endTime.month,endTime.day,endTime.hour,0)
    if bbox != None:
        df = stations_timeseries(
            start=startUtc, 
            end=endUtc,
            network=2,
            varsoperator="AND",
            status="ACTIVE",
            sensorvars=1,
            country="US",
            state=state,
            bbox=bbox,
            vars=['air_temp','relative_humidity',"solar_radiation","precip_accum","fuel_moisture",'wind_speed'],
            verbose=False
            )
    else:
        df = stations_timeseries(
            start=startUtc, 
            end=endUtc,
            network=2,
            varsoperator="AND",
            status="ACTIVE",
            sensorvars=1,
            country="US",
            state=state,
            vars=['air_temp','relative_humidity',"solar_radiation","precip_accum","fuel_moisture",'wind_speed'],
            verbose=False
            )
        
    return df
    

if __name__ == '__main__':
    
    dbName = "mesoDB"
    #create_db(dbName)
    
    bbox = [-118.3942105848254, 33.92822141009315, -117.43124059110482, 34.71977126831858]
    ##tempDf = get_synData(dt.datetime(2022,1,1,0),dt.datetime(2022,1,5,0),bbox=bbox) # With bbox
    ##tempDf = get_synData(dt.datetime(2022,1,1,0),dt.datetime(2022,1,5,0)) # Without bbox
    ##mesoDf,siteData = synData_to_df(tempDf)

    ##insert_data(dbName,mesoDf,siteData)
    
    # Check the size of a table within the database
    ##check_col_size(dbName, "Temperature")

    # Check station data
    ##stations = check_stid("mesoDB")
    
    # Check the contents of a given table in the database
    ##tempData = check_table(dbName,"Precip")

    # Set up parameters
    dbName = "mesoDB"
    tableNames = ["Temperature","RelativeHumidity"]
    tableFields = ['stationID', 'datetime', 'value']
    tableSTID = ["ALDC1"]
    startDate = "2022-01-01 00:00"
    endDate = "2022-01-02 00:00"
    #bbox = [39.8, 40.1, -105.5, -105.2]
    bbox = [-118.3942105848254, 33.92822141009315, -117.43124059110482, 34.71977126831858]
    df = query_db(dbName, tableNames=tableNames, stationIDs=None, state=["TX"], bbox=None, startDate=startDate, endDate=endDate)

