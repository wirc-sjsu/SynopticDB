# Import Necessary Libraries
import datetime as dt
from datetime import timedelta
import os.path as osp
import os
import pandas as pd
import sqlite3
from synoptic.services import stations_timeseries
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class synopticError(Exception):
    pass

class synopticDB(object):
    
    # Constructor for SynopticDB class
    #
    # @ Param folder_path - path where this script is located
    #
    def __init__(self, folderPath=osp.join(osp.abspath(os.getcwd()),"synDB.db")):
        self.dbPath = osp.join(folderPath)
        # Open connection to sqlite database
        conn = sqlite3.connect(self.dbPath)
        dbTableNames = self.get_table_names()
        if not "Stations" in dbTableNames:
            # Create a cursor object to execute SQL queries
            c = conn.cursor()
            # Create the Stations table with three columns: STID, latitude, and longitude
            c.execute('''CREATE TABLE Stations
                      (STID TEXT PRIMARY KEY, latitude REAL, longitude REAL, elevation REAL, state TEXT,
                       UNIQUE(STID, state))''')
        # Commit changes to the database
        conn.commit()
        # Close the database connection
        conn.close()


    # Queries the database based on the request parameters provided by the user
    #
    # @ Param tableName - a list of tables from the database
    # @ Param stationIDs - a list of STIDS (e.g., ["ATNC1","SFTC1",...])
    # @ Param state - a list of abbrieviated states (e.g., ["CA","TX",...])
    # @ Param bbox - list of 4 coordinates for a bounding box (e.g., [minLon, minLat, maxLon, maxLat])
    # @ Param startDate - datetime which the user wants the temporal range to start
    # @ Param endDate - datetime which the user wants the temporal range to end
    #
    # @ returns a dataframe containing all the data the user requested
    #
    def query_db(self, tableNames=None, stationIDs=None, state=None, bbox=None, startDate=None, endDate=None):
    
        # Open connection to sqlite database
        conn = sqlite3.connect(self.dbPath)
        
        # Create a cursor object to execute SQL queries
        c = conn.cursor()
    
        # Check if tableNames parameter is provided and if the tables are available in the database
        if tableNames != None:
            availTables = []
            for table in tableNames:
                c.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
                if c.fetchone() is not None:
                    availTables.append(table)
            if len(availTables) == 0:
                logging.warning("No table names provided. Pick from avaiable tables below:")
                logging.warning(self.get_table_names())
                return
        else:
            logging.warning("No table names provided. Pick from avaiable tables below:")
            logging.warning(self.get_table_names())
            return
    
        # Check if stationIDs parameter is provided
        if stationIDs != None:
            stids = []
            for stid in stationIDs:
                c.execute(f"SELECT STID FROM Stations WHERE STID='{stid}'")
                if c.fetchone() is not None:
                    stids.append(stid)
            if len(stids) == 0:
                logging.warning("None of the STIDs provided are in the database")
                return
        elif bbox != None:
            # If bbox parameter is provided but stationIDs is None, use bbox to find STIDs in Stations table
            bboxQuery = f"SELECT STID FROM Stations WHERE longitude>{bbox[0]} AND longitude<{bbox[2]} AND latitude>{bbox[1]} AND latitude<{bbox[3]}"
            c.execute(bboxQuery)
            stids = [row[0] for row in c.fetchall()]
            if len(stids) == 0:
                logging.warning("No STIDs in the database lie within the bbox coordinates")
                return
        elif state != None:
            # Check state parameter
            stids = []
            for s in state:
                c.execute("SELECT STID FROM Stations WHERE state = ?", (s,))
                result = c.fetchall()
                if result is not None:
                    stids.extend([x[0] for x in result])
            if not stids:
                logging.warning("No STIDs in the database match the provided state(s).")
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
            query = f"SELECT STID, datetime, value, units FROM {table} WHERE STID IN ({','.join('?'*len(stids))}) AND datetime BETWEEN ? AND ?"
            c.execute(query, stids + [startDate, endDate])
            rows = c.fetchall()
            if len(rows) > 0:
                df = pd.DataFrame(rows, columns=['STID', 'datetime', 'value', 'units'])
                df['table'] = table
                dfs.append(df)
    
        # Close connection to sqlite database
        conn.close()
    
        # Concatenate dataframes and return result
        if len(dfs) > 0:
            result = pd.concat(dfs, axis=0)
            return result
        else:
            logging.warning("No data found in time range")
            return


    # Insert data into the database
    #
    # @ Param dbName - the name of the database
    # @ Param observationDf - dataframe containing all the data to be put in the database
    #
    def insert_data(self, observationDf):
        # Open connection to sqlite database
        conn = sqlite3.connect(self.dbPath,timeout=3600)
        # Create a cursor object to execute SQL queries
        c = conn.cursor()
        # Get a list of all the current tables in the database
        # Insert station data to Stations table and observation data to corresponding database tables
        if not isinstance(observationDf, list):
            observationDf = [observationDf]
        for site in observationDf:
            values = [site.attrs["STID"], site.attrs['longitude'], site.attrs['latitude'], site.attrs['ELEVATION'], site.attrs['STATE']]
            c.execute("INSERT OR IGNORE INTO Stations (STID, longitude, latitude, elevation, state) VALUES (?, ?, ?, ?, ?)", values)
            for observationCol in site.columns:
                dbTableNames = self.get_table_names()
                if not observationCol in dbTableNames:
                    c.execute("CREATE TABLE {} ({} {}, {} {}, {} {}, {} {}, UNIQUE(STID, datetime))".format(observationCol, "STID", "TEXT", "datetime", "TEXT", 
                                                                                                     "value", "REAL","units","TEXT"))
                # Fill the corresponding column into the database 
                # (e.g., air_temp column will go in the air_temp Table)
                obsValue = site[observationCol]
                stationID = [site.attrs["STID"]] * len(obsValue)
                datetimeStr = site.index.strftime('%Y-%m-%d %H:%M:%S')
                units = site.attrs["UNITS"][observationCol]
                for idx, value in enumerate(obsValue):
                    values = [stationID[idx], datetimeStr[idx], value, units]
                    c.execute(f"INSERT OR IGNORE INTO {observationCol} (STID, datetime, value, units) VALUES (?, ?, ?, ?)", values)
                # Commit changes to the database
                conn.commit()
        # close the database connection
        conn.close()
    
    
    # Uses SynopticPy to get data from the Synoptic Weather Site and insert the data into the database
    #
    # @ Param startTime - datetime which the user wants the temporal range to start
    # @ Param endTime - datetime which the user wants the temporal range to end
    # @ Param bbox - bounding box from which the user can request a spatial range
    # @ Param state - states the user wants data from
    # @ Param vars - list of variables to query (potential vars = 'air_temp','relative_humidity',"solar_radiation","precip_accum","fuel_moisture",'wind_speed')
    #
    def get_synData(self,startTime=None,endTime=None,network=None,bbox=None,state=None,allVars=False,vars =['air_temp','relative_humidity']):
        if allVars == False:
            operator = "OR"
        else:
            operator = "AND"
        # If either startTime or endTime are None values, grab the last day's data
        # Check startTime and endTime parameters
        if startTime is None or endTime is None:
            endTime = dt.datetime.now().replace(minute=0, second=0, microsecond=0)
            startTime = endTime - timedelta(days=1)
        tmpTime = startTime + dt.timedelta(days=1)
        while tmpTime <= endTime:
            logging.info('getting data from synoptic between {} and {}'.format(startTime,tmpTime))
            startUtc = "{:04d}{:02d}{:02d}{:02d}{:02d}".format(startTime.year,startTime.month,startTime.day,startTime.hour,0)
            endUtc = "{:04d}{:02d}{:02d}{:02d}{:02d}".format(tmpTime.year,tmpTime.month,tmpTime.day,tmpTime.hour,0)
            startTime = startTime + dt.timedelta(days=1)
            tmpTime = tmpTime + dt.timedelta(days=1)
            try:
                df = stations_timeseries(
                    start=startUtc, 
                    end=endUtc,
                    network=network,
                    varsoperator=operator,
                    country="US",
                    state=state,
                    bbox=bbox,
                    vars=vars,
                    verbose=False
                )
            except Exception as e:
                logging.warning('get_synData with exception {}'.format(e))
                continue
            # Insert the queired data to the dataabse
            self.insert_data(df)

    # Check the station data within the database
    #
    # @ Param dbName - the name of the database
    #
    # @ returns the station data from within the database
    #
    def check_stid(self):
        # create a connection to the mesoDB database
        conn = sqlite3.connect(self.dbPath)
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
    def check_col_size(self,tableName):
        # Connect to the database
        conn = sqlite3.connect(self.dbPath)
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
            logging.info(f"There are {result[0]} rows in the {tableName} table.")
        else:
            logging.warning(f"There are no rows in the {tableName} table.")
        # Close the database connection
        conn.close()
    
    
    # Check the contents of one of the tables within the database
    #
    # @ Param dbName - the name of the database
    # @ Param tableName - the name of the table to be requested
    #
    # @ returns a dataframe of all the data from the requested table
    #
    def check_table(self,tableName):
        # create a connection to the mesoDB database
        conn = sqlite3.connect(self.dbPath)
        # define the SQL query to select the STID, latitudes, and longitudes columns from the Station table
        query = f"SELECT STID, datetime, value FROM {tableName}"
        # execute the query and store the results in a pandas dataframe
        dfStations = pd.read_sql_query(query, conn)
        # close the database connection
        conn.close()
        return dfStations
    

    # Retrieve all table names from a SQLite database and return them in a list.
    #
    def get_table_names(self):
        # Connect to the SQLite database
        conn = sqlite3.connect(self.dbPath)
        # Get a cursor object
        c = conn.cursor()
        # Query the SQLite master table for all table names
        c.execute("SELECT name FROM sqlite_master WHERE type='table';")
        # Fetch all the table names and store them in a list
        tableNames = [row[0] for row in c.fetchall()]
        # Close the cursor and connection
        c.close()
        conn.close()
        # Return the list of table names
        return tableNames
    

if __name__ == '__main__':
    
    # Any lines with double hashtags (##) indicate possible usages for the database if the hashtags are removed
    dbName = "mesoDB"
    synDb = synopticDB()
    tempDf = synDb.get_synData(dt.datetime(2000,1,1,0),dt.datetime(2023,1,1,0),network=2,state=None,
                               vars=['air_temp','relative_humidity',"solar_radiation","precip_accum","fuel_moisture",'wind_speed']) # Without bbox

    # Check the size of a table within the database
    ##check_col_size(dbName, "Temperature")

    # Check station data
    ##stations = check_stid("mesoDB")
    
    # Check the contents of a given table in the database
    ##tempData = check_table(dbName,"Precip")

    # Query data from the database
    ##tableNames = ["fuel_moisture","air_temp"]
    ##tableSTID = ["ALDC1"]   
    ##startDate = "2022-01-09 00:00"
    ##endDate = "2022-01-10 00:00"
    ##df = synDb.query_db(tableNames=tableNames, stationIDs=["AATC1"], state=["CA"], bbox=None, startDate=startDate, endDate=endDate)
