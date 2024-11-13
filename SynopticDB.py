# Import Necessary Libraries
import datetime as dt
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import os.path as osp
import os
import pandas as pd
import sqlite3
import toml
from .utils import ensure_list, get_networks, get_stations, get_timeseries
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class SynopticError(Exception):
    pass

class SynopticDB(object):
    # Constructor for SynopticDB class
    #
    # @ Param folderPath - path where this script is located
    #
    def __init__(self, folderPath=osp.join(osp.abspath(os.getcwd()), "synDB.db")):
        self.dbPath = osp.join(folderPath)
        # Get the users token
        try:
            # Manually expand the tilde in the file path
            tokenPath = os.path.expanduser('~/.config/SynopticPy/config.toml')
            # Open and parse the TOML file
            config = toml.load(tokenPath)
            # Access the 'token' value
            self.token = config['default']['token']
        except:
            # If the user does not have a token in the right location, give the user the instructions on how to set up their token
            raise SynopticError("Token not found. Follow instructions here to add a token: https://github.com/blaylockbk/SynopticPy#-setup")
        # Initialize the parameters for querying the database
        self.init_params()
        # Open connection to sqlite database. Using "with" prevents database corruption
        with sqlite3.connect(self.dbPath) as conn:
            # Create a cursor object to execute SQL queries
            c = conn.cursor()
            # Get a list of all of the avaialable tables in the database
            dbTableNames = self.list_table_names()
            # Create a Metadata table
            if not "Metadata" in dbTableNames:
                # Create the Metadata table with key and value structure
                c.execute('''CREATE TABLE Metadata 
                       (key TEXT PRIMARY KEY, value TEXT)''') 
                # Commit changes to the database
                conn.commit()
                # Add creation date to Metadata
                c.execute("INSERT OR IGNORE INTO Metadata (key, value) VALUES (?, ?)", 
                        ("creation_date_utc", dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d_%H:%M:%S')))
                # Commit changes to the database
                conn.commit()
                logging.info("Created a Metadata table")
            # Add the Stations table if it is not in the database
            if not "Stations" in dbTableNames:
                # Create the Stations table with eight columns: STID, NAME, LATITUDE, LONGITUDE, ELEVATION, ELEVATION_UNITS, STATE, LAST_ACTIVE, & NETWORK_ID
                c.execute('''CREATE TABLE Stations
                        (STID TEXT PRIMARY KEY, NAME TEXT, STATE TEXT, LATITUDE REAL, LONGITUDE REAL, ELEVATION REAL, ELEVATION_UNITS TEXT, LAST_ACTIVE DATETIME, NETWORK_ID INTEGER,
                        UNIQUE(STID, NAME, STATE))''')
                # Commit changes to the database
                conn.commit()
                # Get all of the Synoptic station data
                self.build_stations_table()
                logging.info("Created a Stations table")
            # Add the Networks table if it is not in the database
            if not "Networks" in dbTableNames:
                c.execute('''CREATE TABLE Networks
                        (NETWORK_ID INTEGER PRIMARY KEY, NETWORK_NAME_SHORT TEXT, NETWORK_NAME_LONG TEXT,
                        UNIQUE(NETWORK_ID, NETWORK_NAME_SHORT))''')
                # Commit changes to the database
                conn.commit()
                # Get Synoptic network ids and insert them into the database
                self.build_networks_table()
                logging.info("Created a Networks table")

    # Initializes the parameters for getting data for and querying the database
    #
    def init_params(self):
        self.params = {'endDatetime': dt.datetime.utcnow(), 'startDatetime':(dt.datetime.utcnow() - timedelta(days = 1)),
                       'stationIDs': None, 'networks': None,'minLatitude': None, 'maxLatitude': None, 'minLongitude': None, 
                        'maxLongitude': None, 'states': None, 'networks': None, 'vars': None, 'makeFile': True}

    # Get the Synoptic station network identifiers and insert them into the database (ex., RAWS, NWS, ...)
    #
    def build_networks_table(self):
        # Open connection to sqlite database. Using "with" prevents database corruption
        with sqlite3.connect(self.dbPath) as conn:
            # Create a cursor object to execute SQL queries
            c = conn.cursor()
            networkDict = get_networks(self.token)
            logging.info(f"Adding {len(networkDict['MNET'])} networks to database")
            # Iterate through your data and insert it into the table
            for network in networkDict['MNET']:
                try:
                    networkID = network['ID']
                    networkSN = network['SHORTNAME']
                    networkLN = network['LONGNAME']
                    values = [networkID,networkSN,networkLN]
                    c.execute("INSERT INTO Networks (NETWORK_ID, NETWORK_NAME_SHORT, NETWORK_NAME_LONG) VALUES (?, ?, ?)", values)
                    # Commit changes to the database
                    conn.commit()
                except Exception as e:
                    logging.warning(f"build_networks_table with exception: {e}")
                    pass
    
    # Get all of the stations from Synoptic and save their metadata
    #
    def build_stations_table(self):
        logging.info(f"Getting station metadata")
        # Open connection to sqlite database. Using "with" prevents database corruption
        with sqlite3.connect(self.dbPath) as conn:
            # Create a cursor object to execute SQL queries
            c = conn.cursor()
            stationDict = get_stations(self.token)
            logging.info(f"Adding {len(stationDict['STATION'])} stations to database")
            for station in stationDict["STATION"]:
                try:
                    stationId = station["STID"]
                    stationName = station["NAME"]
                    stationState = station["STATE"]
                    stationLat = station["LATITUDE"]
                    stationLon = station["LONGITUDE"]
                    elevation = station["ELEVATION"]
                    elevationUnits = station["UNITS"]['elevation']
                    lastActive = station["PERIOD_OF_RECORD"].get('end')
                    if lastActive != None:
                        lastActive = dt.datetime.strptime(
                            lastActive, "%Y-%m-%dT%H:%M:%SZ"
                        ).strftime("%Y-%m-%d %H:%M:%S")
                    mnet = station["MNET_ID"]
                    values = [
                        stationId, stationName, stationState, stationLat, stationLon, 
                        elevation, elevationUnits, lastActive, mnet
                    ]
                    c.execute(f"INSERT OR IGNORE INTO Stations (STID, NAME, STATE, LATITUDE, LONGITUDE, ELEVATION, ELEVATION_UNITS, LAST_ACTIVE, NETWORK_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", values)
                    # Commit changes to the database
                    conn.commit()
                except Exception as e:
                    logging.warning(f"build_stations_table with exception: {e}")
                    pass
        logging.info("Done getting station metadata at time")

    # Insert data into the database
    #
    # @ Param listOfDfs- list of dataframes with data from Synoptic
    #
    def insert_data(self, listOfDfs):
        logging.debug('SynopticDB.insert_data - Adding data to the repo')
        # Open connection to sqlite database
        with sqlite3.connect(self.dbPath) as conn:
            # Create a cursor object to execute SQL queries
            c = conn.cursor()
            # Put the listOfDfs into a list if it isn't already in a list
            if not isinstance(listOfDfs, list):
                listOfDfs = [listOfDfs]
            logging.debug(f'SynopticDB.insert_data - Number of inserts: {len(listOfDfs)}')
            # Get a list of all of the avaialbe tables in the database
            dbTableNames = self.list_table_names()
            # Each site in the listOfDfs is one station's data
            for count, siteDf in enumerate(listOfDfs):
                if count % 1000 == 0:
                    logging.debug(f'SynopticDB.insert_data - Inserting: {count} / {len(listOfDfs)}')
                # List of all of the columns from the dataframe
                dfVariableNames = siteDf.keys()
                # Convert the datetime to datetime format
                siteDf.index = pd.to_datetime(siteDf.index)
                for idx, row in siteDf.iterrows():
                    # Loop through all of the column names in the row
                    for variable in  dfVariableNames:
                        # Unsure of how to handle these variables
                        bannedVars = ['cloud_layer_1','weather_summary']
                        if variable in bannedVars:
                            continue
                        # get the current value in the row
                        try:
                            currValue = row[variable]
                        except:
                            continue
                        # If the current value is a NaN, continue
                        try:
                            if np.isnan(float(currValue)):
                                continue
                        except:
                            if isinstance(currValue, str):
                                if row[variable].lower() in ("nan", "n/a", "na", "none", "nonetype"):
                                    continue
                        # Used to correctly label the value in the database (either string or numeric value)
                        thisType = "REAL" if isinstance(currValue, (int, float)) else "TEXT"
                        thisValue = float(currValue) if thisType == "REAL" else str(currValue)
                        # Check if the current table name is already in the database
                        if variable not in dbTableNames:
                            # If the variable name from the MesoWest file isn't in the database already
                            c.execute("CREATE TABLE {} ({} {}, {} {}, {} {}, {} {}, UNIQUE(STID, DATETIME, VALUE))".format(
                                        variable, "STID", "TEXT", "DATETIME", "DATETIME", "VALUE", thisType, "UNITS","TEXT")) 
                            # Add the variable name to the current table names in the database
                            dbTableNames.append(variable)
                        # Prepare the data to be inserted into the database
                        stationID = siteDf.attrs['STID']
                        datetime = row.name.strftime('%Y-%m-%d %H:%M:%S')
                        dataValue = thisValue
                        dataUnit = siteDf.attrs['UNITS'].get(variable)
                        values = [stationID, datetime, dataValue, dataUnit]
                        # Insert data into the database
                        c.execute(f"INSERT OR IGNORE INTO {variable} (STID, DATETIME, VALUE, UNITS) VALUES (?, ?, ?, ?)", values)
                # Commit changes to the database
                conn.commit()

    # Uses SynopticPy to get data from the Synoptic Weather Site and insert the data into the database
    #
    def get_synData(self, max_retries=5):
        # Get all of the database parameters
        startTime = self.params.get('startDatetime')
        endTime = self.params.get('endDatetime')
        # If either startTime or endTime are None values, grab the last day's data
        if startTime is None or endTime is None:
            endTime = dt.datetime.now(dt.timezone.utc)
            startTime = endTime - relativedelta(hours=1)
        tmpTime = startTime + relativedelta(hours=1)
        # Get the data from Synoptic 
        while tmpTime <= endTime:
            logging.info('Getting data between {} and {}'.format(startTime,tmpTime))
            startUtc = startTime.strftime('%Y%m%d%H00')
            endUtc = tmpTime.strftime('%Y%m%d%H00')
            # Iterate and get all of the data for all of the provided state values
            try:
                get_timeseries(self, startUtc, endUtc, max_retries)
                logging.debug('Finished adding data to database')
                logging.debug(' ')
            except Exception as e:
                logging.warning('get_synData with exception: {}'.format(e))
            # Update metadata
            self.update_metadata(tmpTime)
            # Increment the time by one hour
            startTime += relativedelta(hours=1)
            tmpTime += relativedelta(hours=1)

    # Get ALL of the data from the United States from ALL sources
    #
    def get_all_synoptic_data(self):
        # Get all the station IDs in the database
        allSTIDs = self.find_stids_from_params(None, None, None, None)
        numOfStids = len(allSTIDs["STID"])
        i = 0
        # Run get_synData for all of the available stations in the database
        # Note: Synoptic can handle 1875 stations pulled at one time 
        while numOfStids >= 0:
            self.params['stationIDs'] = allSTIDs["STID"][i:i+1875]
            self.get_synData()
            numOfStids=-1875
            i+=1875

    # Queries the database based on the request parameters provided by the user
    #
    # @returns a dataframe containing all the data the user requested
    #
    def query_db(self):
        # Query parameters. Also ensures the values are in list format if they are needed in said format
        tableNames = ensure_list(self.params.get("vars"))
        stationIDs = self.params.get("stationIDs")
        networks = self.params.get("networks")
        state = self.params.get("states")
        minLat = self.params.get("minLatitude")
        maxLat = self.params.get("maxLatitude")
        minLon = self.params.get("minLongitude")
        maxLon = self.params.get("maxLongitude")
        startDate = self.params.get("startDatetime").strftime("%Y-%m-%d %H:%M:%S")
        endDate = self.params.get("endDatetime").strftime("%Y-%m-%d %H:%M:%S")
        makeFile = self.params.get("makeFile")
        bbox = [minLat, maxLat, minLon, maxLon]
        with sqlite3.connect(self.dbPath) as conn:
            # Create a cursor object to execute SQL queries
            c = conn.cursor()
            # Check if tableNames parameter is provided and if the tables are available in the database
            if not len(tableNames):
                logging.error("No table names provided. Pick from available tables below:")
                self.list_table_names()
                raise SynopticError("No table names provided")
            # Define a dictionary to store dataframes for each table
            dfs = {}
            # List of station ids in the table that can be queired
            availStids = []
            for table in tableNames:
                c.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
                if c.fetchone() is None:
                    logging.warning(f"Table '{table}' does not exist in the database.")
                    continue
                # Get the station ids in the database that the user requests
                stids = self.find_stids_from_params(stationIDs, networks, bbox, state)
                # This variable will be used later to generate a csv of all of the station information requested
                availStids += stids
                # Check if either startDate or endDate are None values, grab the last day's data
                if startDate is None or endDate is None:
                    currDate = dt.datetime.now(dt.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                    startDate = (currDate - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
                    endDate = currDate.strftime("%Y-%m-%d %H:%M:%S")
                # Query data from the table and stids within startDate and endDate
                query = f"SELECT * FROM {table} WHERE STID IN ({','.join('?'*len(stids))}) AND DATETIME BETWEEN ? AND ?"
                c.execute(query, stids + [startDate, endDate])
                rows = c.fetchall()
                if len(rows) > 0:
                    df = pd.DataFrame(rows, columns=['STID', 'DATETIME', f'{(table).upper()}_VALUE', f'{(table).upper()}_UNITS'])
                    # Store the dataframe in the dictionary with the table name as the key
                    dfs[table] = df
        # Merge dataframes from different tables
        result = self.merge_dataframes(dfs)
        if not isinstance(result, pd.DataFrame):
            logging.warning("No data was found in the database with the given parameters")
            return None, None
        if len(result)>0:
            result = self.sort_dataframe(result)
        # Get all station data for stations within the query
        stationDf = self.query_station_data_by_ids(availStids)
        # Create a data and station file if the makeFile parameter is True
        if makeFile:
            now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d_%H:%M:%S")
            result.to_csv(f"SYN_{now}.csv")
            stationDf.to_csv(f"SYN_stations_{now}.csv",index=False)
        return result, stationDf

    # Finds all of the station ids that match with the parameters
    #
    # @param stationIDs - list of station ids
    # @param networks - list of station networks 
    # @param bbox - a bounding box (list of 4 float values) with geographical coordinates in WGS84 degrees
    # @param states - a list of states (abbreviations)
    #
    def find_stids_from_params(self, stationIDs, networks, bbox, states):
        stationIDs = ensure_list(stationIDs)
        networks = ensure_list(networks)
        bbox = ensure_list(bbox)
        states = ensure_list(states)
        # Open connection to SQLite database
        with sqlite3.connect(self.dbPath) as conn:
            c = conn.cursor()
            # Build the SQL query based on the provided parameters
            query = "SELECT STID FROM Stations"
            conditions = []
            queryParams = []
            # These if statements look to see if any of the user query parameters have been provided
            # Note: queryParams uses extend as the sqlite database expects a flat list of values
            if len(stationIDs):
                conditions.append(f"STID IN ({','.join(['?']*len(stationIDs))})")
                queryParams.extend(stationIDs)
            if len(networks):
                conditions.append("NETWORK_ID IN ({})".format(','.join(['?']*len(networks))))
                queryParams.extend(networks)
            if len(bbox):
                bbox = np.array(bbox).astype(np.float64)
                conditions.append("LATITUDE BETWEEN ? AND ? AND LONGITUDE BETWEEN ? AND ?")
                queryParams.extend(bbox)
            if len(states):
                conditions.append(f"STATE IN ({','.join(['?']*len(states))})")
                queryParams.extend(states)
            # If any of the user query parameters were not None, add the query conditions to the sqlite call
            if len(conditions) > 0:
                query += ' WHERE ' + ' AND '.join(conditions)
            # Execute the query with the provided parameters
            stids = [result[0] for result in c.execute(query, queryParams).fetchall()]
            #stids = c.execute(query, queryParams).fetchall()
            return stids

    # Merge all of the dataframes from the query function into a single dataframe
    #
    # @param dfs - list of dataframes from query function
    #
    # @returns a single dataframe
    #
    def merge_dataframes(self, dfs):
        if not dfs:
            return None
        # Initialize the result dataframe with the first table's data
        result = dfs.popitem()[1]
        for table, df in dfs.items():
            # Merge dataframes based on 'stid' and 'datetime' columns, handling NaN values
            result = pd.merge(result, df, on=['stid', 'datetime'], how='outer', suffixes=('', f'_{table}'))
        return result
    
    # Returns a dataframe with all of the station data requested
    #
    # @param stationIDs - list of station ids 
    #
    # @return a dataframe with all the requested data for each station in the stationIDs list
    #
    def query_station_data_by_ids(self,stationIDs):
        # Open connection to SQLite database
        with sqlite3.connect(self.dbPath) as conn:
            # Create a cursor object to execute SQL queries
            c = conn.cursor()
            # Construct the SQL query to fetch data for the specified station IDs
            query = f"SELECT * FROM Stations WHERE STID IN ({','.join(['?']*len(stationIDs))})"
            # Execute the query with the list of station IDs as parameters
            c.execute(query, stationIDs)
            # Fetch all the data rows
            data = c.fetchall()
            # Create a DataFrame from the fetched data
            columns = [desc[0] for desc in c.description]
            df = pd.DataFrame(data, columns=columns)
            return df

    # Returns a list of all table names in the database
    #
    def list_table_names(self):
        # Connect to the SQLite database
        with sqlite3.connect(self.dbPath) as conn:
            # Get a cursor object
            c = conn.cursor()
            # Query the SQLite master table for all table names
            c.execute("SELECT name FROM sqlite_master WHERE type='table';")
            # Fetch all the table names and store them in a list
            tableNames = [row[0] for row in c.fetchall()]
            # Return the list of table names
            return tableNames
    
    # Check the contents of one of a table within the database
    #
    # @ Param tableName - the name of the table to be requested
    #
    # @ returns a dataframe of all the data from the requested table
    #
    def check_table(self, tableName):
        tables = self.list_table_names()
        if tableName in tables:
            # create a connection to the mesoDB database
            with sqlite3.connect(self.dbPath) as conn:
                if tableName == 'Metadata':
                    c = conn.cursor()
                    c.execute("SELECT key, value FROM metadata")
                    rows = c.fetchall()
                    return rows
                # define the SQL query to select the variables from a given table
                query = f"SELECT * FROM {tableName}"
                # execute the query and store the results in a pandas dataframe
                dfTable = pd.read_sql_query(query, conn)
                invalidTables = ['Networks', 'Stations']
                if tableName not in invalidTables:
                    dfTable = self.sort_dataframe(dfTable)
                return dfTable
        else:
            SynopticError("The table provided is not in the database")

    # Sort the given dataframe by station ID and then by datetime
    #
    # @ returns a dataframe sorted by station ID and datetime
    #
    def sort_dataframe(self,df):
        sortedDf = df.sort_values(by=["STID", "DATETIME"], ignore_index=True)
        return sortedDf

    # Remove a table from the database
    #
    # @param tableName - name of the table to be removed
    #
    def remove_table(self,tableName):
        logging.info("Are you sure you want to delete the table?")
        userInput = input("Enter y or n: ")
        while userInput != "y" or userInput != "n":
            userInput = input("Enter y or n: ")
        if userInput == "y":
            # Connect to the SQLite database
            with sqlite3.connect(self.dbPath) as conn:
                # Create a cursor object to interact with the database
                c = conn.cursor()
                # Specify the name of the table you want to delete
                tableName = tableName
                # Execute the DROP TABLE statement
                c.execute(f'DROP TABLE IF EXISTS {tableName}')
                # Commit the changes to the database
                conn.commit()
    
    # Update metadata for last modified
    #
    def update_metadata(self, utcTime):
        logging.info(f'Updating metadata {utcTime}')
        metadata = dict(self.check_table('Metadata'))
        with sqlite3.connect(self.dbPath) as conn:
             # Create a cursor object to interact with the database
             c = conn.cursor()
             # Update the last updated
             c.execute("INSERT OR REPLACE INTO Metadata (key, value) VALUES (?, ?)",
                 ("last_updated_utc", dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d_%H:%M:%S')))
             # Update the last data time
             last_get_data_utc = metadata.get('last_get_data_utc')
             if last_get_data_utc is not None:
                 last_get_data_utc = dt.datetime.strptime(last_get_data_utc, '%Y-%m-%d_%H:%M:%S')
                 last_get_data_utc = max(last_get_data_utc, utcTime.replace(tzinfo=None))
             last_get_data_utc = utcTime.strftime('%Y-%m-%d_%H:%M:%S')    
             c.execute("INSERT OR REPLACE INTO Metadata (key, value) VALUES (?, ?)",
                 ("last_get_data_utc", last_get_data_utc))
             # Commit the changes to the database
             conn.commit()

    # List the typical variables found in MesoWest stations that can be used in the database query function
    #
    def list_variables(self):
        listOfVars = ['air_temp','relative_humidity','wind_speed','wind_direction','wind_gust','solar_radiation','precip_accum','fuel_moisture']   
        logging.info("Typical Requested Variables:")
        for var in listOfVars:
            logging.info(var) 

