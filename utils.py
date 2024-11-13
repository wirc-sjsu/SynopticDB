import synoptic.services as ss
import logging
import requests

# Transform any input into a list version of it
#
def ensure_list(input_data):
    # Check if input is already a list
    if isinstance(input_data, list):
        return [elem for elem in input_data if elem != None]
    else:
        # Wrap input in a list if it's not already a list
        if input_data != None:
            return [input_data]
        else:
            return []

# Get the network names and IDs from MesoWest
#
def get_networks(token):
    url = f'https://api.synopticdata.com/v2/networks?&token={token}'
    try:
        # Send an HTTP GET request to the URL
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Get the content of the response (the data)
            rawStationData = response
            return rawStationData.json()
    except Exception as e:
        logging.warning(e)

# Retrieves all of MesoWest stations information for the United States
#
def get_stations(token):
    url=f'https://api.synopticdata.com/v2/stations/metadata?&token={token}&country=us'
    try:
        # Send an HTTP GET request to the URL
        response = requests.get(url)
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Get the content of the response (the data)
            rawStationData = response
            return rawStationData.json()
    except Exception as e:
        logging.warning(e)
        
# Request Synoptic data using timeseries
#
def get_timeseries(db, startUtc, endUtc, max_retries=5):
    stids = db.params.get('stationIDs')
    states = db.params.get('states')
    minLat = db.params.get('minLatitude')
    maxLat = db.params.get('maxLatitude')
    minLon = db.params.get('minLongitude')
    maxLon = db.params.get('maxLongitude')
    bbox = [minLon, minLat, maxLon, maxLat]
    states = db.params.get('states')
    networks = db.params.get('networks')
    tableNames = db.params.get('vars')
    try:
        # Request data to Synoptic
        df = ss.stations_timeseries(
            start=startUtc, 
            end=endUtc,
            network=networks,
            stid = stids,
            country="US",
            state=states,
            bbox=bbox,
            vars=tableNames,
            verbose=False
        )
        # Insert the queried data to the database
        db.insert_data(df)
    except:
        max_retries -= 1
        if max_retries > 0:
            logging.warning('request_synoptic - SynopticDB failed, remaining tries {}'.format(max_retries))
            get_timeseries(db, startUtc, endUtc, max_retries)
        else:
            logging.warning('request_synoptic - SynopticDB failed to get data')