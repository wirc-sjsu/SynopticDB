from bs4 import BeautifulSoup
import logging
import requests

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

