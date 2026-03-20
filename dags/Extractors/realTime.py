import json
import os
import requests
from .AExtractor import Extractor
from urllib.parse import urlparse
from dotenv import load_dotenv


class ExtractorRealTime(Extractor):
    
    def extract_data(self, api_endpoint):
        pass
    
    def extract_data_Tom_Tom(api_data, token, point):
        params = {
            "key": token,
            "point": point,
            "unit": "KMPH",
            "openLr": "false"
        }
        response = requests.get(api_data, params=params)
        key = ""
        return response, key
        
    def extract_data_google(api_data, token, origin, destination):
        
        headers = {
            "X-Goog-Api-Key": token,
            "X-Goog-FieldMask": "routes.duration,routes.staticDuration",
            "Content-Type": "application/json"
        }
        
        body = {
            "origin": { "address": {origin} },
            "destination": { "address": {destination} },
            "routingPreference": "TRAFFIC_AWARE_OPTIMAL",
            "travelMode": "DRIVE"
        }
        
        response = requests.get(api_data, headers=headers, data=json.dumps(body))
        key = ""
        return response, key