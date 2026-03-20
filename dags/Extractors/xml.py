import json
import os
import requests
from .AExtractor import Extractor
from io import BytesIO
from dataclasses import dataclass


@dataclass
class ExtractorXML(Extractor):
    def extract_data(self, api_endpoint):
        response = requests.get(api_endpoint)
        data = response.text
        json_bytes = BytesIO(json.dumps(data).encode("utf-8"))
        json_bytes.seek(0)
        key = api_endpoint
        return json_bytes, key
    
    def extract_data_Tom_Tom(api_data, token, point):
        pass
        
    def extract_data_google(api_data, token, origin, destination):
        pass
