import io
import os
from abc import ABC, abstractmethod
from .AExtractor import Extractor
import requests
import hashlib

class ExtractorJSON(Extractor):
    
    def hash_path(self, path):
        return hashlib.md5(path.encode()).hexdigest()
    
    def extract_data(self, api_endpoint, params=None, headers=None):
        response = requests.request("GET", api_endpoint, headers=headers, params=params)
        response.raise_for_status()
        return response.json(), f"{self.hash_path(api_endpoint)}.json"
    
    def extract_real_time_data(self, api_endpoint, type, api_key=None, params=None):
        if params is None:
            params = {}
        if api_key:
            # TomTom usually uses 'key' as the parameter name
            params['key'] = api_key
        return self.extract_data(api_endpoint, params=params)