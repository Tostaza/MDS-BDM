import io
import os
from abc import ABC, abstractmethod
from .AExtractor import Extractor
import requests
import hashlib

class ExtractorJSON(Extractor):
    
    def hash_path(self, path):
        return hashlib.md5(path.encode()).hexdigest()
    
    def extract_data(self, api_endpoint):
        response = requests.get(api_endpoint)
        response.raise_for_status()
        return response.json(), f"{self.hash_path(api_endpoint)}.json"
    
    def extract_data(self, api_endpoint, querystring, headers):
        response = requests.request("GET", api_endpoint, headers=headers, params=querystring)
        response.raise_for_status()
        return response.json()

    def extract_data(self, api_endpoint, headers):
        response = requests.request("GET", api_endpoint, headers=headers)
        response.raise_for_status()
        return response.json()

    def extract_real_time_data(self, api_endpoint, type):
        pass