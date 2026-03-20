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
        return response.json(), f"{self.hash_path(api_endpoint)}.json"

    def extract_real_time_data(self, api_endpoint, type):
        pass