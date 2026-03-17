import io
import os
from abc import ABC, abstractmethod
from .AExtractor import Extractor
import requests

class JSON(Extractor):
    
    def extract_data(self, api_endpoint):
        
        response = requests.get(api_endpoint)
        key = api_endpoint
        return response.json(), key