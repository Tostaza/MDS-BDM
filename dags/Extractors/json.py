import io
import os
from abc import ABC, abstractmethod
from .AExtractor import Extractor
import requests

class ExtractorJSON(Extractor):
    
    def extract_data(self, api_endpoint):
        response = requests.get(api_endpoint)
        key = api_endpoint
        return response.json(), key

    def extract_data_Tom_Tom(api_data, token, point):
        pass
    
    def extract_data_google(api_data, token, origin, destination):
        pass