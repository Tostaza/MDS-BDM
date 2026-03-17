from abc import abstractmethod
from dataclasses import dataclass

from .AExtractor import Extractor
import requests


@dataclass
class csv(Extractor):
    

    def extract_data(self, api_endpoint):
        response = requests.get(api_endpoint)
        key = api_endpoint
        return response, key