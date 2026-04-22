import json
import os
import requests
from .AExtractor import Extractor
from io import BytesIO
from dataclasses import dataclass
import hashlib


@dataclass
class ExtractorXML(Extractor):
    
    def hash_path(self, path):
        return hashlib.md5(path.encode()).hexdigest()
    
    def extract_data(self, api_endpoint, params=None, headers=None):
        response = requests.get(api_endpoint, headers=headers, params=params)
        data = response.text
        json_bytes = BytesIO(json.dumps(data).encode("utf-8"))
        json_bytes.seek(0)
        return json_bytes, f"{self.hash_path(api_endpoint)}.xml"

    def extract_real_time_data(self, api_endpoint, type, api_key=None):
        pass