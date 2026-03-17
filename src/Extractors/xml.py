import json
import os
import requests
from .AExtractor import Extractor
from io import BytesIO
from dataclasses import dataclass


@dataclass
class XML(Extractor):
    def extract_data(self, api_endpoint):
        response = requests.get(api_endpoint)
        data = response.text
        json_bytes = BytesIO(json.dumps(data).encode("utf-8"))
        json_bytes.seek(0)
        key = api_endpoint
        return json_bytes, key
