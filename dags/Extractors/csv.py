from abc import abstractmethod
from dataclasses import dataclass
from .AExtractor import Extractor
import requests
import kagglehub
import os
import pandas as pd
from datasets import load_dataset
import hashlib


@dataclass
class ExtractorCSV(Extractor):
    
    def hash_path(self, path):
        return hashlib.md5(path.encode()).hexdigest()
    
    def extract_data(self, api_endpoint, source):
        if source is None:
            return
        if source == "KaggleHub":
            return self.kaggle_hub_extract(api_endpoint)
        elif source == "HuggingFace":
            return self.huggingface_extract(api_endpoint)
        else:
            response = requests.get(api_endpoint)
            return response, f"{self.hash_path(api_endpoint)}.csv"
    
    def extract_data_Tom_Tom(api_data, token, point):
        pass
    
    def extract_data_google(api_data, token, origin, destination):
        pass
    
    def kaggle_hub_extract(self, api_endpoint):
        path2 = kagglehub.dataset_download(api_endpoint)
        path_file  = os.path.join(path2,os.listdir(path2)[0])
        df = pd.read_csv(path_file)
        return df, f"{self.hash_path(path_file)}.csv"
        
    def huggingface_extract(self, api_endpoint):
        ds = load_dataset(api_endpoint)
        data = ds['train'].to_pandas()
        return data, f"{self.hash_path(api_endpoint)}.csv"