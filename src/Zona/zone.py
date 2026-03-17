from abc import ABC, abstractmethod
from dataclasses import dataclass
import json
import os

@dataclass
class zone(ABC):
    
    @abstractmethod
    def create_bucket(self, name_bucket):
        pass
    
    @abstractmethod
    def load_all_data(self, bucket_name, json_bytes):
        pass
        
    @abstractmethod
    def download_all_data(self, bucket_name):
        pass