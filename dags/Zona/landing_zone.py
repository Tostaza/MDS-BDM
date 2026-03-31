import json
import os
from abc import ABC, abstractmethod

from src.Clients import Client
from .zone import *
from ..Clients.seaweedfs import Seaweedfs
from ..Clients.client import Client

@dataclass
class landingZone(zone):
    
    client: Client = None
    
    def __post_init__(self):
        self.client = Seaweedfs()
        self.client.connect()
        
    def create_bucket(self, name_bucket):
        return super().create_bucket(name_bucket)
    
    def load_all_data(self, bucket_name, json_bytes):
        self.client.load(bucket_name, json_bytes)
        
    def download_all_data(self, bucket_name):
        return self.client.download(bucket_name)
        