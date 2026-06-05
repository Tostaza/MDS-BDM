from dataclasses import dataclass
import json
import io
from abc import ABC, abstractmethod
import boto3

@dataclass
class Client(ABC):
    
    aws_key: str = None
    aws_password: str = None
    api_endpoint: str = None
    
    @abstractmethod
    def connect(self):
        pass
    
    @abstractmethod
    def upload_object(self, bucket, key, obj_bytes):
        pass

    @abstractmethod
    def load_doc(self, bucket, df, key):
        pass

    @abstractmethod
    def load_xml(self, bucket, json_bytes, key):
        pass

    @abstractmethod
    def load_json(self, bucket, doc, key):
        pass

    @abstractmethod
    def download(self, bucket, key, file_path):
        pass

    @abstractmethod
    def create_bucket(self, name_bucket):
        pass

    @abstractmethod
    def download_all(self, bucket: str):
        pass

    @abstractmethod
    def load_real_time(self, bucket, doc, key):
        pass
