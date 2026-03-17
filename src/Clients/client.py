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
    def connect():
        pass
    
    @abstractmethod
    def load():
        pass
    
    @abstractmethod
    def download():
        pass