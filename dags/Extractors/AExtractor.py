from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class Extractor(ABC):
    
    @abstractmethod
    def extract_data(self, api_endpoint):
        pass
    
    @abstractmethod
    def extract_data_Tom_Tom(api_data, token, point):
        pass
    
    @abstractmethod
    def extract_data_google(api_data, token, origin, destination):
        pass