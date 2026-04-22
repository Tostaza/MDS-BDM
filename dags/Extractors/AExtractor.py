from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class Extractor(ABC):
    
    @abstractmethod
    def extract_data(self, api_endpoint, params=None, headers=None):
        pass

    @abstractmethod
    def extract_real_time_data(self, api_endpoint, type, api_key=None):
        pass