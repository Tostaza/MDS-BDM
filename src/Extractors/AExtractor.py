from abc import ABC, abstractmethod
from dataclasses import dataclass

from src.Clients.seaweedfs import Seaweedfs


@dataclass
class Extractor:
    
    @abstractmethod
    def extract_data():
        pass