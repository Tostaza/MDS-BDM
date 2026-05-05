from abc import ABC, abstractmethod


class ClientMotorProcessamiento(ABC):
    @abstractmethod
    def connect(self):
        pass
