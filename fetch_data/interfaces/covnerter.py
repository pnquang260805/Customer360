from abc import ABC, abstractmethod

class ConvertData(ABC):
    @abstractmethod
    def convert(self, data : str) -> dict:
        pass