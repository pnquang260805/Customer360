from abc import ABC, abstractmethod

class Extractor(ABC):
    def extract(self, content : str):
        pass