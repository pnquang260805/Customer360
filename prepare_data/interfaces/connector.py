from abc import ABC, abstractmethod
from typing import List, Dict

class Connector(ABC):
    @abstractmethod
    def _connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def insert_one(self, db_name : str, table_or_collection : str, data : Dict[str, any]):
        pass