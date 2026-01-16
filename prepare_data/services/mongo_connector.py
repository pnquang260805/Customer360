import pymongo
from typing import List, Any

from interfaces.connector import Connector
from common.logger import log, auto_log


class MongoConnector(Connector):
    def __init__(self, host: str, port: int, username: str = None, password: str = None) -> None: # type: ignore
        super().__init__()
        self.host = host
        self.port = port
        self.username = username
        self.password = password


    @auto_log()
    def insert_one(self, db: str, collection:  str, data: dict):
        db_mongo = self.client[db]
        coll = db_mongo[collection]
        inserted_data = coll.insert_one(data)
        log.info(f"Inserted with id: {inserted_data.inserted_id}")

    @auto_log()
    def insert_many(self, db: str, collection: str, data: List[dict]):
        db_mongo = self.client[db]
        coll = db_mongo[collection]
        inserted_data = coll.insert_many(data)
        log.info(f"Inserted {len(inserted_data.inserted_ids)} documents")

    @auto_log()
    def connect(self):
        if self.password is not None and self.username is not None:
            url = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/"
        else:
            url = f"'mongodb://{self.host}:{self.port}/"

        log.info(f"Connecting to {self.host}:{self.port}")

        self.client = pymongo.MongoClient(url)
        log.info(f"Connected to {self.host}:{self.port}")

    @auto_log()
    def disconnect(self):
        self.client.close()
        log.info(f"Closed connection in {self.host}:{self.port}")

        