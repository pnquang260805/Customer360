from typing import Dict, Any, List
import psycopg2

from common.logger import log, auto_log

class PostgresConnector:
    def __init__(self, database: str, user : str, password: str, host: str, port: int) -> None:
        try:
            self.conn = psycopg2.connect(database=database, user=user, password = password, host = host, port = port)
            self.cursor = self.conn.cursor()
        except Exception as e:
            log.error(e)
            raise(e)
    
    @auto_log()
    def close(self):
        self.conn.close()
    
    @auto_log()
    def insert(self, table: str, columns : List[str], values : List[Any]) -> None:
        literal = ",".join(["%s"]*len(values))
        cols = ",".join(columns)
        query = f"insert into {table} ({cols}) values ({literal})"
        try:
            self.cursor.execute(query, values)
            self.conn.commit()
            log.info(f"inserted to {table}")
        except Exception as e:
            log.error(e)
            raise(e)