from decimal import Decimal
import json

from confluent_kafka import Producer
from dataclasses import dataclass

from common.logger import log

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj) # Hoặc str(obj) nếu bạn muốn giữ độ chính xác tuyệt đối
        return super(DecimalEncoder, self).default(obj)

@dataclass
class KafkaService:
    bootstrap : str = "localhost:9092"
    def __post_init__(self):
        conf = {
            "bootstrap.servers": self.bootstrap
        }
        self.producer = Producer(conf)

    def __callback(self, err, msg):
        if err:
            log.error(f"Error when sending message to {msg.topic()}")
        else:
            log.info(f"Message sent to {msg.topic()}")

    def send_msg(self, topic : str, message: dict, key : str) -> None:
        self.producer.produce(topic, json.dumps(message, cls=DecimalEncoder), on_delivery=self.__callback, key=key)
        self.producer.flush()