from enum import Enum


class ExpressKafkaTypes(Enum):
    CONSUMER = 0
    PRODUCER = 1
    UNKNOWN = 2
