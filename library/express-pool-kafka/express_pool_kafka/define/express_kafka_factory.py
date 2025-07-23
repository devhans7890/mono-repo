from abc import ABC, abstractmethod
from express_pool_kafka.kafka_producer_wrapper import KafkaProducerWrapper
from kafka import KafkaConsumer


class ExpressKafkaFactory(ABC):
    @abstractmethod
    def create_consumer(self) -> KafkaConsumer:
        ...

    @abstractmethod
    def create_producer(self) -> KafkaProducerWrapper:
        ...
