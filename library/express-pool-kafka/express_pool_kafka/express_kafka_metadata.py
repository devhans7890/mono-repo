from typing import Dict, Any
from kafka import KafkaConsumer
from .define.express_kafka_factory import ExpressKafkaFactory
from .express_kafka_configuration import ExpressKafkaConfiguration
from .kafka_producer_wrapper import KafkaProducerWrapper
from .express_kafka_types import ExpressKafkaTypes
from .express_kafka_exception import ExpressKafkaException
from .express_kafka_utils import ExpressKafkaUtils
# from confluent_kafka import Consumer, Producer # 추후 성능 문제로 리팩토링시 confluent kafka 도입이 필요할 수 있음


class ExpressKafkaMetadata(ExpressKafkaFactory):
    def __init__(self, express_kafka_types: ExpressKafkaTypes):
        self.express_kafka_types = express_kafka_types
        self.configuration: ExpressKafkaConfiguration = None

    def get_configuration(self) -> ExpressKafkaConfiguration:
        if self.configuration is None:
            raise ExpressKafkaException("Kafka configuration is None")
        return self.configuration

    def build(self, _input: Dict[str, Any]) -> bool:
        if not _input:
            return False
        identifier = _input.get("id")
        if not identifier:
            return False
        if not _input.get("use", False):
            return False
        bootstrap_servers = _input.get("bootstrap.servers")
        if not bootstrap_servers:
            return False
        properties = _input.get("property", {})

        builder = ExpressKafkaConfiguration.Builder(self.express_kafka_types)
        self.configuration = builder.id(identifier) \
            .bootstrap_servers(bootstrap_servers) \
            .properties(properties) \
            .build()
        return True

    def create_consumer(self) -> KafkaConsumer:
        if self.express_kafka_types != ExpressKafkaTypes.CONSUMER:
            raise ExpressKafkaException(
                f"kafka mis-match type. your request is consumer but this object type is {self.express_kafka_types}"
            )
        properties = ExpressKafkaUtils.configuration_check(self.configuration)
        return KafkaConsumer(**properties)

    def create_producer(self) -> KafkaProducerWrapper:
        if self.express_kafka_types != ExpressKafkaTypes.PRODUCER:
            raise ExpressKafkaException(
                f"kafka mis-match type. your request is producer but this object type is {self.express_kafka_types}"
            )
        properties = ExpressKafkaUtils.configuration_check(self.configuration)
        return KafkaProducerWrapper(properties)
