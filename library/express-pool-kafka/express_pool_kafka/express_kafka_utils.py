from .express_kafka_configuration import ExpressKafkaConfiguration
from .express_kafka_exception import ExpressKafkaException
from typing import Dict, Any


class ExpressKafkaUtils:
    @staticmethod
    def configuration_check(express_kafka_configuration: ExpressKafkaConfiguration) -> Dict[str, Any]:
        if not express_kafka_configuration or not express_kafka_configuration.properties:
            raise ExpressKafkaException("Kafka configuration or its properties are null")
        config = express_kafka_configuration.properties.copy()
        return config
