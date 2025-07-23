from .express_kafka_exception import ExpressKafkaException
from .express_kafka_types import ExpressKafkaTypes
from .define.express_kafka_meta_configuration import ExpressKafkaMetaConfiguration
from .define.express_kafka_meta_builder import ExpressKafkaMetaBuilder
from express_utils.express_yml_converter import ExpressYmlConverter
from typing import Dict, Any, List
import json


class ExpressKafkaConfiguration(ExpressKafkaMetaConfiguration):

    def __init__(self,
                 kafka_types: ExpressKafkaTypes,
                 _id: str,
                 bootstrap_servers: str,
                 properties: Dict[str, Any]):
        self.kafka_types = kafka_types
        self._id = _id
        self.bootstrap_servers = bootstrap_servers
        self.properties = properties

    def kafka_types(self) -> ExpressKafkaTypes:
        return self.kafka_types

    def _id(self) -> str:
        return self._id

    def bootstrap_servers(self) -> str:
        return self.bootstrap_servers

    def properties(self) -> Dict[str, str]:
        return self.properties

    def get_properties(self) -> Dict[str, Any]:
        return self.properties

    class Builder(ExpressKafkaMetaBuilder):
        def __init__(self, kafka_types: ExpressKafkaTypes):
            self.kafka_types = kafka_types
            self._id = None
            self._bootstrap_servers = None
            self._properties = None

        def id(self, _id: str) -> 'ExpressKafkaConfiguration.Builder':
            self._id = id
            return self

        def bootstrap_servers(self, bootstrap_servers: str) -> 'ExpressKafkaConfiguration.Builder':
            self._bootstrap_servers = bootstrap_servers
            return self

        def properties(self, properties: Dict[str, Any]) -> 'ExpressKafkaConfiguration.Builder':
            self._properties = self.init_properties(properties)
            return self

        def get_deserializer(self, deserializer_type):
            if deserializer_type == "json":
                return lambda x: json.loads(x.decode('utf-8'))
            elif deserializer_type == "string":
                return lambda x: x.decode('utf-8')
            return None

        def get_serializer(self, serializer_type):
            if serializer_type == "json":
                return lambda x: json.dumps(x).encode('utf-8')
            elif serializer_type == "string":
                return lambda x: x.encode('utf-8')
            return None

        def convert_config(self, kafka_types: ExpressKafkaTypes, config: Dict[str, Any]):
            new_config = config.copy()
            if kafka_types == ExpressKafkaTypes.CONSUMER:
                keys_to_convert = {'key_deserializer', 'value_deserializer'}
                for key in keys_to_convert:
                    if key in config:
                        value = config.pop(key)
                        new_config[key] = self.get_deserializer(value)
            elif kafka_types == ExpressKafkaTypes.PRODUCER:
                keys_to_convert = {'key_serializer', 'value_serializer'}
                for key in keys_to_convert:
                    if key in config:
                        value = config.pop(key)
                        new_config[key] = self.get_serializer(value)
            else:
                raise Exception("kafka_types must be CONSUMER or PRODUCER")
            return new_config

        def init_properties(self, properties: Dict[str, Any]) -> Dict[str, Any]:
            temp_properties = ExpressYmlConverter.dict_keys_to_snake_case(properties)
            temp_properties = self.convert_config(self.kafka_types, temp_properties)
            temp_properties['bootstrap_servers'] = self._bootstrap_servers
            return temp_properties

        def build(self) -> 'ExpressKafkaConfiguration':
            if self.kafka_types == ExpressKafkaTypes.UNKNOWN or not self.kafka_types:
                raise ExpressKafkaException("Kafka Types cannot be UNKNOWN or empty")
            if not self._id:
                raise ExpressKafkaException("ID cannot be None or empty")
            if not self._bootstrap_servers:
                raise ExpressKafkaException("Bootstrap servers cannot be None or empty")
            if not self._properties:
                raise ExpressKafkaException("Properties cannot be None or empty")

            return ExpressKafkaConfiguration(
                kafka_types=self.kafka_types
                , _id=self._id
                , bootstrap_servers=self._bootstrap_servers
                , properties=self._properties
            )
