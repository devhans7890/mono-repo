from express_pool_kafka.express_kafka_types import ExpressKafkaTypes
from abc import abstractmethod
from typing import List, Dict


class ExpressKafkaMetaConfiguration:
    @abstractmethod
    def kafka_types(self) -> ExpressKafkaTypes:
        ...

    @abstractmethod
    def _id(self) -> str:
        ...

    @abstractmethod
    def bootstrap_servers(self) -> str:
        ...

    @abstractmethod
    def topics(self) -> List[str]:
        ...

    @abstractmethod
    def properties(self) -> Dict[str, str]:
        ...
