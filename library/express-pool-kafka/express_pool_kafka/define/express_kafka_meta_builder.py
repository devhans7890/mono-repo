from abc import ABC, abstractmethod


class ExpressKafkaMetaBuilder(ABC):
    @abstractmethod
    def build(self) -> object:
        """
        Builds the Kafka metadata.

        :raises ExpressKafkaException: If there is an error in building.
        """