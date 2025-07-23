from abc import abstractmethod, ABC


class ExpressModelMetadataBuilder(ABC):
    @abstractmethod
    def build(self) -> object:
        ...
