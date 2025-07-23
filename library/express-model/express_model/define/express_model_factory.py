from abc import abstractmethod, ABC


class ExpressModelFactory:
    @abstractmethod
    def build(self) -> object:
        ...
