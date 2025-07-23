from abc import ABC, abstractmethod
from typing import Generic, TypeVar

K = TypeVar('K')
V = TypeVar('V')


class ExpressKafkaRecord(Generic[K, V], ABC):
    @abstractmethod
    def topic(self) -> str:
        ...

    @abstractmethod
    def key(self) -> K:
        ...

    @abstractmethod
    def data(self) -> V:
        ...

    @abstractmethod
    def failure_count(self) -> int:
        ...

    @abstractmethod
    def fail(self) -> None:
        ...
