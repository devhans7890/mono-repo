from typing import Optional, TypeVar
from threading import Lock
from .define.express_kafka_record import ExpressKafkaRecord

K = TypeVar('K')
V = TypeVar('V')


class AtomicInteger:
    def __init__(self, initial: int = 0):
        self.value = initial
        self.lock = Lock()

    def increment_and_get(self) -> int:
        with self.lock:
            self.value += 1
            return self.value

    def get(self) -> int:
        with self.lock:
            return self.value


class ExpressKafkaProduceRecord(ExpressKafkaRecord[K, V]):
    def __init__(self, topic: str, data: V, key: Optional[K] = None):
        self._topic = topic
        self._key = key
        self._data = data
        self._fail_count = AtomicInteger(0)

    def topic(self) -> str:
        return self._topic

    def key(self) -> Optional[K]:
        return self._key

    def data(self) -> V:
        return self._data

    def failure_count(self) -> int:
        return self._fail_count.get()

    def fail(self) -> None:
        self._fail_count.increment_and_get()
