from abc import ABC, abstractmethod


class ExpressBaseThreadCycle(ABC):
    @abstractmethod
    def _initialize(self):
        ...

    @abstractmethod
    def _execute(self):
        ...

    @abstractmethod
    def _finalize(self):
        ...
