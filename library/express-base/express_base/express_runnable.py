from abc import ABC, abstractmethod


class ExpressRunnable(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def run(self):
        ...
