from express_base.express_base_thread_cycle import ExpressBaseThreadCycle
from abc import abstractmethod


class AbstractExpressBaseThreadModel(ExpressBaseThreadCycle):
    def __init__(self):
        self._interrupted: bool = False
        self.daemon: bool = True
        self._name: str = ""
        self.create_time: float

    def get_daemon(self) -> bool:
        return self.daemon

    def set_daemon(self, daemon: bool) -> None:
        self.daemon = daemon

    @abstractmethod
    def _initialize(self) -> None:
        ...

    @abstractmethod
    def _execute(self) -> None:
        ...

    @abstractmethod
    def _finalize(self) -> None:
        ...
