from express_base.abstract_express_base_thread_model import AbstractExpressBaseThreadModel
from express_base.express_base_thread_cycle import ExpressBaseThreadCycle
from express_base.express_base_thread_factory import ExpressBaseThreadFactory
from express_base.express_base_thread_state import ExpressBaseThreadState
from express_base.express_runnable import ExpressRunnable
from express_utils import ExpressExceptionUtils, ExpressLoggerFactory
from abc import abstractmethod
from threading import Thread
from logging import Logger
from time import time
import uuid


class ExpressBaseThread(AbstractExpressBaseThreadModel, ExpressRunnable, ExpressBaseThreadCycle):
    def __init__(self, name: str):
        super().__init__()
        self._name: str = None if name is None else name
        self._sys_logger: Logger = ExpressLoggerFactory.get_logger(self.__class__.__name__)
        self._express_base_thread_state: ExpressBaseThreadState = ExpressBaseThreadState.PREPARE
        self._interrupted: bool = False
        self._create_time: float = 0

    def run(self):
        try:
            self._express_base_thread_state = ExpressBaseThreadState.INIT
            self._initialize()
        except Exception as e:
            self._interrupted = True
            self._sys_logger.error(ExpressExceptionUtils.get_stack_trace(e))

        if not self._interrupted:
            try:
                self._express_base_thread_state = ExpressBaseThreadState.RUNNING
                self._execute()
            except Exception as e:
                self._interrupted = True
                self._sys_logger.error(ExpressExceptionUtils.get_stack_trace(e))

        try:
            self._express_base_thread_state = ExpressBaseThreadState.AWAIT
            self._finalize()
        except Exception as e:
            self._sys_logger.error(ExpressExceptionUtils.get_stack_trace(e))

        self._express_base_thread_state = ExpressBaseThreadState.DESTROY

    def _initialize(self):
        if not self._name:
            self._name = str(uuid.uuid4()).replace('-', '')
        self._create_time = time()
        self._sys_logger.debug(f"Thread {self._name} initialized.")

    @abstractmethod
    def _execute(self):
        ...

    @abstractmethod
    def _finalize(self):
        ...

    def get_thread(self) -> Thread:
        thread = ExpressBaseThreadFactory().new_thread(self)
        thread.name = self._name
        if self.get_daemon():
            thread.setDaemon(self.get_daemon())
        return thread

    def start_with_main_handler_role(self) -> Thread:
        self.set_daemon(False)
        thread = self.get_thread()
        thread.start()
        return thread

    def start_with_sub_handler_role(self) -> Thread:
        thread = self.get_thread()
        thread.start()
        return thread

    def get_express_base_thread_state(self) -> ExpressBaseThreadState:
        return self._express_base_thread_state
