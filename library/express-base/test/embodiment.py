import time
from express_base.express_base_thread import ExpressBaseThread


class CustomThread(ExpressBaseThread):
    def _initialize(self):
        super()._initialize()
        time.sleep(1)

    def _execute(self):
        self._sys_logger.debug("executed")
        time.sleep(1)

    def _finalize(self):
        self._sys_logger.debug("finalized")
        time.sleep(1)


class CustomProcess(ExpressBaseThread):
    def _initialize(self):
        super()._initialize()
        time.sleep(1)

    def _execute(self):
        self._sys_logger.debug("executed")
        time.sleep(1)

    def _finalize(self):
        self._sys_logger.debug("finalized")
        time.sleep(1)