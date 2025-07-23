from express_utils import ExpressLoggerFactory


class SysLogger:
    _instance = None

    def __init__(self):
        if self._instance is not None:
            raise Exception("This class is a singleton. Call get_instance() instead")

    @staticmethod
    def get_instance():
        if SysLogger._instance is None:
            SysLogger._instance = ExpressLoggerFactory.get_logger(SysLogger.__name__)
        return SysLogger._instance
