from express_utils import ExpressLoggerFactory


class DbLogger:
    _instance = None

    def __init__(self):
        if self._instance is not None:
            raise Exception("This class is a singleton. Call get_instance() instead")

    @staticmethod
    def get_instance():
        if DbLogger._instance is None:
            DbLogger._instance = ExpressLoggerFactory.get_logger(DbLogger.__name__)
        return DbLogger._instance
