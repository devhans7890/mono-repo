import yaml
import logging
from logging import config as logging_config
import os

TRACE_LEVEL = 5
logging.addLevelName(TRACE_LEVEL, "TRACE")


def trace(self, message, *args, **kwargs):
    if self.isEnabledFor(TRACE_LEVEL):
        self._log(TRACE_LEVEL, message, args, **kwargs)


logging.Logger.trace = trace


possible_paths = [
    os.path.abspath("./logging-test.yml"),
    os.path.abspath("./resources/logging-test.yml"),
    os.path.abspath("../resources/logging-test.yml"),
    os.path.abspath("./logging.yml"),
    os.path.abspath("./resources/logging.yml"),
    os.path.abspath("../resources/logging.yml"),
]
class ExpressLoggerFactory:
    @staticmethod
    def get_logger(logger_name: str, config_path: str = None) -> logging.Logger:
        if config_path is None:

            config_path = None
            for path in possible_paths:
                if os.path.exists(path):
                    config_path = path
                    break

        if not config_path:
            raise FileNotFoundError("No logging configuration file found in the specified paths.")

        if not hasattr(logging, 'TRACE'):
            setattr(logging, 'TRACE', TRACE_LEVEL)
            logging.addLevelName(TRACE_LEVEL, "TRACE")

        with open(config_path, 'r', encoding='utf-8') as f:
            config_dict = yaml.safe_load(f)
            logging_config.dictConfig(config_dict)

        logger = logging.getLogger(logger_name)
        return logger
