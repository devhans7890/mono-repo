import logging
from config.config_loader import ConfigLoader

def setup_logger(name: str = "fds_engine"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    config = ConfigLoader.get('logger', env_specific=True)
    handlers = config.get('handlers', [])
    level = getattr(logging, config.get('level', 'INFO'))
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')

    if 'stream' in handlers:
        sh = logging.StreamHandler()
        sh.setLevel(level)
        sh.setFormatter(formatter)
        logger.addHandler(sh)

    if 'file' in handlers:
        fh = logging.FileHandler(config.get('log_file', 'logs/fds_engine.log'))
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger

