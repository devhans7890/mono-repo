from express_utils.express_logger_factory import ExpressLoggerFactory

if __name__ == "__main__":
    logger = ExpressLoggerFactory.get_logger("example_logger")
    logger.trace("TRACE MESSAGE")
    logger.debug("DEBUG MESSAGE")
    logger.info("INFO MESSAGE")
    logger.warning("WARNING MESSAGE")
    logger.error("ERROR MESSAGE")
    logger.critical("CRITICAL MESSAGE")