import unittest

from express_utils.express_exception_utils import ExpressExceptionUtils
from express_utils.express_logger_factory import ExpressLoggerFactory

class TestClass(unittest.TestCase):
    def test_print_stack_trace(self):
        try:
            1 / 0
        except Exception as e:
            print(ExpressExceptionUtils.get_stack_trace(e))

    def test_logger_create(self):
        logger = ExpressLoggerFactory.get_logger("test")
        try:
            1 / 0
        except Exception as e:
            logger.info(ExpressExceptionUtils.get_stack_trace(e))

if __name__ == "__main__":
    unittest.main()
