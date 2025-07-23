from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from embodiment import CustomThread, CustomProcess
from datetime import datetime
from express_utils import ExpressLoggerFactory
import unittest
import time


class TestExampleExpressBase(unittest.TestCase):
    # CPU-Bound 작업 시 ProcessPoolExecutor 사용 권장
    def test_cpu_bound_task(self):
        print("")
        custom_process = CustomProcess(name="CustomProcess")
        cp_thread = custom_process.start_with_sub_handler_role()
        logger = ExpressLoggerFactory.get_logger("")
        logger.info(f"{cp_thread.is_alive()} => {cp_thread.name}")

        with ProcessPoolExecutor() as executor:
            future = executor.submit(cp_thread.start)

        while cp_thread.is_alive():
            logger.info(f"{cp_thread.is_alive()} => {cp_thread.name}")
            time.sleep(1)

        logger.info(f"{cp_thread.is_alive()} => {cp_thread.name}")

    # I/O 작업이 많은 경우 ThreadPoolExecutor 사용 권장
    def test_io_bound_task(self):
        print("")
        custom_thread = CustomThread(name="CustomThread")
        ct_thread = custom_thread.start_with_sub_handler_role()
        logger = ExpressLoggerFactory.get_logger("")
        logger.info(f"{ct_thread.is_alive()} => {ct_thread.name}")

        with ThreadPoolExecutor() as executor:
            future = executor.submit(ct_thread.start)

        while ct_thread.is_alive():
            logger.info(f"{ct_thread.is_alive()} => {ct_thread.name}")
            time.sleep(1)

        logger.info(f"{ct_thread.is_alive()} => {ct_thread.name}")

    # CPU-Bound 작업 시 ProcessPoolExecutor 사용 권장
    def test_cpu_bound_task_pool(self):
        print("")
        max_workers = 4
        cp_thread_list = [CustomProcess(name=f"CustomProcess-{_}").start_with_sub_handler_role() for _ in
                          range(max_workers)]
        logger = ExpressLoggerFactory.get_logger("")
        for cp_thread in cp_thread_list:
            logger.info(f"{cp_thread.is_alive()} => {cp_thread.name}")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(proc.start) for proc in cp_thread_list]

        while any(thread.is_alive() for thread in cp_thread_list):
            for cp_thread in cp_thread_list:
                logger.info(f"{cp_thread.is_alive()} => {cp_thread.name}")
            time.sleep(1)

        for cp_thread in cp_thread_list:
            logger.info(f"{cp_thread.is_alive()} => {cp_thread.name}")


    # I/O 작업이 많은 경우 ThreadPoolExecutor 사용 권장
    def test_io_bound_task_pool(self):
        print("")
        max_workers = 4
        threads = [CustomThread(name=f"CustomThread-{i}").start_with_sub_handler_role() for i in range(max_workers)]
        logger = ExpressLoggerFactory.get_logger("")
        for thread in threads:
            logger.info(f"{thread.is_alive()} => {thread.name}")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(thread.start) for thread in threads]

        while any(thread.is_alive() for thread in threads):
            for thread in threads:
                logger.info(f"{thread.is_alive()} => {thread.name}")
                time.sleep(1)

        for thread in threads:
            logger.info(f"{thread.is_alive()} => {thread.name}")


if __name__ == "__main__":
    unittest.main()
