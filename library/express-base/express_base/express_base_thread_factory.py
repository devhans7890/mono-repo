from threading import Thread
from express_base.express_runnable import ExpressRunnable


class ExpressBaseThreadFactory:
    @staticmethod
    def new_thread(express_runnable: ExpressRunnable) -> Thread:
        if not isinstance(express_runnable, ExpressRunnable):
            raise ValueError("express_runnable must be an instance of ExpressRunnable.")
        return Thread(target=express_runnable.run)
