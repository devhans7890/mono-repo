from enum import Enum


class ExpressBaseThreadState(Enum):
    PREPARE = 0
    INIT = 1
    RUNNING = 2
    AWAIT = 3
    DESTROY = 4
