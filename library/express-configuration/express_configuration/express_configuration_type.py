from enum import Enum
from typing import Any


class ExpressConfigurationType(Enum):
    STATIC = "STATIC"
    DYNAMIC = "DYNAMIC"

    @classmethod
    def _missing_(cls, value: Any):
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"{value} is not a valid {cls.__name__}")
