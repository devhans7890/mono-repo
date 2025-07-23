from abc import ABC, abstractmethod
from typing import List, Iterator, Any


class ExpressConfigurationIF(ABC):
    @abstractmethod
    def get_string(self, var1: str) -> str:
        ...

    @abstractmethod
    def get_int(self, var1: str) -> int:
        ...

    @abstractmethod
    def get_float(self, var1: str) -> float:
        ...

    @abstractmethod
    def get_boolean(self, var1: str) -> bool:
        ...

    @abstractmethod
    def get_list(self, var1: str) -> List[Any]:
        ...

    @abstractmethod
    def get_string_list(self, var1: str) -> List[str]:
        ...

    @abstractmethod
    def get_integer_list(self, var1: str) -> List[int]:
        ...

    @abstractmethod
    def get_float_list(self, var1: str) -> List[float]:
        ...

    @abstractmethod
    def get_boolean_list(self, var1: str) -> List[bool]:
        ...

    @abstractmethod
    def get_string_array(self, var1: str) -> List[str]:
        ...

    @abstractmethod
    def get_property(self, var1: str) -> Any:
        ...

    @abstractmethod
    def set_property(self, var1: str, var2: Any) -> None:
        ...

    @abstractmethod
    def save(self) -> None:
        ...

    @abstractmethod
    def reload(self) -> None:
        ...

    @abstractmethod
    def get_keys(self, var1: str) -> Iterator[str]:
        ...
