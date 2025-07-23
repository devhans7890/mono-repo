from abc import abstractmethod
from express_model.express_model_types import ExpressModelTypes
from typing import List


class ExpressModelConfigurationInterface:
    @abstractmethod
    def model_type(self) -> ExpressModelTypes:
        ...

    @abstractmethod
    def id(self) -> str:
        ...
    #
    # @abstractmethod
    # def feature_list(self) -> List[str]:
    #     ...
