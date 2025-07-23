from express_model.express_model_types import ExpressModelTypes
from express_model.express_model_configuration import ExpressModelConfiguration
from pycaret.anomaly import AnomalyExperiment
from pycaret.classification import ClassificationExperiment
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple
import os
import glob


class ExpressModelMetadata:
    def __init__(self):
        self._model_type: Optional[ExpressModelTypes] = None
        self._configuration: Optional[ExpressModelConfiguration] = None
        self._feature_list: List[str]

    def get_configuration(self) -> ExpressModelConfiguration:
        if self._configuration is None:
            raise Exception("ModelConfiguration is None")
        return self._configuration

    def build(self, config: Dict[str, Any]) -> bool:
        if not config:
            return False

        identifier = config.get("id")
        if not identifier:
            return False

        dir = config.get("dir")
        if not dir:
            return False

        model_type = config.get("model_type")
        if not model_type:
            return False
        self._model_type = ExpressModelTypes[model_type.upper()]

        prefix = config.get("prefix")
        if not prefix:
            return False

        if not (dir and prefix):
            return False

        builder = ExpressModelConfiguration.Builder(self._model_type)
        self._configuration = builder.id(identifier) \
            .dir(dir) \
            .prefix(prefix) \
            .build()
        return True

    def _get_model_config(self):
        if self._configuration is None:
            raise Exception("Model Configuration is None")
        return self._configuration

    def create_experiment(self):
        if self._model_type == ExpressModelTypes.SUPERVISED:
            experiment = ClassificationExperiment()
        elif self._model_type == ExpressModelTypes.UNSUPERVISED:
            experiment = AnomalyExperiment()
        else:
            raise Exception(
                f"create_experiment is failed. ExpressModelTypes is Wrong."
            )
        return experiment

    def load_model(self, experiment):
        if experiment is None:
            raise Exception(
                f"experiment is None. Check model type"
            )
        model = experiment.load_model(self._configuration.model_path.replace(".pkl", ""))
        return model
