import os
import glob
from datetime import datetime
from express_model.define.express_model_configuration_interface import ExpressModelConfigurationInterface
from express_model.define.express_model_metadata_builder import ExpressModelMetadataBuilder
from express_model.express_model_types import ExpressModelTypes
from express_model.express_model_exception import ExpressModelException
from pycaret import anomaly, classification
from typing import List


class ExpressModelConfiguration(ExpressModelConfigurationInterface):

    def __init__(self,
                 model_types: ExpressModelTypes,
                 model_path: str,
                 _id: str,
                 dir: str,
                 prefix: str,
                 feature_list: list
                 ):
        self._model_types = model_types
        self._model_path = model_path
        self._id = _id
        self._dir = dir
        self._prefix = prefix
        self._feature_list = feature_list

    @property
    def model_type(self) -> ExpressModelTypes:
        return self._model_types

    @property
    def model_path(self) -> str:
        return self._model_path

    @property
    def id(self) -> str:
        return self._id

    @property
    def dir(self) -> str:
        return self._dir

    @property
    def prefix(self) -> str:
        return self._prefix

    @property
    def feature_list(self) -> list:
        return self._feature_list

    class Builder(ExpressModelMetadataBuilder):
        def __init__(self, model_types: ExpressModelTypes):
            self.__model_types = model_types
            self.__id = None
            self.__dir = None
            self.__prefix = None
            self.__model_path = None

        def id(self, _id: str) -> 'ExpressModelConfiguration.Builder':
            self.__id = _id
            return self

        def dir(self, dir: str) -> 'ExpressModelConfiguration.Builder':
            self.__dir = dir
            return self

        def prefix(self, prefix: str) -> 'ExpressModelConfiguration.Builder':
            self.__prefix = prefix
            return self

        def build(self) -> 'ExpressModelConfiguration':
            if self.__model_types == ExpressModelTypes.UNKNOWN or not self.__model_types:
                raise ExpressModelException("Model Types cannot be UNKNOWN or empty")
            if not self.__id:
                raise ExpressModelException("ID cannot be None or empty")
            if not self.__dir:
                raise ExpressModelException("Model directory cannot be None or empty")
            if not self.__prefix:
                raise ExpressModelException("Model prefix cannot be None or empty")

            self.__model_path = self.__get_latest_model_files(self.__dir, self.__prefix)

            return ExpressModelConfiguration(
                model_types=self.__model_types,
                model_path=self.__model_path,
                _id=self.__id,
                dir=self.__dir,
                prefix=self.__prefix,
                feature_list=self.__extract_feature_list_from_model(self.__model_path)
            )

        def __get_latest_model_files(self, dir: str, prefix: str) -> str:
            possible_dirs = [
                dir,
                os.path.abspath(f"./resources/{dir}")
            ]
            checked_dir = None
            for possible_dir in possible_dirs:
                if os.path.exists(possible_dir):
                    checked_dir = possible_dir
                    break

            if not checked_dir:
                raise FileNotFoundError("No model dir found in the specified dirs.")

            pattern = os.path.join(checked_dir, f"{prefix}*.pkl")
            matching_files = glob.glob(pattern)

            if not matching_files:
                raise ExpressModelException(f"No files found with prefix {prefix} in path {checked_dir}")

            latest_file = max(matching_files, key=lambda x: os.path.getmtime(x))
            return latest_file

        def __extract_feature_list_from_model(self, model_path: str) -> List[str]:
            model = None
            if self.__model_types == ExpressModelTypes.SUPERVISED:
                model = classification.load_model(model_path.replace(".pkl", ""))
            elif self.__model_types == ExpressModelTypes.UNSUPERVISED:
                model = anomaly.load_model(model_path.replace(".pkl", ""))
            else:
                model = None

            if model is None:
                raise ExpressModelException(f"Failed to load model from path: {model_path}. Cannot load feature list.")

            numerical_features = model.named_steps['numerical_imputer'].include
            categorical_features = model.named_steps['categorical_imputer'].include
            return list(numerical_features) + list(categorical_features)
