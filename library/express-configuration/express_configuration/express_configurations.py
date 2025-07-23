from watchdog.observers import Observer
from .config_file_handler import ConfigFileHandler
from typing import List, Dict, Any, Iterator
from collections import defaultdict
from .express_configuration_type import ExpressConfigurationType
from .express_configuration_if import ExpressConfigurationIF
from .express_configuration_utils import ExpressConfigurationUtils
import yaml
import json
import logging
import os


class ExpressConfigurations(ExpressConfigurationIF):
    def __init__(self, file_path: str, configuration_type: ExpressConfigurationType):
        self.file_path = ExpressConfigurationUtils.get_path_file(file_path)
        self.type = configuration_type
        self.configuration = defaultdict()
        self.line_breaker = "\n"
        self.logger = logging.getLogger(self.__class__.__name__)
        self.system_properties = defaultdict()
        self.init()

    def init(self):
        properties = os.environ
        for k, v in properties.items():
            if k is not None and v is not None:
                self.system_properties[str(k)] = v

        try:
            if self.type == ExpressConfigurationType.STATIC:
                self.logger.info(f"Loading the static yml file: {self.file_path}")
                self.load_config()
            elif self.type == ExpressConfigurationType.DYNAMIC:
                self.logger.info(f"Loading the dynamic yml file: {self.file_path}")
                self.load_config()
                self.start_watcher()
        except Exception as e:
            self.logger.error(f"{self.type.value}[{self.file_path}] init failure => {e}")

    def load_config(self):
        try:
            with open(self.file_path, 'r', encoding='utf-8') as file:
                self.configuration = yaml.safe_load(file)
                self.logger.info(f"Loaded configuration: {self.configuration}")
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")

    def start_watcher(self):
        """Start a watchdog observer to watch the file for changes."""
        event_handler = ConfigFileHandler(self)
        observer = Observer()
        observer.schedule(event_handler, path='.', recursive=False)
        observer.start()

    def reload(self):
        self.load_config()
        self.logger.info(f"Reloaded configuration from {self.file_path}")

    def get_string(self, key: str, default: str = "") -> str:
        return self.configuration.get(key, default)

    def get_int(self, key: str, default: int = 0) -> int:
        return int(self.get_string(key, default))

    def get_float(self, key: str, default: float = 0.0) -> float:
        return float(self.get_string(key, default))

    def get_boolean(self, key: str, default: bool = False) -> bool:
        return bool(self.get_string(key, default))

    def get_list(self, key: str, default: List[Any] = None) -> List[Any]:
        value = self.get_string(key, None)
        return value.split(',') if value else (default if default else [])

    def get_dict(self, key: str, default: Dict[str, Any] = None) -> Dict[str, Any]:
        return self.configuration if self.configuration else (default if default else {})

    def get_keys(self, prefix: str = "") -> Iterator[str]:
        return (k for k in self.configuration.keys() if k.startswith(prefix))

    def set_property(self, key: str, value: Any):
        self.configuration[key] = value

    def get_type(self) -> ExpressConfigurationType:
        return self.type

    def get_file_path(self) -> str:
        return self.file_path

    def get_system_properties(self) -> Dict[str, Any]:
        return self.system_properties

    def save(self):
        with open(self.file_path, 'w', encoding='utf-8') as configfile:
            yaml.dump(self.configuration, configfile)

    def get_string_list(self, var1: str) -> List[str]:
        value = self.configuration.get(var1, None)
        if value is None:
            return []
        if isinstance(value, str):
            return value.split(',')
        return []

    def get_integer_list(self, var1: str) -> List[int]:
        value = self.configuration.get(var1, None)
        if value is None:
            return []

        if isinstance(value, str):
            try:
                return [int(v.strip()) for v in value.split(',')]
            except ValueError:
                self.logger.error(f"Failed to parse integer list for key '{var1}'")
        return []

    def get_float_list(self, var1: str) -> List[float]:
        value = self.configuration.get(var1, None)
        if value is None:
            return []
        if isinstance(value, str):
            try:
                return [float(v.strip()) for v in value.split(',')]
            except ValueError:
                self.logger.error(f"Failed to parse float list for key '{var1}'")
        return []

    def get_boolean_list(self, var1: str) -> List[bool]:
        value = self.configuration.get(var1, None)
        if value is None:
            return []
        if isinstance(value, str):
            return [self._parse_boolean(v.strip()) for v in value.split(',')]
        return []

    def _parse_boolean(self, value: str) -> bool:
        true_values = ['true', 'yes', '1']
        false_values = ['false', 'no', '0']
        if value.lower() in true_values:
            return True
        elif value.lower() in false_values:
            return False
        else:
            self.logger.error(f"Failed to parse boolean value '{value}'")
            return False

    def get_string_array(self, var1: str) -> List[str]:
        value = self.configuration.get(var1, None)
        if value is None:
            return []
        if isinstance(value, str):
            return [v.strip() for v in value.split(',')]
        return []

    def get_property(self, var1: str) -> Any:
        return self.configuration.get(var1)

    def __str__(self) -> str:
        if not self.configuration:
            return ""
        string_list = []
        string_list.append(f"    [1] path     : {self.file_path}\n")
        string_list.append(f"    [2] contents\n")
        for key, value in self.configuration.items():
            string_list.append(f"        - {key}: {value}\n")
        return ''.join(string_list)
