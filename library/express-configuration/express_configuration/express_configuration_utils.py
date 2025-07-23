import json
import logging
import os
from typing import Dict, Any
from collections import defaultdict

import yaml
from yaml import load, Loader


class ExpressConfigurationUtils:
    logger = logging.getLogger(__name__)
    FILE_SCHEME = "file:"

    # 자바와 달리 디렉토리 구조가 정해져 있지 않음
    # @staticmethod
    # def properties_static_path_with_debug(env_key: str, debug_path: str, production_path: str) -> str:
    #     env_path = os.getenv(env_key, "")
    #     if not env_path:
    #         file = os.path.join("src", "main", "resources", "debug")
    #         if os.path.exists(file) and os.path.isdir(file):
    #             return production_path if not debug_path else debug_path
    #         else:
    #             return production_path
    #     else:
    #         return env_path

    @staticmethod
    def json_recursive_replace_with_event(usable_env_method: bool, environments: Dict[str, Any],
                                          input_data: Any) -> None:
        if isinstance(input_data, dict):
            for key, value in input_data.items():
                if isinstance(value, str):
                    if usable_env_method and "%{" in value and "}" in value:
                        input_data[key] = ExpressConfigurationUtils.text_replace_with_event(environments, value, "")
                else:
                    ExpressConfigurationUtils.json_recursive_replace_with_event(usable_env_method, environments, value)
        elif isinstance(input_data, list):
            for i, value in enumerate(input_data):
                if isinstance(value, str):
                    if usable_env_method and "%{" in value and "}" in value:
                        input_data[i] = ExpressConfigurationUtils.text_replace_with_event(environments, value, "")
                else:
                    ExpressConfigurationUtils.json_recursive_replace_with_event(usable_env_method, environments, value)

    @staticmethod
    def text_replace_with_event(e: Dict[str, Any], text: str, replace_default_value: str) -> str:
        prefix = "%{"
        postfix = "}"
        temp = text
        if not text:
            return replace_default_value
        else:
            idx = 0
            while True:
                idx1 = text.find(prefix, idx)
                if idx1 < 0:
                    break
                idx2 = text.find(postfix, idx1 + 1)
                if idx2 < 0:
                    break
                idx1_verify = text.find(prefix, idx1 + 1)
                if idx1_verify >= 0 and idx1_verify < idx2:
                    idx1 = idx1_verify
                idx = idx2 + 1
                f = text[idx1:idx2 + 1]
                v = ExpressConfigurationUtils.get_event(e, f, replace_default_value)
                temp = temp.replace(f, v)
            return temp

    @staticmethod
    def get_event(event: Dict[str, Any], input_str: str, default_value: str) -> str:
        index_first = input_str.find("%{")
        index_last = input_str.find("}", index_first + 1)
        replaced_default_value = default_value
        if index_first >= 0 and index_last > 0:
            var_name = input_str[index_first + 2:index_last]
            array = var_name.split(":", 1)
            if len(array) > 1:
                var_name = array[0]
                replaced_default_value = array[1]
            v = event.get(var_name)
            return str(v) if v is not None else replaced_default_value
        else:
            return input_str

    @staticmethod
    def get_environments() -> Dict[str, Any]:
        system_properties = defaultdict()
        for k, v in os.environ.items():
            system_properties[k] = v.strip()
        return system_properties

    @staticmethod
    def load_yaml_to_json_element(usable_env_method: bool, file_path: str) -> Any:
        if file_path is None:
            raise Exception(f"File path not provided")
        yaml_file_path = ExpressConfigurationUtils.get_path_file(file_path)
        try:
            with open(yaml_file_path, 'r', encoding='utf-8') as file:
                yaml_data = yaml.safe_load(file)
                json_data = json.dumps(yaml_data)
                json_element = json.loads(json_data)
                environments = ExpressConfigurationUtils.get_environments()
                ExpressConfigurationUtils.json_recursive_replace_with_event(usable_env_method, environments,
                                                                            json_element)
                return json_element
        except Exception as e:
            raise Exception(f"Failed to load YAML file: {e}")

    @staticmethod
    def get_path_file(file_path: str) -> str:
        try:
            if not file_path:
                raise Exception(f"File path not provided")
            elif os.path.exists(file_path):
                return file_path
            else:
                possible_paths = [
                    os.path.abspath("./" + file_path),
                    os.path.abspath("./resources/" + file_path),
                    os.path.abspath("../resources/" + file_path)
                ]
                for path in possible_paths:
                    if os.path.exists(path):
                        return path.replace('\\', '/')
                return None
        except Exception as e:
            raise Exception(f"Failed to locate file path: {e}")
