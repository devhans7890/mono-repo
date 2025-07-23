import re
from typing import Dict, Any


class ExpressYmlConverter():
    @staticmethod
    def camel_to_snake(name: str) -> str:
        """
        Convert CamelCase or camelCase string to snake_case string.
        """
        # Convert '.' to '_' first to handle keys with dots.
        name = name.replace('.', '_')
        # Then convert CamelCase to snake_case.
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    @staticmethod
    def dict_keys_to_snake_case(d: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert all keys in a dictionary to snake_case, including nested dictionaries and lists.
        """
        new_dict = {}
        for key, value in d.items():
            new_key = ExpressYmlConverter.camel_to_snake(key)
            if isinstance(value, dict):
                new_dict[new_key] = ExpressYmlConverter.dict_keys_to_snake_case(value)
            elif isinstance(value, list):
                new_dict[new_key] = [
                    ExpressYmlConverter.dict_keys_to_snake_case(item) if isinstance(item, dict) else item for item in
                    value
                ]
            else:
                new_dict[new_key] = value
        return new_dict
