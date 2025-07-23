import yaml
from functools import lru_cache

class ConfigLoader:
    _config = None

    @classmethod
    @lru_cache()
    def load(cls, path: str = "resources/config.yaml") -> dict:
        if cls._config is None:
            with open(path, 'r', encoding='utf-8') as f:
                cls._config = yaml.safe_load(f)
        return cls._config

    @classmethod
    def get(cls, section: str, env_specific: bool = False):
        config = cls.load()
        env = config.get('env', 'dev')

        if env_specific and isinstance(config.get(section), dict):
            return config[section].get(env, {})
        return config.get(section, {})