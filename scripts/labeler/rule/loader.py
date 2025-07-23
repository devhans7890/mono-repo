import yaml
from pathlib import Path
from typing import Union
from scripts.labeler.rule.models import RuleSet

def load_rules(path: Union[str, Path] = "rules.yaml") -> RuleSet:
    try:
        with open(path, "r", encoding="utf-8") as f:
            rules_yaml = yaml.safe_load(f)
        return RuleSet(**rules_yaml)
    except Exception as e:
        raise RuntimeError(f"[Rule Loader] Failed to load rules from {path}: {e}")
