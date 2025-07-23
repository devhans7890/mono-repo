from typing import List
import yaml
from scripts.scenario_labeler.model.rule_schema import Rule, Step, OnDetected

def load_rules(filepath: str) -> List[Rule]:
    with open(filepath, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)

    rules = []
    for rule_dict in data['rules']:
        steps = [Step(**s) for s in rule_dict['steps']]
        on_detected = [OnDetected(**o) for o in rule_dict.get('on_detected', [])]
        rule = Rule(**{k: v for k, v in rule_dict.items() if k not in ('steps', 'on_detected')}, steps=steps, on_detected=on_detected)
        rules.append(rule)
    return rules


