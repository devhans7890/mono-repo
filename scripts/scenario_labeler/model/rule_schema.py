from dataclasses import dataclass
from typing import List, Optional

@dataclass
class Step:
    id: str
    step_no: int
    field: str
    comparison_type: str
    aggregation: str
    lookback_period_minutes: int
    operator: Optional[str] = None
    threshold: Optional[float] = None
    cache_key_template: Optional[str] = None

@dataclass
class OnDetected:
    action_type: str
    cache_key_template: str
    field_to_cache: Optional[str] = None
    score_field: Optional[str] = None
    value_template: Optional[str] = None

@dataclass
class Rule:
    id: str
    name: str
    level: str
    action: str
    notify: str
    last_modified: str
    steps: List[Step]
    on_detected: Optional[List[OnDetected]] = None
