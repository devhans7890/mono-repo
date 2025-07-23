from scripts.scenario_labeler.model.rule_schema import Rule
from scripts.scenario_labeler.utils.utils import redis_client, parse_datetime, filter_recent, format_value
from typing import List, Dict, Any, Optional
import re
import json


class FDSEngine:
    def __init__(self, rules: List[dict]):
        self.rules = rules

    def evaluate(self, transactions: List[dict]) -> List[str]:
        result = {}
        for txn in transactions:
            for rule in self.rules:
                index_prefixes = rule.get("index_prefix")
                if index_prefixes:
                    if isinstance(index_prefixes, str):
                        index_prefixes = [index_prefixes]
                    if not any(txn.get("@index", "").startswith(prefix) for prefix in index_prefixes):
                        continue
                if self._evaluate_rule(rule, [txn]):
                    threshold = getattr(rule, "detection_threshold", 1)
                    key = f"""detected:count:{getattr(rule, "id", "unknown")}:{txn.get("@id")}"""
                    count = redis_client.incr(key)
                    if count < threshold:
                        continue
                    else:
                        result = {
                            "rule_id": getattr(rule, "id", "unknown"),
                            "@id": txn.get("@id"),
                            "trData": json.dumps(txn, ensure_ascii=False)
                        }
                        self._on_detected(rule, txn)
                        if rule.get("action") in ["LABEL","BLOCKED"]:
                            return result
        return result

    def _evaluate_rule(self, rule: dict, transactions: List[dict]) -> bool:
        step_block = rule["steps"]
        return self._evaluate_step_block(step_block, transactions)

    def _evaluate_step_block(self, block: dict, transactions: List[dict]) -> bool:
        if "logic" in block:
            logic = block["logic"].lower()
            results = [self._evaluate_step_block(c, transactions) for c in block["conditions"]]
            return all(results) if logic == "and" else any(results)
        else:
            anchor_txn = transactions[0] if len(transactions) == 1 else transactions[-1]
            return self._evaluate_leaf(block, transactions, anchor_txn)

    def _evaluate_leaf(self, step: dict, transactions: List[dict], anchor_txn: dict) -> bool:
        field = step["field"]
        comp = step["comparison_type"]
        lookback_raw = step.get("lookback_period")
        if lookback_raw:
            if isinstance(lookback_raw, str) and lookback_raw[-1] in "smhd":
                unit = lookback_raw[-1]
                amount = int(lookback_raw[:-1])
                lookback = amount * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
            else:
                lookback = int(lookback_raw)  # assume seconds if no unit
        else:
            lookback = 0  # default to single-transaction check

        timestamp_field = step.get("timestamp_field", "timestamp")
        anchor_time = parse_datetime(anchor_txn[timestamp_field])
        if lookback == 0:
            recent_data = [anchor_txn]  # 단건 평가
        else:
            recent_data = [
                txn for txn in transactions
                if 0 <= (anchor_time - parse_datetime(txn.get(timestamp_field, ""))).total_seconds() <= lookback
            ]

        if comp == "operator":
            op = step["operator"]
            thresh = step["threshold"]
            values = [txn.get(field) for txn in recent_data if field in txn]
            if not values:
                return False
            aggregation = step.get("aggregation", "LAST").upper()
            if lookback == 0:
                aggregation = "LAST"  # 단건 평가 시 aggregation 강제 지정
            if aggregation == "MAX":
                value = max(values)
            elif aggregation == "MIN":
                value = min(values)
            elif aggregation == "AVG":
                value = sum(values) / len(values)
            elif aggregation == "SUM":
                value = sum(values)
            elif aggregation == "STD":
                mean = sum(values) / len(values)
                value = (sum((x - mean) ** 2 for x in values) / len(values)) ** 0.5
            elif aggregation == "MEDIAN":
                sorted_vals = sorted(values)
                mid = len(sorted_vals) // 2
                value = (sorted_vals[mid] if len(sorted_vals) % 2 != 0 else
                         (sorted_vals[mid - 1] + sorted_vals[mid]) / 2)
            elif aggregation == "SLOPE":
                try:
                    from scipy.stats import linregress
                    x = list(range(len(values)))
                    slope, _, _, _, _ = linregress(x, values)
                    value = slope
                except ImportError:
                    return False
            else:
                value = values[-1]
            return self._compare(value, op, thresh)

        elif comp == "like":
            pattern = step["pattern"].replace("%", ".*")  # SQL LIKE to regex
            regex = re.compile(f"^{pattern}$")
            values = [txn.get(field) for txn in recent_data if field in txn]
            return any(regex.match(str(val)) for val in values if val)

        elif comp == "regex":
            pattern = step["pattern"]
            regex = re.compile(pattern)
            values = [txn.get(field) for txn in recent_data if field in txn]
            return any(regex.match(str(val)) for val in values if val)

        elif comp == "cache_contains":
            key = step["cache_key_template"].format(**transactions[-1])
            return redis_client.sismember(key, transactions[-1][field])

        elif comp == "cache_not_contains":
            key = step["cache_key_template"].format(**transactions[-1])
            return not redis_client.sismember(key, transactions[-1][field])

        elif comp == "cache_exists":
            key = step["cache_key_template"].format(**transactions[-1])
            return redis_client.exists(key)

        return False

    def _compare(self, v, op, t):
        return {
            ">": v > t,
            ">=": v >= t,
            "<": v < t,
            "<=": v <= t,
            "==": v == t,
            "!=": v != t
        }[op]

    def _on_detected(self, rule: dict, txn: dict):
        if getattr(rule, "action", "").upper() in ["LABEL", "BLOCK"]:
            redis_client.flushall()

        if not rule.get("on_detected"):
            return

        for action in rule["on_detected"]:
            cache_type = action.get("cache_type")
            if cache_type == "object":
                field_val = txn.get(action["field_to_cache"])
                key = action["cache_key_template"].format(**txn)
                if field_val:
                    redis_client.sadd(key, field_val)
            elif cache_type == "personna":
                score = int(parse_datetime(txn[action["score_field"]]).timestamp())
                key = action["cache_key_template"].format(**txn)
                context = {**txn, "rule_id": getattr(rule, "id", "unknown")}
                value = format_value(action["value_template"], context)
                redis_client.zadd(key, {value: score})
