from collections import defaultdict
from typing import List, Optional, Dict, Any
from datetime import datetime
from rule.models import Step, SearchCondition, SearchConditions, TimeCondition, PersonalizationFilters


class RuleEvaluator:
    def __init__(self, step: Step, redis_adapter: Optional[Any] = None):
        self.step = step
        self.redis = redis_adapter

    def evaluate_step(self, docs: List[dict]) -> Optional[dict]:
        filtered = self._filter_docs(docs)
        if not filtered:
            return None
        return self._evaluate_aggregation(filtered)

    def _filter_docs(self, docs: List[dict]) -> List[dict]:
        result = []
        for doc in docs:
            if not self._check_time_condition(doc):
                continue
            if not self._check_search_conditions(doc):
                continue
            if not self._check_personalization(doc):
                continue
            result.append(doc)
        return result

    def _check_time_condition(self, doc: dict) -> bool:
        cond = self.step.time_condition
        if not cond:
            return True
        ts = doc.get(cond.field)
        if ts is None:
            return False
        try:
            ts_val = datetime.fromisoformat(ts)
            return cond.from_ <= ts_val <= cond.to
        except Exception:
            return False

    def _check_search_conditions(self, doc: dict) -> bool:
        conds = self.step.search_conditions
        if not conds:
            return True

        def evaluate(sc: SearchConditions) -> bool:
            results = []
            for c in sc.conditions:
                if isinstance(c, SearchCondition):
                    val = doc.get(c.field)

                    if c.operator in {"exists", "not exists"}:
                        exists = val is not None
                        results.append(exists if c.operator == "exists" else not exists)
                        continue

                    if c.form == "value":
                        cmp_val = c.value
                    elif c.form == "field":
                        cmp_val = doc.get(c.value)
                    else:
                        results.append(False)
                        continue

                    results.append(self._eval_operator(val, c.operator, cmp_val))

                elif isinstance(c, SearchConditions):
                    results.append(evaluate(c))

            return all(results) if sc.logic == "and" else any(results)

        return evaluate(conds)

    def _eval_operator(self, val, op, cmp):
        try:
            if op == "==":
                return val == cmp
            elif op == "!=":
                return val != cmp
            elif op == ">":
                return val > cmp
            elif op == ">=":
                return val >= cmp
            elif op == "<":
                return val < cmp
            elif op == "<=":
                return val <= cmp
        except Exception:
            return False
        return False

    def _check_personalization(self, doc: dict) -> bool:
        filters = self.step.personalization_filters
        if not self.redis or not filters:
            return True

        include_ids = filters.include or []
        for field in self.step.target_fields:
            field_value = doc.get(field)
            if not field_value:
                continue
            for pid in include_ids:
                redis_key = f"p:{pid}:{field_value}"
                if self.redis.exists(redis_key):
                    return True
        return False

    def _evaluate_aggregation(self, docs: List[dict]) -> Optional[dict]:
        base_field = self.step.base_fields[0]
        target_field = self.step.target_fields[0]
        operator = self.step.operator
        threshold = self.step.threshold
        top_n = self.step.top_n

        grouped = defaultdict(list)
        for doc in docs:
            key = doc.get(target_field)
            if key:
                grouped[key].append(doc)

        result = []
        for key, items in grouped.items():
            if operator == "count" and len(items) >= threshold:
                result.append((key, len(items), items))
            elif operator == "sum":
                total = sum(i.get(base_field, 0) for i in items)
                if total >= threshold:
                    result.append((key, total, items))

        result.sort(key=lambda x: x[1], reverse=True)
        top_result = result[:top_n] if top_n else result

        return top_result[0][2][-1] if top_result else None
