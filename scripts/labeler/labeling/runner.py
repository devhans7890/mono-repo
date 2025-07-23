from typing import List, Optional
from rule.models import RuleSet
from db.elastic_adapter import ElasticsearchAdapter
from db.redis_adpter import RedisAdapter
from labeling.evaluator import Evaluator
# Runner의 동작 방식
# es adapter
# evaluator
# 1. ES로부터 데이터 호출(전체? 혹은 키별로?)
# 2. ES로부터 가져온 데이터 1개씩 Evaluator로 rule탐지

class Runner:
    def __init__(
        self,
        ruleset: RuleSet,
        es_adapter: ElasticsearchAdapter,
        redis_adapter: RedisAdapter,
        evaluator: Evaluator,
        label_key: str = "lab1",
    ):
        self.ruleset = ruleset
        self.es_adapter = es_adapter
        self.redis_adapter = redis_adapter
        self.evaluator = evaluator
        self.label_key = label_key

    def run(self, target_keys: List[str], basis_field: str):
        for key in target_keys:
            fallback_doc = None
            rule_matched = None

            for rule in self.ruleset.analysis_rules:
                for step in rule.steps:
                    docs = self.es_adapter.query_documents(
                        index_prefix=step.index_prefix,
                        basis_field_value=key,
                        basis_field_name=basis_field
                    )

                    if self.evaluator.evaluate_step(step, docs):
                        rule_matched = rule.id
                        break

                if rule_matched:
                    break

            if rule_matched:
                self.redis_adapter.set_label(self.label_key, key, rule_matched)
            else:
                # DEFAULT RULE 평가 (선택)
                if self.evaluator.evaluate_default(docs):  # docs는 마지막 step 기준
                    self.redis_adapter.set_label("fallback", key, "fallback")
                else:
                    self.redis_adapter.log_unmatched(self.label_key, key)

        self.redis_writer.flush(self.label_key)

