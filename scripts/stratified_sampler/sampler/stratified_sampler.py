import json
import zipfile
import time
import traceback
import random
from collections import defaultdict
from pathlib import Path
from typing import List, Tuple
from logging import Logger

from elasticsearch import Elasticsearch
import pandas as pd

from common.config_models import StratifiedSamplerConfig
from query.stratified_query_build import build_stratified_query
from es_utils.insertion_utils import safe_bulk_insert


class StratifiedSampler:
    def __init__(self,
                 source_es: Elasticsearch,
                 target_es: Elasticsearch,
                 sampler_config: StratifiedSamplerConfig,
                 index_list: List[str],
                 logger: Logger):

        self.cfg = sampler_config
        self.source_es = source_es
        self.target_es = target_es
        self.indices = index_list
        self.logger = logger

        opt = self.cfg.sampling_option
        self.scroll_time = opt.scroll_time
        self.batch_size = opt.batch_size
        self.max_attempts = opt.max_scroll_attempts
        self.doc_limit = opt.max_scroll_docs_limit
        self.total_sample_size = opt.target_sample_size
        self.dup_check_batch = opt.duplicate_check_batch or 5000
        self.timestamp_field = opt.timestamp_field
        self.timestamp_field_format = opt.timestamp_field_format
        self.transaction_filter = opt.transaction_filter
        self.output_cfg = opt.output
        self.exclude_basis_values = self.cfg.exclusion_basis_values
        self.seen_basis_values = self.exclude_basis_values # 제외할 basis_value 캐시

    def extract_buckets(self, agg_result):
        def walk(buckets, level_keys, prefix=None):
            prefix = prefix or {}
            results = []
            for b in buckets:
                this = prefix.copy()
                this[level_keys[0]] = b["key"]
                this["doc_count"] = b["doc_count"]
                for k, v in b.items():
                    if isinstance(v, dict) and "buckets" in v:
                        results += walk(v["buckets"], level_keys[1:], prefix=this)
                        break
                else:
                    results.append(this)
            return results

        keys = [f"by_{k}" for k in self.cfg.stratification.keys()]
        root_key = keys[0]
        return walk(agg_result[root_key]["buckets"], keys)

    def build_bucket_condition_query(self, bucket):
        must = []
        for key, spec in self.cfg.stratification.items():
            value = bucket[f"by_{key}"]
            field = spec["field"]
            if spec["strat_type"] == "terms":
                must.append({"term": {field: value}})
            elif spec["strat_type"] == "range":
                match = next(r for r in spec["ranges"] if r["key"] == value)
                rng = {}
                if "from" in match:
                    rng["gte"] = match["from"]
                if "to" in match:
                    rng["lt"] = match["to"]
                must.append({"range": {field: rng}})
        if self.transaction_filter:
            for k, v in self.transaction_filter.items():
                must.append({"term": {k: v}})

        return {"size": self.batch_size, "query": {"bool": {"must": must}}}

    def _aggregate_buckets(self, query):
        index_buckets_map = {}
        for index in self.indices:
            self.logger.info(f"Running stratified aggregation for index: {index}")
            try:
                response = self.source_es.search(index=index, body=query)
                agg_result = response["aggregations"]
            except Exception as e:
                self.logger.warning(
                    "Stratified aggregation failed for index '%s'. Error: %s. Query: %s",
                    index, str(e), json.dumps(query, ensure_ascii=False)
                )
                self.logger.debug("Stack trace:\n%s", traceback.format_exc())
                continue

            buckets = self.extract_buckets(agg_result)
            for b in buckets:
                b["index"] = index

            index_buckets_map[index] = buckets
            total_docs = sum(b["doc_count"] for b in buckets)
            self.logger.info("[Index: %s] Total docs: %d", index, total_docs)
            self._log_distribution(index, buckets, total_docs)
        return index_buckets_map

    def run(self):
        self.logger.info("Transaction filter: %s", json.dumps(self.transaction_filter, ensure_ascii=False))
        # 층화추출 쿼리 생성
        try:
            query = build_stratified_query({"stratification": self.cfg.stratification},self.transaction_filter)
        except Exception as e:
            self.logger.error(
                "Failed to build stratified aggregation query. "
                "Invalid stratification common: %s. Error: %s",
                self.cfg.stratification,
                str(e)
            )
            self.logger.debug("Stack trace:\n%s", traceback.format_exc())
            raise

        self.logger.info("Stratified_query: %s", json.dumps(query, ensure_ascii=False))

        # 인덱스 별 집계
        index_buckets_map = self._aggregate_buckets(query)

        basis_ts_log = {}
        total_success, total_failed = 0, 0
        all_buckets = []

        for index, buckets in index_buckets_map.items():
            for b in buckets:
                b["index"] = index
            all_buckets.extend(buckets)
            total_docs = sum(b["doc_count"] for b in buckets)

            self.logger.info("[Index: %s] Total docs: %d", index, total_docs)
            self._log_distribution(index, buckets, total_docs)

            for bucket in buckets:
                quota = self._calculate_sample_quota(bucket["doc_count"], total_docs)
                self.logger.info("Sampling %d documents from bucket: %s", quota,
                                 {k: bucket[k] for k in bucket if k != "doc_count"})
                inserted, failed, ts_log = self._sample_insert_docs_for_bucket(index, bucket, quota)
                total_success += inserted
                total_failed += failed
                basis_ts_log.update(ts_log)

        self._write_total_distribution(all_buckets)
        self.target_es.indices.refresh(index=f"{self.cfg.target_index}-*")

        if self.output_cfg:
            self._write_basis_timestamp_logs(basis_ts_log)

        self.logger.info("Completed sampling for all indices.")
        return total_success, total_failed

    def _sample_insert_docs_for_bucket(self, index: str, bucket: dict, quota: int) -> Tuple[int, int, dict]:
        total_inserted, total_failed = 0, 0
        basis_ts_log = {}

        # 버킷 조건에 맞는 쿼리 생성
        query = self.build_bucket_condition_query(bucket)

        # 스크롤API를 통해 데이터 샘플링
        try:
            response = self.source_es.search(index=index, body=query, scroll=self.scroll_time)
            scroll_id = response.get("_scroll_id")
            if not scroll_id:
                self.logger.warning("Initial scroll failed (no scroll_id) for index %s", index)
                return 0, 0, {}
        except Exception as e:
            self.logger.warning("Initial scroll failed for index %s: %s", index, str(e))
            return 0, 0, {}

        # 추출한 데이터를 target_es에 적재시도
        try:
            attempts = 0
            while attempts < self.max_attempts and total_inserted < quota:
                hits = response.get("hits", {}).get("hits", [])
                if not hits:
                    break
                else:
                    # scroll을 통해 가져온 데이터를 셔플
                    random.shuffle(hits)

                random_shuffled_docs = []
                for doc in hits:
                    basis_val = doc["_source"].get(self.cfg.target_basis_field)
                    if (not basis_val) or (basis_val in self.seen_basis_values):
                        continue
                    self.seen_basis_values.add(basis_val)
                    random_shuffled_docs.append(doc)
                    if len(random_shuffled_docs) >= quota - total_inserted:
                        break

                # random_shuffled_docs의 데이터의 basis_value가 전부 이번 층화 추출에서 한번 이상 샘플링된 상태
                if not random_shuffled_docs:
                    self.logger.debug("No new documents to insert in scroll batch (index=%s)", index)
                    attempts += 1
                    response = self.source_es.scroll(scroll_id=scroll_id, scroll=self.scroll_time)
                    continue

                # random_shuffled_docs의 데이터을 target_es에 적재하려고 시도
                success, failed, success_ids, attempted_insert_count = safe_bulk_insert(
                    self.target_es,
                    self.cfg.target_index,
                    random_shuffled_docs,
                    self.cfg.target_basis_field,
                    self.dup_check_batch,
                    quota - total_inserted,
                    self.logger
                )

                # 한 버킷 내에 self.max_attempts까지의 bulk insert 성공 갯수, 실패 갯수 기록
                total_inserted += success
                total_failed += failed


                if success == 0 and attempted_insert_count > 0:
                    attempts += 1
                    self.logger.info(
                        "Attempt %d: no documents inserted out of %d candidates due to duplication (index=%s, quota_left=%d)",
                        attempts, attempted_insert_count, index, quota - total_inserted
                    )
                else: # success != 0 or attempted_insert_count <= 0
                    attempts += 1
                    self.logger.info(
                        "Attempt %d: inserted %d documents out of %d candidates (index=%s, quota_left=%d)",
                        attempts, success, attempted_insert_count, index, quota - total_inserted
                    )

                if success_ids:
                    for doc in random_shuffled_docs:
                        if doc["_id"] in success_ids:
                            src = doc["_source"]
                            basis_val = src.get(self.cfg.sampling_option.basis_field)
                            ts_val = src.get(self.timestamp_field)
                            if basis_val and ts_val:
                                basis_ts_log[basis_val] = ts_val

                if total_inserted >= quota:
                    break

                response = self.source_es.scroll(scroll_id=scroll_id, scroll=self.scroll_time)
        finally:
            try:
                self.source_es.clear_scroll(scroll_id=scroll_id)
            except Exception:
                self.logger.error("RuntimeError: _sample_insert_docs_for_bucket\n %s", Exception)
                raise RuntimeError

        self.logger.info(
            "Sampling completed for bucket in index %s: %d documents inserted (quota: %d, attempts: %d)",
            index, total_inserted, quota, attempts
        )
        return total_inserted, total_failed, basis_ts_log

    def _calculate_sample_quota(self, bucket_doc_count, total_docs):
        ratio = bucket_doc_count / total_docs if total_docs > 0 else 0
        return max(1, int((self.total_sample_size / len(self.indices)) * ratio))

    def _write_basis_timestamp_logs(self, basis_ts_log: dict):
        basis_value_file_path = Path(self.output_cfg.basis_value_file)
        timestamp_file_path = Path(self.output_cfg.timestamp_file)
        basis_value_file_path.parent.mkdir(parents=True, exist_ok=True)
        timestamp_file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(basis_value_file_path, "w", encoding="utf-8") as f_basis, \
             open(timestamp_file_path, "w", encoding="utf-8") as f_ts:
            for basis_val, ts in basis_ts_log.items():
                f_basis.write(f"{basis_val}\n")
                f_ts.write(f"{ts}\n")

    def _log_distribution(self, index, buckets, total_docs):
        self.logger.info("\n[%s Stratified Bucket Distribution]", index)
        self.logger.info("%-40s | %-10s | %-10s", "Bucket", "Count", "Percent")
        self.logger.info("%s", "-" * 70)
        for b in buckets:
            label = ", ".join(f"{k}={v}" for k, v in b.items() if k != "doc_count")
            count = b["doc_count"]
            percent = (count / total_docs) * 100 if total_docs > 0 else 0
            self.logger.info("%-40s | %10d | %9.2f%%", label, count, percent)

        self.logger.info("\n[Stratification Key Distribution]")
        strat_keys = list(self.cfg.stratification.keys())
        single_key_dist = {key: defaultdict(int) for key in strat_keys}
        for b in buckets:
            for key in strat_keys:
                bucket_key = f"by_{key}"
                single_key_dist[key][b[bucket_key]] += b["doc_count"]

        for key in strat_keys:
            self.logger.info("\n[Distribution by %s]", key)
            self.logger.info("%-30s | %-10s | %-10s", f"{key}_value", "Count", "Percent")
            self.logger.info("%s", "-" * 55)
            for val, count in sorted(single_key_dist[key].items(), key=lambda x: -x[1]):
                percent = (count / total_docs) * 100 if total_docs > 0 else 0
                self.logger.info("%-30s | %10d | %9.2f%%", val, count, percent)
        self.logger.info("\n")

    def _write_total_distribution(self, all_buckets):
        log_dir = getattr(self.output_cfg, "distribution_logs_dir", "output/distribution")
        output_dir = Path(log_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        zip_filename = "summary_{}.zip".format(timestamp)
        zip_path = output_dir / zip_filename

        strat_keys = list(self.cfg.stratification.keys())
        group_records = []
        per_index_group_records = defaultdict(list)
        output_files = []

        for b in all_buckets:
            doc_count = b["doc_count"]
            index_name = b.get("index", "unknown")

            group_entry = {key: b[f"by_{key}"] for key in strat_keys}
            group_entry.update({"index": index_name, "doc_count": doc_count})

            group_records.append(group_entry)
            per_index_group_records[index_name].append(group_entry)

        df = pd.DataFrame(group_records)
        df["doc_count"] = df["doc_count"].astype(int)

        # 전체 stratified group 집계
        total_doc_count = df["doc_count"].sum()
        grouped = df.groupby(strat_keys + ["index"], as_index=False)["doc_count"].sum()
        grouped_total = df.groupby(strat_keys, as_index=False)["doc_count"].sum()
        grouped_total["percent"] = (grouped_total["doc_count"] / total_doc_count * 100).round(2)
        grouped_total.to_csv(output_dir / "stratified_group_distribution_full.csv", index=False, encoding="utf-8")
        output_files.append(output_dir / "stratified_group_distribution_full.csv")

        # 인덱스별 전체 조합 분포
        grouped_idx = grouped.copy()
        index_totals = grouped_idx.groupby("index")["doc_count"].sum().rename("total")
        grouped_idx = grouped_idx.merge(index_totals, on="index")
        grouped_idx["percent"] = (grouped_idx["doc_count"] / grouped_idx["total"] * 100).round(2)
        grouped_idx.drop(columns=["total"], inplace=True)
        index_pivot = grouped_idx.pivot_table(index="index", columns=strat_keys, values=["percent", "doc_count"])
        index_pivot.to_csv(output_dir / "stratified_group_distribution_by_index.csv", encoding="utf-8")
        output_files.append(output_dir / "stratified_group_distribution_by_index.csv")

        # strat key 단일 기준별 집계
        for key in strat_keys:
            df_key = df.groupby(["index", key], as_index=False)["doc_count"].sum()
            df_key_total = df.groupby([key], as_index=False)["doc_count"].sum()

            index_totals = df.groupby("index")["doc_count"].sum().rename("total")
            df_key = df_key.merge(index_totals, on="index")
            df_key["percent"] = (df_key["doc_count"] / df_key["total"] * 100).round(2)
            df_key.drop(columns=["total"], inplace=True)

            df_key_pivot = df_key.pivot(index="index", columns=key, values=["percent", "doc_count"])
            key_index_file = output_dir / f"distribution_by_{key}_per_index.csv"
            df_key_pivot.to_csv(key_index_file, encoding="utf-8")
            output_files.append(key_index_file)

            df_key_total["percent"] = (df_key_total["doc_count"] / total_doc_count * 100).round(2)
            df_key_total.to_csv(output_dir / f"distribution_by_{key}_total.csv", index=False, encoding="utf-8")
            output_files.append(output_dir / f"distribution_by_{key}_total.csv")

        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for f in output_files:
                zipf.write(f, arcname=f.name)

        self.logger.info("Stratified distribution CSV files written and zipped to %s", zip_path.resolve())
