# import logging
# import sys
# import json
# import time
#
# from elasticsearch import Elasticsearch
#
# from es_utils.index_utils import generate_index_candidates, filter_existing_indices
# from sampler.stratified_sampler import StratifiedSampler
# from common.config_models import StratifiedSamplerConfig, load_sampling_options, load_stratification
#
#
# def setup_logger(log_config):
#     logger = logging.getLogger("StratifiedSampler")
#     level = getattr(logging, log_config.level.upper(), logging.INFO)
#     logger.setLevel(level)
#     formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
#
#     if "stream" in log_config.handlers:
#         stream_handler = logging.StreamHandler(sys.stdout)
#         stream_handler.setFormatter(formatter)
#         logger.addHandler(stream_handler)
#
#     if "file" in log_config.handlers:
#         file_handler = logging.FileHandler(log_config.log_file, encoding="utf-8")
#         file_handler.setFormatter(formatter)
#         logger.addHandler(file_handler)
#
#     return logger
#
#
# def main():
#     config = load_sampling_options("sampling_options.yaml")
#     strat_config = load_stratification("stratification.yaml")
#     logger = setup_logger(config.logger)
#
#     logger.info("Loaded yaml files.")
#     logger.info("App common:\n%s", config.model_dump_json())
#     logger.info("Stratification common:\n%s", json.dumps(strat_config, ensure_ascii=False))
#
#     sampler_cfg = StratifiedSamplerConfig.from_config(
#         sampling_option=config.sampling_option,
#         stratification=strat_config,
#         target_index=config.index_definition.sampled_raw_index.index_name,
#         target_basis_field=config.index_definition.sampled_raw_index.basis_field
#     )
#
#     index_candidates = generate_index_candidates(
#         config.index_definition.source_index.index_name,
#         config.index_definition.source_index.index_date,
#         config.index_definition.source_index.lookback_months,
#         config.index_definition.source_index.cut_off_date
#     )
#
#     source_es = Elasticsearch(**config.db["analyzer_server_es"].model_dump())
#     target_es = Elasticsearch(**config.db["ai_server_es"].model_dump())
#
#     existing_indices = filter_existing_indices(source_es, index_candidates, logger)
#
#     sampler = StratifiedSampler(
#         source_es=source_es,
#         target_es=target_es,
#         sampler_config=sampler_cfg,
#         index_list=existing_indices,
#         logger=logger
#     )
#     logger.info("Starting stratified sampling run...")
#     tic = time.time()
#     success, failed = sampler.run()
#     toc = time.time()
#     logger.info("Stratified sampling result – Inserted: %d, Skipped (duplicate/failure): %d", success, failed)
#     logger.info("Total sampling run time: %.3f seconds", toc - tic)
#
#
# if __name__ == "__main__":
#     main()
from common.logger.db_logger import DbLogger
from common.logger.sys_logger import SysLogger
import pandas as pd
import yaml
from datetime import datetime
from dateutil.relativedelta import relativedelta
from elasticsearch import Elasticsearch
from tqdm import tqdm
import csv

# ---------- 1. 설정 로딩 ----------
CONFIG_PATH = "/mnt/data/config.yaml"


with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

ES_HOST = config['es']['host']
INDICES_CONFIG = config['indices']
TIMESTAMP_FIELD = config['query']['timestamp_field']
MONTHS_LOOKBACK = config['query']['months_lookback']
OUTPUT_CSV = config['output']['csv_path']

# ---------- 2. 기준 파일 로딩 ----------
basis_path = "/mnt/data/normal_sampled_basis_values.txt"
timestamp_path = "/mnt/data/normal_sampled_timestamp.txt"

basis_values = pd.read_csv(basis_path, header=None, dtype=str, names=["basis_value"])
timestamps = pd.read_csv(timestamp_path, header=None, names=["timestamp"], parse_dates=["timestamp"])
criteria = pd.concat([basis_values, timestamps], axis=1)

# ---------- 3. Elasticsearch 연결 ----------
es = Elasticsearch(ES_HOST)

# ---------- 4. 유틸리티 함수 ----------
def generate_indices(base_ts: datetime, months_lookback: int, index_prefix: str, date_format: str) -> list[str]:
    return [
        f"{index_prefix}-{(base_ts - relativedelta(months=m)).strftime(date_format)}"
        for m in range(1, months_lookback + 1)
    ]

def build_query(field: str, value: str, ts: pd.Timestamp) -> dict:
    start = (ts - relativedelta(months=MONTHS_LOOKBACK)).isoformat()
    end = ts.isoformat()
    return {
        "query": {
            "bool": {
                "must": [
                    {"term": {field: value}},
                    {"range": {TIMESTAMP_FIELD: {"gte": start, "lt": end}}}
                ]
            }
        },
        "size": 10000
    }

# ---------- 5. 데이터 수집 ----------
results = []

for _, row in tqdm(criteria.iterrows(), total=len(criteria)):
    basis_value = row["basis_value"]
    ts = row["timestamp"]

    for idx_cfg in INDICES_CONFIG:
        index_prefix = idx_cfg["index"]
        basis_field = idx_cfg["basis_field"]
        prefix_fmt = idx_cfg["prefix"]

        indices = generate_indices(ts, MONTHS_LOOKBACK, index_prefix, prefix_fmt)

        for index in indices:
            query = build_query(basis_field, basis_value, ts)
            try:
                resp = es.search(index=index, body=query)
                for hit in resp.get("hits", {}).get("hits", []):
                    source = hit["_source"]
                    source["_index"] = index  # 추적용
                    results.append(source)
            except Exception as e:
                print(f"Index query failed: {index} - {e}")

# ---------- 6. CSV 저장 ----------
df_result = pd.DataFrame(results)
df_result.to_csv(
    OUTPUT_CSV,
    index=False,
    quoting=csv.QUOTE_NONNUMERIC  # 문자열은 쌍따옴표, 숫자는 그대로
)
