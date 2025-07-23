from elasticsearch import Elasticsearch
from typing import Optional
from logging import Logger
from dateutil.relativedelta import relativedelta
from datetime import datetime
import pandas as pd

def generate_index_candidates(index_prefix, date_pattern, lookback_months, end_date):
    end_date = datetime.strptime(end_date, "%Y.%m.%d")
    start = end_date - relativedelta(months=lookback_months)
    if date_pattern == "%Y":
        dates = pd.date_range(start, end_date, freq="Y").strftime(date_pattern)
    elif date_pattern == "%Y.%m":
        dates = pd.date_range(start, end_date, freq="M").strftime(date_pattern)
    elif date_pattern == "%Y.%m.%d":
        dates = pd.date_range(start, end_date, freq="D").strftime(date_pattern)
    else:
        raise ValueError(f"Unsupported pattern: {date_pattern}")
    return ["{}-{}".format(index_prefix, d) for d in dates]

def filter_existing_indices(
        es_client: Elasticsearch,
        index_names: list[str],
        logger: Optional[Logger] = None) -> list[str]:  # 필터링된 유효 인덱스 목록 반환
    existing = [idx for idx in index_names if es_client.indices.exists(index=idx)]
    if logger:
        logger.info("Existing indices: %s", existing)
    return existing


def get_latest_index_mapping(
        es_client: Elasticsearch,
        index_names: list[str],
        logger: Optional[Logger] = None) -> dict:  # 가장 최신 인덱스의 매핑 반환
    existing = filter_existing_indices(es_client, index_names, logger)
    if not existing:
        raise RuntimeError("No valid indices found")
    latest_index = sorted(existing)[-1]
    if logger:
        logger.info("Using latest index: %s", latest_index)
    mapping = es_client.indices.get_mapping(index=latest_index)
    return mapping[latest_index]["mappings"]

