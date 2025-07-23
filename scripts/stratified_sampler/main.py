import logging
import sys
import json
import time

from elasticsearch import Elasticsearch

from es_utils.index_utils import generate_index_candidates, filter_existing_indices
from sampler.stratified_sampler import StratifiedSampler
from common.config_models import StratifiedSamplerConfig, load_sampling_options, load_stratification


def setup_logger(log_config):
    logger = logging.getLogger("StratifiedSampler")
    level = getattr(logging, log_config.level.upper(), logging.INFO)
    logger.setLevel(level)
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")

    if "stream" in log_config.handlers:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    if "file" in log_config.handlers:
        file_handler = logging.FileHandler(log_config.log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def main():
    config = load_sampling_options("sampling_options.yaml")
    strat_config = load_stratification("stratification.yaml")
    logger = setup_logger(config.logger)

    logger.info("Loaded yaml files.")
    logger.info("App common:\n%s", config.model_dump_json())
    logger.info("Stratification common:\n%s", json.dumps(strat_config, ensure_ascii=False))

    sampler_cfg = StratifiedSamplerConfig.from_config(
        sampling_option=config.sampling_option,
        stratification=strat_config,
        target_index=config.index_definition.sampled_raw_index.index_name,
        target_basis_field=config.index_definition.sampled_raw_index.basis_field
    )

    index_candidates = generate_index_candidates(
        config.index_definition.source_index.index_name,
        config.index_definition.source_index.index_date,
        config.index_definition.source_index.lookback_months,
        config.index_definition.source_index.cut_off_date
    )

    source_es = Elasticsearch(**config.db["analyzer_server_es"].model_dump())
    target_es = Elasticsearch(**config.db["ai_server_es"].model_dump())

    existing_indices = filter_existing_indices(source_es, index_candidates, logger)

    sampler = StratifiedSampler(
        source_es=source_es,
        target_es=target_es,
        sampler_config=sampler_cfg,
        index_list=existing_indices,
        logger=logger
    )
    logger.info("Starting stratified sampling run...")
    tic = time.time()
    success, failed = sampler.run()
    toc = time.time()
    logger.info("Stratified sampling result – Inserted: %d, Skipped (duplicate/failure): %d", success, failed)
    logger.info("Total sampling run time: %.3f seconds", toc - tic)


if __name__ == "__main__":
    main()
