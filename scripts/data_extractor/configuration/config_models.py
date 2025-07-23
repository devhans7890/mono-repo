import os
from typing import List, Optional, Dict, Any, Set

import yaml
from pydantic import BaseModel, Field, ValidationError


class SourceIndexConfig(BaseModel):
    index_name: str
    index_date: str
    basis_field: str
    lookback_months: int
    cut_off_date: str


class SampledIndexConfig(BaseModel):
    index_name: str
    basis_field: str


class IndexDefinition(BaseModel):
    source_index: SourceIndexConfig
    sampled_raw_index: SampledIndexConfig


class OutputPaths(BaseModel):
    distribution_logs_dir: str
    basis_value_file: str
    timestamp_file: str


class SamplingOption(BaseModel):
    basis_field: str
    timestamp_field: str
    timestamp_field_format: str
    exclude_basis_values_file_path: Optional[str] = Field(None, description="Path to file containing basis values to exclude from sampling")
    transaction_filter: Dict[str, str]
    scroll_time: str
    batch_size: int
    max_scroll_attempts: int
    max_scroll_docs_limit: int
    target_sample_size: int
    output: OutputPaths
    duplicate_check_batch: Optional[int] = 5000

    def load_exclusion_set(self) -> Set[str]:
        exclusion_set = set()
        if self.exclude_basis_values_file_path and os.path.exists(self.exclude_basis_values_file_path):
            with open(self.exclude_basis_values_file_path, "r", encoding="utf-8") as f:
                exclusion_set = {line.strip() for line in f if line.strip()}
        return exclusion_set


class StratifiedSamplerConfig(BaseModel):
    sampling_option: SamplingOption
    stratification: Dict[str, Any]
    target_index: str
    target_basis_field: str
    exclusion_basis_values: Set[str] = Field(
        default_factory=set,
        description="Values to exclude from sampling"
    )

    @classmethod
    def from_config(
        cls,
        sampling_option: SamplingOption,
        stratification: Dict[str, Any],
        target_index: str,
        target_basis_field: str
    ) -> "StratifiedSamplerConfig":
        exclusion_set = sampling_option.load_exclusion_set()
        return cls(
            sampling_option=sampling_option,
            stratification=stratification,
            target_index=target_index,
            target_basis_field=target_basis_field,
            exclusion_basis_values=exclusion_set
        )


class DBConfig(BaseModel):
    class Config:
        extra = "allow"

class ElasticsearchConfig(DBConfig):
    hosts: List[str]


class SamplingOptions(BaseModel):
    db: Dict[str, DBConfig]
    index_definition: IndexDefinition
    sampling_option: SamplingOption


def load_yaml(config_path: str) -> SamplingOptions:
    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    try:
        return SamplingOptions.model_validate(raw)
    except ValidationError as e:
        raise RuntimeError(f"Invalid config file format: {e}")





