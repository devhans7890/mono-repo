import os
from typing import List, Optional, Dict, Any, Set

import yaml
from pydantic import BaseModel, Field, ValidationError


class ElasticsearchConfig(BaseModel):
    """
    Parameters:
    hosts – list of nodes, or a single node, we should connect to.
    Node should be a dictionary ({“host”: “localhost”, “port”: 9200}),
    the entire dictionary will be passed to the Connection class as kwargs,
    or a string in the format of host[:port] which will be translated to a dictionary automatically.
    If no value is given the Connection class defaults will be used.
    transport_class – Transport subclass to use.
    kwargs – any additional arguments will be passed on to the Transport class and, subsequently, to the Connection instances.
    """
    class Config:
        extra = "allow"


class LoggerConfig(BaseModel):
    level: str = "INFO"
    handlers: List[str] = ["stream"]
    log_file: Optional[str] = "../sampling.log"


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
    transaction_filter: Optional[Dict[str, Any]] = Field(
        default=None, description="Optional transaction filter conditions"
    )
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

class SamplingOptions(BaseModel):
    logger: LoggerConfig
    db: Dict[str, ElasticsearchConfig]
    index_definition: IndexDefinition
    sampling_option: SamplingOption


def load_sampling_options(config_path: str) -> SamplingOptions:
    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    try:
        return SamplingOptions.model_validate(raw)
    except ValidationError as e:
        raise RuntimeError(f"Invalid config file format: {e}")


def load_stratification(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
