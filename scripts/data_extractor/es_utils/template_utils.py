from elasticsearch import Elasticsearch
from typing import Optional
from logging import Logger


def paste_template(
        source_es_client: Elasticsearch,
        target_es_client: Elasticsearch,
        index_prefix: str,
        mapping: dict,
        shard_num: int = 1,
        replica_num: int = 0,
        logger: Optional[Logger] = None) -> None:  # 기존 매핑 기반 템플릿 생성
    template_name = f"{index_prefix}"    # Dfinder에서 인덱스 prefix 명으로 템플릿을 관리중
    existing_template = source_es_client.indices.get_index_template(name=template_name)
    if existing_template and "index_templates" in existing_template:
        for template in existing_template["index_templates"]:
            if template["name"] == template_name:
                if logger:
                    logger.warning("Index template already exists: %s", template_name)
                return
    template_body = {
        "index_patterns": [f"{index_prefix}-*"],
        "template": {
            "settings": {"number_of_shards": shard_num, "number_of_replicas": replica_num},
            "mappings": mapping
        }
    }
    response = target_es_client.indices.put_index_template(name=template_name, body=template_body)
    if not response.get("acknowledged", False):
        raise RuntimeError(f"Failed to create index template: {template_name}")
    if logger:
        logger.info("Created index template: %s", template_name)
