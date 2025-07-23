def build_stratified_query(strat_config, transaction_filter):
    filter_clauses = [{"term": {k: v}} for k, v in transaction_filter.items()]
    return {
        "query": {"bool": {"must": filter_clauses}},
        "size": 0,
        "aggs": build_stratified_aggs(strat_config)
    }


def build_stratified_aggs(strat_config):
    levels = [
        {"name": f"by_{name}", **spec}
        for name, spec in strat_config["stratification"].items()
    ]
    return build_aggs(levels)


def build_aggs(levels):
    if not levels:
        return {}

    current = levels[0]
    name = current["name"]
    field = current["field"]
    strat_type = current["strat_type"]
    mapping = current.get("mapping")

    if strat_type == "range":
        if mapping not in ("long", "integer", "double", "float"):
            # todo
            # 실제로는 더 많은 인덱스 매핑 타입을 지원할 수 있으며 필요시 확장예정
            # 다만, 위의 매핑타입 외엔 집계에 대한 부하가 고려가 되지 않았음.
            # 날짜 필드에 대한 시간집계 같은 경우 range로 나누면 의미있을 것으로 예상 됨.
            # 다만 @timestamp를 range를 통해 층화하지 말고, logstash 파이프라인에서 시간 단위(0~23) 필드를 생성해서
            # integer에 대한 range를 하는게 부하를 확연히 줄일 수 있을 것.
            raise ValueError(f"Field '{field}' must be numeric for range aggregation.")
        return {
            name: {
                "range": {
                    "field": field,
                    "ranges": current["ranges"]
                },
                "aggs": build_aggs(levels[1:])
            }
        }

    elif strat_type == "terms":
        if mapping not in ("keyword", "text", "long", "integer", "double", "float"):
            raise ValueError(f"Field '{field}' must be 'keyword', 'text', or numeric for terms aggregation.")

        # text는 keyword 서브필드를 사용하고, 숫자는 그대로 사용
        if mapping == "text":
            field_name = field + ".keyword"
        else:
            field_name = field

        terms_agg = {
            "field": field_name,
            "size": current.get("size", 10),
            "min_doc_count": current.get("min_doc_count", 1)
        }
        if "missing" in current:
            terms_agg["missing"] = current["missing"]

        return {
            name: {
                "terms": terms_agg,
                "aggs": build_aggs(levels[1:])
            }
        }
    else:
        raise ValueError(f"Unsupported stratification type: {strat_type}")
