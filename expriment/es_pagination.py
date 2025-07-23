# scroll api와 비교했을 때 scroll은 너무 메모리를 많이 쓰므로
# 데이터의 snapshot이 필요하지 않다면 pagination으로 처리하는게 옳다.
# 이는 elasticsearch에서 권장하는 방법

from typing import Iterator, Optional, List
from elasticsearch import Elasticsearch

class ElasticsearchAdapter:
    def __init__(self, es_config):
        self.es = Elasticsearch(**es_config)

    def search_after_query(
            self,
            index_pattern: str,
            query: dict,
            sort: List[dict],
            size: int = 1000,
            source_includes: Optional[List[str]] = None,
    ) -> Iterator[dict]:
        try:
            if self.es.ping():
                print("Elasticsearch 연결 성공")
            else:
                print("Elasticsearch 연결 실패 (ping 실패)")
        except ConnectionError as e:
            print("연결 오류:", str(e))
        except Exception as e:
            print("예기치 못한 오류:", str(e))

        search_after = None
        while True:
            body = {
                "query": query,
                "size": size,
                "sort": sort
            }
            if source_includes:
                body["_source"] = {"includes": source_includes}
            if search_after:
                body["search_after"] = search_after

            response = self.es.search(index=index_pattern, body=body)
            hits = response["hits"]["hits"]
            if not hits:
                return

            for hit in hits:
                yield hit

            search_after = hits[-1]["sort"]

es_config = {
    "hosts": ["http://192.168.124.240:39202"],
    "request_timeout":60,
    "maxsize":25,
    "retry_on_timeout":True
}

adapter = ElasticsearchAdapter(es_config)
# 쿼리는 적당하게 준비
query = {
    "bool": {
        "must": [
            {
                "range": {
                    "@timestamp": {
                        "gte": "2024-01-01T00:00:00",
                        "lte": "2025-01-01T00:00:00"
                    }
                }
            },
            {
                "term": {
                    "subCategory": "로그인"
                }
            }
        ]
    }
}
# query = {"match_all":{}}
sort = [{"@timestamp": "asc"}, {"@id": "asc"}]

# adapter.interactive_search_after_query("event_response-*,audit-*", query, sort)


gen = adapter.search_after_query(
    index_pattern="event_response-*,audit-*",
    query=query,
    sort=sort,
    size=1000
)

while True:
    hit = next(gen, None)
    if hit is None:
        print("모든 문서를 처리했습니다.")
        break
    print(hit)

