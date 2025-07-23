from elasticsearch.helpers import bulk
from datetime import datetime


def get_duplicated_ids(es_client, index_prefix, document_ids, batch_size, logger):
    existing_ids = set()
    for i in range(0, len(document_ids), batch_size):
        batch_ids = document_ids[i:i + batch_size]
        query = {
            "query": {"terms": {"@id": batch_ids}},
            "_source": ["@id"]
        }
        try:
            response = es_client.search(index=f"{index_prefix}-*", body=query, size=len(batch_ids))
            hits = response.get("hits", {}).get("hits", [])
            existing_ids.update(hit["_source"]["@id"] for hit in hits)
        except Exception as e:
            logger.error("Duplicate check error: %s", str(e))
    return list(existing_ids)


def get_duplicated_basis_field(es_client, index_prefix, basis_field, basis_field_val_list, batch_size, logger):
    existing_basis_field = set()
    for i in range(0, len(basis_field_val_list), batch_size):
        batch_basis_field_val_list = basis_field_val_list[i:i + batch_size]
        query = {
            "query": {"terms": {"basis_field": batch_basis_field_val_list}},
            "_source": ["@id"]
        }
        try:
            response = es_client.search(index=f"{index_prefix}-*", body=query, size=len(batch_basis_field_val_list))
            hits = response.get("hits", {}).get("hits", [])
            existing_basis_field.update(hit["_source"][basis_field] for hit in hits)
        except Exception as e:
            logger.error("Duplicate check error: %s", str(e))
    return list(existing_basis_field)


def safe_bulk_insert(
        es_client,
        index_prefix,
        documents,
        basis_field,
        duplicate_check_batch_size,
        target_sample_size,
        logger):

    candidate_docs = []

    # 이미 documents는 랜덤 셔플된 상태
    for doc in documents:
        doc_id = doc.get("_id")
        basis_val = doc.get("_source", {}).get(basis_field)
        if basis_val is None:
            logger.warning("Missing basis field [%s] in document id=%s", basis_field, doc_id)
            continue
        candidate_docs.append(doc)
        if len(candidate_docs) >= target_sample_size:
            break

    if not candidate_docs:
        logger.warning("No insertable documents found after deduplication.")
        return 0, len(documents), [], 0

    doc_ids = [doc["_id"] for doc in candidate_docs]
    basis_vals = [doc["_source"][basis_field] for doc in candidate_docs]

    duplicated_ids = set(
        get_duplicated_ids(es_client, index_prefix, doc_ids, duplicate_check_batch_size, logger)
    )
    duplicated_basis_field_values = set(
        get_duplicated_basis_field(es_client, index_prefix, basis_field, basis_vals, duplicate_check_batch_size, logger)
    )

    actions = []
    action_id_map = {}
    duplicated_doc_cnt = 0
    insert_attempted_doc_cnt = 0

    for doc in candidate_docs:
        insert_attempted_doc_cnt += 1
        doc_id = doc["_id"]
        source = doc["_source"]
        basis_val = source.get(basis_field)
        if doc_id in duplicated_ids or basis_val in duplicated_basis_field_values:
            duplicated_doc_cnt += 1
            continue
        index_suffix = source.get("@index_day", datetime.today().strftime("%Y.%m"))
        index_name = f"{index_prefix}-{index_suffix}"
        action = {
            "_index": index_name,
            "_id": doc_id,
            "_source": source
        }
        actions.append(action)
        action_id_map[doc_id] = action
        if len(actions) >= target_sample_size:
            break

    if not actions:
        logger.info("All documents were duplicates after checking %d candidates.", insert_attempted_doc_cnt)
        return 0, duplicated_doc_cnt, [], insert_attempted_doc_cnt

    try:
        success_count, bulk_failed_response = bulk(es_client, actions, raise_on_error=False)

        failed_ids = {
            item['index']['_id'] for item in bulk_failed_response
            if 'index' in item and item['index'].get('status', 200) >= 300
        }
        success_ids = [doc_id for doc_id in action_id_map.keys() if doc_id not in failed_ids]

        failed_count = len(failed_ids) + duplicated_doc_cnt
        logger.info(
            "Inserted %d documents, %d duplicates (including %d bulk failures) from %d insert_attempted_doc_cnt candidates.",
            len(success_ids), failed_count, len(failed_ids), insert_attempted_doc_cnt
        )

        return len(success_ids), failed_count, success_ids, insert_attempted_doc_cnt

    except Exception as e:
        logger.error("Bulk insert failed: %s", str(e))
        return 0, len(actions), [], insert_attempted_doc_cnt