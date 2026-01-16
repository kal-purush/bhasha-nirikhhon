import json

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError

es = Elasticsearch(hosts=["http://localhost:9200"])


def update_or_insert(body):
    doc_id = body["username"]
    try:
        res = es.get(index="orders", id=doc_id)

        # Update
        updated_body = {
            "script": {
                "source": "ctx._source.orders.add(params.order)",
                "lang": "painless",
                "params": {"order": body["order"]}
            }
        }
        es.update(index="orders", id=doc_id, body=updated_body)
    except NotFoundError:
        res = es.index(index="orders", id=doc_id, body=body)
    return res


if __name__ == "__main__":
    data = {
        "username": "user1",
        "order": {
            "id": "order1",
            "timestamp": "2022-01-01T00:00:00Z",
            "items": [
                {"id": "item1", "name": "item1", "price": 10, "quantity": 1},
                {"id": "item2", "name": "item2", "price": 20, "quantity": 2},
            ],
        },
    }
    print(update_or_insert(data))