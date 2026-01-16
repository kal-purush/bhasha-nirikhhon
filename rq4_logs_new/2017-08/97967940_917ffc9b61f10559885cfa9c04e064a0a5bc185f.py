from datetime import datetime
from elasticsearch import Elasticsearch

import logging
logger = logging.getLogger()
logging.basicConfig()
#logger.setLevel(logging.DEBUG)


es = Elasticsearch()

if False:
    if es.indices.exists('test'):
        es.indices.delete(index='test')
    print es.indices.create('test', body={
        'settings': {
            'index': {
                'refresh_interval': '5s',
            },
        },
        'mappings': {
            'domain': {
                "_all":            { "enabled": False },
                "_source":         { "enabled": False },
                'properties': {
                    '@timestamp': {
                        'type': 'date',
                    },
                    'domain': {
                        "index": "not_analyzed",
                        'type': 'keyword',
                    }
                }
            }
        },
    })

from elasticsearch.helpers import bulk

#print es.index(index='test', doc_type='domain', body=[action2])
#es.bulk(index='test', doc_type='domain', body=[action2])
import time
import random
while False:
    print datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    b = []
    for i in range(100):
        b.append({
            "_index": "test",
            "_type": "domain",
            "_source": {
                '@timestamp': datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                'domain': 'example{}.com'.format(random.randint(1,10))
            }
        })
    print bulk(es, b)
    time.sleep(1)


from pprint import pprint
pprint (es.search(
    index='test',
    doc_type='domain',
    body={
        'query': {
        },
        'aggs': {
            'total': {
                'date_histogram': {
                    'interval': '1m',
                    'field': '@timestamp'
                },
                'aggs': {
                    'dt': {
                        'terms': {
                            'field': 'domain',
                            "size": 2,
                            "order":{"_count":"desc"}
                        },
                    }
                }
            }
        }
    }
))

#{"index":["test"],"ignore_unavailable":true,"preference":1501620080720}
#{"size":0,"query":{"bool":{"must":[{"query_string":{"query":"*","analyze_wildcard":true}},{"range":{"@timestamp":{"gte":1501619433173,"lte":1501620333174,"format":"epoch_millis"}}}],"must_not":[]}},"_source":{"excludes":[]},"aggs":{"2":{"date_histogram":{"field":"@timestamp","interval":"1m","time_zone":"America/Santiago","min_doc_count":1},"aggs":{"3":{"terms":{"field":"domain","size":5,"order":{"_count":"desc"}}}}}}}

#{"search_type":"query_then_fetch","ignore_unavailable":true,"index":"test"}
#{"size":0,"query":{"bool":{"filter":[{"range":{"@timestamp":{"gte":"1501618077324","lte":"1501618257324","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"*"}}]}},"aggs":{"8":{"terms":{"field":"domain","size":500,"order":{"_term":"desc"},"min_doc_count":1},"aggs":{"9":{"date_histogram":{"interval":"1m","field":"@timestamp","min_doc_count":0,"extended_bounds":{"min":"1501618077324","max":"1501618257324"},"format":"epoch_millis"},"aggs":{}}}}}}