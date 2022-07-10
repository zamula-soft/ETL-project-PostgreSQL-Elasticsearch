import logging

import elasticsearch
from elasticsearch import Elasticsearch
# from backoff import backoff
from elasticsearch.helpers import streaming_bulk


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Connected')
    else:
        print('It could not connect!')
    return _es

def create_index(es_object, index_name):
    created = False
    # index settings
    settings = {
        "settings": {
            "refresh_interval": "1s",
            "analysis":
            {
                "filter": {
                    "english_stop": {
                        "type": "stop",
                        "stopwords": "_english_"
                    },
                    "english_stemmer": {
                        "type": "stemmer",
                        "language": "english"
                    },
                    "english_possessive_stemmer": {
                        "type": "stemmer",
                        "language": "possessive_english"
                    },
                    "russian_stop": {
                        "type": "stop",
                        "stopwords": "_russian_"
                    },
                    "russian_stemmer": {
                        "type": "stemmer",
                        "language": "russian"
                    }
                },
                "analyzer": {
                    "ru_en": {
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "english_stop",
                            "english_stemmer",
                            "english_possessive_stemmer",
                            "russian_stop",
                            "russian_stemmer"
                        ]
                    }
                }
            },

            "mappings": {
                "dynamic": "strict",
                "properties": {
                    "id": {
                        "type": "keyword"
                    },
                    "imdb_rating": {
                        "type": "float"
                    },
                    "genre": {
                        "type": "keyword"
                    },
                    "title": {
                        "type": "text",
                        "analyzer": "ru_en",
                        "fields": {
                            "raw": {
                                "type": "keyword"
                            }
                        }
                    },
                    "description": {
                        "type": "text",
                        "analyzer": "ru_en"
                    },
                    "director": {
                        "type": "text",
                        "analyzer": "ru_en"
                    },
                    "actors_names": {
                        "type": "text",
                        "analyzer": "ru_en"
                    },
                    "writers_names": {
                        "type": "text",
                        "analyzer": "ru_en"
                    },
                    "actors": {
                        "type": "nested",
                        "dynamic": "strict",
                        "properties": {
                            "id": {
                                "type": "keyword"
                            },
                            "name": {
                                "type": "text",
                                "analyzer": "ru_en"
                            }
                        }
                    },
                    "writers": {
                        "type": "nested",
                        "dynamic": "strict",
                        "properties": {
                            "id": {
                                "type": "keyword"
                            },
                            "name": {
                                "type": "text",
                                "analyzer": "ru_en"
                            }
                        }
                    }
                }
            }
        }
    }
    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created

# if __name__ == '__main__':
#   logging.basicConfig(level=logging.ERROR)


class ElasticsearchLoader:
    def __init__(self, **kwargs):
        es = connect_elasticsearch()
        self.client = es
        #elasticsearch.Elasticsearch(**kwargs)
        pass

    def load_data(self, es_dataset, chunk_size):
        try:
            create_index(es_dataset, es_dataset.index_name)
            bulks_processed = 0
            not_ok = []
            # generator = self._ndjson_generator(es_dataset)
            for cnt, response in enumerate(streaming_bulk(self.client, es_dataset, chunk_size)):
                ok, result = response
                if not ok:
                    not_ok.append(result)
                if cnt % chunk_size == 0:
                    bulks_processed += 1
                # self._logger.debug(
                #     f"Bulk number {bulks_processed} processed, already processed docs {cnt}.")
                if len(not_ok):
                    # self._logger.error(
                    #     f"NOK DOCUMENTS (log limited to 10) in batch {bulks_processed}: {not_ok[-10:]}")
                    not_ok = []
        # self._logger.info(
        #     f"Refreshing index {es_dataset.es_index_name} to make indexed documents searchable.")
            self.client.indices.refresh(index=es_dataset.es_index_name)
        except:
            pass
        else:
            return cnt + 1


# Load должен:
# загружать данные пачками;
# без потерь переживать падение Elasticsearch;
# принимать/формировать поле, которое будет считаться id в Elasticsearch.
# import logging
# from contextlib import contextmanager
# from typing import List
# from elasticsearch import Elasticsearch, helpers
# from backoff import backoff
#
#
# s = Elasticsearch(es_host, verify_certs=False)


# https://practicum.yandex.ru/trainer/middle-python/lesson/3a786e5e-a9d4-41f2-b511-1aafa15da954/
