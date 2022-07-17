import logging
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, bulk


def connect_elasticsearch():
    client = Elasticsearch(
        "http://localhost:9200",
    )
    client.info()
    if client.ping():
        return client
    return None


def create_test_index(client, index_name):
    request_body = {
        "settings": {
            "number_of_shards": 5,
            "number_of_replicas": 1
        },

        'mappings': {
            'examplecase': {
                'properties': {
                    'address': {'index': 'not_analyzed', 'type': 'string'},
                    'date_of_birth': {'index': 'not_analyzed', 'format': 'dateOptionalTime', 'type': 'date'},
                    'some_PK': {'index': 'not_analyzed', 'type': 'string'},
                    'fave_colour': {'index': 'analyzed', 'type': 'string'},
                    'email_domain': {'index': 'not_analyzed', 'type': 'string'},
                }}}
    }
    print("creating 'example_index' index...")
    client.indices.create(index='example_index', body=request_body)


def create_index(client, index_name):
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
        if not client.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            client.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


class ElasticsearchLoader:
    def __init__(self, **kwargs):
        self.client = connect_elasticsearch()

    def load_data(self, transformed_data, chunk_size):
        if self.client is None:
            self.client = connect_elasticsearch()
        if not self.client.indices.exists(index='movies'):
            create_index(self.client, 'movies')

        streaming_bulk(self.client, 'update', transformed_data, chunk_size, ignore=400, raise_on_error=True)

        try:
            bulks_processed = 0
            not_ok = []
            streaming_bulk(self.client, 'update', transformed_data, chunk_size, raise_on_error=False)

            for cnt, response in enumerate(streaming_bulk(self.client, transformed_data, chunk_size)):
                ok, result = response
                if not ok:
                    not_ok.append(result)
                if cnt % chunk_size == 0:
                    bulks_processed += 1
                self._logger.debug(
                    f"Bulk number {bulks_processed} processed, already processed docs {cnt}.")
                if len(not_ok):
                    self._logger.error(
                        f"NOK DOCUMENTS (log limited to 10) in batch {bulks_processed}: {not_ok[-10:]}")
                    not_ok = []
            self._logger.info(
                f"Refreshing index {es_dataset.es_index_name} to make indexed documents searchable.")
            self.client.indices.refresh(index='movies')
        except:
            pass
        else:
            return cnt + 1
