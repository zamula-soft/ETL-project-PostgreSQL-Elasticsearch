# ETL Process должен:
# запускать внутренние компоненты.

from postgres_to_es.extract.extractor import PostgresExtractor
from postgres_to_es.transform.transformer import DataTransform
from postgres_to_es.load.loader import ElasticsearchLoader

PG_BATCH_SIZE = 20
TF_BATCH_SIZE = 5
ES_BATCH_SIZE = 5



def etl_process():

    pg_extractor = PostgresExtractor()
    data_transformer = DataTransform()
    es_loader = ElasticsearchLoader()

    raw_generator = pg_extractor.get_batches(PG_BATCH_SIZE)
    for raw_data in raw_generator:
        transformed_data = data_transformer.transform_data(raw_data, TF_BATCH_SIZE)
        for ts_data in transformed_data:
            es_loader.load_data(transformed_data, ES_BATCH_SIZE)

    # transformer = DataTransform()
    # transformed_data = transformer.transform_data(raw_data, TF_BATCH_SIZE)
    # print(transformed_data)

    # defined_params = {k: v for k, v in kwargs.items() if v is not None}
    # es_host = defined_params.pop("es_host", "localhost:9200")
    # es_loader = ElasticsearchLoader(hosts=[es_host])
    # es_loader = ElasticsearchLoader()
    # es_loader.load_data(transformed_data, ES_BATCH_SIZE)


if __name__ == '__main__':
    etl_process()
