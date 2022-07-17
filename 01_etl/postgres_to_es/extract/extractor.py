# # # # Extract должен:
# # # # читать данные пачками;
# # # # спокойно переживать падение PostgreSQL;
# # # # начинать читать с последней обработанной записи.
# # #
# # #
# # # Чтение пачки данных вместо всего набора позволяет не потреблять много ресурсов у PostgreSQL и Elasticsearch.
# # # Также это позволит не читать данные с самого начала, а вычитывать только то, что изменилось.
# # # Размер пачки лучше сделать конфигурируемым для поиска подходящего объёма пачки данных.
# #
# #
# # Если какая-то из баз данных упадёт или не будет доступна по сети, то процесс дождётся, когда база поднимется,
# # и продолжит обработку с того места, где произошла проблема. При этом не будет спамить запросами раз в секунду,
# # а будет увеличивать интервал опроса баз данных.
#
#
# Меняем запись в таблице film_work. Изменение требует обновить только один фильм.
# Тут всё просто: так как любое изменение фильма должно сопровождаться актуализацией поля modified,
# то можно завязаться на него и вычитывать все фильмы, отсортировав по этому полю.

# Меняем запись в таблице person. Изменение затрагивает уже все фильмы,
# в которых этот человек был сценаристом, актёром или режиссёром.
# Это заставляет задуматься об отдельном процессе загрузки людей, отличном от загрузки кинопроизведений.

# Меняем запись в таблице genre. Любое исправление жанра может отразиться на большом количестве фильмов и сериалов,
# поэтому надо организовать загрузку пачками. Сами жанры можно вычитывать из БД по полю modified.
# Также стоит сделать отдельный процесс по загрузке данных при изменении жанров.
import os
import logging
from dotenv import load_dotenv

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
from postgres_to_es.state.state import State, JsonFileStorage

PG_SQL = """SELECT
   fw.id,
   fw.title,
   fw.description,
   fw.rating,
   fw.type,
   fw.created,
   fw.modified,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'person_role', pfw.role,
               'person_id', p.id,
               'person_name', p.full_name
           )
       ) FILTER (WHERE p.id is not null),
       '[]'
   ) as persons,
   array_agg(DISTINCT g.name) as genres
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE fw.modified > %(state)s
GROUP BY fw.id
ORDER BY fw.modified
LIMIT 100;
"""


def connect():
    load_dotenv()
    dbname = os.environ.get('PSQL_DBNAME')
    user = os.environ.get('PSQL_USER')
    password = os.environ.get('PSQL_PASSWORD')
    host = os.environ.get('PSQL_HOST')
    port = os.environ.get('PSQL_PORT')
    dsl = {'dbname': dbname, 'user': user, 'password': password, 'host': host, 'port': int(port)}

    return psycopg2.connect(**dsl, cursor_factory=DictCursor)


class PostgresExtractor:
    def __init__(self):
        pg_conn = connect()
        self.pg_conn = pg_conn
        self.cursor = self.pg_conn.cursor()  # cursor_factory=DictCursor)

    def get_state(self):
        file_path = os.environ.get('FILE_PATH')
        js_storage = JsonFileStorage(file_path)
        state = State(js_storage)
        str_state = {'state': state.get_state('modified')}
        return str_state

    def get_batches(self, batch_size):
        with self.cursor as cursor:
            stmt = sql.SQL(PG_SQL)
            cursor.execute(stmt, self.get_state())
            while True:
                records = cursor.fetchmany(batch_size)
                if not records:
                    break
                yield list(records)



