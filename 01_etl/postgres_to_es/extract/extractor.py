import os
import logging
from contextlib import closing
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor

from postgres_to_es.config.settings import Settings
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
WHERE fw.modified > %(state)s OR pfw.modified > %(state)s OR gfw.modified > %(state)s 
GROUP BY fw.id
ORDER BY fw.modified
LIMIT 100;
"""


def connect(settings):
    dbname = settings.psql_dbname
    user = settings.psql_user
    password = settings.psql_password
    host = settings.psql_host
    port = settings.psql_port
    dsl = {'dbname': dbname, 'user': user, 'password': password, 'host': host, 'port': int(port)}

    return psycopg2.connect(**dsl, cursor_factory=DictCursor)


class PostgresExtractor:
    def __init__(self):
        settings = Settings()
        pg_conn = connect(settings)
        self.pg_conn = pg_conn
        self.file_path = settings.file_path

    def get_state(self):
        js_storage = JsonFileStorage(self.file_path)
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
