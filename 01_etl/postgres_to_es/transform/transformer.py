import json
import uuid
from dataclasses import dataclass, field
from datetime import date
from typing import Optional


@dataclass
class Movies:
    title: str
    description: str
    creation_date: date
    rating: float = field(default=0.0)
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    genres: list[str] | None = None
    director: list[str] | None = None
    actor_names: list[str] | None = None
    writers_names: list[str] | None = None
    actors: list[str] | None = None
    writers: list[str] | None = None


@dataclass()
class Persons:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    full_name: str | None = None
    role: str | None = None


def get_director(persons: Persons) -> str:
    for person in persons:
        if person['person_role'] == 'director':
            return person['person_name']
    return None


def get_actors_names(persons: Persons) -> str:
    result = ""
    for person in persons:
        if person['person_role'] == 'actor':
            result += person['person_name'] + ", "
    return None if result == "" else result


def get_actors(persons: Persons) -> dict:
    result = []
    for person in persons:
        if person['person_role'] == 'actor':
            d_item = {}
            d_item['id'] = person['person_id']
            d_item['name'] = person['person_name']
            result.append(d_item)

    return None if result == [] else result


def get_writers_names(persons: Persons) -> str:
    result = ""
    for person in persons:
        if person['person_role'] == 'writer':
            result += person['person_name'] + ", "
    return None if result == "" else result


def get_writers(persons: Persons) -> dict:
    result = []
    for person in persons:
        if person['person_role'] == 'writer':
            d_item = {}
            d_item['id'] = person['person_id']
            d_item['name'] = person['person_name']
            result.append(d_item)

    return None if result == [] else result


class DataTransform:
    def __init__(self):
        self.index_name = 'movies'

    def transform_data(self, raw_data: Movies, batch_size: int):
<<<<<<< HEAD
        for row in raw_data:
            data =[]
=======
        data = []
        for row in raw_data:
>>>>>>> origin/main
            persons = row['persons']
            director = get_director(persons)
            actor_names = get_actors_names(persons)
            writers_name = get_writers_names(persons)
            actors = get_actors(persons)
            writers = get_writers(persons)
<<<<<<< HEAD
            item = {'_op_type': 'update', '_index': self.index_name, #'_type': '_doc',
                    'source': {
                        '_id': row['id'], 'imdb_rating': row['rating'],
                        'genre': row['genres'], 'title': row['title'], 'description': row['description'],
                        'director': director, 'actors_names': actor_names,
                        'writers_names': writers_name, 'actors': actors, 'writers': writers}}
            data.append(item)
            json_data = json.dumps(item)
            yield json_data
=======
            item = {'_index': self.index_name, '_type': '_doc',
                    '_id': row['id'], 'imdb_rating': row['rating'],
                    'genre': row['genres'], 'title': row['title'], 'description': row['description'],
                    'director': director, 'actors_names': actor_names,
                    'writers_names': writers_name, 'actors': actors, 'writers': writers}
            data.append(item)
        json_data = json.dumps(data)
        return json_data

# from pydantic import BaseModel, BaseSettings, Field, FilePath
#
# class PostgresConnection(BaseModel):
#     dbname: str = Field("movies_database", env='DB_NAME')
#     user: str = Field("app", env='DB_USER')
#     password: str = Field("123qwe", env='DB_PASSWORD')
#     host: str = Field("192.168.248.132", env='DB_HOST')
#     port: int = Field(5432, env='DB_PORT')
#
#
# class ElasticConnection(BaseModel):
#     es_host: str = Field("192.168.248.132", env='ELASTIC_HOST')
#     es_port: str = Field("9200", env='ELASTIC_PORT')
#     es_uri: str = Field("http://192.168.248.132:9200", env='ELASTIC_URI')
#
#
# class Etl(BaseModel):
#     state_file: FilePath = Field("etl/state.json", env="FILE_STATE_PATH")
#
#
# class Settings(BaseSettings):
#     postgres_conn: PostgresConnection = PostgresConnection()
#     elastic_conn: ElasticConnection = ElasticConnection()
#     etl_state_file_path: Etl = Etl()
#
#     class Config:
#         env_file = 'etl/.env'
#         env_file_encoding = 'utf-8'
#         case_sensitive = True
>>>>>>> origin/main
