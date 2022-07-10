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
        for row in raw_data:
            data =[]
            persons = row['persons']
            director = get_director(persons)
            actor_names = get_actors_names(persons)
            writers_name = get_writers_names(persons)
            actors = get_actors(persons)
            writers = get_writers(persons)
            item = {'_op_type': 'update', '_index': self.index_name, #'_type': '_doc',
                    'source': {
                        '_id': row['id'], 'imdb_rating': row['rating'],
                        'genre': row['genres'], 'title': row['title'], 'description': row['description'],
                        'director': director, 'actors_names': actor_names,
                        'writers_names': writers_name, 'actors': actors, 'writers': writers}}
            data.append(item)
            json_data = json.dumps(item)
            yield json_data
