from datetime import datetime
from elasticsearch_dsl import Document, Date, Integer, Keyword, Text, Float, Nested
from elasticsearch_dsl.connections import connections

# Define a default Elasticsearch client
connections.create_connection(hosts=['localhost'])

class Movies(Document):
    id = Keyword()
    title = Text(analyzer='snowball', fields={'raw': Keyword()})
    description = Text(analyzer='snowball')
    genre = Keyword()
    published_from = Date()
    imdb_rating = Float()
    director = Text(analyzer='snowball')
    actors_names = Text(analyzer='snowball')
    writers_names = Text(analyzer='snowball')
    actors = Nested()
    writers = Nested()

    class Index:
        name = 'movies'
        settings = {
          "number_of_shards": 1,
        }

    def save(self, ** kwargs):
        self.lines = len(self.body.split())
        return super(Movies, self).save(** kwargs)

    def is_published(self):
        return datetime.now() >= self.published_from

# create the mappings in elasticsearch
Movies.init()

# create and save and article
movie = Movies(meta={'id': 42}, title='Hello world!', tags=['test'])
movie.body = ''' looong text '''
movie.published_from = datetime.now()
movie.save()

movie = Movies.get(id=42)
print(movie.is_published())

# Display cluster health
print(connections.get_connection().cluster.health())