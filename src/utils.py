from SPARQLWrapper import JSON

class Config:
    PREFIXES = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX movie: <http://data.linkedmdb.org/resource/movie/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        """
    LMDB_PAGE_SIZE = 100
    FREEBASE_PAGE_SIZE = 250
    
    FREEBASE_DEFAULT_DELAY = 60
    
    GOOGLE_API_KEY = ""
    
    LMDB_FREEBASE_ACTORS_FILE = '../out/lmdb_freebase_actors'
    LMDB_FREEBASE_FILMS_FILE = '../out/lmdb_freebase_films' # with actors
    LMDB_FREEBASE_FILMS_TMPFILE = '../out/lmdb__freebase_films.tmp' # without actors
    FREEBASE_LMDB_ACTORS_FILE = '../out/freebase_lmdb_actors'
    FREEBASE_LMDB_FILMS_FILE = '../out/freebase_lmdb_films'
    FREEBASE_IMDB_FILMS_FILE = '../out/freebase_imdb_films'
    FREEBASE_IMDB_ACTORS_FILE = '..out/freebase_imdb_actors'

    FREEBASE_LMDB_ACTOR_MAPPING_FILE = '../out/freebase_lmdb_actor_mapping'
    FREEBASE_LMDB_FILM_MAPPING_FILE = '../out/freebase_lmdb_film_mapping'
    FREEBASE_IMDB_ACTOR_MAPPING_FILE = '../out/freebase_imdb_actor_mapping'
    FREEBASE_IMDB_FILM_MAPPING_FILE = '../out/freebase_imdb_film_mapping'

class SPARQLEndpoints:
    LMDB = "http://data.linkedmdb.org/sparql"    

class Portal:
    IMDB = "http://www.imdb.com"
    FREEBASE = "http://www.freebase.com"
    ROTTEN_TOMATOES = "http://www.rottentomatoes.com"

class LMDBMovieConcept:
    FILM                = "film"
    ACTOR               = "actor"
    DIRECTOR            = "director"
    WRITER              = "writer"
    PRODUCER            = "producer"
    MUSIC_CONTRIBUTOR   = "music_contributor"
    CINEMATOGRAPHER     = "cinematographer"
    
class FreebaseMovieConcept:
    FILM                = "/film/film"
    ACTOR               = "/film/actor"
    
    
def run_query(sparql, query):
    """
    just executes a given query on a defined 
    sparql endpoint and returns the result as json
    """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()