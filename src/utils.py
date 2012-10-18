from SPARQLWrapper import JSON

class Config:
    PREFIXES = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX movie: <http://data.linkedmdb.org/resource/movie/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        """
    PAGE_SIZE = 100
    GOOGLE_API_KEY = ""
    
    LMDB_ACTORS_FILE = '../out/lmdb_actors'
    LMDB_FILMS_FILE = '../out/lmdb_films' # with actors
    LMDB_FILMS_TMPFILE = '../out/lmdb_films.tmp' # without actors
    FREEBASE_ACTORS_FILE = '../out/freebase_actors'
    FREEBASE_FILMS_FILE = '../out/freebase_films'

    ACTOR_MAPPING_FILE = '../out/actor_mapping'
    FILM_MAPPING_FILE = '../out/film_mapping'

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
    
def run_query(sparql, query):
    """
    just executes a given query on a defined 
    sparql endpoint and returns the result as json
    """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()