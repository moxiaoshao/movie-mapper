class LMDBSettings:
    PREFIXES = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX movie: <http://data.linkedmdb.org/resource/movie/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        """

    PAGE_SIZE = 100

    ENDPOINT = "http://data.linkedmdb.org/sparql"
