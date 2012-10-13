from SPARQLWrapper import JSON
from json import dumps

from utils import Config

class LMDBWrapper:
    def __init__(self, sparql):
        self._sparql = sparql
    
    def get_page_count(self, portal, movie_concept):
        """
        Returns the number of foaf:page links to the given portal
        """
        query = """
            %s
            SELECT count(?instance)
            WHERE {
                ?instance foaf:page ?link FILTER regex(str(?link), "^%s", "i") .
                ?instance rdf:type movie:%s .
            }
        """ % (Config.PREFIXES, portal, movie_concept)
    
        self._sparql.setQuery(query)
        self._sparql.setReturnFormat(JSON)
    
        results = self._sparql.query().convert()
    
        for result in results["results"]["bindings"]:
            return int(result[".1"]["value"])
        
    def get_actors(self, portal, offset = 0, limit = 10):
        """
        Returns a list of dicts which contain actor infos as

        actor['actorid']
        actor['name']
        actor['freebase']
        """
        result = []
        
        query = """
            %s
            SELECT ?actorid ?name ?page
            WHERE {
                ?instance movie:actor_actorid ?actorid .
                ?instance movie:actor_name ?name .
                ?instance foaf:page ?page FILTER regex(str(?page), "^%s", "i") .
                ?instance rdf:type movie:actor .
            }
            OFFSET %i
            LIMIT %i
        """ % (Config.PREFIXES, portal, offset, limit)
    
        self._sparql.setQuery(query)
        self._sparql.setReturnFormat(JSON)
    
        for actor in self._sparql.query().convert()["results"]["bindings"]:
            result.append({
                           'actorid'    : actor['actorid']['value'],
                           'name'       : actor['name']['value'],
                           'freebase'   : actor['page']['value']
                           })
            
        return result

    def get_films(self, portal, offset = 0, limit = 10):
        """
        Returns a list of dicts which contain film infos as
        
        movie['filmid']
        movie['name'] (using rdfs:label)        
        movie['freebase']
        movie['date'] (optional)
        """
        result = []
        
        query = """
            %s
            SELECT *
            WHERE {
                ?instance foaf:page ?page FILTER regex(str(?page), "^%s", "i") .
                ?instance movie:filmid ?filmid .
                ?instance rdfs:label ?name .                
                OPTIONAL { ?instance movie:initial_release_date ?date . }
            }            
            OFFSET %i
            LIMIT %i
        """ % (Config.PREFIXES, portal, offset, limit)        
        
        self._sparql.setQuery(query)
        self._sparql.setReturnFormat(JSON)        
        
        for film in self._sparql.query().convert()["results"]["bindings"]:
            # create a new result entry
            result.append({'filmid' : film['filmid']['value'],
                          'name' : film['name']['value'],
                          'freebase' : film['page']['value'],                          
                          'date' : film['date']['value'] if ('date' in film) else ''})
            
        return result