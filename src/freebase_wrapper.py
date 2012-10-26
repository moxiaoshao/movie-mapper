import urllib2
import json

from utils import Portal, FreebaseMovieConcept

class FreebaseWrapper:
    def __init__(self, freebase):
        self.__freebase = freebase
        self.__filmcursor = None
        
    def get_portal_key(self, portal):
        if Portal.IMDB == portal:
            return '/authority/imdb/title'
        else:
            raise ValueError("given portal is not supported %s" % portal)
        
    
    def get_count(self, portal, concept, estimate=False):
        """
        Returns the number of instances of a given concept
        which have a link to the given portal.
        """
        key = self.get_portal_key(portal)
        
        query = [{'type' : concept,
                  'key' : [{'namespace': key, 'value' : None}],                  
                  'return' : 'count' if not estimate else 'estimate-count'}]
        
        response = json.loads(self.__freebase
                              .mqlread(query=json.dumps(query))                              
                              .execute())
        
        return response['result'][0] if len(response['result']) > 0 else 0
        
    def get_actor_by_guid(self, guid):
        """
        Returns relevant actor data by guid.
        
        guid is the freebase hex guid
        
        """
        if guid[0] != '#':
            guid = '#' + guid
        query = [{'guid' : '%s' % guid,
                  'name' : None}]
        response = json.loads(self.__freebase
                              .mqlread(query=json.dumps(query))
                              .execute())
        return response['result'][0] if len(response['result']) > 0 else None
    
    def get_film_by_guid(self, guid):
        """
        Returns relevant movie data by guid.
        """
        if guid[0] != '#':
            guid = '#' + guid

        query =[{'guid' : '%s' % guid,
                 'name' : None,
                 'type' : '/film/film',
                 'initial_release_date' : None,
                 'starring' : [{'actor' : {'guid' : None }}]
                 }]
        response = json.loads(self.__freebase
                              .mqlread(query=json.dumps(query))
                              .execute())
        
        if len(response['result']) > 0:
            result = response['result'][0]
            actors = ",".join([actor['actor']['guid'] for actor in result['starring']])
            result['starring'] = actors
            return result
        else:
            return None
        
    def get_films(self, portal, limit=100, cursor=""):
        """
        This function is implemented according to the official freebase MQL
        documentation (http://wiki.freebase.com/wiki/MQL_Manual/mqlread# \
        Fetching_Large_Result_Sets_with_Cursors).
        Unfortunately, this runs into an 503 "Backend Error" after returning a
        fixed number of movies. I reported this on the official mailing list:
        http://lists.freebase.com/pipermail/freebase-discuss/2012-October/009817.html
        """
        key = self.get_portal_key(portal) 
        
        query = [{'id' : None, 
                  'guid' : None,                 
                  'name' : None,
                  'directed_by' : [{'name' : None}],
                  'written_by' : [{'name' : None}],
                  'produced_by' : [{'name' : None}],
                  'initial_release_date' : None,
                  'genre' : [{'name' : None}],
                  'type' : FreebaseMovieConcept.FILM,                  
                  'starring' : [{'actor' : {'guid' : None, 'imdb_entry' : []}}],
                  key : [], # there are films with multiple links
                  'limit' : limit,
                  }]
        response = json.loads(self.__freebase
                              .mqlread(query=json.dumps(query), cursor=cursor)
                              .execute())
        
        return (response['result'], response['cursor'])
    
    def get_films_in_order(self, 
                           portal, 
                           limit=100,                         
                           start_name=''):
        """
        This is an alternate implementation to get the relevant movies. I
        implemented it because of the bug described in get_films doc.
        This method orders the movies by guid and uses the latest retrieved
        guid to simulate paging.
        """
        key = self.get_portal_key(portal)
        
        query = [{'id' : None,
                  'guid' : None,
                  'name' : None,                
                  'name>' : start_name,
                  'directed_by' : [{'name' : None}],
                  'written_by' : [{'name' : None}],
                  'produced_by' : [{'name' : None}],
                  'initial_release_date' : None,
                  'genre' : [{'name' : None}],
                  'type' : FreebaseMovieConcept.FILM,                  
                  'starring' : [{'actor' : {'guid' : None, 'imdb_entry' : []}}],
                  'key' : [{'namespace' : key, 'value' : None}],
                  'limit' : limit,
                  'sort' : 'name'
                  }]
        response = json.loads(self.__freebase
                              .mqlread(query=json.dumps(query))
                              .execute())
        
        return response['result']
        
    
    def get_film_description(self, film_id):
        #url = 'https://www.googleapis.com/rpc'
        #requests = [{
        #  'method': 'freebase.text.get', 
        #  'apiVersion': 'v1', 
        #  'params': {
        #    'id': film_id.split('/')[1:3]
        #  }
        #}]
        #headers = { 'Content-Type': 'application/json' }
        #req = urllib2.Request(url, json.dumps(requests), headers)
        #response = urllib2.urlopen(req)
        #result = json.loads(response.read())
        #if  len(result) > 0 and \
        #    result[0].has_key('result') and \
        #    result[0]['result'].has_key('result'):
        #    return result[0]['result']['result'].replace('\n', '')
        #else:
        #    return ""
        response = self.__freebase.text().get(id=film_id).execute()
        return response['result'].replace('\n', '')
