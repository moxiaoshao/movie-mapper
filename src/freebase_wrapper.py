import json

from utils import Config

class FreebaseWrapper:
    def __init__(self, freebase):
        self._freebase = freebase
        
    def get_actor_by_guid(self, guid):
        """
        Returns relevant actor data by guid.
        
        guid is the freebase hex guid
        
        """
        if guid[0] != '#':
            guid = '#' + guid
        query = [{'guid' : '%s' % guid,
                  'name' : None}]
        response = json.loads(self._freebase
                              .mqlread(query=json.dumps(query))
                              .execute())
        return response['result'][0] if len(response['result']) > 0 else None
    
    def get_film_by_guid(self, guid):
        """
        Returns relevant movie data by guid.
        """
        if guid[0] != '#':
            guid = '#' + guid
        print guid
        query =[{'guid' : '%s' % guid,
                 'name' : None,
                 'type' : '/film/film',
                 'initial_release_date' : None,                 
                 'starring' : [{'actor' : {'guid' : None }}]
                 }]
        response = json.loads(self._freebase
                              .mqlread(query=json.dumps(query))
                              .execute())
        
        if len(response['result']) > 0:
            result = response['result'][0]
            actors = ",".join([actor['actor']['guid'] for actor in result['starring']])
            result['starring'] = actors
            return result
        else:
            return None        
        
        
        
            
        
