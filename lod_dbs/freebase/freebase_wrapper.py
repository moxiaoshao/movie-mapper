import urllib2
import json

from apiclient import discovery
from apiclient.errors import HttpError

from settings import *

class FreebaseConcept:
    FILM = '/film/film'
    ACTOR = '/film/actor'


class FreebaseWrapper:
    def __init__(self, google_api_key):
        self.__freebase = discovery.build('freebase', 'v1',
                developerKey=google_api_key)
        self.__filmcursor = None
        self.__google_api_key = google_api_key

    def __set_authority_keys(self, portal):
        if 'imdb' in portal:
            self.__film_authority_key = '/authority/imdb/title'
            self.__actor_authority_key = '/authority/imdb/name'

    def get_count(self, portal, concept, estimate=False):
        """
        Returns the number of instances of a given concept
        which have a link to the given portal.
        """
        self.__set_authority_keys(portal)

        query = [{'type' : concept, 'key' : [{
            'namespace': self.__film_authority_key,
            'value' : None}],
            'return' : 'count' if not estimate else 'estimate-count'}]

        response = json.loads(self.__freebase
                              .mqlread(query=json.dumps(query))
                              .execute())

        return response['result'][0] if len(response['result']) > 0 else 0

    def get_actor_by_guid(self, guid):
        """
        Returns relevant actor data by guid.

        guid is the freebase hex guid

        returns dictionary with the following keys:
        - id
        - guid
        - imdb_id
        - name
        - date_of_birth (optional)
        - place_of_birth (optional)
        - height_meters (optional)
        - weight_kg (optional)
        - gender (optional)

        """
        if guid[0] != '#':
            guid = '#' + guid
        result = {}

        query = [{'type': '/film/actor',
                    'id': None,
                    'guid': guid,
                    'name': None,
                    '/people/person/date_of_birth': { 'value': None,
                        'optional': True},
                    '/people/person/place_of_birth': { 'name': None,
                        'optional': True},
                    '/people/person/height_meters': {'value': None,
                        'optional': True},
                    '/people/person/weight_kg': {'value': None,
                        'optional': True},
                    '/people/person/gender': [{'name': None,
                        'optional': True}],
                    'key': [{'namespace': '/authority/imdb/name',
                        'value': None}]
                    }]
        response = json.loads(self.__freebase
                              .mqlread(query=json.dumps(query))
                              .execute())
        if len(response['result']) > 0:
            actor = response['result'][0]
            #print '=== actor:', actor
            result['id'] = actor['id']
            result['guid'] = actor['guid']
            result['imdb_id'] = actor['key'][0]['value']
            result['name'] = actor['name']
            if actor['/people/person/date_of_birth']:
                result['date_of_birth'] = \
                    actor['/people/person/date_of_birth']['value']
            if actor['/people/person/place_of_birth']:
                result['place_of_birth'] = \
                    actor['/people/person/place_of_birth']['name']
            if actor['/people/person/weight_kg']:
                result['weight_kg'] = \
                        actor['/people/person/weight_kg']['value']
            if actor['/people/person/gender']:
                result['gender'] = \
                        ','.join([g['name'] for g in actor['/people/person/gender']])
            #print '=== actor(edit):', result
        return result

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
        self.__set_authority_keys(portal)
        query = [{'id': None,
                  'guid': None,
                  'name': None,
                  'directed_by': [{'name': None, 'optional': True}],
                  'written_by': [{'name': None, 'optional': True}],
                  'produced_by': [{'name': None, 'optional': True}],
                  'initial_release_date': None,
                  'genre': [{'name': None, 'optional': True}],
                  'type': FreebaseConcept.FILM,
                  'starring': [{'actor': {'guid': None,
                                          'key': [{
                                                   'namespace':
                                                   self.__actor_authority_key,
                                                   'value': None
                                                   }]
                                         }}],
                  'key': [{'namespace': self.__film_authority_key,
                      'value': None}],
                  'limit': limit,
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
        query = [{'id' : None,
                  'guid' : None,
                  'name' : None,
                  'name>' : start_name,
                  'directed_by' : [{'name' : None, 'optional': True}],
                  'written_by' : [{'name' : None, 'optional': True}],
                  'produced_by' : [{'name' : None, 'optional': True}],
                  'initial_release_date' : None,
                  'genre' : [{'name' : None, 'optional': True}],
                  'starring': [{'actor': {'guid': None,
                                          'key': [{
                                                   "namespace":
                                                   self.__actor_authority_key,
                                                   "value": None
                                                  }]
                                         }}],
                  'type' : FreebaseConcept.FILM,
                  'key' : [{'namespace' : self.__film_authority_key,
                      'value' : None}],
                  'limit' : limit,
                  'sort' : 'name'
                  }]
        print query
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

    def get_film_descriptions(self, film_ids):
        requests = []
        result = {}
        url = 'https://www.googleapis.com/rpc'
        for film_id in film_ids:
            requests.append({
                'method': 'freebase.text.get',
                'apiVersion': 'v1',
                'params': {
                    'id': film_id.split('/')[1:3],
                    'key': self.__google_api_key,
                    }
                })
        headers = { 'Content-Type': 'application/json' }
        req = urllib2.Request(url, json.dumps(requests), headers)
        response = json.loads(urllib2.urlopen(req).read())
        for i in range(0, len(film_ids)):
            result[film_ids[i]] = \
                response[i]['result']['result'].replace('\n','') \
                if 'result' in response[i] else None
        return result


