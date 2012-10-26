import csv
import time
import sys
import json

from SPARQLWrapper import SPARQLWrapper
from apiclient import discovery
from apiclient.errors import HttpError

from lmdb_wrapper import LMDBWrapper
from utils import Config, Portal, SPARQLEndpoints, LMDBMovieConcept, \
    FreebaseMovieConcept
from freebase_wrapper import FreebaseWrapper



def get_and_persist_lmdb_actors(f):
    """
    This function queries LMDB for all actors which have a foaf:page link to free-
    base and stores the result in f.
    """
    result = []
    try:
        
        print "getting actor count from freebase"                  
        actorCount = lmdb.get_page_count(Portal.FREEBASE, LMDBMovieConcept.ACTOR)
        
        print "loading %i actors from lmdb" % actorCount
        with open(f, 'w') as fOut:
            csvwriter = csv.DictWriter(fOut, 
                                       ['actorid', 'name', 'freebase_guid'], 
                                       delimiter=';')
            csvwriter.writeheader() 
            for i in range(0, actorCount, Config.LMDB_PAGE_SIZE):
                loaded = False
                delay = 1
                print 'getting actors from %i to %i' \
                        % (i, (i + Config.LMDB_PAGE_SIZE))
                while not loaded:
                    time.sleep(delay)
                    try:
                        actors = lmdb.get_actors(Portal.FREEBASE,
                                                 i,  # offset 
                                                 Config.LMDB_PAGE_SIZE)  # limit
                    except IOError as _:
                        delay *= 2
                        print "a connection error occured," \
                              " setting retry-delay to %i (%s)" \
                              % (delay, _.args)
                    else:
                        loaded = True
                csvwriter.writerows(actors)
                result += actors
                fOut.flush()
    except IOError as ioError:
        print str(ioError)
    else:
        return result
    
def get_and_persist_lmdb_films(f):
    """
    This function queries LMDB for all films which have a foaf:page link to 
    freebase and stores the results f.
    """
    result = []
    try:
        print "getting film count from freebase"
        filmCount = lmdb.get_page_count(Portal.FREEBASE, LMDBMovieConcept.FILM)
        print "loading %i films from lmdb" % filmCount
                
        with open(f, 'w') as fOut:
            csvwriter = csv.DictWriter(fOut,
                                       ['filmid', 
                                        'name', 
                                        'date', 
                                        'freebase_guid'],
                                       delimiter=';')
            csvwriter.writeheader()
            for i in range(0, filmCount, Config.LMDB_PAGE_SIZE):
                loaded = False
                delay = 1
                print 'getting films from %i to %i \r' \
                        % (i, (i + Config.LMDB_PAGE_SIZE))
                while not loaded:
                    time.sleep(delay)
                    try:
                        films = lmdb.get_films(Portal.FREEBASE,
                                               i,  # offset 
                                               Config.LMDB_PAGE_SIZE)                        
                    except IOError as _:
                        delay *= 2
                        print "a connection error occured," \
                              " setting retry-delay to %i (%s)" \
                              % (delay, _.args)
                    else:
                        loaded = True
                csvwriter.writerows(films)
                result += films
                fOut.flush()
    except IOError as ioError:
        print str(ioError)
    else:
        return result

def get_and_persist_lmdb_actors_by_film(fin, fout):
    """
    This function takes a list of films (each film is a dictionary) and queries
    LMDB to get all actors for each film. The complete film information is then
    stored in fout.
    """
    result = []
    try:
        print "loading films from file"
        films = []
        with open(fin, 'r') as fIn:
            csvreader = csv.DictReader(fIn, delimiter=';')
            for film in csvreader:
                films.append(film)

        print "getting actors for film"
        with open(fout, 'w') as fOut:
            csvwriter = csv.DictWriter(fOut,
                                       ['filmid', 
                                        'name', 
                                        'actors', 
                                        'date', 
                                        'freebase_guid'],
                                       delimiter=';')
            csvwriter.writeheader()
            n = len(films)
            i = 0            
            for film in films:
                if i % Config.LMDB_PAGE_SIZE == 0:
                    print "processed %i of %i films" % (i, n) 
                loaded = False
                delay = 1
                while not loaded:
                    time.sleep(delay)
                    try:
                        film['actors'] = ",".join(lmdb.get_actors_by_film(Portal.FREEBASE,
                                                                          int(film['filmid'])))
                    except IOError as _:
                        delay *= 2  # increase sleep delay in case of connection error
                        print "a connection error occured," \
                              " setting retry-delay to %i (%s)" \
                              % (delay, _.args)
                    else:
                        loaded = True
                    if loaded:
                        csvwriter.writerow(film)
                i+=1
            result += films
    except IOError as ioError:
        print str(ioError)
    else:
        return result
        
def get_and_persist_freebase_films(f):
    """
    This function queries freebase for all films which have a imdb_id set. The
    films are persisted in the given file.
    """
    print "loading films from freebase"
    result = []
    try:
        film_count = freebase.get_count(Portal.IMDB, 
                                        FreebaseMovieConcept.FILM, 
                                        True)
        print "getting %i films from freebase" % film_count
        with open(f, 'w') as fout:
            portal_key = freebase.get_portal_key(Portal.IMDB)
            dictwriter = csv.DictWriter(fout,
                                        ['id',
                                         'guid',
                                         'name',
                                         'initial_release_date',
                                         'directed_by',
                                         'written_by',
                                         'produced_by',
                                         'genre',
                                         'actors',
                                         'description',
                                         portal_key],
                                         delimiter=';',
                                         extrasaction='ignore')
            dictwriter.writeheader()
            
            i = 0 # no of films read
            cursor = "" # needed for paging
            while i < film_count:
                loaded = False
                retry_delay = Config.FREEBASE_DEFAULT_DELAY
                while not loaded:
                    t0 = time.time()
                    try:
                        print "getting films %i to %i" \
                              % (i, i + Config.FREEBASE_START_PAGE_SIZE)
                        response = freebase.get_films(Portal.IMDB,
                                                      Config.FREEBASE_START_PAGE_SIZE,
                                                      cursor)
                        print "got films, getting descriptions from text api"
                        for film in response[0]:
                            film['directed_by'] = ",".join([_['name'] for _ in film['directed_by']])
                            film['written_by'] = ",".join([_['name'] for _ in film['written_by']])
                            film['produced_by'] = ",".join([_['name'] for _ in film['produced_by']])
                            film['genre'] = ",".join([_['name'] for _ in film['genre']])
                            film['actors'] = ",".join([_['actor']['guid'] for _ in film['starring']])
                            film['description'] = freebase.get_film_description(film['id'])
                            film[portal_key] = ",".join(film[portal_key])
                    except (IOError, HttpError) as _:
                        retry_delay *= 2  # increase sleep delay in case of connection error
                        print "a connection error occured," \
                              " setting retry-delay to %i\n(%s)" \
                              % (retry_delay, str(_))
                    else:
                        loaded = True
                        for film in response[0]:
                            dictwriter.writerow({k:(v.encode('utf8') \
                                                 if isinstance(v, unicode) else v) \
                                                 for k,v in film.items()})
                        cursor = response[1]
                        i += Config.FREEBASE_START_PAGE_SIZE
                        result += response[0]
                        fout.flush()
                        print "took %.2f seconds" % (time.time() - t0)
                        time.sleep(Config.FREEBASE_DEFAULT_DELAY)
    except IOError as ioError:
        print str(ioError)
    else:
        return result
    
def get_and_persist_freebase_films_in_order(f):
    """
    This function queries freebase for all films which have a imdb_id set. The
    films are persisted in the given file.
    This function uses the get_films_in_order method due to the bug in imdb which
    fails to use a cursor for more than a few thousand of entities (see
    FreebaseWrapper.get_films for details).
    """
    print "loading films from freebase"
    result = []
    try:
        film_count = freebase.get_count(Portal.IMDB, 
                                        FreebaseMovieConcept.FILM,
                                        True)
#        film_count = 199174
        print "getting %i films from freebase" % film_count
        with open(f, 'w') as fout:
            portal_key = freebase.get_portal_key(Portal.IMDB)
            dictwriter = csv.DictWriter(fout,
                                        ['id',
                                         'guid',
                                         'name',
                                         'initial_release_date',
                                         'directed_by',
                                         'written_by',
                                         'produced_by',
                                         'genre',
                                         'actors',
                                         'description',
                                         'imdb'],
                                         delimiter=';',
                                         extrasaction='ignore')
            dictwriter.writeheader()
            
            i = 0 # no of films read
            startname = '' # needed for paging
            tmpname = ''
            got_more = True
            limit = Config.FREEBASE_START_PAGE_SIZE
            successful_queries = 0
            while got_more:
                loaded = False
                while not loaded:                    
                    t0 = time.time()
                    try:
                        print "getting films %i to %i with startname: %s" % \
                                (i, i + limit, startname)
                        response = freebase.get_films_in_order(Portal.IMDB,
                                                               limit,
                                                               startname)
                        print "got films, getting descriptions from text api"
                        for film in response:
                            film['directed_by'] = ",".join([_['name'] for _ in film['directed_by']])
                            film['written_by'] = ",".join([_['name'] for _ in film['written_by']])
                            film['produced_by'] = ",".join([_['name'] for _ in film['produced_by']])
                            film['genre'] = ",".join([_['name'] for _ in film['genre']])
                            film['actors'] = ",".join([_['actor']['guid'] for _ in film['starring']])
                            try:
                                film['description'] = freebase.get_film_description(film['id'])
                            except HttpError as _:
                                film['description'] = ""
                            film['imdb'] = ",".join(set([_['value'].rstrip('\n') for _ in film['key']]))
                            tmpname = film['name']
                    except (IOError, HttpError) as _:
                        if (limit - 50) > Config.FREEBASE_MIN_PAGE_SIZE:
                            limit -= 50
                        else:
                            limit = Config.FREEBASE_MIN_PAGE_SIZE
                        print ("Connection error occured!\n" \
                               + "\twaiting %i seconds to retry\n" \
                               + "\treducing limit to %i \n" \
                               + "\t%s") \
                               % (Config.FREEBASE_ERROR_DELAY, limit, str(_))
                        time.sleep(Config.FREEBASE_ERROR_DELAY)
                        #init_freebase()
                        successful_queries = 0
                    else:
                        loaded = True
                        successful_queries += 1
                        # try to fetch more per query after 10 successful queries
                        if successful_queries == 10:
                            if (limit + 50) < Config.FREEBASE_MAX_PAGE_SIZE:
                                limit += 50
                            else:
                                limit = Config.FREEBASE_MAX_PAGE_SIZE
                            successful_queries = 0
                        for film in response:
                            # write encoded from utf-8
                            dictwriter.writerow({k:(v.encode('utf8') \
                                                 if isinstance(v, unicode) else v) \
                                                 for k,v in film.items()})
                        # update the next startname which is needed to get
                        # the next bunch of movies
                        got_more = startname != tmpname
                        startname = tmpname                       
                        i += limit 
                        result += response
                        fout.flush()
                        print "took %.2f seconds" % (time.time() - t0)
                        time.sleep(Config.FREEBASE_DEFAULT_DELAY)
    except IOError as ioError:
        print str(ioError)
    else:
        return result

def get_and_persist_freebase_actors_by_lmdb_actors(lmdb_actors_file, fout):
    i = 0
    result = []
    try:
        with open(lmdb_actors_file, 'r') as fin:
            with open(fout, 'w') as fOut:
                dictreader = csv.DictReader(fin,
                                            delimiter=';')
                dictwriter = csv.DictWriter(fOut,
                                            ['guid', 'name', 'lmdb'],
                                            delimiter=';')
                dictwriter.writeheader()
                
                for actor in dictreader:
                    i += 1
                    freebase_actor = freebase.get_actor_by_guid( \
                                                        actor['freebase_guid'])
                    if freebase_actor is not None:
                        freebase_actor['lmdb'] = actor['actorid']
                        dictwriter.writerow({k:(v.encode('utf8') \
                                             if isinstance(v, unicode) else v) \
                                             for k,v in freebase_actor.items()})
                    result += freebase_actor
                    if i % Config.LMDB_PAGE_SIZE == 0:
                        print "Actors queried: %i   \r" % i
                        fOut.flush()
    except IOError as ioError:
        print str(ioError)
    else:
        return result
        
def get_and_persist_freebase_films_by_lmdb_films(lmdb_films_file, fout):
    result = []
    i = 0
    try:
        with open(lmdb_films_file, 'r') as fin:
            with open(fout, 'w') as fOut:
                dictreader = csv.DictReader(fin, delimiter=';')
                dictwriter = csv.DictWriter(fOut,
                                            ['guid', 
                                             'name', 
                                             'starring', 
                                             'initial_release_date', 
                                             'lmdb'],
                                            delimiter=';',
                                            extrasaction='ignore')
                dictwriter.writeheader()
                for film in dictreader:
                    i += 1
                    freebase_film = freebase.get_film_by_guid( \
                                                        film['freebase_guid'])
                    if freebase_film is not None:
                        freebase_film['lmdb'] = film['filmid']
                        dictwriter.writerow({k:(v.encode('utf8') \
                                             if isinstance(v, unicode) else v) \
                                             for k,v in freebase_film.items()})
                    result += freebase_film
                    if i % Config.LMDB_PAGE_SIZE == 0:
                        print "Films queried: %i   \r" % i
                        fOut.flush()
    except IOError as ioError:
        print str(ioError)
    else:
        return result
    
def create_mappings(source_file, map_file, key_map):
    try:
        with open(source_file, 'r') as sourcef:
            with open(map_file, 'w') as mapf:
                dictreader = csv.DictReader(sourcef,
                                            delimiter=';'                                        
                                            )
                dictwriter = csv.DictWriter(mapf,
                                            key_map.values(),
                                            delimiter=';')
                dictwriter.writeheader()
                for row in dictreader:
                    map_row = {}
                    for key in key_map:
                        if key_map[key] is not None:
                            map_row[key_map[key]] = row[key] 
                    print map_row
                    dictwriter.writerow(map_row)
                map.flush()
    except IOError as ioError:
        print str(ioError)

def init_freebase():
    return FreebaseWrapper(discovery.build('freebase',
                                           'v1',
                                            developerKey=Config.GOOGLE_API_KEY))
# hit and run
if __name__ == "__main__":
    
    if len(sys.argv) <= 1:
        print "usage: program.py <google_api_key>"
        sys.exit(1)
    Config.GOOGLE_API_KEY = sys.argv[1]
    
    # connect to LMDB
    sparql_lmdb = SPARQLWrapper(SPARQLEndpoints.LMDB)
    lmdb = LMDBWrapper(sparql_lmdb)

    # connect to freebase
    freebase = init_freebase()

    # process
    # lmdb <-> freebase stuff
#    get_and_persist_lmdb_actors(Config.LMDB_FREEBASE_ACTORS_FILE)
#    get_and_persist_lmdb_films(Config.LMDB_FREEBASE_FILMS_TMPFILE)
#    get_and_persist_lmdb_actors_by_film(Config.LMDB_FREEBASE_FILMS_TMPFILE,
#                                        Config.LMDB_FREEBASE_FILMS_FILE)
#    get_and_persist_freebase_actors_by_lmdb_actors(Config.LMDB_FREEBASE_ACTORS_FILE,
#                                                   Config.FREEBASE_LMDB_ACTORS_FILE)
#    get_and_persist_freebase_films_by_lmdb_films(Config.LMDB_FREEBASE_FILMS_FILE,
#                                                 Config.FREEBASE_LMDB_FILMS_FILE)
#    create_mappings(Config.LMDB_FREEBASE_ACTORS_FILE,
#                    Config.FREEBASE_LMDB_ACTOR_MAPPING_FILE,
#                    {'actorid' : 'lmdb_id', 'freebase_guid' : 'freebase_guid'})
#    create_mappings(Config.LMDB_FREEBASE_FILMS_FILE,
#                    Config.FREEBASE_LMDB_FILM_MAPPING_FILE,
#                    {'filmid' : 'lmdb_id', 'freebase_guid' : 'freebase_guid'})
#    
    # lmdb <-> imdb stuff
    get_and_persist_freebase_films_in_order(Config.FREEBASE_IMDB_FILMS_FILE)
    sys.exit(0)
