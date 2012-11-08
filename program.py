import csv
import time
from datetime import datetime
import sys
import json

from SPARQLWrapper import SPARQLWrapper
from apiclient import discovery
from apiclient.errors import HttpError

from lod_dbs.lmdb import LMDBWrapper, LMDBConcept, LMDBSettings
from lod_dbs.freebase import FreebaseWrapper, FreebaseConcept, FreebaseSettings
from lod_dbs.imdb import IMDBWrapper, IMDBSettings
from lod_dbs.settings import Portal

# paths
LMDB_FREEBASE_ACTORS_FILE = 'out/lmdb_freebase_actors'
LMDB_FREEBASE_FILMS_FILE = '.out/lmdb_freebase_films' # with actors
LMDB_FREEBASE_FILMS_TMPFILE = 'out/lmdb__freebase_films.tmp' # without actors

FREEBASE_LMDB_ACTORS_FILE = 'out/freebase_lmdb_actors'
FREEBASE_LMDB_FILMS_FILE = 'out/freebase_lmdb_films'

FREEBASE_IMDB_FILMS_FILE = 'out/freebase_imdb_films'
FREEBASE_IMDB_ACTORS_FILE = 'out/freebase_imdb_actors'

IMDB_FREEBASE_FILMS_FILE = 'out/imdb_freebase_films'
IMDB_FREEBASE_ACTORS_FILE = 'out/imdb_freebase_actors'

FREEBASE_LMDB_ACTOR_MAPPING_FILE = 'out/freebase_lmdb_actor_mapping'
FREEBASE_LMDB_FILM_MAPPING_FILE = 'out/freebase_lmdb_film_mapping'

FREEBASE_IMDB_ACTOR_MAPPING_FILE = 'out/freebase_imdb_actor_mapping'
FREEBASE_IMDB_FILM_MAPPING_FILE = 'out/freebase_imdb_film_mapping'

def get_and_persist_lmdb_actors(f):
    """
    This function queries LMDB for all actors which have a foaf:page link to free-
    base and stores the result in f.
    """
    result = []
    try:
        print "getting actor count from lmdb"
        actorCount = lmdb.get_page_count(Portal.FREEBASE, LMDBConcept.ACTOR)

        print "loading %i actors from lmdb" % actorCount
        with open(f, 'w') as fOut:
            csvwriter = csv.DictWriter(fOut,
                                       ['actorid', 'name', 'freebase_guid'],
                                       delimiter=';')
            csvwriter.writeheader()
            for i in range(0, actorCount, LMDBSettings.PAGE_SIZE):
                loaded = False
                delay = 1
                print 'getting actors from %i to %i' \
                        % (i, (i + LMDBSettings.PAGE_SIZE))
                while not loaded:
                    time.sleep(delay)
                    try:
                        actors = lmdb.get_actors(Portal.FREEBASE,
                                                 i,  # offset
                                                 LMDBSettings.PAGE_SIZE)  # limit
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
        filmCount = lmdb.get_page_count(Portal.FREEBASE, LMDBConcept.FILM)
        print "loading %i films from lmdb" % filmCount

        with open(f, 'w') as fOut:
            csvwriter = csv.DictWriter(fOut,
                                       ['filmid',
                                        'name',
                                        'date',
                                        'freebase_guid'],
                                       delimiter=';')
            csvwriter.writeheader()
            for i in range(0, filmCount, LMDBSettings.PAGE_SIZE):
                loaded = False
                delay = 1
                print 'getting films from %i to %i \r' \
                        % (i, (i + LMDBSettings.PAGE_SIZE))
                while not loaded:
                    time.sleep(delay)
                    try:
                        films = lmdb.get_films(Portal.FREEBASE,
                                               i,  # offset
                                               LMDBSettings.PAGE_SIZE)
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
                if i % LMDBSettings.PAGE_SIZE == 0:
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
        film_count = freebase.get_count(Portal.IMDB, FreebaseConcept.FILM, True)
        print "getting %i films from freebase" % film_count
        with open(f, 'w') as fout:
            portal_key = 'imdb'
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
            limit = FreebaseSettings.START_PAGE_SIZE
            got_more = True
            successful_queries = 0
            while got_more:
                loaded = False
                while not loaded:
                    t0 = time.time()
                    try:
                        print "[%s] getting films %i to %i" \
                                % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                      i,
                                      i + limit)
                        response = freebase.get_films(Portal.IMDB,
                                                      FreebaseSettings.START_PAGE_SIZE,
                                                      cursor)
                        print "got films, getting descriptions from text api"
                        descriptions = freebase.get_film_descriptions(
                                [film['id'] for film in response[0]])
                        for film in response[0]:
                            film['directed_by'] = ",".join(
                                    [_['name'] if _['name'] is not None else "" for _ in film['directed_by']])
                            film['written_by'] = ",".join(
                                    [_['name'] if _['name'] is not None else "" for _ in film['written_by']])
                            film['produced_by'] = ",".join(
                                    [_['name'] if _['name'] is not None else "" for _ in film['produced_by']])
                            film['genre'] = ",".join(
                                    [_['name'] if _['name'] is not None else "" for _ in film['genre']])
                            film['actors'] = ",".join(
                                    [_['actor']['guid'] for _ in film['starring']])
                            film['description'] = descriptions[film['id']]
                            film['imdb'] = ",".join(
                                    set([_['value'].rstrip('\n') for _ in film['key']]))
                    except (IOError, HttpError) as _:
                        if (limit - 50) > FreebaseSettings.MIN_PAGE_SIZE:
                            limit -= 50
                        else:
                            limit = FreebaseSettings.MIN_PAGE_SIZE
                        print ("Connection error occured!\n" \
                                + "\twaiting %i seconds to retry\n" \
                                + "\treducing limit to %i\n" \
                                + "\t%s") \
                                % (FreebaseSettings.ERROR_DELAY, limit, str(_))
                        time.sleep(FreebaseSettings.ERROR_DELAY)
                        successful_queries = 0
                    else:
                        loaded = True
                        successful_queries += 1
                        # try to fetch more per query after 10 successful
                        # queries
                        if successful_queries == 10:
                            if (limit + 50) < FreebaseSettings.MAX_PAGE_SIZE:
                                limit += 50
                            else:
                                limit = FreebaseSettings.MAX_PAGE_SIZE
                            successful_queries = 0
                        for film in response[0]:
                            # write encoded from utf-8
                            dictwriter.writerow({k:(v.encode('utf8') \
                                                 if isinstance(v, unicode) else v) \
                                                 for k,v in film.items()})
                        cursor = response[1]
                        got_more = cursor
                        i += limit
                        result += response[0]
                        fout.flush()
                        print "took %.2f seconds" % (time.time() - t0)
                        time.sleep(FreebaseSettings.DEFAULT_DELAY)
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
                                        FreebaseConcept.FILM,
                                        True)
#        film_count = 199174
        print "getting %i films from freebase" % film_count
        with open(f, 'w') as fout:
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
#            dictwriter.writeheader()

            i = 0 # no of films read
            startname = '' # needed for paging
            tmpname = ''
            got_more = True
            limit = FreebaseSettings.START_PAGE_SIZE
            successful_queries = 0
            while got_more:
                loaded = False
                while not loaded:
                    t0 = time.time()
                    try:
                        print "getting films %i to %i with startname: %s" % \
                                (i, i + limit, startname.encode('utf-8'))
                        response = freebase.get_films_in_order(Portal.IMDB,
                                                               limit,
                                                               startname)
                        print "got films, getting descriptions from text api"
                        for film in response:
                            film['directed_by'] = ",".join([_['name'] if _['name'] is not None else "" for _ in film['directed_by']])
                            film['written_by'] = ",".join([_['name'] if _['name'] is not None else "" for _ in film['written_by']])
                            film['produced_by'] = ",".join([_['name'] if _['name'] is not None else "" for _ in film['produced_by']])
                            film['genre'] = ",".join([_['name'] if _['name'] is not None else "" for _ in film['genre']])
                            film['actors'] = ",".join([_['actor']['guid'] for _ in film['starring']])
                            try:
                                film['description'] = freebase.get_film_description(film['id'])
                            except HttpError as _:
                                film['description'] = ""
                            film['imdb'] = ",".join(set([_['value'].rstrip('\n') for _ in film['key']]))
                            tmpname = film['name']
                    except (IOError, HttpError) as _:
                        if (limit - 50) > FreebaseSettings.MIN_PAGE_SIZE:
                            limit -= 50
                        else:
                            limit = FreebaseSettings.MIN_PAGE_SIZE
                        print ("Connection error occured!\n" \
                               + "\twaiting %i seconds to retry\n" \
                               + "\treducing limit to %i \n" \
                               + "\t%s") \
                               % (FreebaseSettings.ERROR_DELAY, limit, str(_))
                        time.sleep(FreebaseSettings.ERROR_DELAY)
                        #init_freebase()
                        successful_queries = 0
                    else:
                        loaded = True
                        successful_queries += 1
                        # try to fetch more per query after 10 successful queries
                        if successful_queries == 10:
                            if (limit + 50) < FreebaseSettings.MAX_PAGE_SIZE:
                                limit += 50
                            else:
                                limit = FreebaseSettings.MAX_PAGE_SIZE
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
                        time.sleep(FreebaseSettings.DEFAULT_DELAY)
    except IOError as ioError:
        print str(ioError)
    else:
        return result

def get_and_persist_freebase_actors_by_guid(guids, fout):
    try:
        n = len(guids)
        print "getting %i actors from freebase" % n
        with open(fout, 'w') as f_out:
            dictwriter = csv.DictWriter(f_out,
                    ['id', 'guid', 'name', 'date_of_birth', 'place_of_birth',
                        'height_meters', 'weight_kg', 'gender', 'imdb_id'],
                    delimiter=';')
            dictwriter.writeheader()
            i = 0
            for guid in guids:
                loaded = False
                while not loaded:
                    try:
                        actor = freebase.get_actor_by_guid(guid)
                    except (IOError, HttpError) as error:
                        print ("Connection error occured!\n" \
                                + "\twaiting %i seconds to retry\n" \
                                + "\t%s") \
                                % (FreebaseSettings.ERROR_DELAY, str(error))
                        time.sleep(FreebaseSettings.ERROR_DELAY)
                    else:
                        loaded = True
                        i += 1
                        if actor:
                            dictwriter.writerow({k:(v.encode('utf8') \
                                    if isinstance(v, unicode) else v)
                                    for k,v in actor.items()})
                        if i % FreebaseSettings.START_PAGE_SIZE == 0:
                            print "Actors queried: %i (%.2f%%)" % (i ,
                                    (i / float(n) * 100))
                            f_out.flush()
    except IOError:
        print sys.exc_info()

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
                    if i % LMDBSettings.PAGE_SIZE == 0:
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
                    if i % LMDBSettings.PAGE_SIZE == 0:
                        print "Films queried: %i   \r" % i
                        fOut.flush()
    except IOError as ioError:
        print str(ioError)
    else:
        return result

def get_and_persist_imdb_films_by_freebase_films(freebase_films_file, fout):
    result = []
    i = 0
    t0 = time.time()
    try:
        print "getting imdb films"
        with open(freebase_films_file, 'r') as f_in:
            with open(fout, 'w') as f_out:
                dictreader = csv.DictReader(f_in, delimiter=';')
                dictwriter = csv.DictWriter(f_out,
                        ['imdb_id', 'name', 'initial_release_date',
                        'directed_by', 'written_by', 'produced_by', 'genre',
                        'actors', 'descriptions'],
                        delimiter=';')
                dictwriter.writeheader()
                for film in dictreader:
                    print '=== film[\'id\']', film['id']
                    loaded = False
                    while not loaded:
                        try:
                            if ',' in film['imdb']:
                                imdb_id = film['imdb'].split(',')[0][2:]
                            else:
                                imdb_id = film['imdb'][2:]
                            print '=== imdb_id:', imdb_id
                            imdb_film = imdb.get_film_by_id(imdb_id)
                        except:
                            print 'Unexpected error:', str(sys.exc_info())
                            time.sleep(IMDBSettings.ERROR_DELAY)
                        else:
                            loaded = True
                            i += 1
                            imdb_film['imdb_id'] = imdb_id
                            dictwriter.writerow({k:(v.encode('utf8') \
                                                 if isinstance(v, unicode) else v) \
                                                 for k,v in imdb_film.items()})
                            time.sleep(IMDBSettings.DEFAULT_DELAY)
                            if i % IMDBSettings.PAGE_SIZE == 0:
                                print 'Films queried: %i (%.2f seconds)' % (i,
                                        (time.time() - t0))
                                f_out.flush()

    except IOError as ioError:
        print str(ioError)

def get_and_persist_imdb_actors_by_id(ids, fout):
    try:
        n = len(ids)
        print "getting %i actors from imdb" % n
        with open(fout, 'w') as f_out:
            dictwriter = csv.DictWriter(f_out,
                ['id', 'name', 'date_of_birth', 'place_of_birth', 'height',
                    'biographies'], delimiter=';')
            dictwriter.writeheader()
            i = 0
            for imdb_id in ids:
                loaded = False
                try:
                    imdb_id = imdb_id[2:] # strip the nm prefix
                    actor = imdb.get_actor_by_id(imdb_id)
                except (IOError, HttpError) as error:
                    print ("Connection error occured!\n" \
                        + "\twaiting %i seconds to retry\n" \
                            + "\t%s") \
                            % (IMDBSettings.ERROR_DELAY, str(error))
                    time.sleep(IMDBSettings.ERROR_DELAY)
                else:
                    loaded = True
                    i += 1
                    if actor:
                        actor['id'] = imdb_id
                        dictwriter.writerow({k:(v.encode('utf8') \
                                if isinstance(v, unicode) else v)
                                for k,v in actor.items()})
                    if i % IMDBSettings.PAGE_SIZE == 0:
                        print "Actors queried: %i (%.2f%%)" % (i,
                                (i / float(n) * 100))
                        f_out.flush()
    except IOError:
        print sys.exc_info()

def create_mappings(source_file, map_file, key_map):
    try:
        with open(source_file, 'r') as sourcef:
            with open(map_file, 'w') as mapf:
                dictreader = csv.DictReader(sourcef,
                                            delimiter=';',
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
                    dictwriter.writerow(map_row)
                mapf.flush()
    except IOError as ioError:
        print str(ioError)

def get_column_values(filename, column_number, skip_header=False,
        sep_value_by=None):
    result = set()
    i = 0
    try:
        with open(filename, 'r') as f_in:
            reader = csv.reader(f_in, delimiter=';')
            if skip_header:
                reader.next()
            for line in reader:
                i += 1
                if sep_value_by:
                    line_vals = line[column_number].split(sep_value_by)
                    result = result.union(line_vals)
                else:
                    result.add(line[column_number])
                if i % 1000 == 0:
                    print '=== lines read:', i
    except IOError as ioError:
        print str(ioError)
    else:
        return result

# hit and run
if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print "usage: program.py <google_api_key>"
        sys.exit(1)
    # connect to LMDB
    lmdb = LMDBWrapper()

    # connect to freebase
    freebase = FreebaseWrapper(sys.argv[1])

    # connect to imdb
    imdb = IMDBWrapper()

    # process
    # lmdb <-> freebase stuff
    #get_and_persist_lmdb_actors(LMDB_FREEBASE_ACTORS_FILE)
    #get_and_persist_lmdb_films(LMDB_FREEBASE_FILMS_TMPFILE)
    #get_and_persist_lmdb_actors_by_film(LMDB_FREEBASE_FILMS_TMPFILE,
    #                                    LMDB_FREEBASE_FILMS_FILE)
    #get_and_persist_freebase_actors_by_lmdb_actors(LMDB_FREEBASE_ACTORS_FILE,
    #                                               FREEBASE_LMDB_ACTORS_FILE)
    #get_and_persist_freebase_films_by_lmdb_films(LMDB_FREEBASE_FILMS_FILE,
    #                                             FREEBASE_LMDB_FILMS_FILE)
    #create_mappings(LMDB_FREEBASE_ACTORS_FILE,
    #                FREEBASE_LMDB_ACTOR_MAPPING_FILE,
    #                {'actorid' : 'lmdb_id', 'freebase_guid' : 'freebase_guid'})
    #create_mappings(LMDB_FREEBASE_FILMS_FILE,
    #                FREEBASE_LMDB_FILM_MAPPING_FILE,
    #                {'filmid' : 'lmdb_id', 'freebase_guid' : 'freebase_guid'})

    # freebase <-> imdb region

    # Get freebase films which have an imdb id

    #get_and_persist_freebase_films(FREEBASE_IMDB_FILMS_FILE)

    # Get IMDb films by their corresponding freebase film

    #get_and_persist_imdb_films_by_freebase_films(FREEBASE_IMDB_FILMS_FILE,
    #        IMDB_FREEBASE_FILMS_FILE)

    # Get freebase actors which appear in previously collected films

    #freebase_actor_guids = get_column_values(FREEBASE_IMDB_FILMS_FILE, 8,
    #        skip_header=True, sep_value_by=',')
    #get_and_persist_freebase_actors_by_guid(freebase_actor_guids,
    #        FREEBASE_IMDB_ACTORS_FILE)

    # Get IMDb actors by their corresponding freebase actor

    imdb_actor_ids = get_column_values(FREEBASE_IMDB_ACTORS_FILE, 8,
            skip_header=True)
    get_and_persist_imdb_actors_by_id(imdb_actor_ids,
            IMDB_FREEBASE_ACTORS_FILE)

    sys.exit(0)
