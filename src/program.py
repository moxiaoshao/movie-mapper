import csv
from json import dumps

from SPARQLWrapper import SPARQLWrapper
from SPARQLWrapper.SPARQLExceptions import QueryBadFormed

from lmdb_wrapper import LMDBWrapper
from utils import Config, Portal, SPARQLEndpoints, LMDBMovieConcept, run_query


# connect to LMDB
sparql_lmdb = SPARQLWrapper(SPARQLEndpoints.LMDB)
lmdb = LMDBWrapper(sparql_lmdb)

def get_and_persist_lmdb_actors(f):
    result = []
    try:
        actorCount = lmdb.get_page_count(Portal.FREEBASE, LMDBMovieConcept.ACTOR)
        print "loading %i actors from lmdb" % actorCount
                
        with open(f, 'w') as fOut:
            csvwriter = csv.DictWriter(fOut, ['actorid', 'name', 'freebase'], delimiter=';') 
            for i in range(0, actorCount, Config.PAGE_SIZE):
                print 'getting actors from %i to %i' % (i, (i + Config.PAGE_SIZE))
                actors = lmdb.get_actors(Portal.FREEBASE, 
                                            i, # offset
                                            Config.PAGE_SIZE) # limit 
                csvwriter.writerows(actors)
                result += actors
                fOut.flush()
    except IOError as ioError:
        print str(ioError)
        
    return result    
    
def get_and_persist_lmdb_films(f):
    result = []
    try:
        # filmCount = lmdb.get_page_count(Portal.FREEBASE, LMDBMovieConcept.FILM)
        filmCount = 100
        print "loading %i films from lmdb" % filmCount
                
        with open(f, 'w') as fOut:
            csvwriter = csv.DictWriter(fOut, 
                                       ['filmid', 'name', 'actors', 'date', 'freebase'], 
                                       delimiter=';')
            
            for i in range(0, filmCount, Config.PAGE_SIZE):
                print 'getting films from %i to %i' % (i, (i + Config.PAGE_SIZE))
                films = lmdb.get_films(Portal.FREEBASE, 
                                       i, #offset 
                                       Config.PAGE_SIZE)
                
                for film in films:
                    film['actors'] = ",".join(lmdb.get_actors_by_film(Portal.FREEBASE, 
                                                             int(film['filmid'])))
                    csvwriter.writerow(film)
                
                result += films
                fOut.flush()
    except IOError as ioError:
        print str(ioError)
        
    return result

        
# hit and run
#get_and_persist_lmdb_actors('../out/ldmb_actors')
#get_and_persist_lmdb_films('../out/lmdb_films')