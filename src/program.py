from SPARQLWrapper import SPARQLWrapper
from SPARQLWrapper.SPARQLExceptions import QueryBadFormed

from lmdb_wrapper import LMDBWrapper
from utils import Config, Portal, SPARQLEndpoints, LMDBMovieConcept


# connect to LMDB
sparql_lmdb = SPARQLWrapper(SPARQLEndpoints.LMDB)

lmdb = LMDBWrapper(sparql_lmdb)

# actorCount = lmdb.get_page_count(Portal.FREEBASE, LMDBMovieConcept.ACTOR)


def get_and_persist_lmdb_actors(f):
    try:
        # actorCount = lmdb.get_page_count(Portal.FREEBASE, LMDBMovieConcept.ACTOR)
        actorCount = 100
        with open(f, 'w') as fOut:
            for i in range(0, actorCount, Config.PAGE_SIZE):
                print 'getting actors from %i to %i' % (i, (i + Config.PAGE_SIZE))
                actors = lmdb.get_actors(Portal.FREEBASE, 
                                            i, # offset
                                            Config.PAGE_SIZE) # limit 
                for actor in actors:                    
                    fOut.write("%s;%s;%s\n" % (actor['actorid'], 
                                               actor['name'], 
                                               actor['freebase']))
                fOut.flush()
    except IOError as ioError:
        print str(ioError)
    except QueryBadFormed as queryEx:
        print str(queryEx)
    
def get_and_persist_lmdb_films(f):
    try:
        # movieCount = lmdb.get_page_count(Portal.FREEBASE, LMDBMovieConcept.FILM)
        movieCount = 100
        with open(f, 'w') as fOut:
            for i in range(0, movieCount, Config.PAGE_SIZE):
                print 'getting films from %i to %i' % (i, (i + Config.PAGE_SIZE))
                films = lmdb.get_films(Portal.FREEBASE, 
                                       i, #offset 
                                       Config.PAGE_SIZE)
                
                for film in films:
                    fOut.write("%s;%s;%s;%s;%s\n" % (film['filmid'],
                                                     film['name'],
                                                     film['actors'],
                                                     film['date'],
                                                     film['freebase']))
                fOut.flush()
    except IOError as ioError:
        print str(ioError)

# hit and run
#get_and_persist_lmdb_actors('../out/ldmb_actors')
get_and_persist_lmdb_films('../out/lmdb_films')
