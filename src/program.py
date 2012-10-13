import csv

from SPARQLWrapper import SPARQLWrapper
from SPARQLWrapper.SPARQLExceptions import QueryBadFormed

from lmdb_wrapper import LMDBWrapper
from utils import Config, Portal, SPARQLEndpoints, LMDBMovieConcept


# connect to LMDB
sparql_lmdb = SPARQLWrapper(SPARQLEndpoints.LMDB)
lmdb = LMDBWrapper(sparql_lmdb)

def get_and_persist_lmdb_actors(f):
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
                fOut.flush()
    except IOError as ioError:
        print str(ioError)
    except QueryBadFormed as queryEx:
        print str(queryEx)
    
def get_and_persist_lmdb_films(f):
    try:
        filmCount = lmdb.get_page_count(Portal.FREEBASE, LMDBMovieConcept.FILM)
        print "loading %i films from lmdb" % filmCount
                
        with open(f, 'w') as fOut:
            csvwriter = csv.DictWriter(fOut, ['filmid', 'name', 'date', 'freebase'], delimiter=';')
            for i in range(0, filmCount, Config.PAGE_SIZE):
                print 'getting films from %i to %i' % (i, (i + Config.PAGE_SIZE))
                films = lmdb.get_films(Portal.FREEBASE, 
                                       i, #offset 
                                       Config.PAGE_SIZE)
                csvwriter.writerows(films)
                fOut.flush()
    except IOError as ioError:
        print str(ioError)

# hit and run
#get_and_persist_lmdb_actors('../out/ldmb_actors')
#get_and_persist_lmdb_films('../out/lmdb_films')
