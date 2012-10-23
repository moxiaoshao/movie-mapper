"""
Returns the rdf types of all instances in LMDB which
have a owl:sameas relation to dbpedia. Types are grouped
and ordered by count descending.
"""

from SPARQLWrapper import SPARQLWrapper, JSON

lmdb = SPARQLWrapper('http://data.linkedmdb.org/sparql')

query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX movie: <http://data.linkedmdb.org/resource/movie/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>

    SELECT ?type (count(?instance) as ?count)
    WHERE {
        ?instance owl:sameAs ?link FILTER regex(str(?link), "^http://dbpedia.org", 'i') .
        ?instance rdf:type ?type .
    }
    GROUP BY ?type
    ORDER BY DESC(?count) ?type
""" 

lmdb.setQuery(query)
lmdb.setReturnFormat(JSON)

results = lmdb.query().convert()

d = {}
n = 0
for result in results['results']['bindings']:
    d[result['type']['value']] = int(result['count']['value'])

for key in sorted(d, key=d.get, reverse=True):
    print d[key], key
    n += d[key]

print "total: %i" % n
