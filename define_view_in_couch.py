# -*- coding: utf-8 -*-

import couchdb
from couchdb.design import ViewDefinition


SERVER_URL = 'YOUR COUCHDB URL'    #ex: http://localhost:5984
DB_USER = 'YOUR USER'
DB_PASSWD = 'YOUR PASSWORD'
DB = 'YOUR DB NAME'

server = couchdb.Server(SERVER_URL)
server.resource.credentials = (DB_USER, DB_PASSWD)


try:
    db = server.create(DB)

except couchdb.http.PreconditionFailed, e:
    db = server[DB]

    def mapper(doc):
        if doc['author']['location'] == "Tokyo":
            yield (doc['id'], doc['text'])


    ## if you need to use reduce function, please remove bellow the comment-tag.
    #def reducer(keys, values, rereduce):
    #    return max(values)


    view = ViewDefinition('index', 'map_location', mapper, language='python')
    #view = ViewDefinition('index', 'map_location', mapper, reducer, language='python')
    view.sync(db)

    records = [(row.key, row.value) for row in db.view('index/map_location')]

    for record in records:
        print record[1]    #value
