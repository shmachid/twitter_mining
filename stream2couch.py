# -*- coding: utf-8 -*-

import sys
import time
from datetime import datetime
import tweepy
import json
import jsonpickle
import couchdb


if (len(sys.argv) < 2):
    print "Usage: please check your parameter"
    sys.exit()


QUERY = sys.argv[1:]

SERVER_URL = 'YOUR COUCHDB URL'    #ex: http://localhost:5984
DB_USER = 'YOUR USER'
DB_PASSWD = 'YOUR PASSWORD'
DB = 'YOUR DB NAME'

CONSUMER_KEY = 'YOUR CONSUMERKEY'
CONSUMER_SECRET = 'YOUR CONSUMER_SECRET'
ACCESS_TOKEN = 'YOUR ACCESS_TOKEN'
ACCESS_TOKEN_SECRET = 'YOUR ACCESS_TOKEN_SECRET'

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

server = couchdb.Server(SERVER_URL)
server.resource.credentials = (DB_USER, DB_PASSWD)


try:
    db = server.create(DB)

except couchdb.http.PreconditionFailed, e:
    db = server[DB]

class CustomStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        results = {}
        try:
            if status.id_str in db:
                return True

            pickled = jsonpickle.encode(status)
            results = json.loads(pickled)
            del results['_api']

            db[results['id_str']] = results

        except Exception, e:
            print >> sys.stderr, "Encountered Exception:", e
            pass

    def on_error(self, status_code):
        print >> sys.stderr, "Encountered error with status code:", status_code
        return True

    def on_timeout(self):
        print >> sys.stderr, "Timeout..."
        return True 


streaming_api = tweepy.streaming.Stream(auth, CustomStreamListener(), timeout=60)
print >> sys.stderr, 'Filtering parameters: "%s"' % (' '.join(sys.argv[1:]),)

try:  # sample(): streaming_api.sample()
    streaming_api.filter(follow=None, track=QUERY)

except Exception, e:
    print >> sys.stderr, "Error: '%s' '%s'" % (str(datetime.now()), str(e))

finally:
    print >> sys.stderr, "disconnecting..."
    streaming_api.disconnect()
