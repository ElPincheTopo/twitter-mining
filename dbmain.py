import sys, os

import listeners
import handlers
import utils

import time, tweepy, sys
import operator
import traceback

TWEET_COLUMNS = [
        utils.DbColumn( 'coordinates', extract_func=operator.attrgetter('coordinates'), convert_func=utils.from_str ),
        utils.DbColumn( 'created_at', extract_func=operator.attrgetter('created_at'), convert_func=utils.from_date ),
        #utils.DbColumn( 'entities', extract_func=operator.attrgetter('entities'), convert_func=utils.from_str ),
        utils.DbColumn( 'id_str', extract_func=operator.attrgetter('id_str'), convert_func=utils.from_str ),
        utils.DbColumn( 'in_reply_to_screen_name', extract_func=operator.attrgetter('in_reply_to_screen_name'), convert_func=utils.from_str ),
        utils.DbColumn( 'in_reply_to_status_id_str', extract_func=operator.attrgetter('in_reply_to_status_id_str'), convert_func=utils.from_str ),
        utils.DbColumn( 'place', extract_func=operator.attrgetter('place'), convert_func=utils.from_str ),
        utils.DbColumn( 'retweet_count', extract_func=operator.attrgetter('retweet_count'), convert_func=utils.from_int ),
        utils.DbColumn( 'source', extract_func=operator.attrgetter('source'), convert_func=utils.from_str ),
        utils.DbColumn( 'text', extract_func=operator.attrgetter('text'), convert_func=utils.from_str ),
        utils.DbColumn( 'truncated', extract_func=operator.attrgetter('truncated'), convert_func=utils.from_bool )
    ]

def main():

    if None in [os.getenv( 'CONSUMER_KEY' ), os.getenv( 'CONSUMER_SECRET' ), os.getenv( 'APPLICATION_KEY'), os.getenv( 'APPLICATION_SECRET') ]:
        print >> sys.stderr, 'Missing auth tokens'
        sys.exit(1)

    auth = tweepy.OAuthHandler( os.getenv( 'CONSUMER_KEY' ), os.getenv( 'CONSUMER_SECRET' ) )
    auth.set_access_token( os.getenv( 'APPLICATION_KEY'), os.getenv( 'APPLICATION_SECRET') )
    api = tweepy.API(auth)
    #tweet_handler = handlers.SimpleHandler( table='tweet', columns=handlers.TweetColumns )
    #user_handler = handlers.SimpleHandler( table='twitter_user', columns=handlers.UserColumns, property='user' )
    new_handler = handlers.SimpleHandler( table='tweet', columns=TWEET_COLUMNS )
    stream = tweepy.Stream(auth, listeners.DatabaseListener( api, [ new_handler ] ) )

    print >> sys.stderr, "Streaming started..."
    while True:
        try: 
            stream.filter(locations=[ -99.36,19.11,-98.97,19.57 ])
        except Exception, e:
            print  >> sys.stderr,  "error! %s" % e
            traceback.print_exc()
            stream.disconnect()

if __name__ == '__main__':
    main()

