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
        utils.DbColumn( 'entities', extract_func=operator.attrgetter('entities'), convert_func=utils.from_str ),
        utils.DbColumn( 'id_str', extract_func=operator.attrgetter('id_str'), convert_func=utils.from_str ),
        utils.DbColumn( 'in_reply_to_screen_name', extract_func=operator.attrgetter('in_reply_to_screen_name'), convert_func=utils.from_str ),
        utils.DbColumn( 'in_reply_to_status_id_str', extract_func=operator.attrgetter('in_reply_to_status_id_str'), convert_func=utils.from_str ),
        utils.DbColumn( 'place', extract_func=operator.attrgetter('place'), convert_func=utils.from_str ),
        utils.DbColumn( 'retweet_count', extract_func=operator.attrgetter('retweet_count'), convert_func=utils.from_int ),
        utils.DbColumn( 'source', extract_func=operator.attrgetter('source'), convert_func=utils.from_str ),
        utils.DbColumn( 'text', extract_func=operator.attrgetter('text'), convert_func=utils.from_str ),
        utils.DbColumn( 'truncated', extract_func=operator.attrgetter('truncated'), convert_func=utils.from_bool ),
        utils.DbColumn( 'user_id', extract_func=operator.attrgetter('user.id_str'), convert_func=utils.from_str )
    ]

USER_COLUMNS = [
        utils.DbColumn( 'created_at', extract_func=operator.attrgetter( 'user.created_at' ), convert_func=utils.from_date ),
        utils.DbColumn( 'description', extract_func=operator.attrgetter( 'user.description' ), convert_func=utils.from_str ),
        utils.DbColumn( 'favourites_count', extract_func=operator.attrgetter( 'user.favourites_count' ), convert_func=utils.from_int ),
        utils.DbColumn( 'followers_count', extract_func=operator.attrgetter( 'user.followers_count' ), convert_func=utils.from_int ),
        utils.DbColumn( 'friends_count', extract_func=operator.attrgetter( 'user.friends_count' ), convert_func=utils.from_int ),
        utils.DbColumn( 'geo_enabled', extract_func=operator.attrgetter( 'user.geo_enabled' ), convert_func=utils.from_bool ),
        utils.DbColumn( 'id_str', extract_func=operator.attrgetter( 'user.id_str' ), convert_func=utils.from_str ),
        utils.DbColumn( 'lang', extract_func=operator.attrgetter( 'user.lang' ), convert_func=utils.from_str ),
        utils.DbColumn( 'location', extract_func=operator.attrgetter( 'user.location' ), convert_func=utils.from_str ),
        utils.DbColumn( 'name', extract_func=operator.attrgetter( 'user.name' ), convert_func=utils.from_str ),
        utils.DbColumn( 'protected', extract_func=operator.attrgetter( 'user.protected' ), convert_func=utils.from_bool ),
        utils.DbColumn( 'screen_name', extract_func=operator.attrgetter( 'user.screen_name' ), convert_func=utils.from_str ),
        utils.DbColumn( 'statuses_count', extract_func=operator.attrgetter( 'user.statuses_count' ), convert_func=utils.from_int ),
        utils.DbColumn( 'time_zone', extract_func=operator.attrgetter( 'user.time_zone' ), convert_func=utils.from_str ),
        utils.DbColumn( 'url', extract_func=operator.attrgetter( 'user.url' ), convert_func=utils.from_str ),
        utils.DbColumn( 'utc_offset', extract_func=operator.attrgetter( 'user.utc_offset' ), convert_func=utils.from_int ),
        utils.DbColumn( 'verified', extract_func=operator.attrgetter( 'user.verified' ), convert_func=utils.from_bool )
]

def main():

    if None in [os.getenv( 'CONSUMER_KEY' ), os.getenv( 'CONSUMER_SECRET' ), os.getenv( 'APPLICATION_KEY'), os.getenv( 'APPLICATION_SECRET') ]:
        print >> sys.stderr, 'Missing auth tokens'
        sys.exit(1)

    auth = tweepy.OAuthHandler( os.getenv( 'CONSUMER_KEY' ), os.getenv( 'CONSUMER_SECRET' ) )
    auth.set_access_token( os.getenv( 'APPLICATION_KEY'), os.getenv( 'APPLICATION_SECRET') )
    api = tweepy.API(auth)
    tweet_handler = handlers.SimpleHandler( table='tweet', columns=TWEET_COLUMNS )
    user_handler = handlers.SimpleHandler( table='twitter_user', columns=USER_COLUMNS )
    stream = tweepy.Stream(auth, listeners.DatabaseListener( api, [ tweet_handler, user_handler ] ) )

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

