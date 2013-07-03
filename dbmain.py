import sys, os

import listeners
import handlers
import time, tweepy, sys

import traceback

## authentication


def main():

    if None in [os.getenv( 'CONSUMER_KEY' ), os.getenv( 'CONSUMER_SECRET' ), os.getenv( 'APPLICATION_KEY'), os.getenv( 'APPLICATION_SECRET') ]:
        print >> sys.stderr, 'Missing auth tokens'
        sys.exit(1)

    auth = tweepy.OAuthHandler( os.getenv( 'CONSUMER_KEY' ), os.getenv( 'CONSUMER_SECRET' ) )
    auth.set_access_token( os.getenv( 'APPLICATION_KEY'), os.getenv( 'APPLICATION_SECRET') )
    api = tweepy.API(auth)
    tweet_handler = handlers.SimpleHandler( table='tweet', columns=handlers.TweetColumns )
    user_handler = handlers.SimpleHandler( table='twitter_user', columns=handlers.UserColumns, property='user' )
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

