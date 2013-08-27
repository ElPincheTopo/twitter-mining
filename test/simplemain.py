import fix_imports

from slistener import SListener
from db_listener import DatabaseListener
import time, tweepy, sys, os

tweepy.debug()

auth = tweepy.OAuthHandler( os.getenv( 'CONSUMER_KEY' ), os.getenv( 'CONSUMER_SECRET' ) )
auth.set_access_token( os.getenv( 'APPLICATION_KEY'), os.getenv( 'APPLICATION_SECRET') )
api = tweepy.API(auth)

def main():
    print os.getenv( 'CONSUMER_KEY' )
    stream = tweepy.Stream(auth, DatabaseListener(api=api, handler=None) )
    print "Sampling started..."

if __name__ == '__main__':
    main()