import sys
sys.path.append('tweepy-1.8-py2.7.egg')

from slistener import SListener
import time, tweepy, sys


def main():

    if None in [os.getenv( 'CONSUMER_KEY' ), os.getenv( 'CONSUMER_SECRET' ), os.getenv( 'APPLICATION_KEY'), os.getenv( 'APPLICATION_SECRET') ]:
        print >> sys.stderr, 'Missing auth tokens'
        sys.exit(1)

    auth = tweepy.OAuthHandler( os.getenv( 'CONSUMER_KEY' ), os.getenv( 'CONSUMER_SECRET' ) )
    auth.set_access_token( os.getenv( 'APPLICATION_KEY'), os.getenv( 'APPLICATION_SECRET') )
    api = tweepy.API(auth)
 
    listen = SListener(api, 'mexico')
    stream = tweepy.Stream(auth, listen)

    print "Streaming started..."
    try: 
        stream.filter(locations=[ -99.36,19.11,-98.97,19.57 ])
    except:
        print "error!"
        stream.disconnect()

if __name__ == '__main__':
    main()

