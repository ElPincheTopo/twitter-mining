import sys

from listeners import DatabaseListener
from handlers import SimpleHandler
import time, tweepy, sys

## authentication
auth = tweepy.OAuthHandler('HKxUdwCuKc9sEFsqroOzng', '4cKTl3HXp3lakGos5p7EUs6ojnfrnEVrRmUtDJmQ')
auth.set_access_token('158718524-mZh3DnPnybuMeoLNbZtecirZavjY7pb44gcDkgWq', '2mxRJS0Ge2foPKMIXhE7AiH1kcfLONMfzEfIrxFRA0o')
api = tweepy.API(auth)

def main():
 
    stream = tweepy.Stream(auth, DatabaseListener( api, SimpleHandler() ) )

    print >> sys.stderr, "Streaming started..."
    while True:
        try: 
            stream.filter(locations=[ -99.36,19.11,-98.97,19.57 ])
        except Exception, e:
            print  >> sys.stderr,  "error! %s" % e
            stream.disconnect()

if __name__ == '__main__':
    main()

