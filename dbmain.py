import sys

from listeners import DatabaseListener
import time, tweepy, sys

## authentication
auth = tweepy.OAuthHandler('HKxUdwCuKc9sEFsqroOzng', '4cKTl3HXp3lakGos5p7EUs6ojnfrnEVrRmUtDJmQ')
auth.set_access_token('158718524-mZh3DnPnybuMeoLNbZtecirZavjY7pb44gcDkgWq', '2mxRJS0Ge2foPKMIXhE7AiH1kcfLONMfzEfIrxFRA0o')
api = tweepy.API(auth)

def main():
 
    stream = tweepy.Stream(auth, DatabaseListener(api) )

    print "Streaming started..."
    try: 
        stream.filter(locations=[ -99.36,19.11,-98.97,19.57 ])
    except:
        print "error!"
        stream.disconnect()

if __name__ == '__main__':
    main()

