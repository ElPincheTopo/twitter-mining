import sys
sys.path.append('tweepy-1.8-py2.7.egg')

from slistener import SListener
import time, tweepy, sys

## authentication
auth = tweepy.OAuthHandler('HKxUdwCuKc9sEFsqroOzng', '4cKTl3HXp3lakGos5p7EUs6ojnfrnEVrRmUtDJmQ')
auth.set_access_token('158718524-mZh3DnPnybuMeoLNbZtecirZavjY7pb44gcDkgWq', '2mxRJS0Ge2foPKMIXhE7AiH1kcfLONMfzEfIrxFRA0o')
api = tweepy.API(auth)

def main():
 
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

