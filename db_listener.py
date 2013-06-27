from tweepy import StreamListener
import json, time, sys

# =======================
class DatabaseListener( StreamListener ):
    ''' Store tweets in db'''

    def __init__(self, api = None, handler=None):
        ''' Builds a new DataBase Listener
            @params: handler: The database handler
        '''
        self.api = api or API()
        self.handler = handler
        self.counter = 0

    def on_data(self, data):
        ''' Main hander'''
        self.counter += 1
        print >> open('joe.txt', 'a'), data

