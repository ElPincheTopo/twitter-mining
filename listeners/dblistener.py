from tweepy import StreamListener, API
import json, time, sys

# ========================================
class DatabaseListener( StreamListener ):

    # ----------------------------------------------------------
    def __init__(self, api, handlers):
        ''' '''
        self.api = api
        self.handlers = handlers
        self.counter = 0

    # ----------------------------------------------------------
    def on_data(self, data):
        ''' '''
        if  'in_reply_to_status' in data:
            return self.on_status(data)
        elif 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False
        elif 'warning' in data:
            warning = json.loads(data)['warnings']
            print warning['message']
            return false

    # ----------------------------------------------------------
    def on_status(self, status):
        ''' '''
        print  'on status'
        for handler in self.handlers:
            handler.store( json.loads( status.strip().decode( 'utf-8', 'ignore' ) ) )
        return True

    # ----------------------------------------------------------
    def on_delete(self, status_id, user_id):
        ''' '''
        return

    # ----------------------------------------------------------
    def on_limit(self, track):
        ''' '''
        return

    # ----------------------------------------------------------
    def on_error(self, status_code):
        ''' '''
        return False

    # ----------------------------------------------------------
    def on_timeout(self):
        ''' '''
        sys.stderr.write("Timeout, sleeping for 60 seconds...\n")
        time.sleep(60)
        return 

