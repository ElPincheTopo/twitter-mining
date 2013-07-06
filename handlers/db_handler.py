import simplejson as json
import sys, codecs

from utils import Tweet
from utils import *

# set up output encoding
if not sys.stdout.isatty():
    # here you can set encoding for your 'out.txt' file
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

TABLE_NAME = 'tweet'

INSERT = 'INSERT INTO %s (%s) VALUES (%s);'

# ----------------------------------------------------------


USER_COLUMNS = [
    ('created_at', 'created_at', from_date),
    ('description', 'description', from_str),
    ('favourites_count', 'favourites_count', from_int),
    ('followers_count', 'followers_count', from_int),
    ('friends_count', 'friends_count', from_int),
    ('geo_enabled', 'geo_enabled', from_bool),
    ('id_str', 'id_str', from_str),
    ('lang', 'lang', from_str),
    ('location', 'location', from_str),
    ('name', 'name', from_str),
    ('protected', 'protected', from_bool),
    ('screen_name', 'screen_name', from_str),
    ('statuses_count', 'statuses_count', from_int),
    ('time_zone', 'time_zone', from_str),
    ('url', 'url', from_str),
    ('utc_offset', 'utc_offset', from_int),
    ('verified', 'verified', from_bool)
]


# ========================================
class SimpleHandler():
    ''' '''

    # ----------------------------------------------------------
    def __init__(self, table, columns, property=None):
        ''' '''
        self.columns = columns
        self.table_name = table
        self.property = property
        self.columns_insert = ', '.join( [ column.column_name for column in self.columns ] )

    # ----------------------------------------------------------
    def store( self, tweet ):
        ''' '''
        values = ', '.join( [ column.convert_func( column.extract_func( tweet ) ) for column in self.columns ] )
        print INSERT % ( self.table_name, self.columns_insert, values )
        



# ========================================
if __name__ == '__main__':
    import fileinput
    tuit_handler = SimpleHandler(table='tweet', columns=COLUMNS)
    #user_handler = SimpleHanlder(table='tweet_user', columns=USER_COLUMNS)
    for line in fileinput.input():
        status = json.loads( line.strip().decode( 'utf-8', 'ignore' ) )
        print status
        #print >> sys.stdout, tuit_handler.store( status )



