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



