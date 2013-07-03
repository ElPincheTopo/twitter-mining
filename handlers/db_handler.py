import json
import sys, codecs

# set up output encoding
if not sys.stdout.isatty():
    # here you can set encoding for your 'out.txt' file
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

TABLE_NAME = 'tweet'

INSERT = 'INSERT INTO %s (%s) VALUES (%s);'

# ----------------------------------------------------------
def to_int(value):
    ''' '''
    return str(value)


# ----------------------------------------------------------
def to_str(value):
    ''' '''
    if value in [ None, '', [], {} ]: return "''"
    new_val = value
    if type( value ) is dict:
        new_val = json.dumps( value ).encode( 'utf-8', 'ignore' ).replace("'", '"')
    return "'%s'" % new_val.replace("'", '"')


# ----------------------------------------------------------
def to_date(value):
    ''' '''
    return "'%s'::timestamp" % value


# ----------------------------------------------------------
def to_bool(value):
    ''' '''
    return 'True' if value else 'False'


# ----------------------------------------------------------
COLUMNS = [ ('coordinates', ' coordinates', to_str),
                        ('created_at', 'created_at', to_date),
                        ('entities', 'entities', to_str),
                        ('id_str', 'id_str', to_str),
                        ('in_reply_to_screen_name', 'in_reply_to_screen_name', to_str),
                        ('in_reply_to_status_id_str', 'in_reply_to_status_id_str', to_str),
                        ('in_reply_to_user_id_str', 'in_reply_to_user_id_str', to_str),
                        ('place', 'place', to_str),
                        ('retweet_count', 'retweet_count', to_int),
                        ('source', 'source', to_str),
                        ('text', 'text', to_str),
                        ('truncated', 'truncated', to_bool)
          ]


USER_COLUMNS = [
    ('created_at', 'created_at', to_date),
    ('description', 'description', to_str),
    ('favourites_count', 'favourites_count', to_int),
    ('followers_count', 'followers_count', to_int),
    ('friends_count', 'friends_count', to_int),
    ('geo_enabled', 'geo_enabled', to_bool),
    ('id_str', 'id_str', to_str),
    ('lang', 'lang', to_str),
    ('location', 'location', to_str),
    ('name', 'name', to_str),
    ('protected', 'protected', to_bool),
    ('screen_name', 'screen_name', to_str),
    ('statuses_count', 'statuses_count', to_int),
    ('time_zone', 'time_zone', to_str),
    ('url', 'url', to_str),
    ('utc_offset', 'utc_offset', to_int),
    ('verified', 'verified', to_bool)
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
        self.columns_insert = ', '.join( [ column for _, column, _ in self.columns ] )

    # ----------------------------------------------------------
    def store( self, status ):
        ''' '''
        if self.property is not None:
            status = status[ self.property ]
        values = ', '.join( [ func( status[ column_name ] ) for column_name, _, func in self.columns ] )
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



