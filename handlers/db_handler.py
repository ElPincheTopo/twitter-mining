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
        new_val = json.dumps( value ).encode( 'utf-8', errors='ignore' ).replace("'", '"')
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
                        ('truncated', 'truncated', to_bool),
                        ('user', 'user_data', to_str) ]

# ========================================
class SimpleHandler():
    ''' '''

    # ----------------------------------------------------------
    def __init__(self, table=TABLE_NAME, columns=COLUMNS):
        ''' '''
        self.columns = columns
        self.table_name = table
        self.columns_insert = ', '.join( [c[1] for c in self.columns ] )

    # ----------------------------------------------------------
    def store( self, status ):
        ''' '''
        status = json.loads( status.decode( 'utf-8', 'ignore' ) )
        values = ', '.join( [ c[2]( status[ c[ 0 ] ] ) for c in self.columns ] )
        print >> sys.stdout, INSERT % ( self.table_name, self.columns_insert, values )



# ========================================
if __name__ == '__main__':
    import fileinput
    handler = SimpleHandler()
    for line in fileinput.input():
        handler.store( line.strip() )



