import json

TABLE_NAME = 'tweet'

INSERT = 'INSERT INTO %s (%s) VALUES (%s)'

def to_str(value):
    ''' '''
    new_val = (u'%s' % value).replace("u'", '"').replace("'", '"')
    return "'%s'" % new_val


def to_date(value):
    ''' '''
    return "'%s'::timestamp" % value


def to_bool(value):
    ''' '''
    return 'True' if value else 'False'


COLUMNS = [ ('coordinates', ' coordinates', to_str),
                        ('created_at', 'created_at', to_date),
                        ('entities', 'entities', to_str),
                        ('id_str', 'id_str', to_str),
                        ('in_reply_to_screen_name', 'in_reply_to_screen_name', to_str),
                        ('in_reply_to_status_id_str', 'in_reply_to_status_id_str', to_str),
                        ('in_reply_to_user_id_str', 'in_reply_to_user_id_str', to_str),
                        ('place', 'place', to_str),
                        ('retweet_count', 'retweet_count', to_str),
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
        status = json.loads( status )
        values = ', '.join( [ c[2]( status[ c[ 0 ] ] ) for c in self.columns ] ) 
        print INSERT % (self.table_name, self.columns_insert, values)







