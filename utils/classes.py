import json

# ==================================================
class Tweet():
    ''' '''
    
    # --------------------------------------------------
    def __init__(self, from_dict=None, **kargs):
        ''' '''
        self.data = {}
        if from_dict and from_dict is not None:
            self.data = from_dict

    # --------------------------------------------------
    def __getattr__(self, attr):
        ''' '''
        ''' WARNING: This must be the most inneficient code ever written '''
        if attr in self.data:
            value = self.data[ attr ]
            return Tweet( from_dict=value ) if type( value ) is dict else value
        else:
            raise AttributeError('Tweet has no attribute %s' % attr)


    # --------------------------------------------------
    def __str__(self):
        ''' '''
        return json.dumps( self.data ).encode( 'utf-8', 'ignore' ).replace("'", '"')


# ==================================================
class DbColumn():
    ''' '''

    # --------------------------------------------------
    def __init__( self, column_name, extract_func, convert_func ):
        ''' '''
        self.column_name = column_name
        self.extract_func = extract_func
        self.convert_func = convert_func