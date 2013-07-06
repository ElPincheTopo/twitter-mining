
import json
from utils import Tweet

# ----------------------------------------------------------
def from_int(value):
    ''' '''
    if value is None: return "0"
    return str(value)


# ----------------------------------------------------------
def from_str(value):
    ''' '''
    if value in [ None, '', [], {} ]: return "''"
    new_val = value
    if type( value ) in [ dict, list]:
        new_val = json.dumps( value ).encode( 'utf-8', 'ignore' ).replace("'", '"')
    if isinstance(value, Tweet):
        new_val = str( value )
    return "'%s'" % new_val.replace("'", '"')


# ----------------------------------------------------------
def from_date(value):
    ''' '''
    return "'%s'::timestamp" % value


# ----------------------------------------------------------
def from_bool(value):
    ''' '''
    return 'True' if value else 'False'


__all__ = ['from_int', 'from_str', 'from_date', 'from_bool']

