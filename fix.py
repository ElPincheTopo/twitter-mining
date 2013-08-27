
import sys
import utils
from handlers import SimpleHandler
import operator
import simplejson as json

USER_COLUMNS = [
        utils.DbColumn( 'created_at', extract_func=operator.attrgetter( 'created_at' ), convert_func=utils.from_date ),
        utils.DbColumn( 'description', extract_func=operator.attrgetter( 'description' ), convert_func=utils.from_str ),
        utils.DbColumn( 'favourites_count', extract_func=operator.attrgetter( 'favourites_count' ), convert_func=utils.from_int ),
        utils.DbColumn( 'followers_count', extract_func=operator.attrgetter( 'followers_count' ), convert_func=utils.from_int ),
        utils.DbColumn( 'friends_count', extract_func=operator.attrgetter( 'friends_count' ), convert_func=utils.from_int ),
        utils.DbColumn( 'geo_enabled', extract_func=operator.attrgetter( 'geo_enabled' ), convert_func=utils.from_bool ),
        utils.DbColumn( 'id_str', extract_func=operator.attrgetter( 'id_str' ), convert_func=utils.from_str ),
        utils.DbColumn( 'lang', extract_func=operator.attrgetter( 'lang' ), convert_func=utils.from_str ),
        utils.DbColumn( 'location', extract_func=operator.attrgetter( 'location' ), convert_func=utils.from_str ),
        utils.DbColumn( 'name', extract_func=operator.attrgetter( 'name' ), convert_func=utils.from_str ),
        utils.DbColumn( 'protected', extract_func=operator.attrgetter( 'protected' ), convert_func=utils.from_bool ),
        utils.DbColumn( 'screen_name', extract_func=operator.attrgetter( 'screen_name' ), convert_func=utils.from_str ),
        utils.DbColumn( 'statuses_count', extract_func=operator.attrgetter( 'statuses_count' ), convert_func=utils.from_int ),
        utils.DbColumn( 'time_zone', extract_func=operator.attrgetter( 'time_zone' ), convert_func=utils.from_str ),
        utils.DbColumn( 'url', extract_func=operator.attrgetter( 'url' ), convert_func=utils.from_str ),
        utils.DbColumn( 'utc_offset', extract_func=operator.attrgetter( 'utc_offset' ), convert_func=utils.from_int ),
        utils.DbColumn( 'verified', extract_func=operator.attrgetter( 'verified' ), convert_func=utils.from_bool )
]

sHandler = SimpleHandler(table='twitter_user', columns=USER_COLUMNS)

for c, line in enumerate( open( sys.argv[1] ) ):
    data = line.strip().split('|')
    tweet_id, user_data = data[0], data[1].replace('""', '"')[1:-1]
    tweet = utils.Tweet( from_dict=json.loads( user_data ) )
    
    user_id = tweet.id_str
    print "UPDATE tweet SET user_id='%s' WHERE id_str='%s';" % (user_id, tweet_id)   
    sHandler.store( tweet )
