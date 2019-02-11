import time
import pymongo
from pymongo import MongoClient
import twitter
import config


def start_rest_collection():
    runtime = 0
    while 1:
        rest_collection_once()
        time.sleep(10)
        runtime=runtime+10
        if runtime > 3600:
            print ("finish REST catching")
            return 0
    return 0
def rest_collection_once():

    ''':
    OAUTH
    '''

    auth = twitter.oauth.OAuth(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET, config.CONSUMER_KEY, config.CONSUMER_SECRET)
    twitter_api = twitter.Twitter(auth=auth)

    '''
    connect mongodb database
    '''
    client = MongoClient(config.MONGO_HOST)
    db = client.tweet_db_rest
    tweet_collection = db.tweet_collection
    tweet_collection.create_index([("id", pymongo.ASCENDING)], unique=True)  # make sure the collected tweets are unique

    '''
    define query in REST API
    '''

    count = 100

    q = "place:791e00bcadc4615f"

    '''
    fetch data
    '''

    search_results = twitter_api.search.tweets(count=count,q=q)
    # pprint(search_results['search_metadata'])

    statuses = search_results["statuses"]

    since_id_new = statuses[-1]['id']

    for statuse in statuses:

        try:

            tweet_collection.insert(statuse)
            print("From REST API " + statuse['text'])

        except:
            pass