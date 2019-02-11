import pymongo
from pymongo import MongoClient
import tweepy
import config
import time

'''
OAUTH
'''
def start_streaming_collection():

    auth = tweepy.OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)
    auth.set_access_token(config.ACCESS_TOKEN , config.ACCESS_TOKEN_SECRET)

    api = tweepy.API(auth)

    '''
    connect mongodb database
    '''
    client = MongoClient(config.MONGO_HOST)
    db = client.tweet_db_stream
    tweet_collection = db.tweet_collection
    tweet_collection.create_index([("id", pymongo.ASCENDING)], unique=True)  # make sure the collected tweets are unique

    '''
    define query in Stream API
    '''

    track = ['glasgow']
    # keyword

    locations = [-4.6459,55.648,-3.8428,56.0657]
    #
    '''
    fetch data
    '''
    start = time.time()
    class MyStreamListener(tweepy.StreamListener):

        def on_status(self, status):
            print ("From Streaming API " +status.text)
            try:
                tweet_collection.insert(status._json)
            except:
                pass

            nowtime = time.time()
            if nowtime - start > 3600 :
                print ("finish Streaming catching")
                return False
        def on_error(self, status_code):
            if status_code == 420:
                #returning False in on_data disconnects the stream
                return False


    myStreamListener = MyStreamListener()

    myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

    myStream.filter(track=track, locations = locations, async=True)


    return 0