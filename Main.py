import pymongo
import config
import draw
import sys
import os
from pymongo import MongoClient
import twitter
import RESTMongo
import StreamMongo
import import_test_db


init_string = "Hi at first please ensure you finished setting the config.py\n" \
              "It may cost many seconds to check database connection and create collection ..."


def sys_exit():
    sys.exit()



def func1():
    print("No1 Function")
    StreamMongo.start_streaming_collection()
    RESTMongo.start_rest_collection()

def func2():
    print("No2 Function")
    import_test_db.import_test_db()

def func3():
    print("No3 Function")
    draw.draw_rest()
    draw.draw_stream()
    draw.draw()


def func4():
    print("histogram of geo")
    draw.draw_geo()


def func5():
    print("histogram of place")
    draw.draw_place()


def func6():
    print("histogram of re-tweets")
    draw.draw_retweeted()


def func7():
    print("histogram of quote")
    draw.draw_quote()

def func8():
    print("histogram of redundant")
    draw.draw_rongyu()

def func9():
    client = MongoClient(config.MONGO_HOST)
    restdb = client.tweet_db_rest
    streamdb = client.tweet_db_stream
    restcol = restdb.tweet_collection
    streamcol = streamdb.tweet_collection

    empty_rest = restcol.delete_many({})
    empty_stream = streamcol.delete_many({})

    print(empty_rest.deleted_count," data have been deleted")
    print(empty_stream.deleted_count, " data have been deleted")


def numbers_to_functions_to_strings(argument):
    if argument == "0":
        sys.exit();
    switcher = {
        '1': func1,
        '2': func2,
        '3': func3,
        '4': func4,
        '5': func5,
        '6': func6,
        '7': func7,
        '8': func8,
        '9': func9,
    }
    # Get the function from switcher dictionary
    func = switcher.get(argument, lambda: "nothing")
    # Execute the function
    return func()


if __name__ == '__main__':
    '''
    Init
    '''
    print(init_string)

    '''
    OAUTH
    '''
    auth = twitter.oauth.OAuth(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET, config.CONSUMER_KEY,
                               config.CONSUMER_SECRET)
    twitter_api = twitter.Twitter(auth=auth)

    '''
    connect mongodb
    '''

    mongodb_address = config.MONGO_HOST
    client = MongoClient(mongodb_address)
    dbrest = client.tweet_db_rest
    tweet_collection = dbrest.tweet_collection
    tweet_collection.create_index([("id", pymongo.ASCENDING)], unique=True)
    dbstream = client.tweet_db_stream
    tweet_collection = dbstream.tweet_collection
    tweet_collection.create_index([("id", pymongo.ASCENDING)], unique=True)
    print("well done")

    '''
    loop
    '''
    selection = 1000000000000000000

    selection = input(
        "selection\n0 exit \n1 start one hour catching \n2 import a test db instead of catching \n3 draw the whole histogram \n4 draw geo \n5 draw place \n6 draw re-tweets \n7 draw quote \n8 draw redundant\n9 empty\n")
    numbers_to_functions_to_strings(selection)

    while selection != 0:
        os.system('cls' if os.name == 'nt' else 'clear')
        selection = input(
            "selection\n0 exit \n1 start one hour catching \n2 import a test db instead of catching \n3 draw the whole histogram \n4 draw geo \n5 draw place \n6 draw re-tweets \n7 draw quote \n8 draw redundant \n9 empty\n")
        numbers_to_functions_to_strings(selection)
