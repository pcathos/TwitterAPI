# -*- coding: utf-8 -*-
from tweepy import API
from tweepy import Cursor
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from datetime import datetime
import pymongo
import json
import twitter_credentials
# import selectDB
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
import config

'''
转换时间格式 把数据库里面 old_data 格式转换成 画图可以识别的格式
https://www.cnblogs.com/65702708/archive/2011/04/17/2018936.html
'''


def convertDate(old_date):
    new_date = datetime.strptime(old_date, '%a %b %d %H:%M:%S +0000 %Y');
    return new_date


def cover_to_min(old_date):
    new_date = datetime.strptime(old_date, '%a %b %d %H:%M:%S +0000 %Y');
    date_min = new_date.strftime("%M");
    return date_min


# # # # TWITTER CLIENT # # # #
class TwitterClient():
    def __init__(self, twitter_user=None):
        self.auth = TwitterAuthenticator().authenticate_twitter_app()
        self.twitter_client = API(self.auth)

        self.twitter_user = twitter_user

    def get_twitter_client_api(self):
        return self.twitter_client

    def get_user_timeline_tweets(self, num_tweets):
        tweets = []
        for tweet in Cursor(self.twitter_client.user_timeline, id=self.twitter_user).items(num_tweets):
            tweets.append(tweet)
        return tweets

    def get_friend_list(self, num_friends):
        friend_list = []
        for friend in Cursor(self.twitter_client.friends, id=self.twitter_user).items(num_friends):
            friend_list.append(friend)
        return friend_list

    def get_home_timeline_tweets(self, num_tweets):
        home_timeline_tweets = []
        for tweet in Cursor(self.twitter_client.home_timeline, id=self.twitter_user).items(num_tweets):
            home_timeline_tweets.append(tweet)
        return home_timeline_tweets


# # # # TWITTER AUTHENTICATER # # # #
class TwitterAuthenticator():

    def authenticate_twitter_app(self):
        auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        return auth


# # # # TWITTER STREAMER # # # #
class TwitterStreamer():
    """
    Class for streaming and processing live tweets.
    """

    def __init__(self):
        self.twitter_autenticator = TwitterAuthenticator()

    def stream_tweets(self, fetched_tweets_filename, hash_tag_list):
        # This handles Twitter authetification and the connection to Twitter Streaming API
        listener = TwitterListener(fetched_tweets_filename)
        auth = self.twitter_autenticator.authenticate_twitter_app()
        stream = Stream(auth, listener)

        # This line filter Twitter Streams to capture data by the keywords:
        stream.filter(track=hash_tag_list)


# # # # TWITTER STREAM LISTENER # # # #
class TwitterListener(StreamListener):
    """
    This is a basic listener that just prints received tweets to stdout.
    """

    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    def on_data(self, data):
        try:
            print(data)
            with open(self.fetched_tweets_filename, 'a') as tf:
                tf.write(data)
            return True
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True

    def on_error(self, status):
        if status == 420:
            # Returning False on_data method in case rate limit occurs.
            return False
        print(status)


class TweetAnalyzer():
    """
    Functionality for analyzing and categorizing content from tweets.
    """
    """
    这里面是可以用的 数据 这写加上昨天晚上给你发的selectDB就能解决大部分数据 你看画折线图和直方图有啥区别
    """

    def tweets_to_data_frame(self, tweets):
        # 这里是生成表  比如第一个  就是一个叫tweets的列 里面的内容就是每一条tweet的text，后面类推

        df = pd.DataFrame(data=[tweet["text"] for tweet in tweets], columns=['tweets'])

        # df['id'] = np.array([tweet["id"] for tweet in tweets])
        df['len'] = np.array([len(tweet["text"]) for tweet in tweets])
        df['date'] = np.array([convertDate(tweet["created_at"]) for tweet in tweets])
        # df['source'] = np.array([tweet["source"] for tweet in tweets])
        # df['likes'] = np.array([tweet["favorite_count"] for tweet in tweets])
        # df['retweets'] = np.array([tweet["retweet_count"] for tweet in tweets])
        # df['geo'] = np.array([tweet["geo"] for tweet in tweets])

        return df

def draw():
    client = MongoClient(config.MONGO_HOST)
    restdb = client.tweet_db_rest
    streamdb = client.tweet_db_stream
    restcol = restdb.tweet_collection
    streamcol = streamdb.tweet_collection

    arr = []
    n = 0


    for rest_tweet in restcol.find():
        stamp = str(cover_to_min(rest_tweet['created_at']))
        arr.append(int(stamp))
        n = n + 1
    for stream_tweet in streamcol.find():
        stamp = str(cover_to_min(stream_tweet['created_at']))
        arr.append(int(stamp))
        n = n + 1
    print (n)

    a = np.array(arr)
    print(a)
    plt.hist(a, bins=[0, 10, 20, 30, 40, 50, 60], edgecolor="black")
    plt.title("Histogram of All Data")
    plt.xlabel("Minutes")
    plt.ylabel("Amount of Tweets")
    plt.show()

def draw_rest():
    client = MongoClient(config.MONGO_HOST)
    restdb = client.tweet_db_rest
    restcol = restdb.tweet_collection

    arr = []
    n = 0

    for rest_tweet in restcol.find():
        stamp = str(cover_to_min(rest_tweet['created_at']))
        arr.append(int(stamp))
        n = n + 1
    print(n)

    a = np.array(arr)
    print(a)
    plt.hist(a, bins=[0, 10, 20, 30, 40, 50, 60], edgecolor="black")
    plt.title("Histogram of REST API")
    plt.xlabel("Minutes")
    plt.ylabel("Amount of Tweets")
    plt.show()

def draw_stream():
    client = MongoClient(config.MONGO_HOST)
    streamdb = client.tweet_db_stream
    streamcol = streamdb.tweet_collection

    arr = []
    n = 0

    for stream_tweet in streamcol.find():
        stamp = str(cover_to_min(stream_tweet['created_at']))
        arr.append(int(stamp))
        n = n + 1
    print(n)

    a = np.array(arr)
    print(a)
    plt.hist(a, bins=[0, 10, 20, 30, 40, 50, 60], edgecolor="black")
    plt.title("Histogram of Streaming API")
    plt.xlabel("Minutes")
    plt.ylabel("Amount of Tweets")
    plt.show()

def draw_geo():
    twitter_client = TwitterClient()
    tweet_analyzer = TweetAnalyzer()
    api = twitter_client.get_twitter_client_api()
    client = MongoClient(config.MONGO_HOST)
    db = client.tweet_db_rest
    tweet_collection = db.tweet_collection

    tweets = []
    arr = []
    tweets_count = 0
    i = 0

    for tweet in tweet_collection.find({"geo": None}):
        stamp = str(cover_to_min(tweet['created_at']))
        arr.append(int(stamp))
        i = i + 1
    print (tweets_count)
    print(i)

    a = np.array(arr)
    print(a)
    plt.hist(a, bins=[0, 10, 20, 30, 40, 50, 60], edgecolor="black")
    plt.title("Histogram of geo")
    plt.xlabel("Minutes")
    plt.ylabel("Amount of Tweets")
    plt.show()


def draw_place():
    twitter_client = TwitterClient()
    tweet_analyzer = TweetAnalyzer()
    api = twitter_client.get_twitter_client_api()
    client = MongoClient(config.MONGO_HOST)
    db = client.tweet_db_stream
    tweet_collection = db.tweet_collection

    tweets = []
    arr = []
    tweets_count = 0
    pla = 0

    for tweet in tweet_collection.find({"place.full_name": "Glasgow, Scotland"}):
        stamp = str(cover_to_min(tweet['created_at']))
        arr.append(int(stamp))
        pla = pla + 1
    print(tweets_count)
    print(pla)

    a = np.array(arr)
    print(a)
    plt.hist(a, bins=[0, 10, 20, 30, 40, 50, 60], edgecolor="black")
    plt.title("Histogram of Place in Glasgow")
    plt.xlabel("Minutes")
    plt.ylabel("Amount of Tweets")
    plt.show()


def draw_retweeted():
    twitter_client = TwitterClient()
    tweet_analyzer = TweetAnalyzer()
    api = twitter_client.get_twitter_client_api()
    client = MongoClient(config.MONGO_HOST)
    db = client.tweet_db_stream
    tweet_collection = db.tweet_collection

    tweets = []
    arr = []
    tweets_count = 0
    n = 0

    for tweet in tweet_collection.find({"retweeted_status.retweeted": False}):
        stamp = str(cover_to_min(tweet['created_at']))
        arr.append(int(stamp))
        n = n + 1
    print(tweets_count)
    print(n)

    a = np.array(arr)
    print(a)
    plt.hist(a, bins=[0, 10, 20, 30, 40, 50, 60], edgecolor="black")
    plt.title("Histogram of Re-Tweets")
    plt.xlabel("Minutes")
    plt.ylabel("Amount of Tweets")
    plt.show()


def draw_quote():
    twitter_client = TwitterClient()
    tweet_analyzer = TweetAnalyzer()
    api = twitter_client.get_twitter_client_api()
    client = MongoClient(config.MONGO_HOST)
    db = client.tweet_db_stream
    tweet_collection = db.tweet_collection

    tweets = []
    arr = []
    tweets_count = 0
    n = 0

    for tweet in tweet_collection.find({"is_quote_status": True}):
        stamp = str(cover_to_min(tweet['created_at']))
        arr.append(int(stamp))
        n = n + 1
    print(tweets_count)
    print(n)

    a = np.array(arr)
    print(a)
    plt.hist(a, bins=[0, 10, 20, 30, 40, 50, 60], edgecolor="black")
    plt.title("Histogram of Quote")
    plt.xlabel("Minutes")
    plt.ylabel("Amount of Tweets")
    plt.show()

def draw_rongyu():
    client = MongoClient(config.MONGO_HOST)
    db = client.tweet_db_stream
    tweet_collection = db.tweet_collection
    restdb = client.tweet_db_rest
    streamdb = client.tweet_db_stream
    restcol = restdb.tweet_collection
    streamcol = streamdb.tweet_collection

    rest_tweet = []
    stream_tweet = []
    arr = []
    tweets_count = 0

    redundant = 0

    for rest_tweet in restcol.find():
        for stream_tweet in streamcol.find():
            if rest_tweet['id'] == stream_tweet['id']:
                stamp = str(cover_to_min(stream_tweet['created_at']))
                arr.append(int(stamp))
                redundant = redundant + 1

    print(redundant)
    print(tweets_count)


    a = np.array(arr)
    print(a)
    plt.hist(a, bins=[0, 10, 20, 30, 40, 50, 60], edgecolor="black")
    plt.title("Histogram of Redundant")
    plt.xlabel("Minutes")
    plt.ylabel("Amount of Tweets")
    plt.show()

