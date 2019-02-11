# -*- coding: utf-8 -*-
import pymongo
import config
myclient = pymongo.MongoClient(config.MONGO_HOST)
restdb = myclient["tweet_db_rest"]
streamdb = myclient["tweet_db_stream"]
restcol = restdb["tweet_collection"]
streamcol = streamdb["tweet_collection"]

mycol = streamdb["tweet_collection"]

geoQuery = {"geo":None}
placeQuery = {"place.full_name":"Glasgow, Scotland"}
retweetsQuery = {"retweeted_status.retweeted":False}
quoteStatusTrueQuery = {"is_quote_status":True}

"""
这里是直接从数据库里面提取要算的数
"""
geoNoneCount = mycol.find(geoQuery).count()
placeGlaCount = mycol.find(placeQuery).count()
retweetedCount = mycol.find(retweetsQuery).count()
quoteStatusTrueCount = mycol.find(quoteStatusTrueQuery).count()


# for x in mycol.find(geoQuery):
#     print(x)

print(geoNoneCount,placeGlaCount,retweetedCount,quoteStatusTrueCount)
rongyu = 0
for rest_tweet in restcol.find():
    for stream_tweet in streamcol.find():
        if rest_tweet['id'] == stream_tweet['id']:
            rongyu = rongyu +1

print (rongyu)