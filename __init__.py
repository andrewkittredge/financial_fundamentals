
import pymongo
from financial_fundamentals.mongo_timeseries import MongoTimeseries


def mongo_fundamentals_cache(metric, mongo_host='localhost', mongo_port=27017):
    mongo_client = pymongo.MongoClient(mongo_host, mongo_port)
    mongo_collection = mongo_client.fundamentals.fundamentals
    db = MongoTimeseries(mongo_collection=mongo_collection, 
                         metric=metric)
    
    