'''
Created on Jul 28, 2013

@author: akittredge
'''
import pymongo
import numpy as np
import pandas as pd
from abc import ABCMeta

class MongoCache(object):
    def __init__(self, mongo_collection, metric):
        self._ensure_indexes(mongo_collection)
        self._collection = mongo_collection
        self._metric = metric
        
class VectorCacheDriver(object):
    __metaclass__ = ABCMeta

class MongoVectorCacheDriver(VectorCacheDriver):
    def __init__(self, collection):
        self._collection = collection
        
    @classmethod
    def _ensure_indexes(cls, collection):
        collection.ensure_index([('date', pymongo.ASCENDING), 
                                 ('symbol', pymongo.ASCENDING)])
        collection.ensure_index('symbol')
        
    def get(self, metric, indentifiers, index):
        data = pd.DataFrame(index=index)
        uncached_data = {}
        for identifier in indentifiers:
            records = self._collection.find({'identifier' : identifier,
                                             'metric' : metric,
                                             'date' : {'$gte' : index[0],
                                                       '$lte' : index[-1]},
                                             })
            records = list(records)
            if not records:
                uncached_data[identifier] = index
            else:
                identifier_data = pd.DataFrame(records, columns=['date', 'value'])
                identifier_data.set_index('date', inplace=True)
                identifier_data.rename(columns={'value' : identifier}, inplace=True)
                missing_dates = index - identifier_data.index 
                if len(missing_dates) != 0:
                    uncached_data[identifier] = missing_dates
                data = data.merge(identifier_data,
                                  left_index=True,
                                  right_index=True)
                
        return data, uncached_data
        
    def set(self, metric, data):
        docs = []
        for identifier in data:
            docs.extend({'identifier' : identifier, 
                         'date' : date, 
                         'metric' : metric,
                         'value' : value} for date, value in data[identifier].iteritems())
        self._collection.insert(docs)

    @classmethod
    def price_db(cls, host='localhost', port=27017):
        client = pymongo.MongoClient(host, port)
        collection = client.prices.prices
        return cls(collection, 'price')
    