'''
Created on Jul 28, 2013

@author: akittredge
'''
import pymongo
import pytz
import numpy as np

class MongoCache(object):
    def __init__(self, mongo_collection, metric):
        self._ensure_indexes(mongo_collection)
        self._collection = mongo_collection
        self._metric = metric

class MongoTimeseries(MongoCache):
    @classmethod
    def _ensure_indexes(cls, collection):
        collection.ensure_index([('date', pymongo.ASCENDING), 
                                 ('symbol', pymongo.ASCENDING)])
        collection.ensure_index('symbol')
        
    def get(self, symbol, dates):
        records = self._collection.find({'symbol' : symbol,
                                         'date' : {'$in' : dates},
                                         }).sort('symbol')
        for record in records:
            yield (record['date'].replace(tzinfo=pytz.UTC), 
                   np.float(record[self._metric]))
        
    def set(self, symbol, records):
        for date, value in records:
            key = {'symbol' : symbol, 'date' : date}
            data = {'symbol' : symbol,
                    self._metric : value,
                    'date' : date}
            self._collection.update(key, data, upsert=True)

    @classmethod
    def price_db(cls, host='localhost', port=27017):
        client = pymongo.MongoClient(host, port)
        collection = client.prices.prices
        return cls(collection, 'price')
        
        
class MongoIntervalseries(MongoTimeseries):
    @classmethod
    def _ensure_indexes(cls, collection):
        collection.ensure_index([('start', pymongo.ASCENDING),
                                 ('end', pymongo.ASCENDING),
                                 ('symbol', pymongo.ASCENDING)])
        
    def get(self, symbol, date):
        cursor = self._collection.find({'symbol' : symbol,
                                        'start' : {'$lte' : date},
                                        '$or' : [{'end' : {'$gte' : date}},
                                                  {'end' : None}],
                                        }
                                         )
        try:
            record = cursor.next()
        except StopIteration:
            return None
        else:
            return np.float(record[self._metric])
                    
    def set_interval(self, symbol, start, end, value):
        data = {'symbol' : symbol,
                'start' : start,
                'end' : end,
                self._metric : value}
        self._collection.insert(data)