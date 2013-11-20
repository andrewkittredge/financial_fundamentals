'''
Created on Sep 12, 2013

@author: akittredge
'''

import unittest
import pymongo
from financial_fundamentals.mongo_drivers import MongoIntervalseries,\
    MongoTimeseries
import pytz
from tests.infrastructure import IntervalseriesTestCase

class MongoTestCase(unittest.TestCase):
    host, port = 'localhost', 27017
    def setUp(self):
        client = pymongo.MongoClient(self.host, self.port)
        self.db = client.test
        self.db.prices.drop()
        self.collection = self.db[self.collection_name]
        self.collection.drop()


class MongoIntervalSeriesTestCase(MongoTestCase, IntervalseriesTestCase):
    collection_name = 'fundamentals'
    def setUp(self):
        super(MongoIntervalSeriesTestCase, self).setUp()
        self.cache = self.build_cache(metric=self.metric)

        
    def build_cache(self, metric):
        return MongoIntervalseries(self.collection, metric)
    
    def find_in_database(self, start, end, symbol):
        db_record = self.collection.find({'start' : start,
                              'end' : end,
                              'symbol' : symbol}).next()
        return db_record[self.metric]
        
    def insert_into_database(self, data):
        self.collection.insert(data)
    
    def update_database(self, data, query):
        self.collection.update(spec=query,
                               document={'$set' : data},
                               upsert=True)
        
    
        
class MongoTimeSeriesTestCase(MongoTestCase):
    metric = 'price'
    collection_name = 'prices'
    def setUp(self):
        super(MongoTimeSeriesTestCase, self).setUp()
        self.cache = self.build_cache(metric=self.metric)
        
    def build_cache(self, metric):
        return MongoTimeseries(self.collection, metric)
        
    def test_get(self):
        import datetime
        metric = self.metric
        symbol, date, price = ('ABC', 
                               datetime.datetime(2012, 12, 1, tzinfo=pytz.UTC), 
                               6.5)
        key = {'symbol' : symbol, 'date' : date}
        data = {'symbol' : symbol,
                metric : price,
                'date' : date}
        self.collection.update(key, data, upsert=True)
        cache_date, cache_price = self.cache.get(symbol=symbol, dates=[date]).next()
        self.assertEqual(cache_price, price)
        self.assertEqual(cache_date, date)

    def test_set(self):
        import datetime
        metric = self.metric
        symbol, date, price = ('ABC',
                                datetime.datetime(2012, 12, 1, tzinfo=pytz.UTC), 
                                6.5)
        self.cache.set(symbol=symbol, records=[(date, price)])
        test_data = self.collection.find({'symbol' : symbol})[0]
        self.assertEqual(test_data[metric], price)
        self.assertEqual(test_data['symbol'], symbol)