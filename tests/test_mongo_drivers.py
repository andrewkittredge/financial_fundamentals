'''
Created on Sep 12, 2013

@author: akittredge
'''

import unittest
import pymongo
from financial_fundamentals.mongo_drivers import MongoIntervalseries,\
    MongoTimeseries
import pytz
class MongoTestCase(unittest.TestCase):
    host, port = 'localhost', 27017
    def setUp(self):
        client = pymongo.MongoClient(self.host, self.port)
        self.db = client.test_database
        self.collection = self.db.prices

    def tearDown(self):
        self.collection.drop()

from financial_fundamentals.test_infrastructure import IntervalseriesTestCase
class MongoIntervalSeriesTestCase(MongoTestCase, IntervalseriesTestCase):
    def setUp(self):
        super(MongoIntervalSeriesTestCase, self).setUp()
        self.cache = MongoIntervalseries(self.collection, 
                                         self.metric)
    
    def find_in_database(self, start, end, symbol):
        db_record = self.collection.find({'start' : start,
                              'end' : end,
                              'symbol' : symbol}).next()
        return db_record[self.metric]
        
    def insert_into_database(self, data):
        self.collection.insert(data)
        
class MongoTimeSeriesTestCase(MongoTestCase):
    metric = 'price'
    def setUp(self):
        super(MongoTimeSeriesTestCase, self).setUp()
        self.cache = MongoTimeseries(self.collection, self.metric)
        
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
        