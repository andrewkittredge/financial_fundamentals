'''
Created on Jul 28, 2013

@author: akittredge
'''
import pymongo
import pytz
import numpy as np



class MongoTimeseries(object):
    def __init__(self, mongo_collection, metric):
        self._ensure_indexes(mongo_collection)
        self._collection = mongo_collection
        self._metric = metric
        
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
            yield self._beautify_record(record, self._metric)
        
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
    
    @staticmethod
    def _beautify_record(record, metric):
        '''Cast metric to np.float and make date tz-aware.
        
        '''

        return record['date'].replace(tzinfo=pytz.UTC), np.float(record[metric])
        
        
class MongoIntervalseries(MongoTimeseries):
    def __init__(self, collection, metric): 
        super(MongoIntervalseries, self).__init__(mongo_collection=collection,
                                                  metric=metric)
    
    @classmethod
    def _ensure_indexes(cls, collection):
        collection.ensure_index([('start', pymongo.ASCENDING),
                                 ('end', pymongo.ASCENDING),
                                 ('symbol', pymongo.ASCENDING)])
        
    def get(self, symbol, date):
        cursor = self._collection.find({'symbol' : symbol,
                                        'start' : {'$lte' : date},
                                        'end' : {'$gte' : date}})
        try:
            record = cursor.next()
        except StopIteration:
            return None
        else:
            # Might be able to get mongo to do this.
            record['date'] = date
            record.pop('start')
            record.pop('end')
            return self._beautify_record(record, self._metric)
                    
    def set_interval(self, symbol, start, end, value):
        data = {'symbol' : symbol,
                'start' : start,
                'end' : end,
                self._metric : value}
        self._collection.insert(data)

    @staticmethod
    def _beautify_record(record, metric):
        '''Cast metric to np.float and make date tz-aware.
        
        '''
        record[metric] = np.float(record[metric])
        record['date'] = record['date'].replace(tzinfo=pytz.UTC)
        return record

import unittest
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
        