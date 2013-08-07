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
        
    def get(self, symbols, dates):
        records = self._collection.find({'symbol' : {'$in' : list(symbols)},
                                     'date' : {'$in' : dates},
                                     }).sort('symbol')
        for record in records:
            yield self._beautify_record(record, self._metric)
        
    def set(self, symbol, records):
        for record in records:
            key = {'symbol' : symbol, 'date' : record['date']}
            data = {'symbol' : symbol,
                    self._metric : record[self._metric],
                    'date' : record['date']}
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
        record[metric] = np.float(record[metric])
        record['date'] = record['date'].replace(tzinfo=pytz.UTC)
        return record
        
        
class MongoIntervalseries(MongoTimeseries):
    def __init__(self, mongo_collection, metric, gets_sets): 
        super(MongoIntervalseries, self).__init__(mongo_collection=mongo_collection,
                                                  metric=metric)
        self._get_set = gets_sets
    
    @classmethod
    def _ensure_indexes(cls, collection):
        collection.ensure_index([('start', pymongo.ASCENDING),
                                 ('end', pymongo.ASCENDING),
                                 ('symbol', pymongo.ASCENDING)])
        
    def get(self, symbols, dates):
        for symbol in symbols:
            for date in dates:
                cursor = self._collection.find({'symbol' : symbol,
                                                'start' : {'$lte' : date},
                                                'end' : {'$gte' : date}})
                try:
                    record = cursor.next()
                except StopIteration:
                    yield self._beautify_record(self._get_set(symbol, date), 
                                                self._metric)
                else:
                    # Might be able to do this in the find.
                    record['date'] = date
                    record.pop('start')
                    record.pop('end')
                    yield self._beautify_record(record, self._metric)
                    

import unittest
class MongoTestCase(unittest.TestCase):
    metric = 'price'
    def setUp(self):
        client = pymongo.MongoClient('localhost', 27017)
        self.db = client.test_database
        self.collection = self.db.prices
        
    def tearDown(self):
        self.collection.drop()
        
import mock
import datetime
class MongoIntervalseriesTestCase(MongoTestCase):
    def setUp(self):
        super(MongoIntervalseriesTestCase, self).setUp()
        self.mock_getter_setter = mock.MagicMock()
        self.cache = MongoIntervalseries(self.collection, 
                                         self.metric,
                                         self.mock_getter_setter)
        
    def test_cache_miss(self):
        symbol = 'ABC'
        date = datetime.datetime(2012, 12, 1)
        list(self.cache.get([symbol], [date]))
        self.mock_getter_setter.assert_called_once_with(symbol, date)
        
    def test_cache_hit(self):
        symbol = 'ABC'
        interval_start = datetime.datetime(2012, 12, 1)
        interval_end = datetime.datetime(2012, 12, 31)
        price = 100.
        data = {'symbol' : symbol,
                'start' : interval_start,
                'end' : interval_end,
                self.metric : price}
        self.collection.insert(data)
        date = datetime.datetime(2012, 12, 14)
        cached_record = self.cache.get(symbols=[symbol], dates=[date]).next()
        self.assertEqual(cached_record[self.metric], np.float(price))
        self.assertEqual(cached_record['date'], date.replace(tzinfo=pytz.UTC))
        self.mock_getter_setter.assert_not_called()
        
class MongoTimeSeriesTestCase(MongoTestCase):
    def setUp(self):
        super(MongoTimeSeriesTestCase, self).setUp()
        self.cache = MongoTimeseries(self.collection, self.metric)
        
    def test_find(self):
        metric = self.metric
        symbol, date, price = ('ABC', 
                               datetime.datetime(2012, 12, 1, tzinfo=pytz.UTC), 
                               6.5)
        key = {'symbol' : symbol, 'date' : date}
        data = {'symbol' : symbol,
                metric : price,
                'date' : date}
        self.collection.update(key, data, upsert=True)
        test_data = self.cache.get(symbols=[symbol], dates=[date]).next()
        self.assertEqual(test_data[metric], price)
        self.assertEqual(test_data['date'], date.replace(tzinfo=pytz.UTC))
        self.assertEqual(test_data['symbol'], symbol)
        
    def test_set(self):
        metric = self.metric
        symbol, date, price = ('ABC',
                                datetime.datetime(2012, 12, 1, tzinfo=pytz.UTC), 
                                6.5)
        data = {'symbol' : symbol,
                metric : price,
                'date' : date}
        self.cache.set(symbol=symbol, records=[data])
        test_data = self.collection.find({'symbol' : symbol})[0]
        self.assertEqual(test_data[metric], price)
        self.assertEqual(test_data['symbol'], symbol)
        