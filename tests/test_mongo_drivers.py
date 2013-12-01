'''
Created on Sep 12, 2013

@author: akittredge
'''

import unittest
import pymongo
from financial_fundamentals.mongo_drivers import  MongoVectorCacheDriver
import pytz
from tests.infrastructure import IntervalseriesTestCase
import pandas as pd
from pandas.util.testing import assert_frame_equal, assert_index_equal

class MongoTestCase(unittest.TestCase):
    host, port = 'localhost', 27017
    def setUp(self):
        client = pymongo.MongoClient(self.host, self.port)
        self.db = client.test
        self.db.prices.drop()
        self.collection = self.db[self.collection_name]
        self.collection.drop()

class TestMongoSet(MongoTestCase):
    collection_name = 'cache'
    def setUp(self):
        MongoTestCase.setUp(self)
        self.cache = MongoVectorCacheDriver(collection=self.collection)
        
    def test_set(self):
        metric = 'price'
        test_data = {'a' : [1, 2, 3], 'b' : [4, 5, 6]}
        test_df = pd.DataFrame(test_data)
        self.cache.set(metric=metric, data=test_df)
        records = self.collection.find({'identifier' : 'a', 
                                             'metric' :metric})
        self.assertSetEqual(set(test_data['a']), set(record['value'] for record in records))
        
    def test_set_get(self):
        metric = 'price'
        test_data = {'a' : [1, 2, 3], 'b' : [4, 5, 6]}
        index = pd.date_range('2012-12-1', '2012-12-3')
        test_df = pd.DataFrame(test_data, index=index)
        self.cache.set(metric=metric, data=test_df)
        cache_data, missing_data = self.cache.get(metric=metric,
                                                  indentifiers=test_data.keys(), 
                                                  index=index)
        assert_frame_equal(test_df, cache_data)
        
    def test_missing_dates(self):
        metric = 'price'
        test_data = {'a' : [1, 2, 3], 'b' : [4, 5, 6]}
        index = pd.date_range('2012-12-1', '2012-12-3')
        test_df = pd.DataFrame(test_data, index=index)
        self.cache.set(metric=metric, data=test_df)
        longer_index = pd.date_range('2012-11-30', '2012-12-5')
        cache_data, missing_data = self.cache.get(metric=metric, 
                                                  indentifiers=test_data.keys(), 
                                                  index=longer_index)
        assert_index_equal(missing_data['a'], pd.DatetimeIndex(['2012-11-30', 
                                                                '2012-12-4', 
                                                                '2012-12-5']))
        
        
if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(TestMongoSet('test_set_get'))
    unittest.TextTestRunner().run(suite)