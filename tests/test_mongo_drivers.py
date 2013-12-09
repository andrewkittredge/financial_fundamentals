'''
Created on Sep 12, 2013

@author: akittredge
'''

import unittest
import pymongo

import pandas as pd
from pandas.util.testing import assert_frame_equal
import datetime
import financial_fundamentals.io.mongo as ff_mongo
from financial_fundamentals.mongo_drivers import MongoDataStore

TEST_HOST, TEST_PORT = 'localhost', 27017
class MongoTestCase(unittest.TestCase):
    def setUp(self):
        self.data_store, self.collection = build_data_store(collection_name=self.collection_name)
        
def build_data_store(host=TEST_HOST, port=TEST_PORT, collection_name='test'):
    client = pymongo.MongoClient(host, port)
    db = client.test
    collection = db[collection_name]
    collection.drop()
    data_store = MongoDataStore(collection=collection)
    return data_store, collection

class TestMongoSet(MongoTestCase):
    collection_name = 'cache'

    def test_set(self):
        metric = 'price'
        test_data = {'a' : [1, 2, 3], 'b' : [4, 5, 6]}
        test_df = pd.DataFrame(test_data)
        test_df.index.name = metric
        self.cache.set(metric=metric, data=test_df)
        records = self.collection.find({'identifier' : 'a', 
                                        metric : {'$exists' : True}})
        self.assertSetEqual(set(test_data['a']), 
                            set(record[metric] for record in records))
        
    def test_set_get(self):
        metric = 'price'
        test_data = {'a' : [1, 2, 3]}
        index = pd.date_range('2012-12-1', '2012-12-3')
        test_df = pd.DataFrame(test_data, index=index)
        test_df.index.name = 'date'
        self.cache.set(metric=metric, data=test_df)
        cache_data = self.cache.get(metric=metric,
                                    identifier='a', 
                                    index=index)
        assert_frame_equal(test_df, cache_data)


        
class MongoIOTest(MongoTestCase):
    collection_name = 'vector_cache'
    def test_read_frame(self):
        self.collection.insert({'identifier' : 'a', 
                                'foo' : 1, 
                                'date' : datetime.datetime(2012, 12, 1)})
        self.collection.insert({'identifier' : 'a', 
                                'foo' : 2, 
                                'date' : datetime.datetime(2012, 12, 2)})
        qry = {'identifier' : 'a',
               'date' : {'$gte' : datetime.datetime(2012, 11, 30),
                         '$lte' : datetime.datetime(2012, 12, 3)},
               'foo' : {'$exists' : True},
               }
        df = ff_mongo.read_frame(qry=qry, 
                                 columns=['date', 'foo'], 
                                 collection=self.collection, 
                                 index_col='date')
        test_df = pd.DataFrame({'foo' : [1, 2]}, 
                               index=pd.date_range('2012-12-1', 
                                                   '2012-12-2'))
        test_df.index.name = 'date'
        assert_frame_equal(df, test_df)
    
    def test_write_frame(self):
        df = pd.DataFrame({'foo' : [1, 2]}, 
                          index=pd.date_range('2012-12-1', '2012-12-2'))
        df.index.name = 'date'
        ff_mongo.write_frame(metric='price',
                             frame=df, 
                             collection=self.collection)
        documents = list(self.collection.find())
        self.assertEqual(len(documents), 2)
        
        
    def test_empty_df(self):
        df = ff_mongo.read_frame(qry={},
                                 columns=[],
                                 collection=self.collection,
                                 index_col='date')
        self.assertTrue(df.empty)
        
if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(TestMongoSet('test_set_get'))
    unittest.TextTestRunner().run(suite)