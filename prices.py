'''
Created on Jul 2, 2013

@author: akittredge
'''
import unittest

from pymongo import MongoClient

mongohost, mongoport = 'localhost', 27017
YahooPriceGetter = None

class MongoPriceCache(object):
    _client = MongoClient(mongohost, mongoport)
    _collection = _client.prices.prices
    def __init__(self, getter=YahooPriceGetter, collection=None):
        self._collection = collection or self._collection
        
    def get(self, symbols, start, end):
        pass
    
    def get_dataframe(self):
        #get the range and convert it into a dataframe.
        pass
    
    
    def _set(self, symbol_price_ranges):
        '''symbol_price_range looks like [{'symbol' : 'XYZ', 'price' : 5, 'date' : datetime.datetime(2012, 12, 1)}]
        
        '''
        pass
    
    def _get_range(self, symbols, start, end):
        #get prices from yahoo.
        pass


import datetime
import mock
class MongoPriceCacheTestCase(unittest.TestCase):
    test_db_host, test_db_port = 'localhost', 27017
    def setUp(self):
        client = MongoClient(self.test_db_host, self.test_db_port)
        self.db = client.test_database
        self.collection = client.test_database.prices
        self.cache = MongoPriceCache(collection=self.collection)
        
    
    def tearDown(self):
        self.db.drop_collection(self.collection)
        
    mock.patch('MongoPriceCache._collection.get')
    def test_get_cache_miss(self):
        mock_collection = mock.Mock()
        mock_collection.find.return_value = None
        cache = MongoPriceCache(collection=mock_collection)
        get_range_mock = mock.Mock()
        cache._get_range = get_range_mock
        range_start = datetime.datetime(2012, 12, 1)
        range_end = datetime.datetime(2012, 12, 31)
        symbols = ['XYZ',]
        prices = cache.get(symbols=symbols,
                           start=range_start,
                           end=range_end,
                           )
        mock_collection.find.assert_called_once_with({'date' : {'$gte' : range_start,
                                                                '$lte' : range_end,
                                                                },
                                                      'symbol' : {'$in' : symbols,},
                                                      })
        get_range_mock.assert_called_once_with(symbols=symbols, 
                                               start=range_start,
                                               end=range_end)

    def test_set(self):
        price_date = datetime.datetime(2012, 12, 1)
        symbol = 'XYZ'
        price = 5.
        symbol_price_ranges = [{'symbol' : symbol, 
                                'price' : price, 
                                'date' : price_date}]
        self.cache._set(symbol_price_ranges)
        db_price = self.collection.find_one({'date' : price_date, 'symbol' : symbol})
        self.assertEqual(db_price, price)
        
    def test_get_range(self):
        price_date = datetime.datetime(2013, 1, 1)
        symbol = 'ABC'
        price = 6.
        self.collection.insert({'date' : price_date, 
                                'symbol' : symbol, 
                                'price' : price})
        prices_from_cache = self.cache._get_range(symbols={symbol}, 
                                                 start=price_date - datetime.timedelta(days=1),
                                                 end=price_date + datetime.timedelta(days=1))
        self.assertEqual(len(prices_from_cache.items()), 1)
        self.assertEqual(len(prices_from_cache[symbol]), 1)
        price = prices_from_cache[symbol][0]
        self.assertDictEqual({'date' : price_date, 'symbol' : symbol}, price)
        