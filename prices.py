'''
Created on Jul 2, 2013

@author: akittredge
'''
import unittest

from pymongo import MongoClient
from pandas.io.data import get_data_yahoo
from itertools import groupby

mongohost, mongoport = 'localhost', 27017
YahooPriceGetter = None

class MongoPriceCache(object):
    '''The beginning of timeseries database.
    
    '''
    _client = MongoClient(mongohost, mongoport)
    _collection = _client.prices.prices
    def __init__(self, getter=YahooPriceGetter, collection=None):
        self._collection = collection or self._collection
        self._getter = getter
        
    def get(self, symbols, dates):
        '''returns a dictionary of symbol : {date : price} pairs.
        
        '''
        cached_records = self._collection.find({'symbol' : {'$in' : symbols},
                                                'date' : {'$in' : dates},
                                                }).sort('symbol', 'date')
        symbols_to_get = set(symbols)
        necessary_dates = set(dates)
        for cached_symbol, prices in groupby(cached_records, 
                                            key=lambda price : price['symbol']):
            cached_dates = set(price['date'] for price in prices)
            if not necessary_dates - cached_dates:
                symbols_to_get.remove(cached_symbol)
                yield cached_symbol, prices
        for symbol, prices in self._get_set(symbols_to_get, dates):
            yield symbol, prices
        
    def get_dataframe(self):
        #get the range and convert it into a dataframe.
        pass
    
    def _get_set(self, symbols, start, end):
        new_records = self._getter.get(symbols, start, end)
        self._set(new_records)
        return new_records
    
    def _set(self, symbol_price_ranges):
        '''symbol_price_range looks like [{'symbol' : 'XYZ', 'price' : 5, 'date' : datetime.datetime(2012, 12, 1)}]
        
        '''
        pass

class YahooPriceGetter(object):
    def get(self, symbols, start, end):
        for symbol in symbols:
            yield get_data_yahoo(name=symbol, start=start, end=end)
            
import datetime
import mock
class MongoPriceCacheTestCase(unittest.TestCase):
    test_db_host, test_db_port = 'localhost', 27017
    def setUp(self):
        client = MongoClient(self.test_db_host, self.test_db_port)
        self.db = client.test_database
        self.collection = client.test_database.prices
        self.mock_getter = mock.Mock()
        self.cache = MongoPriceCache(getter=self.mock_getter, 
                                     collection=self.collection)
        
    
    def tearDown(self):
        self.db.drop_collection(self.collection)
        
    mock.patch('MongoPriceCache._collection.get')
    def test_cache_miss(self):
        dates = [datetime.datetime(2012, 12, day) for day in range(1, 32)]
        symbols = ['XYZ',]
        mock_collection = mock.Mock()
        mock_collection.find.return_value = []
        self.cache._collection = mock_collection
        self.cache.get(symbols=symbols, dates=dates)
        mock_collection.find.assert_called_once_with({'date' : {'$in' : dates},
                                                      'symbol' : {'$in' : symbols,},
                                                      })
        self.mock_getter.get.assert_called_once_with(symbols=symbols, 
                                               dates=dates)

    def test_set(self):
        price_date = datetime.datetime(2012, 12, 1)
        symbol = 'XYZ'
        price = 5.
        symbol_price_ranges = [{'symbol' : symbol, 
                                'price' : price, 
                                'date' : price_date}]
        self.cache._set(symbol_price_ranges)
        db_price = self.collection.find_one({'date' : price_date, 
                                             'symbol' : symbol})
        self.assertEqual(db_price, price)
        
    def test_get_cache_hit(self):
        price_date = datetime.datetime(2013, 1, 1)
        symbol = 'ABC'
        price = 6.
        dates = [datetime.datetime(2013, 1, d) for d in [1, 2, 3]]
        for date in dates:
            self.collection.insert({'date' : date, 
                                    'symbol' : symbol, 
                                    'price' : price})
        prices_from_cache = self.cache.get(symbols={symbol}, dates=dates)
        self.assertEqual(len(prices_from_cache.items()), 1)
        self.assertEqual(len(prices_from_cache[symbol]), len(dates))
        price = prices_from_cache[symbol][0]
        self.assertDictEqual({'date' : datetime.datetime(2013, 1, 1), 'price' : 6.}, price)
        
    
        
        
    def test_partial_miss(self):
        '''When we've already cached part of a range.
        
        '''
        symbol = 'ABC'
        cached_dates = [datetime.datetime(2012, 12, 1), 
                        datetime.datetime(2012, 12, 2)]
        for cached_date in cached_dates:
            self.collection.insert({'symbol' : symbol, 'date' : cached_date})
        first_missing_date, last_missing_date = (datetime.datetime(2012, 11, 1), 
                                                 datetime.datetime(2013, 1, 1))
        missing_dates = [first_missing_date, last_missing_date]
        self.cache.get(symbols=[symbol], dates=cached_dates + missing_dates)
        args = {'symbols' : {symbol,},
                'start' : datetime.datetime(2012, 11, 30),
                'end' : datetime.datetime(2012, 12, 31)}
        self.cache.get(symbols={symbol}, dates=missing_dates + cached_dates)
        self.mock_getter.get.assert_called_once_with(symbols={symbol}, 
                                                     dates=missing_dates + cached_dates)
        
        