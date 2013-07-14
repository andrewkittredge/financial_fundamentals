'''
Created on Jul 2, 2013

@author: akittredge
'''


from pymongo import MongoClient

from itertools import groupby

mongohost, mongoport = 'localhost', 27017

from pandas.io.data import get_data_yahoo
class YahooPriceGetter(object):
    def get(self, symbols, start, end):
        for symbol in symbols:
            yield get_data_yahoo(name=symbol, start=start, end=end)
            

class MongoPriceCache(object):
    '''The beginning of timeseries database.'''
    _client = MongoClient(mongohost, mongoport)
    _collection = _client.prices.prices
    def __init__(self, getter=YahooPriceGetter, collection=None):
        self._collection = collection or self._collection
        self._getter = getter
        
    def get(self, symbols, dates):
        '''returns an generator of (symbol, price_list) pairs.'''
        cached_records = self._collection.find({'symbol' : {'$in' : list(symbols)},
                                                'date' : {'$in' : dates},
                                                }).sort('symbol')
        symbols_to_get = set(symbols)
        necessary_dates = set(dates)
        for cached_symbol, prices in groupby(cached_records, 
                                            key=lambda price : price['symbol']):
            prices = list(prices)
            cached_dates = set(price['date'] for price in prices)
            if necessary_dates.issubset(cached_dates):
                symbols_to_get.remove(cached_symbol)
                yield cached_symbol, prices
        for symbol, prices in self._get_set(symbols_to_get, dates):
            yield symbol, prices
        
    def get_dataframe(self):
        #get the range and convert it into a dataframe.
        pass
    
    def _get_set(self, symbols, dates):
        for symbol in symbols:
            prices = self._getter.get(symbol=symbol, dates=dates)
            if prices:
                self._set(symbol, prices)
                yield symbol, prices
    
    def _set(self, symbol, prices):
        self._collection.insert({'symbol' : symbol, 
                                     'price' : price['price'],
                                     'date' : price['date']}
                                    for price in prices)

        
import unittest
import datetime
import mock
class MongoPriceCacheTestCase(unittest.TestCase):
    test_db_host, test_db_port = 'localhost', 27017
    def setUp(self):
        client = MongoClient(self.test_db_host, self.test_db_port)
        self.db = client.test_database
        self.collection = client.test_database.prices
        self.mock_getter = mock.Mock()
        self.mock_getter.get.return_value = []
        self.cache = MongoPriceCache(getter=self.mock_getter, 
                                     collection=self.collection)

    def tearDown(self):
        self.db.drop_collection(self.collection)

    mock.patch('MongoPriceCache._collection.get')
    def test_cache_miss(self):
        dates = [datetime.datetime(2012, 12, day) for day in range(1, 32)]
        symbols = ['XYZ',]
        list(self.cache.get(symbols=symbols, dates=dates))
        self.mock_getter.get.assert_called_once_with(symbols=set(symbols), 
                                                    dates=dates)

    def test_set(self):
        price_date = datetime.datetime(2012, 12, 1)
        symbol = 'XYZ'
        price = 5.
        symbol_price_ranges = [{'symbol' : symbol, 
                                'price' : price, 
                                'date' : price_date},
                               ]
        self.cache._set(symbol, symbol_price_ranges)
        db_price = self.collection.find_one({'date' : price_date, 
                                             'symbol' : symbol})
        self.assertEqual(db_price['price'], price)

    def test_get_cache_hit(self):
        symbol = 'ABC'
        price = 6.
        dates = [datetime.datetime(2013, 1, d) for d in [1, 2, 3]]
        for date in dates:
            self.collection.insert({'date' : date, 
                                    'symbol' : symbol, 
                                    'price' : price})
        prices_from_cache = list(self.cache.get(symbols={symbol}, dates=dates))
        self.assertEqual(len(prices_from_cache), 1)
        ABC_prices = prices_from_cache[0][1]
        self.assertSetEqual(set(price['date'] for price in ABC_prices), 
                            set(dates))
        for ABC_price in ABC_prices:
            self.assertEqual(ABC_price['price'], price)

    def test_partial_miss(self):
        '''When we've already cached part of a range.'''
        symbol = 'ABC'
        cached_dates = [datetime.datetime(2012, 12, 1), 
                        datetime.datetime(2012, 12, 2)]
        for cached_date in cached_dates:
            self.collection.insert({'symbol' : symbol, 'date' : cached_date})
        first_missing_date, last_missing_date = (datetime.datetime(2012, 11, 1), 
                                                 datetime.datetime(2013, 1, 1))
        missing_dates = [first_missing_date, last_missing_date]
        self.cache.get(symbols=[symbol], dates=cached_dates + missing_dates)
        list(self.cache.get(symbols={symbol}, dates=missing_dates + cached_dates))
        self.mock_getter.get.assert_called_once_with(symbols={symbol}, 
                                                     dates=missing_dates + cached_dates)
        
    def test_cache_set(self):
        '''Ensure that asking for something twice is not a cache miss.'''
        symbols = ['ABC']
        dates = [datetime.datetime(2012, 12, 1), datetime.datetime(2012, 12, 2)]
        self.mock_getter.get.return_value = [{'date' : datetime.datetime(2012, 12, 1), 'price' : 6},
                                             {'date' : datetime.datetime(2012, 12, 2), 'price' : 7},
                                            ]
        list(self.cache.get(symbols=symbols, dates=dates))
        self.mock_getter.get.assert_called_once()
        new_mock_getter = mock.Mock()
        new_mock_getter.get.return_value = []
        self.cache._getter = new_mock_getter
        list(self.cache.get(symbols=symbols, dates=dates))
        self.assertFalse(new_mock_getter.get.called)