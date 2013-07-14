'''
Created on Jul 2, 2013

@author: akittredge
'''


from pymongo import MongoClient

from itertools import groupby

mongohost, mongoport = 'localhost', 27017

from pandas.io.data import get_data_yahoo
import numpy as np

    
def get_prices_from_yahoo(symbol, start, end):
    '''Jack Diedrich told me to make this a function rather than a class.'''
    prices = get_data_yahoo(name=symbol, start=start, end=end)
    for price in prices.iterrows():
        yield {'date' : price[0],
               'price' : np.float(price[1]['Close'])}

class MongoPriceCache(object):
    '''The beginning of timeseries database.'''
    _client = MongoClient(mongohost, mongoport)
    _collection = _client.prices.prices
    def __init__(self, gets_prices=get_prices_from_yahoo, collection=None):
        self._collection = collection or self._collection
        self._get_prices = gets_prices
        
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
            for price in prices:
                price['price'] = np.float(price['price'])
            cached_dates = set(price['date'] for price in prices)
            if necessary_dates.issubset(cached_dates):
                symbols_to_get.remove(cached_symbol)
                yield cached_symbol, prices

        for symbol in symbols_to_get:
            yield symbol, self._get_set(symbol=symbol, dates=dates)


    def get_dataframe(self):
        #get the range and convert it into a dataframe.
        pass

    def _get_set(self, symbol, dates):
        start_date, end_date = min(dates), max(dates)
        prices = self._get_prices(symbol=symbol, 
                                  start=start_date,
                                  end=end_date)
        prices = list(prices)
        for price in prices:
            price['price'] = np.float(price['price'])
        self._set(symbol, prices)
        missing_dates = set(dates) - set(price['date'] for price in prices)
        self._set(symbol, prices=({'price' : np.NaN, 'date' : missing_date}
                                  for missing_date in missing_dates))
        return prices

    def _set(self, symbol, prices):
        for price in prices:
            key = {'symbol' : symbol, 'date' : price['date']}
            data = {'symbol' : symbol, 
                    'price' : price['price'],
                    'date' : price['date']}
            self._collection.update(key, data, upsert=True)

import unittest
import datetime
import mock
TEST_DB_HOST, TEST_DB_PORT = 'localhost', 27017
class MongoPriceCacheTestCase(unittest.TestCase):
    def setUp(self):
        client = MongoClient(TEST_DB_HOST, TEST_DB_PORT)
        self.db = client.test_database
        self.collection = client.test_database.prices
        self.mock_getter = mock.Mock()
        self.mock_getter.return_value = []
        self.cache = MongoPriceCache(gets_prices=self.mock_getter, 
                                     collection=self.collection)

    def tearDown(self):
        self.db.drop_collection(self.collection)

    mock.patch('MongoPriceCache._collection.get')
    def test_cache_miss(self):
        dates = [datetime.datetime(2012, 12, day) for day in range(1, 32)]
        symbols = ['XYZ',]
        list(self.cache.get(symbols=symbols, dates=dates))
        start_date, end_date = min(dates), max(dates)
        self.mock_getter.assert_called_once_with(symbol=symbols[0],
                                                     start=start_date,
                                                     end=end_date) 

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
            self.collection.insert({'symbol' : symbol, 
                                    'date' : cached_date,
                                    'price' : 6.})
        first_missing_date, last_missing_date = (datetime.datetime(2012, 11, 1), 
                                                 datetime.datetime(2013, 1, 1))
        missing_dates = [first_missing_date, last_missing_date]
        self.cache.get(symbols=[symbol], dates=cached_dates + missing_dates)
        list(self.cache.get(symbols={symbol}, dates=missing_dates + cached_dates))
        all_dates = missing_dates + cached_dates
        start_date, end_date = min(all_dates), max(all_dates)
        self.mock_getter.assert_called_once_with(symbol=symbol,
                                                     start=start_date,
                                                     end=end_date)

    def test_cache_set(self):
        '''Ensure that asking for something twice is not a cache miss.'''
        symbols = ['ABC']
        dates = [datetime.datetime(2012, 12, 1), datetime.datetime(2012, 12, 2)]
        self.mock_getter.return_value = [{'date' : datetime.datetime(2012, 12, 1), 'price' : 6},
                                             {'date' : datetime.datetime(2012, 12, 2), 'price' : 7},
                                            ]
        list(self.cache.get(symbols=symbols, dates=dates))
        self.mock_getter.assert_called_once()
        new_mock_getter = mock.Mock()
        new_mock_getter.return_value = []
        self.cache._getter = new_mock_getter
        list(self.cache.get(symbols=symbols, dates=dates))
        self.assertFalse(new_mock_getter.called)
        
    def test_fill_unavailable(self):
        '''if the getter can't get prices for a date the cache should put in NaNs.'''
        symbol = 'ABC'
        gotten_dates = [{'date' : datetime.datetime(2012, 12, 2), 'price' : 6},]
        self.mock_getter.return_value = gotten_dates
        requested_dates = {datetime.datetime(2012, 12, 1), 
                           datetime.datetime(2012, 12, 2), 
                           datetime.datetime(2012, 12, 3)}
        list(self.cache._get_set(symbol=symbol, dates=requested_dates))
        for missing_date in requested_dates - set(price['date'] for price in gotten_dates):
            price = self.collection.find_one({'date' : missing_date, 
                                          'symbol' : symbol})
            self.assertTrue(np.isnan(price['price']))       
        
        
class YahooPricesTestCase(unittest.TestCase):
    def test_get_prices_from_yahoo(self):
        prices = list(get_prices_from_yahoo(symbol='GOOG', 
                                   start=datetime.datetime(2012, 12, 1),
                                   end=datetime.datetime(2012, 12, 31)))
        
    
class IntegrationTests(unittest.TestCase):
    def setUp(self):
        client = MongoClient(TEST_DB_HOST, TEST_DB_PORT)
        self.db = client.test_database
        self.collection = client.test_database.prices
        
        self.cache = MongoPriceCache(collection=self.collection)
        
    def tearDown(self):
        self.db.drop_collection(self.collection)
    def test_the_whole_thing(self):
        symbol = 'GOOG'
        dates = [(datetime.datetime(2012, 12, 1) + \
                                    datetime.timedelta(days=1 * n)) 
                                   for n in range(30)]
        cached_values = self.cache.get(symbols={symbol}, 
                            dates=dates
                            )
        symbol, prices = cached_values.next()
        self.assertEqual(symbol, 'GOOG')
        self.assertGreater(len(prices), 10)
        mock_getter = mock.Mock()
        mock_getter.return_value = []
        self.cache._get_prices = mock_getter
        list(self.cache.get(symbols={symbol}, dates=dates))
        self.assertFalse(mock_getter.called)
        
        
if __name__ == '__main__':
    cache = MongoPriceCache()
    print list(cache.get(symbols={'GOOG'}, dates=[(datetime.datetime(2012, 12, 1) + datetime.timedelta(days=1 * n)) for n in range(30)]))