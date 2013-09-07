'''
Created on Jul 29, 2013

@author: akittredge
'''

import pandas as pd
import pytz
from zipline.utils.tradingcalendar import get_trading_days
import datetime
from financial_fundamentals import prices
from financial_fundamentals.sqlite_drivers import SQLiteTimeseries
import sqlite3
from financial_fundamentals.mongo_drivers import MongoTimeseries

def _load_from_cache(cache,
                    indexes={},
                    stocks=[],
                    start=pd.datetime(1990, 1, 1, 0, 0, 0, 0, pytz.utc),
                    end=datetime.datetime.now().replace(tzinfo=pytz.utc),
                    ):
    '''Equivalent to zipline.utils.factory.load_from_yahoo.
    
    '''
    datetime_index = get_trading_days(start=start, end=end)
    df = pd.DataFrame(index=datetime_index)
    python_datetimes = list(datetime_index.to_pydatetime())
    for symbol in stocks:
        # there's probably a clever way of avoiding building a dictionary.
        values = {date : value for date, value in cache.get(symbol=symbol, 
                                                           dates=python_datetimes)}
        series = pd.Series(values)
        df[symbol] = series
        
    for name, ticker in indexes.iteritems():
        values = {date : value for date, value in cache.get(symbol=ticker, 
                                                           dates=python_datetimes)}
        df[name] = pd.Series(values)
    return df

class FinancialDataTimeSeriesCache(object):
    def __init__(self, gets_data, database):
        self._get_data = gets_data
        self._database = database
        
    def get(self, symbol, dates):
        '''yield date, data pairs in no particular order.
        dates is a list of UTC datetimes.

        '''
        cached_values = self._database.get(symbol=symbol, dates=dates)
        missing_dates = set(dates)
        for date, value in cached_values:
            missing_dates.remove(date)
            yield date, value
        if missing_dates:
            for date, value in self._get_set(symbol, dates=missing_dates):
                yield date, value
            
    def _get_set(self, symbol, dates):
        new_records = list(self._get_data(symbol, dates))
        self._database.set(symbol, new_records)
        return new_records
    
    @classmethod
    def build_sqlite_price_cache(cls, 
                                 sqlite_file_path, 
                                 table='prices', 
                                 metric='Adj Close'):
        connection = sqlite3.connect(sqlite_file_path)
        db = SQLiteTimeseries(connection=connection, 
                              table=table, 
                              metric=metric)
        cache = cls(gets_data=prices.get_prices_from_yahoo,
                    database=db)
        return cache
    
    @classmethod
    def build_mongo_price_cache(cls,
                                mongo_host='localhost', 
                                mongo_port=27017):
        mongo_driver = MongoTimeseries.price_db(host=mongo_host, port=mongo_port)
        cache = cls(gets_data=prices.get_prices_from_yahoo, database=mongo_driver)
        return cache

    load_from_cache = _load_from_cache
    
    
class FinancialDataRangesCache(object):
    def __init__(self, gets_data, database):
        self._get_data = gets_data
        self._database = database
        
    def get(self, symbol, dates):
        '''Return cache's metric value for the passed dates.
        
        dates should be UTC.
        '''
        for date in dates:
            # Not looking for more than one date at a time because the set
            # operation will set multiple dates per call.
            cached_value = self._database.get(symbol=symbol, date=date)
            if cached_value:
                yield date, cached_value
            else:
                yield date, self._get_set(symbol=symbol, date=date)
    
    def _get_set(self, symbol, date):
        start, value, end = self._get_data(symbol=symbol, date=date)
        self._database.set_interval(symbol=symbol, start=start, end=end, value=value)
        return value

    load_from_cache = _load_from_cache


import unittest
from financial_fundamentals.mongo_drivers import MongoTestCase, MongoIntervalseries
class FinancialDataTimeSeriesCacheTestCase(MongoTestCase, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from financial_fundamentals.test_infrastructure import turn_on_request_caching
        turn_on_request_caching()
        
    def test_load_from_cache(self):
        cache = FinancialDataTimeSeriesCache(gets_data=None, database=None)
        test_date, test_price = datetime.datetime(2012, 12, 3, tzinfo=pytz.UTC), 100
        cache.get = lambda *args, **kwargs : [(test_date, test_price),
                                              (datetime.datetime(2012, 12, 4, tzinfo=pytz.UTC), 101),
                                              (datetime.datetime(2012, 12, 5, tzinfo=pytz.UTC), 102),
                                              ]
        symbol = 'ABC'
        df = cache.load_from_cache(start=datetime.datetime(2012, 11, 30, tzinfo=pytz.UTC),
                                   end=datetime.datetime(2013, 1, 1, tzinfo=pytz.UTC),
                                   stocks=[symbol])
        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn(symbol, df.keys())
        self.assertEqual(df[symbol][test_date], test_price)
        
    def run_load_from_cache_yahoo(self, cache):
        symbol = 'GOOG'
        df = cache.load_from_cache(stocks=[symbol], 
                              start=datetime.datetime(2012, 12, 1, tzinfo=pytz.UTC), 
                              end=datetime.datetime(2012, 12, 31, tzinfo=pytz.UTC))
        self.assertEqual(df['GOOG'][datetime.datetime(2012, 12, 3, tzinfo=pytz.UTC)], 695.25)
        
        cache._get_data = None # Make sure we're using the cached value
        df = cache.load_from_cache(stocks=[symbol],
                                   start=datetime.datetime(2012, 12, 1, tzinfo=pytz.UTC),
                                   end=datetime.datetime(2012, 12, 31, tzinfo=pytz.UTC))

    def run_load_from_cache_multiple_tickers(self, cache):
        cache = FinancialDataTimeSeriesCache.build_sqlite_price_cache(sqlite_file_path=':memory:', 
                                                                      table='price', 
                                                                      metric='Adj Close')
        symbols = ['GOOG', 'AAPL']  
        df = cache.load_from_cache(stocks=symbols,
                                   start=datetime.datetime(2012, 12, 1, tzinfo=pytz.UTC), 
                                   end=datetime.datetime(2012, 12, 31, tzinfo=pytz.UTC))
        self.assertEqual(df['GOOG'][datetime.datetime(2012, 12, 3, tzinfo=pytz.UTC)], 695.25)
        self.assertEqual(df['AAPL'][datetime.datetime(2012, 12, 31, tzinfo=pytz.UTC)], 522.16)

    def test_sqlite(self):
        cache = FinancialDataTimeSeriesCache.build_sqlite_price_cache(sqlite_file_path=':memory:', 
                                                                      table='price', 
                                                                      metric='Adj Close')
        self.run_load_from_cache_yahoo(cache=cache)
        cache = FinancialDataTimeSeriesCache.build_sqlite_price_cache(sqlite_file_path=':memory:', 
                                                                       table='price', 
                                                                       metric='Adj Close')
        self.run_load_from_cache_multiple_tickers(cache=cache)
    
    def _build_mongo_cache(self):
        db_driver = MongoTimeseries(mongo_collection=self.collection, 
                                    metric='Adj Close')
        cache = FinancialDataTimeSeriesCache(gets_data=prices.get_prices_from_yahoo,
                                             database=db_driver)
        return cache
    
    def test_mongo_single(self):
        self.run_load_from_cache_yahoo(self._build_mongo_cache())
    
    def test_mongo_multiple(self):
        self.run_load_from_cache_multiple_tickers(self._build_mongo_cache())
        
    def test_indexes(self):
        cache = FinancialDataTimeSeriesCache.build_sqlite_price_cache(sqlite_file_path=':memory:', 
                                                                      table='price', 
                                                                      metric='Adj Close')
        df = cache.load_from_cache(indexes={'SPX' : '^GSPC'},
                                   start=datetime.datetime(2012, 12, 1, tzinfo=pytz.UTC),
                                   end=datetime.datetime(2012, 12, 31, tzinfo=pytz.UTC))
        self.assertEqual(df['SPX'][datetime.datetime(2012, 12, 3, tzinfo=pytz.UTC)], 1409.46)
        
import mock
class FinancialDataRangesCacheTestCase(unittest.TestCase):
    def setUp(self):
        self.mock_data_getter = mock.Mock()
        self.mock_db = mock.Mock()
        self.date_range_cache = FinancialDataRangesCache(gets_data=self.mock_data_getter,
                                                         database=self.mock_db)
        
    def test_get_cache_hit(self):
        symbol = 'ABC'
        date = datetime.datetime(2012, 12, 1)
        value = 100.
        self.mock_db.get.return_value = value
        cache_date, cache_value = self.date_range_cache.get(symbol=symbol, dates=[date]).next()
        self.assertEqual(cache_value, value)
        self.assertEqual(cache_date, date)

        
    def test_cache_miss(self):
        symbol = 'ABC'
        date = datetime.datetime(2012, 12, 1)
        self.mock_db.get.return_value = None
        mock_get_set = mock.Mock()
        self.date_range_cache._get_set = mock_get_set
        self.mock_db.get.return_value = None
        self.date_range_cache.get(symbol=symbol, dates=[date]).next()
        mock_get_set.assert_called_once_with(symbol=symbol, date=date)


class MongoDataRangesIntegrationTestCase(MongoTestCase):
    metric = 'price'
    def setUp(self):
        super(MongoDataRangesIntegrationTestCase, self).setUp()
        self.mock_getter = mock.Mock()
        self.mongo_db = MongoIntervalseries(collection=self.collection,
                                            metric=self.metric)
        self.cache = FinancialDataRangesCache(gets_data=self.mock_getter, 
                                              database=self.mongo_db)
        
    def test_init(self):
        self.assertIs(self.cache._database, self.mongo_db)
    
    def test_set(self):
        price = 100.
        symbol = 'ABC'
        date = datetime.datetime(2012, 12, 15, tzinfo=pytz.UTC)
        range_start, range_end = datetime.datetime(2012, 12, 1), datetime.datetime(2012, 12, 31)
        self.mock_getter.return_value = (range_start,
                                         price,
                                         range_end)
        cache_date, cache_price = self.cache.get(symbol=symbol, dates=[date]).next()
        self.assertEqual(cache_price, price)
        self.assertEqual(cache_date, date)
        self.assertEqual(self.collection.find({'start' : range_start,
                                               'end' : range_end,
                                               'symbol' : symbol}).next()['price'], price)