import pymongo
from financial_fundamentals.mongo_drivers import MongoIntervalseries,\
    MongoTimeseries
from financial_fundamentals.time_series_cache import FinancialDataRangesCache,\
    FinancialDataTimeSeriesCache
from financial_fundamentals.prices import get_prices_from_yahoo
import pytz
import sqlite3

import os
from financial_fundamentals import accounting_metrics, sqlite_drivers


def mongo_fundamentals_cache(metric, mongo_host='localhost', mongo_port=27017):
    mongo_client = pymongo.MongoClient(mongo_host, mongo_port)
    mongo_collection = mongo_client.fundamentals.fundamentals
    db = MongoIntervalseries(collection=mongo_collection, 
                         metric=metric.metric_name)
    cache = FinancialDataRangesCache(gets_data=metric.get_data, database=db)
    return cache

def mongo_price_cache(mongo_host='localhost', mongo_port=27017):
    client = pymongo.MongoClient(mongo_host, mongo_port)
    collection = client.prices.prices
    db = MongoTimeseries(mongo_collection=collection, metric='price')
    cache = FinancialDataTimeSeriesCache(gets_data=get_prices_from_yahoo, 
                                         database=db)
    return cache

DEFAULT_PRICE_PATH = os.path.join(os.path.expanduser('~'), '.prices.sqlite')
def sqlite_price_cache(db_file_path=DEFAULT_PRICE_PATH):
    '''Return a cache that persists prices downloaded from yahoo.
    
    '''
    return FinancialDataTimeSeriesCache.build_sqlite_price_cache(sqlite_file_path=db_file_path, 
                                                                 table='prices', 
                                                                 metric='Adj Close')
    
DEFAULT_FUNDAMENTALS_PATH = os.path.join(os.path.expanduser('~'), '.fundamentals.sqlite')
def sqlite_fundamentals_cache(metric, db_file_path=DEFAULT_FUNDAMENTALS_PATH):
    connection = sqlite3.connect(db_file_path)
    driver = sqlite_drivers.SQLiteIntervalseries(connection=connection,
                                                 table='fundamentals',
                                                 metric=metric.metric_name)
    cache = FinancialDataRangesCache(gets_data=metric.get_data, database=driver)
    return cache
    
    
import unittest
class InitMethodTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from financial_fundamentals.test_infrastructure import turn_on_request_caching
        turn_on_request_caching()

    def test_sqlite_price_cache(self):
        import datetime
        cache = sqlite_price_cache(db_file_path=':memory:')
        prices = list(cache.get(symbol='GOOG', 
                                dates=[datetime.datetime(2013, 8, 1),
                                       datetime.datetime(2013, 8, 28),
                                       datetime.datetime(2012, 8, 12)]))
        date_prices = {date : value for date, value in prices}
        self.assertAlmostEqual(date_prices[datetime.datetime(2013, 8, 12, tzinfo=pytz.UTC)], 
                               885.51, delta=.1)
        
    def test_sqlite_fundamentals_cache(self):
        import datetime
        cache = sqlite_fundamentals_cache(metric=accounting_metrics.QuarterlyEPS, 
                                          db_file_path=':memory:')
        symbol = 'GOOG'
        dates = [datetime.datetime(2011, 12, 1, tzinfo=pytz.UTC),
                 datetime.datetime(2012, 12, 3, tzinfo=pytz.UTC), 
                 datetime.datetime(2012, 12, 4, tzinfo=pytz.UTC),
                 ]
        values = cache.get(symbol=symbol, dates=dates)
        value_d = {date : value for date, value in values}
        self.assertEqual(value_d[datetime.datetime(2012, 12, 3, tzinfo=pytz.UTC)], 6.53)
        
class EndToEndTests(unittest.TestCase):
    def test_quarterly_eps_sqlite(self):
        from financial_fundamentals.test_infrastructure import turn_on_request_caching
        turn_on_request_caching()
        import datetime
        from financial_fundamentals.accounting_metrics import QuarterlyEPS
        start, end = (datetime.datetime(2013, 1, 1, tzinfo=pytz.UTC), 
                      datetime.datetime(2013, 8, 1, tzinfo=pytz.UTC))
        cache = sqlite_fundamentals_cache(metric=QuarterlyEPS, 
                                          db_file_path=':memory:')
        earnings = cache.load_from_cache(stocks=['GOOG', 'AAPL'], 
                                         start=start, 
                                         end=end)
        pass

