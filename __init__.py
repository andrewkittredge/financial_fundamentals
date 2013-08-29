import pymongo
from financial_fundamentals.mongo_drivers import MongoIntervalseries,\
    MongoTimeseries
from financial_fundamentals.time_series_cache import FinancialDataRangesCache,\
    FinancialDataTimeSeriesCache
from financial_fundamentals.edgar import filing_before
from financial_fundamentals.prices import get_prices_from_yahoo
import pytz
import sqlite3
from financial_fundamentals.sqlite_drivers import SQLiteTimeseries
import financial_fundamentals
import os


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
DEFAULT_PRICE_PATH = os.path.join(os.path.expanduser('~'), 'prices')
def sqlite_price_cache(db_file_path=DEFAULT_PRICE_PATH):
    '''Return a cache that persists prices downloaded from yahoo.
    
    '''
    connection = sqlite3.connect(db_file_path)
    driver = SQLiteTimeseries(connection=connection, 
                              table='prices', 
                              metric='Adj Close')
    cache = FinancialDataTimeSeriesCache(gets_data=get_prices_from_yahoo,
                                         database=driver)
    return cache

import unittest
class InitMethodTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import requests_cache
        requests_cache.configure('fundamentals_cache_test')

    def test_sqlite_price_cache(self):
        import datetime
        cache = sqlite_price_cache(db_file_path=':memory:')
        prices = list(cache.get(symbol='GOOG', 
                           dates=[datetime.datetime(2013, 8, 1),
                                  datetime.datetime(2013, 8, 28)]))
        date_prices = {date : value for date, value in prices}
        self.assertAlmostEqual(date_prices[datetime.datetime(2013, 8, 12, tzinfo=pytz.UTC)], 
                               885.51, delta=.1)

if __name__ == '__main__':
    import requests_cache
    requests_cache.configure('fundamentals_cache_test')
    from financial_fundamentals.accounting_metrics import QuarterlyEPS
    import datetime
    price_cache = mongo_price_cache()
    print list(price_cache.get(symbols=['GOOG'], dates=[datetime.datetime(2013, 8, 1, tzinfo=pytz.UTC)]).next()[1])
    cache = mongo_fundamentals_cache(QuarterlyEPS)
    print cache.get(symbols=['GOOG'], dates=[datetime.datetime(2013, 1, 1)]).next()
    print cache.get(symbols=['AAPL'], dates=[datetime.datetime(2013, 1, 1)]).next()
