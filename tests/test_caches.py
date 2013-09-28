'''
Created on Sep 12, 2013

@author: akittredge
'''


import unittest
from financial_fundamentals import sqlite_price_cache, accounting_metrics
from financial_fundamentals.caches import sqlite_fundamentals_cache
import pytz
from . infrastructure import turn_on_request_caching
import datetime
from financial_fundamentals.accounting_metrics import QuarterlyEPS
import sqlite3
from financial_fundamentals.sqlite_drivers import SQLiteIntervalseries
from financial_fundamentals.time_series_cache import FinancialDataRangesCache


class InitMethodTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        turn_on_request_caching()

    def test_sqlite_price_cache(self):
        cache = sqlite_price_cache(db_file_path=':memory:')
        prices = cache.get(symbol='GOOG', 
                            dates=[datetime.datetime(2013, 8, 1, tzinfo=pytz.UTC),
                                   datetime.datetime(2013, 8, 28, tzinfo=pytz.UTC),
                                   datetime.datetime(2012, 8, 12, tzinfo=pytz.UTC)])
        date_prices = {date : value for date, value in prices}
        self.assertAlmostEqual(date_prices[datetime.datetime(2013, 8, 28, tzinfo=pytz.UTC)], 
                               848.55, delta=.1)
        
    def test_sqlite_fundamentals_cache(self):
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

class TestFundamentalsCache(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        turn_on_request_caching()
        
    def test_border_behavior(self):
        '''Fundamentals were being inserted twice because of an off by one error.'''
        metric = accounting_metrics.QuarterlyEPS
        symbol = 'CSCO'
        connection = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES)
        driver = SQLiteIntervalseries(connection=connection,
                                     table='fundamentals',
                                     metric=metric.metric_name)
        cache = FinancialDataRangesCache(gets_data=metric.get_data, database=driver)
        dates = {datetime.datetime(2010, 2, 17)}
        list(cache.get(symbol, dates=dates))
        
        count_query = "SELECT COUNT(*)  AS count FROM fundamentals WHERE symbol = '{}'".format(symbol)
        count = connection.execute(count_query).next()['count']
        self.assertEqual(count, 1)
        list(cache.get(symbol, dates=dates))
        count = connection.execute(count_query).next()['count']
        self.assertEqual(count, 1)
        
class EndToEndTests(unittest.TestCase):
    def test_quarterly_eps_sqlite(self):
        turn_on_request_caching()
        start, end = (datetime.datetime(2013, 1, 1, tzinfo=pytz.UTC), 
                      datetime.datetime(2013, 8, 1, tzinfo=pytz.UTC))
        cache = sqlite_fundamentals_cache(metric=QuarterlyEPS, 
                                          db_file_path=':memory:')
        earnings = cache.load_from_cache(stocks=['GOOG', 'AAPL'], 
                                         start=start, 
                                         end=end)
