'''
Created on Jan 26, 2013

@author: akittredge
'''

from datetime import date
from edgar import filing_before
from accounting_metrics import QuarterlyEPS
import logging
import unittest
import sqlite3
import os

quarter_end_days = ((3, 31), (6, 30), (9, 30), (12, 31))
quarter_closes = lambda year : (date(year, month[0], month[1]) for month in 
                                quarter_end_days)

years_quarter_closes = lambda years : (quarter_close for year in years for 
                                       quarter_close in quarter_closes(year))

logger = logging.getLogger('fundamentals')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

def price_to_earnings(ticker, date_, price):
    
    filing = filing_before(ticker=ticker, 
                           filing_type='10-Q', 
                           date_after=date_)
    return price /(  QuarterlyEPS.value(filing) * 4)

class SQLLiteMultiplesCache(object):
    create_stm = '''CREATE TABLE {:s} (date text, ticker text, metric text, value real)'''
    def __init__(self, db_path='multiples.db', table='multiples'):
        self.table = table
        self.connection = sqlite3.connect(db_path)
        
    def create_database(self):
        connection = self.connection
        with connection:
            connection.execute(self.create_stm.format(self.table))
    
    def _get_filing_before_date(self, ticker, date_after, filing_type):
        return filing_before(ticker=ticker,
                             filing_type=filing_type,
                             date_after=date_after)
        
    def _get_multiple_value(self, ticker, date_, metric):
        filing = self._get_filing_before_date(ticker=ticker,
                                              filing_type=metric.filing_type,
                                              date_after=date_)
        return metric.value_from_filing(filing)
    
    get_multiple_stm = '''SELECT value FROM {} WHERE ticker = ? AND metric = ? AND date = ?'''
    def _get_db_value(self, ticker, date_, metric):
        args = (ticker, date_,  metric.metric_name)
        return self.connection.execute(self.get_multiple_stm.format(self.table), 
                                        args).fetchone()
    
    insert_stmt = '''INSERT INTO {} (ticker, date, metric, value) VALUES (?, ?, ?, ?)'''
    def _set_db_value(self, ticker, date_, metric, value):
        with self.connection:
            args = (ticker, date_, metric.metric_name, value)
            self.connection.execute(self.insert_stmt.format(self.table), args)
            
    def get(self, ticker, date_, metric):
        value = self._get_db_value(ticker, date_, metric)
        if not value:
            value = self._get_multiple_value(ticker, date_, metric)
            self._set_db_value(ticker, date_, metric, value)
        return value
    
class TestsMultiplesCache(unittest.TestCase):
    db_path = '/tmp/multiples.db'
    def setUp(self):
        try:
            os.remove(self.db_path)
        except OSError:
            pass
        self.cache = SQLLiteMultiplesCache(db_path=self.db_path)
        self.cache.create_database()
    
    def tearDown(self):
        os.remove(self.db_path)
        
    def test_db_creation(self):
        try:
            os.remove(self.db_path)
        except OSError:
            pass
        cache = SQLLiteMultiplesCache(db_path=self.db_path)
        cache.create_database()
        qry = 'SELECT name FROM sqlite_master WHERE type = "table"'
        self.assertIn(cache.table, cache.connection.execute(qry).fetchone())
        
    def test_get_value(self):
        assert_value = 3.14
        class FakeMetric(object):
            filing_type='10-Q'
            metric_name = 'earnings'
            @classmethod
            def value_from_filing(cls, *args, **kwargs):
                return assert_value + 1
        ticker = 'AAPL'
        date_ = date(2012, 1, 1)
        val_from_cache = self.cache.get(ticker, date_, FakeMetric)
        self.assertEqual(val_from_cache, assert_value)
        self.assertEqual(self.cache._get_db_value(ticker, date_, FakeMetric), 
                         assert_value)
        

@unittest.SkipTest
class TestsFundamentals(unittest.TestCase):
    def setUp(self):
        import requests_cache
        requests_cache.configure('fundamentals_cache_test')

    def test_pe(self):
        dec_28_2011_appl_pe = 14.55 # from http://ycharts.com/companies/AAPL/pe_ratio
        dec_28_2011_appl_price = 402.64
        metric_date = date(2011, 12, 28)
        computed_pe = price_to_earnings('aapl', metric_date, dec_28_2011_appl_price)
        self.assertAlmostEqual(computed_pe, dec_28_2011_appl_pe, delta=.1)