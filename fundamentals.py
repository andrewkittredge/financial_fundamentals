'''
Created on Jan 26, 2013

@author: akittredge
'''

from datetime import date
from edgar import filing_before, NoFilingFound
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


class SQLLiteMultiplesCache(object):
    
    def __init__(self, db_path='multiples.db', table='multiples'):
        self.table = table
        self.connection = sqlite3.connect(db_path)
        
    create_stm = '''CREATE TABLE {:s} (date text, ticker text, metric text, value real)'''   
    def create_database(self):
        connection = self.connection
        with connection:
            connection.execute(self.create_stm.format(self.table))
    
    def _get_filing_before_date(self, ticker, date_after, filing_type):
        return filing_before(ticker=ticker,
                             filing_type=filing_type,
                             date_after=date_after)
        
    def _get_multiple_value(self, ticker, date_, metric):
        try:
            filing = self._get_filing_before_date(ticker=ticker,
                                              filing_type=metric.filing_type,
                                              date_after=date_)
        except NoFilingFound:
            raise MissingData('Filing for {}, {} not found.'.format(ticker, date_))
        return metric.value_from_filing(filing)
    
    get_multiple_stm = '''SELECT value FROM {} WHERE ticker = ? AND 
                        metric = ? AND date = ?'''
    def _get_db_value(self, ticker, date_, metric):
        args = (ticker, metric.metric_name, date_)
        stmt_template = self.get_multiple_stm.format(self.table)
        val_from_db = self.connection.execute(stmt_template, args).fetchone()
        return val_from_db[0] if val_from_db else None
    
    insert_stmt = '''INSERT INTO {} (ticker, date, metric, value) 
                    VALUES (?, ?, ?, ?)'''
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

class MissingData(Exception):
    pass


class TestsSQLiteMultiplesCache(unittest.TestCase):
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
        class FakeMetric(object):
            filing_type='10-Q'
            metric_name = 'earnings'

            @classmethod
            def value_from_filing(cls, *args, **kwargs):
                return cls.assert_value
        ticker = 'AAPL'
        date_ = date(2012, 1, 1)
        FakeMetric.assert_value = 3.14
        val_from_cache = self.cache.get(ticker, date_, FakeMetric)
        self.assertEqual(val_from_cache, FakeMetric.assert_value)
        val_from_db = self.cache._get_db_value(ticker, date_, FakeMetric)
        self.assertEqual(val_from_db, FakeMetric.assert_value)
        other_metric = FakeMetric()
        msft_price = 2.72
        FakeMetric.assert_value = msft_price
        other_ticker = 'msft'
        other_val_from_cache = self.cache.get(other_ticker, date_, other_metric)
        self.assertEqual(other_val_from_cache, msft_price)
        

if __name__ == '__main__':
    cache = SQLLiteMultiplesCache()
    cache.create_database()
    