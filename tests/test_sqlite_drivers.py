'''
Created on Sep 12, 2013

@author: akittredge
'''



import unittest
import datetime

from financial_fundamentals.sqlite_drivers import SQLiteTimeseries,\
    SQLiteIntervalseries, SQLiteDriver
import pytz
from tests.infrastructure import IntervalseriesTestCase
from zipline.utils.tradingcalendar import get_trading_days
from financial_fundamentals.indicies import S_P_500_TICKERS
import random
from collections import defaultdict

class SQLiteTestCase(unittest.TestCase):
    def setUp(self):
        self.connection = SQLiteDriver.connect(':memory:')
        
class SQLiteTimeseriesTestCase(SQLiteTestCase):
    table = 'price'
    metric = 'Adj Close'
    def setUp(self):
        super(SQLiteTimeseriesTestCase, self).setUp()
        self.driver = SQLiteTimeseries(connection=self.connection, 
                                       table=self.table, 
                                       metric=self.metric)

    def test_single_get(self):
        symbol = 'ABC'
        date = datetime.datetime(2012, 12, 1, tzinfo=pytz.UTC)
        price = 6.5
        self.connection.execute('INSERT INTO {} (symbol, date, metric, value) VALUES (?, ?, ?, ?)'\
                                .format(self.table), (symbol, date, self.metric, price))
        cache_date, cache_price = self.driver.get(symbol=symbol, dates=[date]).next()
        self.assertEqual(cache_price, price)
        self.assertEqual(cache_date, date)
        
    def insert_date_combos(self, symbol_date_combos):
        test_vals = defaultdict(dict)
        for symbol, date in symbol_date_combos:
            price = random.randint(0, 1000)
            qry = 'INSERT INTO {} (symbol, date, metric, value) VALUES (?, ?, ?, ?)'
            qry = qry.format(self.table)
            args = (symbol, date, self.metric, price)
            self.connection.execute(qry, args)
            test_vals[symbol][date] = price
        return test_vals
        
    def test_multiple_get(self):
        symbols = ['ABC', 'XYZ']
        dates = [datetime.datetime(2012, 12, 1, tzinfo=pytz.UTC),
                 datetime.datetime(2012, 12, 2, tzinfo=pytz.UTC),
                 datetime.datetime(2012, 12, 15, tzinfo=pytz.UTC),
                 ]
        symbol_date_combos = [(symbol, date) for symbol in symbols for date in dates]
        price_dict = self.insert_date_combos(symbol_date_combos)
        
        for symbol in symbols:
            cached_values = list(self.driver.get(symbol=symbol, dates=dates))
            cache_dict = {date : price for date, price in cached_values}
            self.assertDictEqual(price_dict[symbol], cache_dict)
        
    def test_date_query(self):
        '''assert we only get the dates we want.'''
        symbols = ['ABC', 'XYZ']
        dates = [datetime.datetime(2012, 12, 1, tzinfo=pytz.UTC),
                 datetime.datetime(2012, 12, 2, tzinfo=pytz.UTC),
                 datetime.datetime(2012, 12, 3, tzinfo=pytz.UTC),
                 ]
        symbol_date_combos = [(symbol, date) for symbol in symbols for date in dates]
        prices = self.insert_date_combos(symbol_date_combos)
        self.insert_date_combos([('ABC', datetime.datetime(2012, 12, 15))])
        for symbol in symbols:
            cached_values = self.driver.get(symbol=symbol, dates=dates)
            cache_dict = {date : price for date, price in cached_values}
            self.assertDictEqual(prices[symbol], cache_dict)
    
    @unittest.skip('slow')
    def test_volume(self):
        '''make sure a larger number of records doesn't choke it somehow.'''
        symbols = S_P_500_TICKERS[:200]
        datetimeindex = get_trading_days(start=datetime.datetime(2012, 1, 1, tzinfo=pytz.UTC), 
                                 end=datetime.datetime(2012, 7, 4, tzinfo=pytz.UTC))
        dates = [datetime.datetime(d.date().year, d.date().month, d.date().day).replace(tzinfo=pytz.UTC) 
                 for d in datetimeindex]
        symbol_date_combos = [(symbol, date) for symbol in symbols for date in dates]
        test_vals = self.insert_date_combos(symbol_date_combos)
        for symbol in symbols:
            cached_values = self.driver.get(symbol=symbol, dates=dates)
            cache_dict = {date : price for date, price in cached_values}    
            self.assertDictEqual(test_vals[symbol], cache_dict)
        
    def test_set(self):
        symbol = 'ABC'
        date = datetime.datetime(2012, 12, 1, tzinfo=pytz.UTC)
        price = 6.5
        self.driver.set(symbol=symbol, records=[(date, price)])
        qry = 'SELECT * FROM {}'.format(self.table)
        row = self.connection.execute(qry).fetchone()
        self.assertEqual(row['symbol'], symbol)
        self.assertEqual(row['metric'], self.metric)
        self.assertEqual(row['value'], price)
        
    def test_lots_of_dates(self):
        '''sqlite can only handle 999 variables.'''
        start = datetime.datetime(1990, 1,1, tzinfo=pytz.UTC)
        end = datetime.datetime.now(pytz.UTC)
        dates = list(get_trading_days(start, end).to_pydatetime())
        for price, date in enumerate(dates):
            self.connection.execute('INSERT INTO {} (symbol, date, metric, value) VALUES (?, ?, ?, ?)'.format(self.table),
                                    ('ABC', date, self.metric, price))
        list(self.driver.get(symbol='ABC', dates=dates))

class SQLiteTimestampTestCase(SQLiteTestCase):
    def test_datetime_type_storage(self):
        '''make sure we can store datetimes in sqlite.'''
        conn = self.connection
        table_name, test_value = 'test_table', 'test_value'
        symbol = 'ABC'
        driver = SQLiteTimeseries(conn, table_name, metric=test_value)
        record_date = datetime.datetime(2012, 12, 1, tzinfo=pytz.UTC)
        driver.set(symbol=symbol, records=[(record_date, 100.)])
        result = conn.cursor().execute('select * from {}'.format(table_name)).fetchone()
        date = result['date']
        self.assertIsInstance(date, datetime.datetime)
        self.assertEqual(date.tzinfo, pytz.UTC)
        
        qry = 'select value from {} where date = ?'.format(table_name)
        results = conn.cursor().execute(qry, (record_date,)).fetchall()
        self.assertEqual(len(list(results)), 1)
        
class SQLiteIntervalseriesTestCase(SQLiteTestCase, IntervalseriesTestCase):
    table = 'fundamentals'
    def setUp(self):
        super(SQLiteIntervalseriesTestCase, self).setUp()
        self.cache = self.build_cache(metric=self.metric)
        
    def build_cache(self, metric):
        return SQLiteIntervalseries(connection=self.connection,
                                    table=self.table,
                                    metric=metric)
        
    def find_in_database(self, start, end, symbol):
        qry = 'SELECT * FROM {} WHERE metric = ? AND start <= ? AND end <= ? AND symbol = ?'.format(self.table)
        row = self.connection.execute(qry, (self.metric, start, end, symbol)).fetchone()
        return row['value']
    
    def insert_into_database(self, data):
        self.connection.execute('INSERT INTO {} (symbol, start, end, metric, value) VALUES (?, ?, ?, ?, ?)'\
                                    .format(self.table), (data['symbol'], 
                                                          data['start'],
                                                          data['end'], 
                                                          self.metric, 
                                                          data[self.metric]))
        
if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(SQLiteIntervalseriesTestCase('test_no_end_interval'))
    unittest.TextTestRunner().run(suite)