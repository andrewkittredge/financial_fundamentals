'''
Created on Aug 21, 2013

@author: akittredge
'''

import numpy as np
import dateutil.parser

class SQLiteTimeseries(object):
    def __init__(self, connection, table, metric):
        connection.row_factory = sqlite3.Row
        self._connection = connection
        self._table = table
        self._metric = metric
        
    get_query = '''SELECT value, symbol, metric, date from {} 
                    WHERE symbol IN (?) 
                    AND metric =  ?  
                '''
    def get(self, symbols, dates):
        '''return metric values for symbols and dates.'''
        with self._connection:
            cursor = self._connection.cursor()
            args = (','.join(symbols), self._metric)
            cursor.execute(self.get_query.format(self._table), args)
            for row in cursor.fetchall():
                yield self._beautify_record(record=row, metric=self._metric)
       
    insert_query = 'INSERT INTO {} (symbol, date, metric, value) VALUES (?, ?, ?, ?)'
    def set(self, symbol, records):
        with self._connection:
            for record in records:
                args = (symbol, record['date'], self._metric, record[self._metric])
                self._connection.execute(self.insert_query.format(self._table), args)
        
    create_stm = 'CREATE TABLE {:s} (date text, symbol text, metric text, value real)'
    @classmethod
    def create_table(cls, connection, table, metric):
        with connection:
            connection.execute(cls.create_stm.format(table, metric))
            
    @staticmethod
    def _beautify_record(record, metric):
        '''Cast metric to np.float and make date tz-aware.
        
        '''
        record = dict(record)
        record[metric] = np.float(record.pop('value'))
        date = dateutil.parser.parse(record['date'])
        record['date'] = date.replace(tzinfo=pytz.UTC)
        return record
        
class SQLiteIntervalseries(object):
    def get(self, symbol, date):
        '''return the metric value of symbol on date.'''
        
    def set_interval(self, symbol, start, end, value):
        '''set value for interval start and end.'''
        
import unittest
import datetime
import pytz
import sqlite3
class SQLiteTimeseriesTestCase(unittest.TestCase):
    table = 'price'
    metric = 'Adj Close'
    def setUp(self):
        self.connection = sqlite3.connect(':memory:')
        SQLiteTimeseries.create_table(connection=self.connection, 
                                      table=self.table, 
                                      metric='Adj Close')
        self.driver = SQLiteTimeseries(connection=self.connection, 
                                       table=self.table, 
                                       metric=self.metric)

    def test_get(self):
        symbol = 'ABC'
        date = datetime.datetime(2012, 12, 1, tzinfo=pytz.UTC)
        price = 6.5
        self.connection.execute('INSERT INTO {} (symbol, date, metric, value) VALUES (?, ?, ?, ?)'\
                                .format(self.table), (symbol, date, self.metric, price))
        cache_value = self.driver.get(symbols=[symbol], dates=[date]).next()
        self.assertEqual(cache_value[self.metric], price)
        self.assertEqual(cache_value['symbol'], symbol)
        self.assertEqual(cache_value['date'], date.replace(tzinfo=pytz.UTC))
        
    def test_set(self):
        symbol = 'ABC'
        date = datetime.datetime(2012, 12, 1, tzinfo=pytz.UTC)
        price = 6.5
        data = {'symbol' : symbol,
                self.metric : price,
                'date' : date,}
        self.driver.set(symbol=symbol, records=[data])
        qry = 'SELECT * FROM {}'.format(self.table)
        row = self.connection.execute(qry).fetchone()
        self.assertEqual(row['symbol'], symbol)
        self.assertEqual(row['metric'], self.metric)
        self.assertEqual(row['value'], price)

        