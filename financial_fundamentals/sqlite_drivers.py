'''
Created on Aug 21, 2013

@author: akittredge
'''

import numpy as np
import dateutil.parser
import sqlite3

import pytz

class SQLiteDriver(object):
    def __init__(self, connection, table, metric):
        connection.row_factory = sqlite3.Row
        self._connection = connection
        self._table = table
        self._metric = metric
        self._ensure_table_exists(connection, table)
        
    @classmethod
    def _ensure_table_exists(cls, connection, table):
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row['name'] for row in cursor.fetchall()]
        if table not in tables:
            cls.create_table(connection, table)
            
    @classmethod
    def create_table(cls, connection, table):
        with connection:
            connection.execute(cls.create_stm.format(table))
            
class SQLiteTimeseries(SQLiteDriver):
    create_stm = 'CREATE TABLE {:s} (date timestamp, symbol text, metric text, value real)'
    get_query = '''SELECT value, symbol, metric, date from {} 
                    WHERE symbol = ?
                    AND date in ({})
                    AND metric =  ?
                '''
    def get(self, symbol, dates):
        '''return metric values for symbols and dates.'''
        with self._connection:
            qry = self.get_query.format(self._table, 
                                        ','.join('?' * len(dates)),
                                        )
            cursor = self._connection.cursor()
            args = [symbol] + dates + [self._metric]
            cursor.execute(qry, args)
            for row in cursor.fetchall():
                yield self._beautify_record(record=row)
       
    insert_query = 'INSERT INTO {} (symbol, date, metric, value) VALUES (?, ?, ?, ?)'
    def set(self, symbol, records):
        '''records is a sequence of date, value items.'''
        with self._connection:
            for date, value in records:
                args = (symbol, date, self._metric, value)
                self._connection.execute(self.insert_query.format(self._table), args)
        
    
    @staticmethod
    def _beautify_record(record):
        '''Cast metric to np.float and make date tz-aware.
        
        '''
        return (dateutil.parser.parse(record['date']).replace(tzinfo=pytz.UTC), 
                    np.float(record['value']))
        
class SQLiteIntervalseries(SQLiteDriver):
    create_stm = 'CREATE TABLE {:s} (start timestamp, end timestamp, symbol text, metric text, value real)'
    get_qry = 'SELECT value FROM {} WHERE metric = ? AND symbol = ? AND start <= ? AND ? <= end'
    def get(self, symbol, date):
        '''return the metric value of symbol on date.'''
        qry = self.get_qry.format(self._table)
        with self._connection:
            row = self._connection.execute(qry, (self._metric,
                                                 symbol,
                                                 date,
                                                 date)).fetchone()
        return row and (np.float(row['value']) if row['value'] else np.NaN)

    insert_query = 'INSERT INTO {} (symbol, start, end, metric, value) VALUES (?, ?, ?, ?, ?)'
    def set_interval(self, symbol, start, end, value):
        '''set value for interval start and end.'''
        qry = self.insert_query.format(self._table)
        with self._connection:
            self._connection.execute(qry, (symbol, start, end, 
                                           self._metric, value))
        