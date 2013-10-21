'''
Created on Aug 21, 2013

@author: akittredge
'''

import numpy as np
import sqlite3

import pytz
import datetime

class SQLiteDriver(object):
    def __init__(self, connection, table, metric):
        connection.row_factory = sqlite3.Row
        self._connection = connection
        self._table = table
        self._metric = metric
        self._ensure_table_exists(connection, table)

    @classmethod
    def _ensure_table_exists(cls, connection, table):
        with connection:
            cursor = connection.cursor()
            cursor.execute(cls._create_stmt.format(table_name=table))
            
    @classmethod
    def connect(cls, database):
        return sqlite3.connect(database, 
                               detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
            
class SQLiteTimeseries(SQLiteDriver):
    _create_stmt = '''CREATE TABLE IF NOT EXISTS {table_name}
                    (date timestamp, symbol text, metric text, value real);
                 '''
    _create_index_stmt = '''
                        CREATE INDEX IF NOT EXISTS 
                        time_series_index ON {table_name} (date, symbol, metric);
                        '''

    @classmethod
    def _ensure_table_exists(cls, connection, table):
        super(SQLiteTimeseries, cls)._ensure_table_exists(connection,
                                                          table)
        with connection:
            cursor = connection.cursor()
            cursor.execute(cls._create_index_stmt.format(table_name=table))

    _get_query = '''SELECT value, date from {} 
                    WHERE symbol = ?
                    AND date BETWEEN ? AND ?
                    AND metric =  ?
                '''
    def get(self, symbol, dates):
        '''return all stored symbol metric values for dates between min(dates) and max(dates).
        
        '''
        with self._connection:
            qry = self._get_query.format(self._table)
            cursor = self._connection.cursor()
            args = [symbol, min(dates), max(dates), self._metric]
            cursor.execute(qry, args)
            for row in cursor.fetchall():
                yield row['date'], np.float(row['value'])
       
    _insert_query = 'INSERT INTO {} (symbol, date, metric, value) VALUES (?, ?, ?, ?)'
    def set(self, symbol, records):
        '''records is a sequence of date, value items.'''
        with self._connection:
            query = self._insert_query.format(self._table)
            self._connection.executemany(query, ((symbol, date, self._metric, value)
                                                 for date, value in records))


class SQLiteIntervalseries(SQLiteDriver):
    _create_stmt = '''CREATE TABLE IF NOT EXISTS {table_name} 
                        (start timestamp, 
                        end timestamp, 
                        symbol text, 
                        metric text, 
                        value real)
                    '''
    _get_qry = '''SELECT value FROM {} \
                    WHERE metric = ? AND symbol = ? AND start <= ? AND (? <= end OR end IS NULL)\
                    '''
    def get(self, symbol, date):
        '''return the metric value of symbol on date.'''
        date = date.replace(tzinfo=None) # can't figure out timezones in sqlite.
        qry = self._get_qry.format(self._table)
        cursor = self._connection.cursor()
        cursor.execute(qry, (self._metric,
                             symbol,
                             date,
                             date))
        row = cursor.fetchone()
        
        return row and (np.float(row['value']) if row['value'] else np.NaN)

    _insert_query = ('INSERT INTO {} '
                     '(symbol, start, end, metric, value) VALUES (?, ?, ?, ?, ?)')
    def set_interval(self, symbol, start, end, value):
        '''set value for interval start and end.'''
        qry = self._insert_query.format(self._table)
        with self._connection:
            cursor = self._connection.cursor()
            cursor.execute(qry, (symbol, 
                                 start, 
                                 end, 
                                 self._metric, 
                                 value))
        self._detect_duplicates()
            
    duplicate_query = ('select * from {table_name} where rowid not in ' 
                       '(select max(rowid) from {table_name}\n'
                       'group by start, end, symbol, metric);')
    
    def _detect_duplicates(self):
        with self._connection:
            qry = self.duplicate_query.format(table_name=self._table)
            assert not self._connection.execute(qry).fetchone()
        

def _tz_aware_timestamp_adapter(val):
    '''from https://gist.github.com/acdha/6655391'''
    datepart, timepart = val.split(b" ")
    year, month, day = map(int, datepart.split(b"-"))
 
    if b"+" in timepart:
        timepart, tz_offset = timepart.rsplit(b"+", 1)
        if tz_offset == b'00:00':
            tzinfo = pytz.utc
        else:
            hours, minutes = map(int, tz_offset.split(b':', 1))
            tzinfo = pytz.utc(datetime.timedelta(hours=hours, minutes=minutes))
    else:
        tzinfo = None
 
    timepart_full = timepart.split(b".")
    hours, minutes, seconds = map(int, timepart_full[0].split(b":"))
 
    if len(timepart_full) == 2:
        microseconds = int('{:0<6.6}'.format(timepart_full[1].decode()))
    else:
        microseconds = 0
 
    val = datetime.datetime(year, month, day, hours, minutes, seconds, microseconds, tzinfo)
 
    return val
 
sqlite3.register_converter('timestamp', _tz_aware_timestamp_adapter)

if __name__ == '__main__':
    connection = SQLiteIntervalseries.connect('/Users/akittredge/.fundamentals.sqlite')
    driver = SQLiteIntervalseries(connection=connection, table='fundamentals', metric='quarterly_eps')
    date =  datetime.datetime(2010, 01, 04)
    print driver.get(symbol='CAT', date=date)