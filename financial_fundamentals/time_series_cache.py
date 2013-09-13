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
from financial_fundamentals.exceptions import NoDataForStock
import warnings

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
        try:
            values = {date : value for date, value in cache.get(symbol=symbol, 
                                                           dates=python_datetimes)}
        except NoDataForStock:
            warnings.warn('No data for {}'.format(symbol))
            continue
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
            missing_dates.discard(date)
            yield date, value
        if missing_dates:
            for date, value in self._get_set(symbol, dates=missing_dates):
                yield date, value
            
    def _get_set(self, symbol, dates):
        new_records = list(self._get_data(symbol, dates))
        self._database.set(symbol, new_records)
        for date, value in new_records:
            if date in dates: # only yield dates that were missing.
                yield date, value
        
    
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
