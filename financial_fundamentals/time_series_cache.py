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

from financial_fundamentals.mongo_drivers import MongoTimeseries
from financial_fundamentals.exceptions import NoDataForStock,\
    ExternalRequestFailed, NoDataForStockOnDate, NoDataForStockForRange
import warnings
import numpy as np
from numbers import Number


class FinancialDataTimeSeriesCache(object):
    '''Cache data, such as prices, that are accurate at some instant in time.
    
    '''
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
            try:
                missing_dates.remove(date)
            except KeyError:
                #_database.get can return data more dates than we asked for.
                continue
            else:
                yield date, value
        if missing_dates:
            for date, value in self._get_set(symbol, dates=missing_dates):
                yield date, value
            
    def _get_set(self, symbol, dates):
        new_records = list(self._get_data(symbol, dates))
        self._database.set(symbol, new_records)
        missing_dates = set(dates)
        for date, value in new_records:
            missing_dates.discard(date)
            if date in dates: # only yield dates that were missing.
                yield date, value
        self._database.set(symbol, ((missing_date, 'NaN') for 
                                    missing_date in missing_dates)
                           )
        for missing_date in missing_dates:
            yield missing_date, np.nan
        
    
    @classmethod
    def build_sqlite_price_cache(cls, 
                                 sqlite_file_path, 
                                 table='prices', 
                                 metric='Adj Close'):
        connection = SQLiteTimeseries.connect(sqlite_file_path)
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
    
    def load_from_cache(self,
                        indexes={},
                        stocks=[],
                        start=pd.datetime(1990, 1, 1, 0, 0, 0, 0, pytz.utc),
                        end=datetime.datetime.now().replace(tzinfo=pytz.utc),
                        ):
        '''Equivalent to zipline.utils.factory.load_from_yahoo.
        
        '''
        assert start <= end, 'start must be before end'
        datetime_index = get_trading_days(start=start, end=end)
        df = pd.DataFrame(index=datetime_index)
        python_datetimes = list(datetime_index.to_pydatetime())
        for symbol in stocks:
            # there's probably a clever way of avoiding building a dictionary.
            try:
                date_values = self.get(symbol=symbol, dates=python_datetimes)
                values = {date : value for date, value in date_values}
            except NoDataForStock:
                warnings.warn('No data for {}'.format(symbol))
            except ExternalRequestFailed as e:
                warnings.warn('Getting data for {} failed {}'.format(symbol,
                                                                     e.message))
            else:
                series = pd.Series(values)
                df[symbol] = series
            
        for name, ticker in indexes.iteritems():
            values = {date : value for date, value in self.get(symbol=ticker, 
                                                               dates=python_datetimes)}
            df[name] = pd.Series(values)
        return df

    
class FinancialIntervalCache(object):
    '''Cache data that is accurate for the interval between to dates.
    For example earnings from SEC filings are `in effect` for an entire quarter,
    until the next filing is submitted.
    
    '''
    def __init__(self, get_data, database):
        self._get_data = get_data
        self._database = database
        
    def get(self, symbol, dates):
        '''Return cache's metric value for the passed dates.
        
        dates should be UTC.
        '''
        for date in dates:
            # Not looking for more than one date at a time because the set
            # operation will set multiple dates per call.
            cached_value = self._database.get(symbol=symbol, date=date)
            if isinstance(cached_value, Number):
                yield cached_value
            else:
                yield self._get_set(symbol=symbol, date=date)
    
    def _get_set(self, symbol, date):
        print 'cache miss', symbol, date
        try:
            start, value, end = self._get_data(symbol=symbol, date=date)
        except NoDataForStockForRange as e:
            value = 'NaN'
            start = e.start
            end = e.end
        start = start and datetime.datetime(start.year, 
                                            start.month, 
                                            start.day, 
                                            tzinfo=pytz.UTC)
        end = end and datetime.datetime(end.year, 
                                        end.month, 
                                        end.day, 
                                        tzinfo=pytz.UTC)
        self._database.set_interval(symbol=symbol, 
                                    start=start, 
                                    end=end, 
                                    value=value)
        return value

    def load_from_cache(self, 
                        stocks, 
                        start=pd.datetime(1990, 1, 1, 0, 0, 0, 0, pytz.utc),
                        end=datetime.datetime.now().replace(tzinfo=pytz.utc)):
        assert start <= end, 'start must be before end'
        datetime_index = get_trading_days(start=start, end=end)
        df = pd.DataFrame(index=datetime_index)
        python_datetimes = list(datetime_index.to_pydatetime())
        for symbol in stocks:
            try:
                values = self.get(symbol=symbol, dates=python_datetimes)
                series = pd.Series(data=values, index=datetime_index)
            except (NoDataForStock, NoDataForStockOnDate) as e:
                warnings.warn('No data for {}'.format(symbol))
            except ExternalRequestFailed as e:
                warnings.warn('Getting data for {} failed {}'.format(symbol,
                                                                     e.message))
            else:
                df[symbol] = series
        return df