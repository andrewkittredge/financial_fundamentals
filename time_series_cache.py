'''
Created on Jul 29, 2013

@author: akittredge
'''
import itertools
class FinancialDataTimeSeriesCache(object):
    def __init__(self, gets_data, database):
        self._get_data = gets_data
        self._database = database
        
    def get(self, symbols, dates):
        cached_records = self._database.find(symbols, dates)
        uncached_symbols = set(symbols)
        get_dates = set(dates)
        for cached_symbol, records in itertools.groupby(cached_records,
                                            key=lambda record : record['symbol']):
            records = list(records)
            cached_dates = set(record['date'] for record in record)
            missing_dates = get_dates - cached_dates
            if missing_dates:
                new_records = self._get_set(symbol=cached_symbol, 
                                            dates=missing_dates)
                records = itertools.chain(records, new_records)
            yield cached_symbol, records
            uncached_symbols.remove(cached_symbol)
                
        for symbol in uncached_symbols:
            yield symbol, self._get_set(symbol, dates)

    def _get_set(self, symbol, dates):
        new_records = list(self._get_data(symbol, dates))
        self._database.set(symbol, new_records)
        return new_records


class FinancialDataRangeSeriesCache(object):
    def __init__(self, gets_data, database):
        self._get_data = gets_data
        self._database = database
        
    def get(self, symbols, dates):
        for symbol in symbols:
            for dates in dates:
                cached_value = 
'''
do we have a date that is within the window.

Ranges we have, ranges we don't.  
''' 