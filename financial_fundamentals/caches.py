import pymongo
from financial_fundamentals.mongo_drivers import MongoIntervalseries,\
    MongoTimeseries
from financial_fundamentals.time_series_cache import FinancialIntervalCache,\
    FinancialDataTimeSeriesCache
from financial_fundamentals.prices import get_prices_from_yahoo


import os
from financial_fundamentals import sqlite_drivers
from financial_fundamentals.accounting_metrics import AccountingMetricGetter
from financial_fundamentals.edgar import HTMLEdgarDriver


def mongo_fundamentals_cache(metric, mongo_host='localhost', mongo_port=27017,
                             filing_getter=HTMLEdgarDriver):
    mongo_client = pymongo.MongoClient(mongo_host, mongo_port)
    mongo_collection = mongo_client.fundamentals.fundamentals
    db = MongoIntervalseries(collection=mongo_collection, 
                         metric=metric.name)
    metric_getter = AccountingMetricGetter(metric=metric,
                                           filing_getter=filing_getter)
    cache = FinancialIntervalCache(get_data=metric_getter.get_data, database=db)
    return cache

def mongo_price_cache(mongo_host='localhost', mongo_port=27017):
    client = pymongo.MongoClient(mongo_host, mongo_port)
    collection = client.prices.prices
    db = MongoTimeseries(mongo_collection=collection, metric='price')
    cache = FinancialDataTimeSeriesCache(gets_data=get_prices_from_yahoo, 
                                         database=db)
    return cache

DEFAULT_PRICE_PATH = os.path.join(os.path.expanduser('~'), '.prices.sqlite')
def sqlite_price_cache(db_file_path=DEFAULT_PRICE_PATH):
    '''Return a cache that persists prices downloaded from yahoo.
    
    '''
    return FinancialDataTimeSeriesCache.build_sqlite_price_cache(sqlite_file_path=db_file_path, 
                                                                 table='prices', 
                                                                 metric='Adj Close')

DEFAULT_FUNDAMENTALS_PATH = os.path.join(os.path.expanduser('~'), '.fundamentals.sqlite')
def sqlite_fundamentals_cache(metric, 
                              db_file_path=DEFAULT_FUNDAMENTALS_PATH, 
                              filing_getter=HTMLEdgarDriver):
    connection = sqlite_drivers.SQLiteIntervalseries.connect(db_file_path)
    driver = sqlite_drivers.SQLiteIntervalseries(connection=connection,
                                                 table='fundamentals',
                                                 metric=metric.name)
    metric_getter = AccountingMetricGetter(metric=metric, 
                                           filing_getter=filing_getter)
    
    cache = FinancialIntervalCache(get_data=metric_getter.get_data, 
                                     database=driver)
    return cache