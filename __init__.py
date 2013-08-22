import pymongo
from financial_fundamentals.mongo_timeseries import MongoIntervalseries,\
    MongoTimeseries
from financial_fundamentals.time_series_cache import FinancialDataRangesCache,\
    FinancialDataTimeSeriesCache
from financial_fundamentals.edgar import filing_before
from financial_fundamentals.prices import get_prices_from_yahoo
import pytz


def mongo_fundamentals_cache(metric, mongo_host='localhost', mongo_port=27017):
    mongo_client = pymongo.MongoClient(mongo_host, mongo_port)
    mongo_collection = mongo_client.fundamentals.fundamentals
    db = MongoIntervalseries(collection=mongo_collection, 
                         metric=metric.metric_name)
    cache = FinancialDataRangesCache(gets_data=metric.get_data, database=db)
    return cache

def mongo_price_cache(mongo_host='localhost', mongo_port=27017):
    client = pymongo.MongoClient(mongo_host, mongo_port)
    collection = client.prices.prices
    db = MongoTimeseries(mongo_collection=collection, metric='price')
    cache = FinancialDataTimeSeriesCache(gets_data=get_prices_from_yahoo, 
                                         database=db)
    return cache

if __name__ == '__main__':
    import requests_cache
    requests_cache.configure('fundamentals_cache_test')
    from financial_fundamentals.accounting_metrics import QuarterlyEPS
    import datetime
    price_cache = mongo_price_cache()
    print list(price_cache.get(symbols=['GOOG'], dates=[datetime.datetime(2013, 8, 1, tzinfo=pytz.UTC)]).next()[1])
    cache = mongo_fundamentals_cache(QuarterlyEPS)
    print cache.get(symbols=['GOOG'], dates=[datetime.datetime(2013, 1, 1)]).next()
    print cache.get(symbols=['AAPL'], dates=[datetime.datetime(2013, 1, 1)]).next()