'''
Created on Jul 2, 2013

@author: akittredge
'''


from pandas.io.data import get_data_yahoo
import datetime
import numpy as np
import pytz
from financial_fundamentals.exceptions import NoDataForStock,\
    ExternalRequestFailed
import re

SYMBOLS_YAHOO_DOES_NOT_HAVE = {'CVH',
                               'HNZ',
                               'PCS',
                               'S',
                               }


def get_prices_from_yahoo(symbol, dates, type_of_price='Adj Close'):
    '''Yields date, value pairs.'''
    if symbol in SYMBOLS_YAHOO_DOES_NOT_HAVE:
        raise NoDataForStock("Cannot download {} from yahoo".format(symbol))
    # Yahoo errors out if you only ask for recent dates.
    start = min(min(dates), 
                datetime.datetime.now(pytz.UTC) - datetime.timedelta(days=30))
    end = max(dates)
    
    prices = _wrapped_get_data_yahoo(symbol=symbol, start=start, end=end)

    for date_price in prices.iterrows():
        date = datetime.datetime(date_price[0].year,
                                  date_price[0].month,
                                  date_price[0].day,
                                  tzinfo=pytz.UTC)
        yield date, np.float(date_price[1][type_of_price])

def _wrapped_get_data_yahoo(symbol, start, end):
    '''Handle the various exceptions that downloading from yahoo raises.'''
    try:
        return get_data_yahoo(symbols=symbol, start=start, end=end)
    except IOError as e:
        if re.match(r'after \d tries, Yahoo! did not return a 200 for url',
                    e.message):
            raise ExternalRequestFailed(e.message)
