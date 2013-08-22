'''
Created on Jul 2, 2013

@author: akittredge
'''


from pandas.io.data import get_data_yahoo
import numpy as np


def get_prices_from_yahoo(symbol, dates):
    '''Jack Diedrich told me to make this a function rather than a class.'''
    start = min(dates)
    end = max(dates)
    prices = get_data_yahoo(name=symbol, start=start, end=end)
    for price in prices.iterrows():
        yield {'date' : price[0],
               'price' : np.float(price[1]['Close'])}
